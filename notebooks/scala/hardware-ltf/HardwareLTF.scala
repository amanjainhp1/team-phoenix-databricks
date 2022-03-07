// Databricks notebook source
// --table reference:  ie2_prod.dbo.hardware_ltf

// COMMAND ----------

// import needed scala/java libraries
import org.apache.spark.sql.functions._

// COMMAND ----------

dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.dropdown("stack", "dev", Seq("dev", "itg", "prod"))
dbutils.widgets.text("aws_iam_role", "")

// COMMAND ----------

// MAGIC %run ../common/Constants

// COMMAND ----------

// MAGIC %run ../common/DatabaseUtils

// COMMAND ----------

// MAGIC %run ../../python/common/secrets_manager_utils

// COMMAND ----------

// MAGIC %python
// MAGIC # retrieve secrets based on incoming/inputted secrets name - variables will be accessible across languages
// MAGIC 
// MAGIC redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")
// MAGIC spark.conf.set("redshift_username", redshift_secrets["username"])
// MAGIC spark.conf.set("redshift_password", redshift_secrets["password"])
// MAGIC 
// MAGIC sqlserver_secrets = secrets_get(dbutils.widgets.get("sqlserver_secrets_name"), "us-west-2")
// MAGIC spark.conf.set("sfai_username", sqlserver_secrets["username"])
// MAGIC spark.conf.set("sfai_password", sqlserver_secrets["password"])

// COMMAND ----------

var configs: Map[String, String] = Map()
configs += ("env" -> dbutils.widgets.get("stack"),
            "sfaiUsername" -> spark.conf.get("sfai_username"),
            "sfaiPassword" -> spark.conf.get("sfai_password"),
            "sfaiUrl" -> SFAI_URL,
            "sfaiDriver" -> SFAI_DRIVER,
            "redshiftUsername" -> spark.conf.get("redshift_username"),
            "redshiftPassword" -> spark.conf.get("redshift_password"),
            "redshiftAwsRole" -> dbutils.widgets.get("aws_iam_role"),
            "redshiftUrl" -> s"""jdbc:redshift://${REDSHIFT_URLS(dbutils.widgets.get("stack"))}:${REDSHIFT_PORTS(dbutils.widgets.get("stack"))}/${dbutils.widgets.get("stack")}?ssl_verify=None""",
            "redshiftTempBucket" -> s"""${S3_BASE_BUCKETS(dbutils.widgets.get("stack"))}redshift_temp/""",
			"redshiftDevGroup" -> REDSHIFT_DEV_GROUP(dbutils.widgets.get("stack")))

// COMMAND ----------

// --Get LTF data from Archer into stage.hardware_ltf_stage

val fReportUnitsQuery = """
SELECT *
FROM Archer_Prod.dbo.f_report_units('LTF-IE2')
WHERE record LIKE ('LTF-%')
"""

val fReportUnits = readSqlServerToDF(configs)
  .option("query", fReportUnitsQuery)
  .load()

writeDFToRedshift(configs, fReportUnits, "stage.f_report_units", "overwrite")

fReportUnits.createOrReplaceTempView("f_report_units")

// COMMAND ----------

// --something like this could be used to derive the forecast_name

val forecastName = spark.sql("""
SELECT DISTINCT 
	  record
	, RIGHT (record, 16) as record_name
FROM f_report_units
""")

forecastName.createOrReplaceTempView("forcast_name")

// COMMAND ----------

val forecastNameRecordNames = forecastName.select("record_name").distinct().collect()
val forecastNameRecordName = forecastNameRecordNames.map(_.getString(0)).array(0)

// COMMAND ----------

val record = "hw_fcst"

// COMMAND ----------

// if forecastName.count() > 1, then exit
// else if record_name in forecastName exists as source_name in version, where record = "hw_fcst", then exit

val versionCount = readRedshiftToDF(configs)
    .option("query", s"""SELECT * FROM prod.version WHERE record = '${record}' AND source_name = '${forecastNameRecordName}'""")
    .load()
    .count()

if (forecastNameRecordNames.size > 1 ) {
  dbutils.notebook.exit("LTF data from Archer contains more than distinct record")
} else if (versionCount >= 1) {
  dbutils.notebook.exit("LTF version already loaded in prod.version table in redshift")
}

// COMMAND ----------

// --add record to version table for 'hw_fcst'

submitRemoteQuery(configs("redshiftUrl"), configs("redshiftUsername"), configs("redshiftPassword"), s"""CALL prod.addversion_sproc('${record}', '${forecastNameRecordName}');""")

// COMMAND ----------

// --something like this could be used to join to, to retrieve version and load_date

val versionQuery = s"""
SELECT
      record
	, MAX(version) AS version
	, MAX(load_date) as load_date
FROM prod.version
WHERE record = '${record}' AND source_name = '${forecastNameRecordName}'
GROUP BY record
"""

val version = readRedshiftToDF(configs)
  .option("query", versionQuery)
  .load()

// COMMAND ----------

val maxVersion = version.select("version").distinct().collect().map(_.getString(0)).mkString("")
val maxLoadDate = version.select("load_date").distinct().collect().map(_.getTimestamp(0)).array(0)

// COMMAND ----------

// --first transformation:

val firstTransformationQuery = s"""
SELECT  a.record
      , a.geo
      , a.geo_type
	  , CAST(NULL AS int) AS geo_input 
      , a.base_prod_number
	  , CAST(NULL AS int) AS sku
      , a.calendar_month
      , SUM(a.units) AS units
FROM stage.f_report_units a --this table would be the first table that we land the data to, from Archer
WHERE 1=1
GROUP BY
	  a.record
	, a.geo
	, a.geo_type
	, a.base_prod_number
	, a.calendar_month
"""

val firstTransformation = readRedshiftToDF(configs)
  .option("query", firstTransformationQuery)
  .load()

firstTransformation.createOrReplaceTempView("first_transformation")

// COMMAND ----------

// --second transformation:

val rdma = readRedshiftToDF(configs)
	.option("dbTable", "mdm.rdma")
	.load()

rdma.createOrReplaceTempView("rdma")

val secondTransformationQuery = s"""
	SELECT
		  '${record}' AS record
		, '${forecastNameRecordName}' AS forecast_name
		, a.calendar_month AS cal_date
        , CAST(NULL AS int) AS geo_id
		, a.geo AS country_alpha2
        , CAST(NULL AS int) AS platform_subset_id
		, b.platform_subset
		, a.base_prod_number AS base_product_number
		, SUM(a.units) AS units
        , CAST("true" as boolean) AS official
		, CAST("${maxLoadDate}" as date) AS load_date
		, "${maxVersion}" AS version
	FROM first_transformation a
	INNER JOIN rdma b
      ON a.base_prod_number = b.base_prod_number
	GROUP BY
		  a.calendar_month
		, a.geo
		, b.platform_subset
		, a.base_prod_number
"""

val secondTransformation = spark.sql(secondTransformationQuery)

secondTransformation.createOrReplaceTempView("second_transformation")

// COMMAND ----------

val hardwareXref = readRedshiftToDF(configs)
	.option("dbtable", "mdm.hardware_xref")
	.load()

hardwareXref.createOrReplaceTempView("hardware_xref")

val thirdTransformationQuery = s"""
	SELECT
		  a.record
		, a.forecast_name
		, a.cal_date
        , a.geo_id
		, a.country_alpha2
        , b.id AS platform_subset_id
		, b.platform_subset
		, a.base_product_number
		, a.units
        , a.official
		, a.load_date
		, a.version
	FROM second_transformation a
	LEFT JOIN hardware_xref b
      ON a.platform_subset = b.platform_subset
"""

val thirdTransformation = spark.sql(thirdTransformationQuery)

// COMMAND ----------

firstTransformation.cache()
writeDFToRedshift(configs, firstTransformation, "stage.hardware_ltf_01", "overwrite")

secondTransformation.cache()
writeDFToRedshift(configs, secondTransformation, "stage.hardware_ltf_02", "overwrite")

thirdTransformation.cache()
writeDFToRedshift(configs, thirdTransformation, "stage.hardware_ltf_03", "overwrite")

writeDFToRedshift(configs, thirdTransformation, "prod.hardware_ltf", "append")

// COMMAND ----------

writeDFToS3(fReportUnits, S3_BASE_BUCKETS(configs("env")) + s"""/proto/${forecastNameRecordName}/${maxVersion}/f_report_units/""", "parquet", "overwrite")
