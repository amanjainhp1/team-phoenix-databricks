// Databricks notebook source
// --table reference:  ie2_prod.dbo.hardware_ltf

// COMMAND ----------

// import needed scala/java libraries
import org.apache.spark.sql.functions._

// COMMAND ----------

dbutils.widgets.text("REDSHIFT_USERNAME", "")
dbutils.widgets.text("REDSHIFT_PASSWORD", "")
dbutils.widgets.text("SFAI_USERNAME", "")
dbutils.widgets.text("SFAI_PASSWORD", "")
dbutils.widgets.dropdown("ENVIRONMENT", "dev", Seq("dev", "itg", "prd"))
dbutils.widgets.text("AWS_IAM_ROLE", "")

// COMMAND ----------

// MAGIC %run ../common/Constants

// COMMAND ----------

// MAGIC %run ../common/DatabaseUtils

// COMMAND ----------

var configs: Map[String, String] = Map()
configs += ("env" -> dbutils.widgets.get("ENVIRONMENT"),
            "sfaiUsername" -> dbutils.widgets.get("SFAI_USERNAME"),
            "sfaiPassword" -> dbutils.widgets.get("SFAI_PASSWORD"),
            "sfaiUrl" -> SFAI_URL,
            "sfaiDriver" -> SFAI_DRIVER,
            "redshiftUsername" -> dbutils.widgets.get("REDSHIFT_USERNAME"),
            "redshiftPassword" -> dbutils.widgets.get("REDSHIFT_PASSWORD"),
            "redshiftAwsRole" -> dbutils.widgets.get("AWS_IAM_ROLE"),
            "redshiftUrl" -> s"""jdbc:redshift://${REDSHIFT_URLS(dbutils.widgets.get("ENVIRONMENT"))}:${REDSHIFT_PORTS(dbutils.widgets.get("ENVIRONMENT"))}/${dbutils.widgets.get("ENVIRONMENT")}?ssl_verify=None""",
            "redshiftTempBucket" -> s"""${S3_BASE_BUCKETS(dbutils.widgets.get("ENVIRONMENT"))}redshift_temp/""")

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
  .cache()

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

version.createOrReplaceTempView("version")

// COMMAND ----------

val maxVersion = version.select("version").distinct().collect().map(_.getString(0)).mkString("")
val maxLoadDate = version.select("load_date").distinct().collect().map(_.getTimestamp(0)).array(0)

// COMMAND ----------

// --first transformation:

val firstTransformation = spark.sql(s"""
SELECT  a.record
      , a.geo
      , a.geo_type
	  , NULL AS geo_input 
      , a.base_prod_number
	  , NULL AS sku
      , a.calendar_month
      , SUM(a.units) AS units
      , CAST("${maxLoadDate}" as date) AS load_date
      , "${maxVersion}" AS version
	  , "${forecastNameRecordName}" as record_name --get this value from the helper table above.
FROM f_report_units a --this table would be the first table that we land the data to, from Archer
WHERE 1=1
GROUP BY
	  a.record
	, a.geo
	, a.geo_type
	, a.base_prod_number
	, a.calendar_month
	, load_date
	, version
""")

firstTransformation.createOrReplaceTempView("first_transformation")

// COMMAND ----------

// --second transformation:
val rdma = readRedshiftToDF(configs)
  .option("dbtable", "mdm.rdma")
  .load()

rdma.createOrReplaceTempView("rdma")

val secondTransformation = spark.sql(s"""
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
		, a.load_date
		, a.version
	FROM first_transformation a
	INNER JOIN rdma b
      ON a.base_prod_number = b.base_prod_number
	GROUP BY
          a.record_name
		, a.calendar_month
		, a.geo
		, b.platform_subset
		, a.base_prod_number
        , a.load_date
		, a.version
""")

secondTransformation.createOrReplaceTempView("second_transformation")

// COMMAND ----------

val hardwareXref = readRedshiftToDF(configs)
  .option("dbtable", "mdm.hardware_xref")
  .load()
  .select("id", "platform_subset")

hardwareXref.createOrReplaceTempView("hardware_xref")

val thirdTransformation = spark.sql(s"""
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
""")

// COMMAND ----------

writeDFToRedshift(configs, thirdTransformation, "prod.hardware_ltf", "append")

// COMMAND ----------

writeDFToS3(fReportUnits, S3_BASE_BUCKETS(configs("env")) + s"""/proto/${forecastNameRecordName}/${maxVersion}/f_report_units/""", "parquet", "overwrite")

// COMMAND ----------


