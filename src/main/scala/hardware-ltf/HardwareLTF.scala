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
dbutils.widgets.dropdown("ENVIRONMENT", "dev", Seq("dev", "itg", "prod"))
dbutils.widgets.text("AWS_IAM_ROLE", "")

// COMMAND ----------

// MAGIC %run ../common/Constants

// COMMAND ----------

val env = dbutils.widgets.get("ENVIRONMENT")

val sfaiUsername = dbutils.widgets.get("SFAI_USERNAME")
val sfaiPassword = dbutils.widgets.get("SFAI_PASSWORD")

val redshiftUsername = dbutils.widgets.get("REDSHIFT_USERNAME")
val redshiftPassword = dbutils.widgets.get("REDSHIFT_PASSWORD")
val redshiftAwsRole = dbutils.widgets.get("AWS_IAM_ROLE")
val redshiftUrl = "jdbc:redshift://" + REDSHIFT_URLS(env) + ":" + REDSHIFT_PORTS(env) + "/" + env + "?ssl_verify=None"
val redshiftTempBucket = S3_BASE_BUCKETS("dev") + "redshift_temp/"

// COMMAND ----------

// MAGIC %run ../common/DatabaseUtils

// COMMAND ----------

// --Get LTF data from Archer into stage.hardware_ltf_stage

val fReportUnitsQuery = """
SELECT *
FROM Archer_Prod.dbo.f_report_units('LTF-IE2')
WHERE record LIKE ('LTF-%')
"""

val fReportUnits = readSqlServerToDF
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

val versionCount = readRedshiftToDF
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

submitRemoteQuery(redshiftUrl, redshiftUsername, redshiftPassword, s"""CALL prod.addversion_sproc('${record}', '${forecastNameRecordName}');""")

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

val version = readRedshiftToDF
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
val rdma = readRedshiftToDF
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

val hardwareXref = readRedshiftToDF
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

writeDFToRedshift(thirdTransformation, "prod.hardware_ltf", "append")

// COMMAND ----------

writeDFToS3(fReportUnits, S3_BASE_BUCKETS(env) + s"""/proto/${forecastNameRecordName}/${maxVersion}/f_report_units/""", "parquet", "overwrite")

// COMMAND ----------


