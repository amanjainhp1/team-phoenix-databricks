// Databricks notebook source
// --table reference:  ie2_prod.dbo.hardware_ltf

// COMMAND ----------

dbutils.widgets.text("REDSHIFT_USERNAME", "")
dbutils.widgets.text("REDSHIFT_PASSWORD", "")

// COMMAND ----------

val jdbcUsername = "databricks_user" //placeholder
val jdbcPassword = "databricksdemo123" //placeholder

val jdbcUrl = "jdbc:sqlserver://sfai.corp.hpicloud.net:1433;database="

// COMMAND ----------

// --Get LTF data from Archer into stage.hardware_ltf_stage

val fReportUnitsQuery = """
SELECT *
FROM Archer_Prod.dbo.f_report_units('LTF-IE2')
WHERE record LIKE ('LTF-%')
"""

val fReportUnits = spark.read
  .format("jdbc")
  .option("url", jdbcUrl)
  .option("query", fReportUnitsQuery)
  .option("user", jdbcUsername)
  .option("password", jdbcPassword)
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
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

val versionCount = spark.read
    .format("com.databricks.spark.redshift")
    .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None")
    .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/")
    .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role")
    .option("user", dbutils.widgets.get("REDSHIFT_USERNAME"))
    .option("password", dbutils.widgets.get("REDSHIFT_PASSWORD"))
    .option("query", s"""SELECT * FROM prod.version WHERE record = '${record}' AND source_name = '${forecastNameRecordName}'""")
    .load()
    .count()

if (forecastNameRecordNames.size > 1 ) {
  dbutils.notebook.exit("LTF data from Archer contains more than distinct record")
} else if (versionCount >= 1) {
  dbutils.notebook.exit("LTF version already loaded in prod.version table in redshift")
}

// COMMAND ----------

// MAGIC %run ../common/CallStoredProcedure

// COMMAND ----------

// --add record to version table for 'hw_fcst'

callStoredProcedure("redshift", "dataos-redshift-core-dev-01.hp8.us", "5439", "/dev?ssl_verify=None", dbutils.widgets.get("REDSHIFT_USERNAME"), dbutils.widgets.get("REDSHIFT_PASSWORD"), s"""CALL prod.addversion_sproc('${record}', '${forecastNameRecordName}');""")

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

val version = spark.read
  .format("com.databricks.spark.redshift")
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None")
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/")
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role")
  .option("user", dbutils.widgets.get("REDSHIFT_USERNAME"))
  .option("password", dbutils.widgets.get("REDSHIFT_PASSWORD"))
  .option("query", versionQuery)
  .load()

version.createOrReplaceTempView("version")

// COMMAND ----------

val maxVersion = version.select("version").distinct().collect().map(_.getString(0)).mkString("")
val maxLoadDate = version.select("load_date").distinct().collect().map(_.getTimestamp(0)).array(0)

// COMMAND ----------

// --first transformation:
import org.apache.spark.sql.functions._

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
val rdma = spark.read
  .format("com.databricks.spark.redshift")
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None")
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/")
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role")
  .option("user", dbutils.widgets.get("REDSHIFT_USERNAME"))
  .option("password", dbutils.widgets.get("REDSHIFT_PASSWORD"))
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

val hardwareXref = spark.read
  .format("com.databricks.spark.redshift")
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None")
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/")
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role")
  .option("user", dbutils.widgets.get("REDSHIFT_USERNAME"))
  .option("password", dbutils.widgets.get("REDSHIFT_PASSWORD"))
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

thirdTransformation.write
  .format("com.databricks.spark.redshift")
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None")
  .option("dbtable", "prod.hardware_ltf")
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/")
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role")
  .option("user", dbutils.widgets.get("REDSHIFT_USERNAME"))
  .option("password", dbutils.widgets.get("REDSHIFT_PASSWORD"))
  .option("tempformat", "CSV GZIP")
  .mode("append")
  .save()

// COMMAND ----------

fReportUnits.write
  .format("parquet")
  .save(s"""s3a://dataos-core-dev-team-phoenix/proto/${forecastNameRecordName}/${maxVersion}/f_report_units/""")

// COMMAND ----------


