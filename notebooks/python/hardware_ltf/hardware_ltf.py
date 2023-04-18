# Databricks notebook source
# --table reference:  ie2_prod.dbo.hardware_ltf

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# --Get LTF data from Archer into stage.hardware_ltf_stage

f_report_units_query = """
SELECT *
FROM Archer_Prod.dbo.f_report_units('LTF-IE2')
WHERE record like 'LTF%'
"""

f_report_units = read_sql_server_to_df(configs) \
    .option("query", f_report_units_query) \
    .load()

write_df_to_redshift(configs, f_report_units, "stage.f_report_units", "overwrite")

f_report_units.createOrReplaceTempView("f_report_units")

# COMMAND ----------

# --derive the forecast_name

forecast_name = spark.sql("""
SELECT DISTINCT 
	  UPPER(record)
	, UPPER(RIGHT(record, 16)) as record_name
FROM f_report_units
""")

# COMMAND ----------

# forecast_name.display()

# COMMAND ----------

forecast_name_record_names = forecast_name.select("record_name").distinct().collect()
forecast_name_record_name = forecast_name_record_names[0][0]

print("forecast_name_record_name: " + forecast_name_record_name)

# COMMAND ----------

record = "HW_FCST"

# COMMAND ----------

# if we have more than one record_name coming from f_report_units, then exit
# else if prod.version already contains the one distinct record and coming from f_report_units, also exit
# else continue

version_count = read_redshift_to_df(configs) \
    .option("query", f"""SELECT * FROM prod.version WHERE record = '{record}' AND source_name = '{forecast_name_record_name}'""") \
    .load() \
    .count()

if len(forecast_name_record_names) > 1:
    raise Exception("LTF data from Archer contains more than one distinct record (n=" + len(forecast_name_record_names) + ")")
elif version_count >= 1:
    raise Exception("LTF version already loaded in prod.version table in redshift")

# COMMAND ----------

# add record to version table for 'HW_FCST'

max_version_info = call_redshift_addversion_sproc(configs, record, forecast_name_record_name)

max_version = max_version_info[0]
max_load_date = max_version_info[1]

print("max_version: " + max_version)
print("max_load_date: " + str(max_load_date))

# COMMAND ----------

# --first transformation:

first_transformation_query = """
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

first_transformation = read_redshift_to_df(configs) \
    .option("query", first_transformation_query) \
    .load()

first_transformation.createOrReplaceTempView("first_transformation")

# COMMAND ----------

# --second transformation:

rdma = read_redshift_to_df(configs) \
    .option("dbTable", "mdm.rdma") \
    .load()

rdma.createOrReplaceTempView("rdma")

second_transformation_query = f"""
	SELECT
		  '{record}' AS record
		, '{forecast_name_record_name}' AS forecast_name
		, a.calendar_month AS cal_date
		, a.geo AS country_alpha2
		, b.platform_subset
		, a.base_prod_number AS base_product_number
		, SUM(a.units) AS units
        , CAST("true" as boolean) AS official
		, CAST("{max_load_date}" as date) AS load_date
		, "{max_version}" AS version
	FROM first_transformation a
	INNER JOIN rdma b
      ON UPPER(a.base_prod_number) = UPPER(b.base_prod_number)
	GROUP BY
		  a.calendar_month
		, a.geo
		, b.platform_subset
		, a.base_prod_number
"""

second_transformation = spark.sql(second_transformation_query)

second_transformation.createOrReplaceTempView("second_transformation")

# COMMAND ----------

hardware_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.hardware_xref") \
    .load()

hardware_xref.createOrReplaceTempView("hardware_xref")

third_transformation_query = """
	SELECT
		  a.record
		, a.forecast_name
		, a.cal_date
		, a.country_alpha2
		, b.platform_subset
		, a.base_product_number
		, a.units
        , a.official
		, a.load_date
		, a.version
	FROM second_transformation a
	LEFT JOIN hardware_xref b
      ON UPPER(a.platform_subset) = UPPER(b.platform_subset)
"""

third_transformation = spark.sql(third_transformation_query)

# COMMAND ----------

first_transformation.cache()
write_df_to_redshift(configs, first_transformation, "stage.hardware_ltf_01", "overwrite")

second_transformation.cache()
write_df_to_redshift(configs, second_transformation, "stage.hardware_ltf_02", "overwrite")

third_transformation.cache()
write_df_to_redshift(configs, third_transformation, "stage.hardware_ltf_03", "overwrite")

submit_remote_query(configs, f"UPDATE prod.hardware_ltf SET official = 0 WHERE record = '{record}';")

write_df_to_redshift(configs, third_transformation, "prod.hardware_ltf", "append")

# COMMAND ----------

write_df_to_s3(f_report_units, "{}/product/norm_ships/fcst/ltf/small_format/{}/{}/".format(constants["S3_BASE_BUCKET"][stack], forecast_name_record_name, max_version), "parquet", "overwrite")
