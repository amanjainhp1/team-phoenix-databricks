# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update Redshift Actuals Hardware Table - Staging

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DecimalType
from pyspark.sql import Row 
from pyspark.sql.functions import col, lit, when

#retrieve relevant data from source db
planet_actuals = read_sql_server_to_df(configs) \
    .option("query", f"""SELECT * FROM [Archer_Prod].dbo.stf_flash_country_speedlic_yeti_vw WHERE record = 'Planet-Actuals' AND date >= '2022-11-01'""") \
    .load()
        
# store data, in raw format from source DB to stage table
write_df_to_redshift(configs, planet_actuals, "stage.planet_actuals", "overwrite")

staging_actuals_units_hw_query = """
SELECT
  'ACTUALS - HW' AS record,
  a.date AS cal_date,
  a.geo AS country_alpha2,
  a.base_prod_number AS base_product_number,
  b.platform_subset,
  sum(a.units) AS base_quantity,
  a.load_date,
  1 AS official,
  a.version,
  'ARCHER' AS source
FROM stage.planet_actuals a
LEFT JOIN mdm.rdma b
  ON UPPER(a.base_prod_number) = UPPER(b.base_prod_number)
WHERE a.units <> 0
GROUP BY
  a.date,
  a.geo,
  a.base_prod_number,
  b.platform_subset,
  a.load_date,
  a.version
"""

staging_actuals_units_hw = read_redshift_to_df(configs) \
    .option("query", staging_actuals_units_hw_query) \
    .load()

       #execute stored procedure to create new version and load date
max_version_info = call_redshift_addversion_sproc(configs, 'ACTUALS - HW', 'ARCHER')

max_version = max_version_info[0]
max_load_date = max_version_info[1]

staging_actuals_units_hw = staging_actuals_units_hw \
    .withColumn("version", when(col("version") != (max_version), max_version)) \
    .withColumn("load_date", when(col("load_date") != (max_load_date), max_load_date))

write_df_to_redshift(configs, staging_actuals_units_hw, "stage.actuals_hw", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update Redshift Actuals Hardware Table - Prod

# COMMAND ----------

# create final dataframe and write out to table
query = f"""
SELECT
    record,
    cal_date,
    country_alpha2,
    base_product_number,
    a.platform_subset,
    source,
    base_quantity,
    a.official,
    a.load_date,
    a.version
FROM stage.actuals_hw a
LEFT JOIN mdm.hardware_xref b ON a.platform_subset = b.platform_subset
LEFT JOIN mdm.product_line_xref c ON b.pl = c.pl
WHERE c.PL_category = 'HW'
    AND c.Technology IN ('INK','LASER','PWA','LF')
    AND a.base_quantity <> 0
"""

redshift_stage_actuals_hw = read_redshift_to_df(configs) \
    .option("query", query) \
    .load()


write_df_to_redshift(configs, redshift_stage_actuals_hw, "prod.actuals_hw", "append")
