# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Working Forecast Promotion

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

# COMMAND ----------

# MAGIC %run ../config_forecasting_engine

# COMMAND ----------

record = 'WORKING_FORECAST_COUNTRY'
addversion_info = call_redshift_addversion_sproc(configs, record, 'WORKING FORECAST COUNTRY')

# COMMAND ----------

wf_country = read_redshift_to_df(configs) \
    .option("query", "SELECT * FROM scen.working_forecast_country WHERE technology = '{technology}'") \
    .load()

tables = [
    ['scen.working_forecast_country', wf_country, "overwrite"]
]

# COMMAND ----------

# MAGIC %run "../../common/delta_lake_load_with_params" $tables=tables

working_country = spark.sql(f"""
SELECT 
    '{record}' AS record
    , to_date(ctry.cal_date, 'yyyy-MM-dd') as cal_date
    , 'MARKET10' AS geography_grain
    , ctry.geography
    , ctry.country_alpha2 AS country
    , ctry.platform_subset
    , ctry.base_product_number
    , ctry.customer_engagement
    , ctry.cartridges
    , ctry.mvtc_adjusted_crgs AS imp_corrected_cartridges
    , '{addversion_info[1]}' AS load_date
    , '{addversion_info[0]}' AS version
FROM scen.working_forecast_country AS ctry
""")

write_df_to_redshift(configs, working_country, "prod.working_forecast_country", "append")

# COMMAND

# for subsequent tasks pass args via task values
dbutils.jobs.taskValues.set(key="wf_country_version", value=addversion_info[1])
