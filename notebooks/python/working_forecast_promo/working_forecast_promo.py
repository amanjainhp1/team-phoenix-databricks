# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

add_version_sproc = """
call prod.addversion_sproc('WORKING_FORECAST','SYSTEM BUILD');  
"""
working_forecast_toner = """
select top 1 record, cast(cal_date as date) cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, cartridges, 
channel_fill, supplies_spares_cartridges, 0 host_cartridges, expected_cartridges, vtc, adjusted_cartridges, cast(NULL as date) load_date, 
cast(NULL as varchar(64)) version
FROM scen.toner_working_fcst
"""


# COMMAND ----------

final_working_forecast_toner = read_redshift_to_df(configs) \
  .option("query",working_forecast_toner) \
  .load()

# COMMAND ----------

write_df_to_redshift(configs,final_working_forecast_toner, "prod.working_forecast", "append", add_version_sproc)

# COMMAND ----------

submit_remote_query(configs, """
UPDATE prod.working_forecast
SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'WORKING_FORECAST'),
version = (SELECT MAX(version) FROM prod.version WHERE record = 'WORKING_FORECAST');
""")

# COMMAND ----------

working_forecast_ink = """
SELECT record, cast(cal_date as date) cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, cartridges, 
channel_fill, supplies_spares_cartridges, 0 host_cartridges, expected_cartridges, vtc, adjusted_cartridges, cast(NULL as date) load_date, 
cast(NULL as varchar(64)) version
FROM scen.ink_working_fcst
"""

# COMMAND ----------

final_working_forecast_ink = read_redshift_to_df(configs) \
  .option("query",working_forecast_ink) \
  .load()

# COMMAND ----------

write_df_to_redshift(configs,final_working_forecast_ink, "prod.working_forecast", "append", add_version_sproc)

# COMMAND ----------

submit_remote_query(configs, """
UPDATE prod.working_forecast
SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'WORKING_FORECAST'),
version = (SELECT MAX(version) FROM prod.version WHERE record = 'WORKING_FORECAST');
""")
