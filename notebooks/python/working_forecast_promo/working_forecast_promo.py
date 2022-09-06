# Databricks notebook source
add_version_sproc = """
call prod.addversion_sproc('working_forecast','system  build');  
"""
working_forecast_toner = """
SELECT record, build_time, cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, cartridges, channel_fill, supplies_spares_cartridges, expected_cartridges, vtc, adjusted_cartridges, supplies_product_family, supplies_family, supplies_mkt_cat, epa_family, hw_pl, region_3, region_4, region_5, market10, fiscal_year_qtr, fiscal_yr, composite_key, cartridge_type, yield,cast(NULL as date) load_date, 
cast(NULL as varchar(64)) version
FROM scen.toner_working_fcst;
"""


# COMMAND ----------

write_df_to_redshift(configs,working_forecast_toner, "prod.working_forecast", "append", add_version_sproc)

# COMMAND ----------

submit_remote_query(configs, """
UPDATE prod.working_forecast
SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'working_forecast'),
version = (SELECT MAX(version) FROM prod.version WHERE record = 'working_forecast');
;""")

# COMMAND ----------

working_forecast_ink = """
SELECT record, build_time, cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, cartridges, channel_fill, supplies_spares_cartridges, expected_cartridges, vtc, adjusted_cartridges, supplies_product_family, supplies_family, supplies_mkt_cat, epa_family, hw_pl, region_3, region_4, region_5, market10, fiscal_year_qtr, fiscal_yr, composite_key, cartridge_type, yield,cast(NULL as date) load_date, cast(NULL as varchar(64)) version
FROM scen.ink_working_fcst;
"""

# COMMAND ----------

write_df_to_redshift(configs,working_forecast_ink, "prod.working_forecast", "append", add_version_sproc)

# COMMAND ----------

submit_remote_query(configs, """
UPDATE prod.working_forecast
SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'working_forecast'),
version = (SELECT MAX(version) FROM prod.version WHERE record = 'working_forecast');
;""")
