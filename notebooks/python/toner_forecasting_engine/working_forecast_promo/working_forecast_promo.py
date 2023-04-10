# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Working Forecast Promotion

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

# COMMAND ----------

add_version_inputs = [
    #['WORKING_FORECAST_TONER', 'WORKING FORECAST TONER'],
    #['WORKING_FORECAST_INK', 'WORKING FORECAST INK'],
    ['WORKING_FORECAST_COUNTRY', 'WORKING FORECAST COUNTRY']
]

for input in add_version_inputs:
    call_redshift_addversion_sproc(configs, input[0], input[1])

# COMMAND ----------

c2c_country = read_redshift_to_df(configs) \
    .option("dbtable", "scen.c2c_adj_country_pf_split")\
    .load()

version = read_redshift_to_df(configs) \
    .option("dbtable", "prod.version") \
    .load()

wf_country = read_redshift_to_df(configs) \
    .option("dbtable", "scen.working_forecast_country") \
    .load()

tables = [
    ['prod.version', version, "overwrite"],
    ['scen.c2c_adj_country_pf_split', c2c_country, "overwrite"],
    ['scen.working_forecast_country', wf_country, "overwrite"]
]

# COMMAND ----------

# MAGIC %run "../../common/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Working Forecast

# COMMAND ----------

# working_promo = spark.sql("""

# with scen_promo_01_filter_vars as (
#     SELECT DISTINCT record
#         , version
#         , source_name
#         , load_date
#         , official
#     FROM prod.version
#     WHERE record in ('IE2-WORKING-FORECAST', 'WORKING_FORECAST_INK', 'WORKING_FORECAST_COUNTRY')
#         AND version = (SELECT MAX(version) FROM prod.version WHERE record IN ('IE2-WORKING-FORECAST', 'WORKING_FORECAST_INK', 'WORKING_FORECAST_COUNTRY'))
# )SELECT wfc.record
#     , to_date(wfc.cal_date, 'yyyy-MM-dd') as cal_date
#     , wfc.geography_grain
#     , wfc.geography
#     , wfc.platform_subset
#     , wfc.base_product_number
#     , wfc.customer_engagement
#     , wfc.cartridges
#     , wfc.channel_fill
#     , wfc.supplies_spares_cartridges
#     , 0.0 as host_cartridges
#     --, CAST(wfc.welcome_kits AS FLOAT) AS welcome_kits
#     , wfc.expected_cartridges
#     , wfc.vtc
#     , wfc.adjusted_cartridges
#     , vars.load_date
#     , vars.version
# FROM scen.working_forecast_combined AS wfc
# JOIN scen_promo_01_filter_vars AS vars
#     ON vars.record = wfc.record
# WHERE 1=1
# """)

# write_df_to_redshift(configs, working_promo, "prod.working_forecast", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Country

# COMMAND ----------

working_country = spark.sql("""
with scen_promo_01_filter_vars as (
SELECT DISTINCT record
    , version
    , source_name
    , load_date
    , official
FROM prod.version
WHERE record in ('IE2-WORKING-FORECAST', 'WORKING_FORECAST_INK', 'WORKING_FORECAST_COUNTRY')
    AND version = (SELECT MAX(version) FROM prod.version WHERE record IN ('IE2-WORKING-FORECAST', 'WORKING_FORECAST_INK', 'WORKING_FORECAST_COUNTRY'))
)SELECT vars.record
    , to_date(ctry.cal_date, 'yyyy-MM-dd') as cal_date
    , 'MARKET10' AS geography_grain
    , ctry.geography
    , ctry.country_alpha2 AS country
    , ctry.platform_subset
    , ctry.base_product_number
    , ctry.customer_engagement
    , ctry.cartridges
    , ctry.mvtc_adjusted_crgs AS imp_corrected_cartridges
    , vars.load_date
    , vars.version
FROM scen.working_forecast_country AS ctry
CROSS JOIN scen_promo_01_filter_vars AS vars
WHERE 1=1
    AND vars.record = 'WORKING_FORECAST_COUNTRY'
""")

write_df_to_redshift(configs, working_country, "prod.working_forecast_country", "append")

# COMMAND ----------


