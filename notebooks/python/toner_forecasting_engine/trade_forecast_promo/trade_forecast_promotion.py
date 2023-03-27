# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Trade Forecast Promotion

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

# COMMAND ----------

add_version_inputs = [
    ['TRADE_FORECAST', 'TRADE FORECAST']
]

for input in add_version_inputs:
    call_redshift_addversion_sproc(configs, input[0], input[1])

# COMMAND ----------

trade_staging = read_redshift_to_df(configs) \
    .option("dbtable", "stage.trade_forecast_staging") \
    .load()

version = read_redshift_to_df(configs) \
    .option("dbtable", "prod.version") \
    .load()

tables = [
    ['stage.trade_forecast_staging', trade_staging, "overwrite"],
    ['prod.version', version, "overwrite"]
]

# COMMAND ----------

# MAGIC %run "../../common/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

trade_forecast_promo = spark.sql("""


with trade_promo_01_filter_vars as (
SELECT record
    , version
    , source_name
    , load_date
    , official
FROM prod.version
WHERE record in ('TRADE_FORECAST')
    AND version = (SELECT MAX(version) FROM prod.version WHERE record IN ('TRADE_FORECAST'))
    
)SELECT vars.record
    , trade.cal_date
    , trade.market10
    , trade.region_5
    , trade.platform_subset
    , trade.base_product_number
    , trade.customer_engagement
    , trade.cartridges
    , vars.load_date
    , vars.version
FROM stage.trade_forecast_staging AS trade
CROSS JOIN trade_promo_01_filter_vars AS vars
WHERE 1=1
    AND vars.record = 'TRADE_FORECAST'
""")

write_df_to_redshift(configs, trade_forecast_promo, "prod.trade_forecast", "append")

# COMMAND ----------


