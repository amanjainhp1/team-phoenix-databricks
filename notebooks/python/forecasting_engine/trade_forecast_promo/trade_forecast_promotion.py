# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Trade Forecast Promotion

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

# COMMAND ----------

record = 'TRADE_FORECAST'
addversion_info = call_redshift_addversion_sproc(configs, record, 'TRADE FORECAST')

# COMMAND ----------

trade_staging = read_redshift_to_df(configs) \
    .option("dbtable", "stage.trade_forecast_staging") \
    .load()

tables = [
    ['stage.trade_forecast_staging', trade_staging, "overwrite"]
]

# COMMAND ----------

# MAGIC %run "../../common/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

trade_forecast_promo = spark.sql(f"""
SELECT 
    '{record}' AS record
    , trade.cal_date
    , trade.market10
    , trade.region_5
    , trade.platform_subset
    , trade.base_product_number
    , trade.customer_engagement
    , trade.cartridges
    , '{addversion_info[1]}' AS load_date
    , '{addversion_info[0]}' AS version
FROM stage.trade_forecast_staging AS trade
""")

write_df_to_redshift(configs=configs, df=trade_forecast_promo, destination="prod.trade_forecast", mode="append")
