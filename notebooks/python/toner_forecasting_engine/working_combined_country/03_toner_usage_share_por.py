# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Toner Usage and Share POR

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

# COMMAND ----------

toner_us = read_redshift_to_df(configs) \
    .option("dbtable", "scen.toner_03_usage_share") \
    .load()

# COMMAND ----------

tables = [
    ["scen.toner_03_usage_share", toner_us, "overwrite"]
]

# COMMAND ----------

# MAGIC %run "../../finance_etl/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

# redshift working forecast does not include override info, have to find a new way to do por's
toner_por = spark.sql("""

    SELECT 'TONER_POR_' +
            CAST(month(current_date()) AS string) +
            CAST(year(current_date()) AS string) AS record
        , current_timestamp() AS promote_timestamp
        , cal_date
        , geography_grain
        , geography
        , platform_subset
        , customer_engagement
        , measure
        , units
        , us_version
        , ib_version
        , override
        , override_timestamp
        , forecaster_name
    FROM scen.toner_03_usage_share
""")

toner_por.createOrReplaceTempView("toner_por")

# COMMAND ----------

spark.sql("""select current_timestamp()""").show()

# COMMAND ----------

write_df_to_redshift(configs, toner_por, "prod.usage_share_por_toner", "overwrite")
