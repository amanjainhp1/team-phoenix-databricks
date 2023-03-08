# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Toner Usage and Share POR

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

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
    FROM toner_03_usage_share
""")

toner_por.createOrReplaceTempView("toner_por")

# COMMAND ----------

write_df_to_redshift(configs, toner_por, "prod.usage_share_por_toner", "overwrite")
