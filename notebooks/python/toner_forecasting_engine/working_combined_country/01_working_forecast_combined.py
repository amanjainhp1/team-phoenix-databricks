# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Working Forecast Combined

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

# COMMAND ----------

# MAGIC %run ../config_forecasting_engine

# COMMAND ----------

combined = spark.sql("""
    SELECT 'WORKING_FORECAST_TONER' AS record
        , wft.cal_date
        , wft.geography_grain
        , wft.geography
        , wft.platform_subset
        , wft.base_product_number
        , wft.customer_engagement
        , wft.vtc
        , wft.cartridges
        , wft.channel_fill
        , wft.supplies_spares_cartridges
        , 0.0 AS host_cartridges
        --, 0.0 AS welcome_kits
        , wft.expected_cartridges
        , wft.adjusted_cartridges
    FROM prod.working_forecast AS wft
    WHERE wft.version = '{}'
        AND record = 'IE2-WORKING-FORECAST'

    UNION ALL

    SELECT 'WORKING_FORECAST_INK' AS record
        , wfi.cal_date
        , wfi.geography_grain
        , wfi.geography
        , wfi.platform_subset
        , wfi.base_product_number
        , wfi.customer_engagement
        , wfi.vtc
        , wfi.cartridges
        , wfi.channel_fill
        , wfi.supplies_spares_cartridges
        , 0.0 AS host_cartridges
        --, wfi.welcome_kits
        , wfi.expected_cartridges
        , wfi.adjusted_cartridges
    FROM prod.working_forecast AS wfi
    WHERE wfi.version = '{}'
        AND record = 'WORKING_FORECAST_INK'
""".format(toner_wf_version, ink_wf_version))


write_df_to_redshift(configs, combined, "scen.working_forecast_combined", "overwrite")

# COMMAND ----------


