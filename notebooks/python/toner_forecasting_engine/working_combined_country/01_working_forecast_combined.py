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

# ink_working_fcst = read_redshift_to_df(configs) \
#     .option("query", "select * from prod.working_forecast where version = '{}'".format(ink_ib_version)) \
#     .load()

# toner_working_fcst = read_redshift_to_df(configs) \
#     .option("query", "select * from prod.working_forecast where version = '{}'".format(toner_ib_version)) \
#     .load()


ink_working_fcst = read_redshift_to_df(configs) \
    .option("dbtable", "scen.ink_working_fcst") \
    .load()

toner_working_fcst = read_redshift_to_df(configs) \
    .option("dbtable", "scen.toner_working_fcst") \
    .load()

# COMMAND ----------

tables = [
    ["ink_working_fcst", ink_working_fcst, "overwrite"],
    ["toner_working_fcst", toner_working_fcst, "overwrite"]
]

# COMMAND ----------

for table in tables:
    df = table[1]
    table_name = table[0]
    df.createOrReplaceTempView('{}'.format(table_name))
    print("row count for " + table_name + ":")
    print(df.count())

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
    FROM toner_working_fcst AS wft

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
    FROM ink_working_fcst AS wfi
""")




# COMMAND ----------

combined.printSchema()

# COMMAND ----------

write_df_to_redshift(configs, combined, "scen.working_forecast_combined", "overwrite")

# COMMAND ----------


