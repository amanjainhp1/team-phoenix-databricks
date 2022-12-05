# Databricks notebook source
# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# MAGIC %run ../../python/common/configs

# COMMAND ----------


# load parquet file
# edw_s3 = spark.read.parquet("s3://dataos-core-itg-team-phoenix-fin/landing/EDW/edw_revenue_units_sales_landing/part-00000-tid-3689856782391625736-5511b3d9-c4a5-4228-be00-374f544ef370-0-1-c000.snappy.parquet")
list_price_gpsy = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/list_price_filtered_historical/2022-11-19/*.parquet")

# list_price_gpsy.count()
list_price_gpsy.display()


# COMMAND ----------


