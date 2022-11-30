# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------


# load parquet file
# edw_s3 = spark.read.parquet("s3://dataos-core-itg-team-phoenix-fin/landing/EDW/edw_revenue_units_sales_landing/part-00000-tid-3689856782391625736-5511b3d9-c4a5-4228-be00-374f544ef370-0-1-c000.snappy.parquet")
list_price_gpsy = spark.read.parquet("s3://dataos-core-prod-team-phoenix/product/list_price_gpsy/2022.05.04.1")

# list_price_gpsy.count()
list_price_gpsy.display()


# COMMAND ----------


