# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# load parquet file
edw_s3 = spark.read.parquet("s3://dataos-core-dev-team-phoenix/landing/EDW/edw_revenue_units_sales_landing/part-00000-tid-619257732289246598-ee523985-f10c-40c7-a7fe-8405c883f0df-1-1-c000.snappy.parquet")




# COMMAND ----------

edw_s3.count()

# COMMAND ----------

edw_s3.display()
