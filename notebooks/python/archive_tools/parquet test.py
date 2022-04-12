# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# load parquet file
ib_s3 = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/usage_share/2022.01.13.1/part-00000-tid-4662628018208631769-185c282d-5904-49bb-8f70-91bab3b1b694-11-1-c000.snappy.parquet")
ib_s3.display()
ib_s3.count()


# COMMAND ----------


