# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# load parquet file
ib_s3 = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/ib/2021.11.22.1/part-00000-tid-462149716575615082-65717b87-5207-4437-9166-4c025bc3d4c2-19-1-c000.snappy.parquet")
ib_s3.display()
ib_s3.count()


# COMMAND ----------


