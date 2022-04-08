# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# load parquet file
ib_s3 = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/ib/2021.12.14.1/part-00000-tid-2974303497789104126-40382a43-01c1-416a-9dae-2ded2fcb06d0-22-1-c000.snappy.parquet")
ib_s3.display()

