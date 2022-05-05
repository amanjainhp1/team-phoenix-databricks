# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# load parquet file
ib_s3 = spark.read.parquet("s3://dataos-core-dev-team-phoenix/spectrum/cupsm/Ink Q2 Pulse 2022.1/*.parquet")
ib_s3.display()
ib_s3.count()


# COMMAND ----------


