# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------


# load parquet file
usage_share_spectrum = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/usage_share/2022.11.18.1/")

# usage_share_spectrum.count()
usage_share_spectrum.display()


# COMMAND ----------

# df.withColumn("age",df.age.cast('int'))
from pyspark.sql.functions import *

newDf = usage_share_spectrum.withColumn("load_date", usage_share_spectrum.load_date.cast('timestamp'))
newDf.display()

# COMMAND ----------

# write to parquet file in s3

s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/usage_share/temp"
write_df_to_s3(newDf, s3_output_bucket, "parquet", "overwrite")
