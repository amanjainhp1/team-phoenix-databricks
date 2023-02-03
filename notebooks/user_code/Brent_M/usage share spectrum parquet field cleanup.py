# Databricks notebook source
# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# MAGIC %run ../../python/common/configs

# COMMAND ----------


# load parquet file
usage_share_spectrum = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/usage_share_m8_de/2022.12.19.1/")

# usage_share_spectrum.count()
usage_share_spectrum.display()


# COMMAND ----------

from pyspark.sql.functions import *

newDf = usage_share_spectrum.withColumn('version', regexp_replace('version', '2022-09-21.1', '2022.09.21.1'))
newDf.show()

# COMMAND ----------

# write to parquet file in s3

s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/demand/temp"
write_df_to_s3(newDf, s3_output_bucket, "parquet", "overwrite")
