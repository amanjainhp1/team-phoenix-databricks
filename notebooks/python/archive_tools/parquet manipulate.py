# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# load parquet file
ib_s3 = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/ib/2022.03.29.1/*.parquet")
# ib_s3.display()



# COMMAND ----------

# ib_s3.count()

# COMMAND ----------

from pyspark.sql.functions import col, upper
ib_s3 = ib_s3 \
    .withColumn("record", upper(col("record"))) \
    .withColumn("customer_engagement", upper(col("customer_engagement"))) \
    .withColumn("measure", upper(col("measure"))) \
    .withColumn("platform_subset", upper(col("platform_subset")))

# ib_s3.display()


# COMMAND ----------

# s3a://dataos-core-prod-team-phoenix/spectrum/ib/
s3_ib_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/ib/2022.03.29.1/"

write_df_to_s3(ib_s3, s3_ib_output_bucket, "parquet", "overwrite")

# COMMAND ----------


