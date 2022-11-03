# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# load parquet file
ib_s3 = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/usage_share/2022.03.30.1/part-00000-tid-8115564011186470121-e71422ba-d821-43a3-8ec2-de78d5282772-5-1-c000.snappy.parquet")
# ib_s3.display()



# COMMAND ----------

# ib_s3.count()
ib_s3.show()

# COMMAND ----------

from pyspark.sql.functions import col, upper
ib_s3 = ib_s3 \
    .withColumn("record", upper(col("record"))) \
    .withColumn("customer_engagement", upper(col("customer_engagement"))) \
    .withColumn("measure", upper(col("measure"))) \
    .withColumn("platform_subset", upper(col("platform_subset")))

# ib_s3.display()


# COMMAND ----------

write_df_to_sqlserver(configs, ib_s3, "archive.dbo.usage_share_archive", "append")

# COMMAND ----------

# s3a://dataos-core-prod-team-phoenix/spectrum/ib/
s3_ib_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/ib/2022.03.29.1/"

write_df_to_s3(ib_s3, s3_ib_output_bucket, "parquet", "overwrite")

# COMMAND ----------


