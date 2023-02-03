# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# load parquet file

DF_usage_share_country = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/demand/2023.01.05.1/*.parquet")

DF_usage_share_country.display()

# COMMAND ----------

# write the dataframe to a Redshift table
write_df_to_redshift(configs, DF_usage_share_country, "prod.usage_share_country", "overwrite")
