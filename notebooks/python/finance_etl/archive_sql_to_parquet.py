# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

#load sfai table for archiving:
df_sql_table = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.dbo.adjusted_revenue_salesprod") \
    .load()

# COMMAND ----------

#write out table to parquet in S3:

#s3a://dataos-core-prod-team-phoenix-fin/
s3_bucket = f"s3a://dataos-core-{stack}-team-phoenix-fin/"
s3_bucket_prefix = "archive/adjusted_revenue_salesprod/sfai/"
df_destination = s3_bucket + s3_bucket_prefix

write_df_to_s3(df_sql_table, df_destination, "parquet", "overwrite", True)
