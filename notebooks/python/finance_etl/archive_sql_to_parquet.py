# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

#load sfai table for archiving:
df_sql_table = read_sql_server_to_df(configs) \
    .option("query", "SELECT * FROM IE2_Financials.dbo.adjusted_revenue_epa WHERE version IN ('2022.11.11.1', '2022.09.14.1', '2022.08.18.1', '2022.08.16.1', '2022.07.13.1', '2022.06.22.1', '2022.05.31.1')") \
    .load()

# COMMAND ----------

#write out table to parquet in S3:

#s3a://dataos-core-prod-team-phoenix-fin/
s3_bucket = f"s3a://dataos-core-{stack}-team-phoenix-fin/"
s3_bucket_prefix = "archive/adjusted_revenue_epa/sfai/"
df_destination = s3_bucket + s3_bucket_prefix

write_df_to_s3(df_sql_table, df_destination, "parquet", "overwrite", True)
