# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

query = """

SELECT [record]
      ,[platform_subset]
      ,[Crg_Base_Prod_Number]
      ,[geography]
      ,[geography_grain]
      ,[cal_date]
      ,[mix_pct]
      ,[product_lifecycle_status]
      ,[customer_engagement]
      ,[load_date]
      ,[official]
  FROM ie2_prod.[dbo].[cartridge_mix_override]
"""

records = read_sql_server_to_df(configs) \
    .option("query", query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, records, "prod.cartridge_mix_override", "overwrite")

# COMMAND ----------

# output dataset to S3 for archival purposes
# write to parquet file in s3

# s3a://dataos-core-dev-team-phoenix/product/list_price_gpsy/

#when we move this to PROD, we'll need to be sure and write out .parquet files for backup reason.  I didn't do this on 5/5/22 when inserting data into the DEV environment.

s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/list_price_gpsy/" + max_version

write_df_to_s3(records, s3_output_bucket, "parquet", "overwrite")
