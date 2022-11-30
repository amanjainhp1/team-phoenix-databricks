# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

cupsm_archive_query = """

SELECT UPPER([record]) as [record]
      ,[cal_date]
      ,UPPER([geography_grain]) as geography_grain
      ,UPPER([geography]) as geography
      ,UPPER([platform_subset]) as platform_subset
      ,UPPER([customer_engagement]) as customer_engagement
      ,UPPER([forecast_process_note]) as forecast_process_note
      ,[forecast_created_date]
      ,UPPER([data_source]) as data_source
      ,[version]
      ,UPPER([measure]) as measure
      ,[units]
      ,UPPER([proxy_used]) as proxy_used
      ,UPPER([ib_version]) as ib_version
      ,[load_date]
FROM archive.[dbo].[usage_share_archive_landing]
WHERE version = '2022.03.30.1'  
  
"""

redshift_cupsm_archive_records = read_sql_server_to_df(configs) \
    .option("query", cupsm_archive_query) \
    .load()

# redshift_ib_archive_records.show()



# COMMAND ----------

# version = redshift_ib_archive_records.select('version').distinct()
version = redshift_cupsm_archive_records.select('version').distinct().head()[0]


# COMMAND ----------

# write to parquet file in s3

# s3a://dataos-core-prod-team-phoenix/spectrum/ib/
s3_cupsm_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/cupsm/" + version

write_df_to_s3(redshift_cupsm_archive_records, s3_cupsm_output_bucket, "parquet", "overwrite")



# COMMAND ----------


