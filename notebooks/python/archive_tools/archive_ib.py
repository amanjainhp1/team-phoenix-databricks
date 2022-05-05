# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

ib_archive_query = """

SELECT
    UPPER(record) as record
    ,cal_date
    ,country as country_alpha2
    ,platform_subset
    ,customer_engagement
    ,measure
    ,units
    ,official
    ,load_date
    ,version
  FROM ie2_prod.dbo.ib
  WHERE version = '2022.05.02.1'
  
"""

redshift_ib_archive_records = read_sql_server_to_df(configs) \
    .option("query", ib_archive_query) \
    .load()

# redshift_ib_archive_records.show()



# COMMAND ----------

# version = redshift_ib_archive_records.select('version').distinct()
version = redshift_ib_archive_records.select('version').distinct().head()[0]


# COMMAND ----------

# write to parquet file in s3

# s3a://dataos-core-prod-team-phoenix/spectrum/ib/
s3_ib_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/ib/" + version

write_df_to_s3(redshift_ib_archive_records, s3_ib_output_bucket, "parquet", "overwrite")



# COMMAND ----------


