# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

usage_share_archive_query = """

select
	UPPER(record) as record,
	cal_date,
	UPPER(geography_grain) as geography_grain,
	UPPER(geography) as geography,
	platform_subset,
	UPPER(customer_engagement) as customer_engagement ,
	UPPER(measure) as measure,
	units,
	ib_version,
	UPPER(source) as source,
	version,
	load_date
from
    ie2_prod.dbo.usage_share
where version = '2022.06.22.1'

"""

redshift_usage_share_archive_records = read_sql_server_to_df(configs) \
    .option("query", usage_share_archive_query) \
    .load()

# redshift_usage_share_archive_records.show()

# COMMAND ----------

# version = redshift_ib_archive_records.select('version').distinct()
version = redshift_usage_share_archive_records.select('version').distinct().head()[0]
# version.show()

# COMMAND ----------

# write to parquet file in s3

# s3a://dataos-core-prod-team-phoenix/spectrum/ib/
s3_usage_share_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/usage_share/" + version

write_df_to_s3(redshift_usage_share_archive_records, s3_usage_share_output_bucket, "parquet", "overwrite")



# COMMAND ----------


