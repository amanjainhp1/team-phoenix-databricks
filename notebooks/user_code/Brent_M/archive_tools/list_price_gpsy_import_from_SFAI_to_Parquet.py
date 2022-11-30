# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

list_price_gpsy_query = """

SELECT UPPER([Product Number]) as product_number
      ,UPPER([Country code]) as country_code
      ,UPPER([Currency code]) as currency_code
      ,UPPER([Price Term code]) as price_term_code
      ,[Price start effective date] as price_start_effective_date
      ,[QBL Sequence Number] as qbl_sequence_number
      ,[List Price] as list_price
      ,UPPER([Product Line]) as product_line
      ,[load_date] load_date
      ,[version] version
FROM [IE2_Landing].[dbo].[list_price_gpsy_landing]
where 1=1
	and version = '2020.01.30.1'
"""

list_price_gpsy_records = read_sql_server_to_df(configs) \
    .option("query", list_price_gpsy_query) \
    .load()

# COMMAND ----------

#version = redshift_acct_rates_records.select('version').distinct()
version = list_price_gpsy_records.select('version').distinct().head()[0]
version

# COMMAND ----------

# output dataset to S3 for archival purposes
# s3a://dataos-core-dev-team-phoenix/product/list_price_gpsy/[version]/

# set the bucket name
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/list_price_gpsy/" + version  

# write the data out in parquet format
write_df_to_s3(list_price_gpsy_records, s3_output_bucket, "parquet", "overwrite")
