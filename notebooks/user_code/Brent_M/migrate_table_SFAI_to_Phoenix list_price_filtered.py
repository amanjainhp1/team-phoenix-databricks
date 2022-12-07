# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

list_price_filtered_query = """

SELECT 
	[sales_product_number]
	,[country_alpha2]
	,[currency_code]
	,[price_term_code]
	,[price_start_effective_date]
	,[QB_sequence_number] as qb_sequence_number
	,[list_price]
	,[product_line]
	,[accountingRate] as accounting_rate
	,[ListPriceUSD] as list_price_usd
	,cast([load_date] as datetime) as load_date
FROM [IE2_Prod].[dbo].[list_price_filtered]
where 1=1
	and load_date = '2020-03-04'

"""

list_price_filtered_records = read_sql_server_to_df(configs) \
    .option("query", list_price_filtered_query) \
    .load()

# COMMAND ----------

# version = redshift_ib_archive_records.select('version').distinct()
load_date = list_price_filtered_records.select('load_date').distinct().head()[0]

dateTimeStr = str(load_date)

#just the date
#print(dateTimeStr[:10])


# COMMAND ----------

# write to parquet file in s3

s3_ib_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/list_price_filtered_historical/" + dateTimeStr[:10]

write_df_to_s3(list_price_filtered_records, s3_ib_output_bucket, "parquet", "overwrite")

