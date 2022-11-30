# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

## prod.actuals_hw LF data
## save data from Redshift that is going to be restated

actuals_lf_query = """

SELECT *
FROM prod.actuals_hw
WHERE 1=1
	AND record = 'ACTUALS_LF'
	AND cal_date BETWEEN '2016-11-01' and '2021-10-01'
	AND base_quantity  <> 0
"""

redshift_actuals_lf_records = read_redshift_to_df(configs) \
    .option("query", actuals_lf_query) \
    .load()

# COMMAND ----------

# write to parquet file in s3

s3_usage_share_output_bucket = constants["S3_BASE_BUCKET"][stack] + "archive/actuals_lf/10_21_2022_restate/"
print (s3_usage_share_output_bucket)


# COMMAND ----------

write_df_to_s3(redshift_actuals_lf_records, s3_usage_share_output_bucket, "parquet", "overwrite")

# COMMAND ----------

## delete data from Redshift that is going to be over-written

query = f"""
      
DELETE
FROM prod.actuals_hw
where 1=1
	AND record = 'ACTUALS_LF'
	AND cal_date BETWEEN '2016-11-01' and '2021-10-01'
"""

submit_remote_query(configs, query)

# COMMAND ----------

## Get data from SFAI that will be restated into Redshift

SFAI_actuals_lf_query = """

SELECT
	'ACTUALS_LF' record, 
	a.cal_date,
	UPPER(a.country_alpha2) AS country_alpha2,
	UPPER(a.base_product_number) AS base_product_number,
	UPPER(a.Platform_subset) AS platform_subset,
	'LF' AS source,
	sum(a.[value]) AS base_quantity,
	1 as official,
    getdate() AS load_date, 
	'2021.11.01.1' AS version
FROM [IE2_Landing].[lfd].[actuals_pro_test_landing_1721] a
WHERE 1=1
	AND [value] <> 0
GROUP BY
	a.cal_date,
	a.country_alpha2,
	a.base_product_number,
	a.Platform_subset 

"""

SFAI_actuals_lf_records = read_sql_server_to_df(configs) \
    .option("query", SFAI_actuals_lf_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, SFAI_actuals_lf_records, "prod.actuals_hw", "append")
