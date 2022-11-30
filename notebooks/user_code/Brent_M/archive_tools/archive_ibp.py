# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

ibp_archive_query = """

SELECT 
	[Plan_Date] as plan_date
	,UPPER([Geo_Region]) as geo_region
	,UPPER([Subregion]) as subregion
	,UPPER([Subregion4]) as subregion4
	,UPPER([Product_Category]) as product_category
	,UPPER([Primary_Base_Prod_PL_Code]) as primary_base_prod_pl_code
	,UPPER([Sales_Product_PL_Code]) as sales_product_pl_code
	,UPPER([Sales_Product]) as sales_product
	,UPPER([Primary_Base_Product]) as primary_base_product
	,UPPER([Sales_Product_w_Opt]) as sales_product_w_opt
	,UPPER([Sales_Product_Desc]) as sales_product_desc
	,UPPER([Calendar_Year_Month]) as calendar_year_month
	,[Units] as units
	,UPPER([Key_Figure]) as key_figure
	,[version]
FROM [IE2_ExternalPartners].[dbo].[ibp]
WHERE version = '2022.11.07.1'
  
"""

redshift_ibp_archive_records = read_sql_server_to_df(configs) \
    .option("query", ibp_archive_query) \
    .load()

# redshift_ib_archive_records.show()



# COMMAND ----------

# version = redshift_ib_archive_records.select('version').distinct()
# ~ 5 mins
version = redshift_ibp_archive_records.select('version').distinct().head()[0]


# COMMAND ----------

# write to parquet file in s3
# ~ 6 mins

# s3a://dataos-core-prod-team-phoenix/archive/ibp_raw/
s3_ib_output_bucket = constants["S3_BASE_BUCKET"][stack] + "archive/ibp_raw/" + version

write_df_to_s3(redshift_ibp_archive_records, s3_ib_output_bucket, "parquet", "overwrite")


