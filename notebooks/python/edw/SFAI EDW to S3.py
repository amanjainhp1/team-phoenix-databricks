# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# edw_revenue_units_sales_landing        --dataos-core-prod-team-phoenix-fin bucket
# edw_revenue_document_currency_landing  --dataos-core-prod-team-phoenix-fin bucket
# edw_revenue_dollars_landing            --dataos-core-prod-team-phoenix-fin bucket
# edw_revenue_units_base_landing         --dataos-core-prod-team-phoenix-fin bucket
# edw_shipment_actuals_landing           --dataos-core-prod-team-phoenix bucket



# COMMAND ----------

## IE2_Landing.dbo.edw_revenue_units_sales_landing

edw_revenue_units_sales_landing_query = """

SELECT [revenue_recognition_fiscal_year_month_code]
      ,UPPER([profit_center_code]) as profit_center_code
      ,UPPER([profit_center_level_0_name]) as profit_center_level_0_name
      ,UPPER([profit_center_level_1_name]) as profit_center_level_1_name
      ,UPPER([profit_center_level_2_Name]) as profit_center_level_2_Name
      ,UPPER([profit_center_level_3_Name]) as profit_center_level_3_Name
      ,UPPER([profit_center_level_4_Name]) as profit_center_level_4_Name
      ,UPPER([profit_center_level_5_Name]) as profit_center_level_5_Name
      ,UPPER([global_business_unit_name]) as global_business_unit_name
      ,UPPER([product_category_name]) as product_category_name
      ,UPPER([business_area_code]) as business_area_code
      ,UPPER([ipg_product_product_line_identifier]) as ipg_product_product_line_identifier
      ,UPPER([ipd_product_platform_subset_name]) as ipd_product_platform_subset_name
      ,UPPER([ipg_product_base_product_number]) as ipg_product_base_product_number
      ,UPPER([ipg_product_base_product_name]) as ipg_product_base_product_name
      ,UPPER([product_base_identifier]) as product_base_identifier
      ,UPPER([manufacturing_product_identifier]) as manufacturing_product_identifier
      ,[working_PandL_summary_base_quantity]
      ,[working_PandL_summary_extended_quantity]
      ,[working_PandL_summary_sales_quantity]
      ,[load_date]
      ,[version]
  FROM [IE2_Landing].[dbo].[edw_revenue_units_sales_landing]

"""

redshift_edw_revenue_units_sales_landing_records = read_sql_server_to_df(configs) \
    .option("query", edw_revenue_units_sales_landing_query) \
    .load()



# COMMAND ----------

# write to parquet file in s3

s3_usage_share_output_bucket = constants["S3_FIN_BUCKET"][stack] + "EDW/edw_revenue_units_sales_landing/"
# print (s3_usage_share_output_bucket)

write_df_to_s3(redshift_edw_revenue_units_sales_landing_records, s3_usage_share_output_bucket, "parquet", "overwrite")



# COMMAND ----------

# IE2_Landing.dbo.edw_revenue_document_currency_landing

edw_revenue_document_currency_landing_query = """
SELECT UPPER([fiscal year name]) as [fiscal year name]
      ,UPPER([fiscal year half code]) as [fiscal year half code]
      ,UPPER([fiscal year month code]) as [fiscal year month code]
      ,UPPER([profit center level 0 name]) as [profit center level 0 name]
      ,UPPER([profit center code]) as [profit center code]
      ,UPPER([profit center level 5 description]) as [profit center level 5 description]
      ,UPPER([document currency code]) as [document currency code]
      ,UPPER([business area code]) as [business area code]
      ,UPPER([business area description]) as [business area description]
      ,[net K$]
      ,[load_date]
      ,[version]
FROM [IE2_Landing].[dbo].[edw_revenue_document_currency_landing]
"""

redshift_edw_revenue_document_currency_landing_records = read_sql_server_to_df(configs) \
    .option("query", edw_revenue_document_currency_landing_query) \
    .load()

# COMMAND ----------

# write to parquet file in s3

s3_usage_share_output_bucket = constants["S3_FIN_BUCKET"][stack] + "EDW/edw_revenue_document_currency_landing/"

write_df_to_s3(redshift_edw_revenue_document_currency_landing_records, s3_usage_share_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# IE2_Landing.dbo.edw_revenue_dollars_landing
edw_revenue_dollars_landing_query = """
select
	UPPER([revenue_recognition_fiscal_year_month_code]) as [revenue_recognition_fiscal_year_month_code],
	UPPER([profit_center_code]) as [profit_center_code],
	UPPER([profit_center_level_0_name]) as [profit_center_level_0_name],
	UPPER([profit_center_level_1_name]) as [profit_center_level_1_name],
	UPPER([profit_center_level_2_Name]) as [profit_center_level_2_Name],
	UPPER([profit_center_level_3_Name]) as [profit_center_level_3_Name],
	UPPER([profit_center_level_4_Name]) as [profit_center_level_4_Name],
	UPPER([profit_center_level_5_Name]) as [profit_center_level_5_Name],
	UPPER([global_business_unit_name]) as [global_business_unit_name],
	UPPER([product_category_name]) as [product_category_name],
	UPPER([business_area_code]) as [business_area_code],
	UPPER([ipg_product_base_product_number]) as [ipg_product_base_product_number],
	UPPER([product_base_identifier]) as [product_base_identifier],
	UPPER([manufacturing_product_identifier]) as [manufacturing_product_identifier],
	UPPER([functional_area_level_11_name]) as [functional_area_level_11_name],
	UPPER([group_account_name]) as [group_account_name],
	UPPER([group_account_identifier]) as [group_account_identifier],
	[transaction_detail_us_dollar_amount],
	[load_date],
	[version]
FROM [IE2_Landing].[dbo].[edw_revenue_dollars_landing]
"""

edw_revenue_dollars_landing_records = read_sql_server_to_df(configs) \
    .option("query", edw_revenue_dollars_landing_query) \
    .load()

# COMMAND ----------

# write to parquet file in s3

s3_usage_share_output_bucket = constants["S3_FIN_BUCKET"][stack] + "EDW/edw_revenue_dollars_landing/"

write_df_to_s3(edw_revenue_dollars_landing_records, s3_usage_share_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# IE2_Landing.dbo.edw_revenue_units_base_landing (NOT USED; should we migrate?)
edw_revenue_units_base_landing_query = """
select
	UPPER([revenue_recognition_fiscal_year_month_code]) as [revenue_recognition_fiscal_year_month_code],
	UPPER([profit_center_code]) as [profit_center_code],
	UPPER([profit_center_level_0_name]) as [profit_center_level_0_name],
	UPPER([profit_center_level_1_name]) as [profit_center_level_1_name],
	UPPER([profit_center_level_2_Name]) as [profit_center_level_2_Name],
	UPPER([profit_center_level_3_Name]) as [profit_center_level_3_Name],
	UPPER([profit_center_level_4_Name]) as [profit_center_level_4_Name],
	UPPER([profit_center_level_5_Name]) as [profit_center_level_5_Name],
	UPPER([global_business_unit_name]) as [global_business_unit_name],
	UPPER([product_category_name]) as [product_category_name],
	UPPER([business_area_code]) as [business_area_code],
	UPPER([ipg_product_product_line_identifier]) as [ipg_product_product_line_identifier],
	UPPER([ipd_product_platform_subset_name]) as [ipd_product_platform_subset_name],
	UPPER([ipg_product_base_product_number]) as [ipg_product_base_product_number],
	UPPER([ipg_product_base_product_name]) as [ipg_product_base_product_name],
	UPPER([product_base_identifier]) as [product_base_identifier],
	UPPER([manufacturing_product_identifier]) as [manufacturing_product_identifier],
	[working_PandL_summary_base_quantity],
	[working_PandL_summary_extended_quantity],
	[working_PandL_summary_sales_quantity],
	[working_PandL_summary_unit_quantity],
	[load_date],
	[version]
from [IE2_Landing].[dbo].[edw_revenue_units_base_landing]
"""

edw_revenue_units_base_landing_records = read_sql_server_to_df(configs) \
    .option("query", edw_revenue_units_base_landing_query) \
    .load()

# COMMAND ----------

# write to parquet file in s3

s3_usage_share_output_bucket = constants["S3_FIN_BUCKET"][stack] + "EDW/edw_revenue_units_base_landing/"

write_df_to_s3(edw_revenue_units_base_landing_records, s3_usage_share_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# IE2_Landing.dbo.edw_shipment_actuals_landing (NOT USED; should we migrate?)
edw_shipment_actuals_landing_query = """
select
	UPPER([financial_close_fiscal_year_month_code]) as [financial_close_fiscal_year_month_code],
	UPPER([profit_center_code]) as [profit_center_code],
	UPPER([profit_center_level_0_name]) as [profit_center_level_0_name],
	UPPER([profit_center_level_1_name]) as [profit_center_level_1_name],
	UPPER([profit_center_level_2_Name]) as [profit_center_level_2_Name],
	UPPER([profit_center_level_3_Name]) as [profit_center_level_3_Name],
	UPPER([profit_center_level_4_Name]) as [profit_center_level_4_Name],
	UPPER([profit_center_level_5_Name]) as [profit_center_level_5_Name],
	UPPER([global_business_unit_name]) as [global_business_unit_name],
	UPPER([product_category_name]) as [product_category_name],
	UPPER([business_area_code]) as [business_area_code],
	UPPER([ipg_product_product_line_identifier]) as [ipg_product_product_line_identifier],
	UPPER([ipd_product_platform_subset_name]) as [ipd_product_platform_subset_name],
	UPPER([ipg_product_base_product_number]) as [ipg_product_base_product_number],
	UPPER([Manufacturing_Product_Identifier]) as [Manufacturing_Product_Identifier],
	[shipment_base_quantity],
	[shipment_unit_quantity],
	[Shipment_Product_ID_Quantity],
	[load_date],
	[version]
FROM [IE2_Landing].[dbo].[edw_shipment_actuals_landing]
"""


edw_shipment_actuals_landing_records = read_sql_server_to_df(configs) \
    .option("query", edw_shipment_actuals_landing_query) \
    .load()

# COMMAND ----------

# write to parquet file in s3

s3_usage_share_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/EDW/edw_shipment_actuals_landing/"

write_df_to_s3(edw_shipment_actuals_landing_records, s3_usage_share_output_bucket, "parquet", "overwrite")
