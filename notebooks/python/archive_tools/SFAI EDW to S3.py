# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

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

# s3a://dataos-core-prod-team-phoenix/spectrum/ib/
s3_usage_share_output_bucket = constants["S3_BASE_BUCKET"][stack] + "landing/EDW/edw_revenue_units_sales_landing/"

write_df_to_s3(redshift_edw_revenue_units_sales_landing_records, s3_usage_share_output_bucket, "parquet", "overwrite")


