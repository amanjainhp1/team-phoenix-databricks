# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

## Old Table (SFAI)	New Table (Phoenix)
## ie2_landing.[dbo].[usage_share_country_spin_landing]	prod.usage_share_country_spin
## ie2_landing.[dbo].[usage_share_override_mature_landing]	prod.usage_share_override_mature
## ie2_landing.[dbo].[dbo].[usage_share_override_landing]	prod.usage_share_override_npi
## ie2_landing.[dbo].[usage_share_adjust]	prod.usage_share_adjust
## ie2_prod.dbo.dbo.scenario_usage_share	prod.scenario_usage_share
## ie2_prod.dbo.cartridge_mix_override	prod.cartridge_mix_override


# COMMAND ----------

usage_share_country_spin_landing_query = """

SELECT [record]
      ,[cal_date]
      ,[geography_grain]
      ,[geography]
      ,[platform_subset]
      ,[customer_engagement]
      ,[forecast_process_note]
      ,[forecast_created_date]
      ,[data_source]
      ,[version]
      ,[measure]
      ,[units]
      ,[proxy_used]
      ,[ib_version]
      ,[load_date]
  FROM [IE2_Landing].[dbo].[usage_share_country_spin_landing]
"""

usage_share_country_spin_landing_records = read_sql_server_to_df(configs) \
    .option("query", usage_share_country_spin_landing_query) \
    .load()

# COMMAND ----------

usage_share_override_mature_landing_query = """

SELECT [record]
      ,[min_sys_dt]
      ,[month_num]
      ,[geography_grain]
      ,[geography]
      ,[platform_subset]
      ,[customer_engagement]
      ,[forecast_process_note]
      ,[post_processing_note]
      ,[forecast_created_date]
      ,[data_source]
      ,[version]
      ,[measure]
      ,[units]
      ,[proxy_used]
      ,[ib_version]
      ,[load_date]
  FROM [IE2_Landing].[dbo].[usage_share_override_mature_landing]
"""

usage_share_override_mature_landing_records = read_sql_server_to_df(configs) \
    .option("query", usage_share_override_mature_landing_query) \
    .load()

# COMMAND ----------

usage_share_override_landing_query = """

SELECT [record]
      ,[min_sys_dt]
      ,[month_num]
      ,[geography_grain]
      ,[geography]
      ,[platform_subset]
      ,[customer_engagement]
      ,[forecast_process_note]
      ,[post_processing_note]
      ,[forecast_created_date]
      ,[data_source]
      ,[version]
      ,[measure]
      ,[units]
      ,[proxy_used]
      ,[ib_version]
      ,[load_date]
  FROM [IE2_Landing].[dbo].[usage_share_override_landing]
"""

usage_share_override_landing_records = read_sql_server_to_df(configs) \
    .option("query", usage_share_override_landing_query) \
    .load()

# COMMAND ----------

usage_share_adjust_query = """

SELECT [record]
      ,[cal_date]
      ,[geography_grain]
      ,[geography]
      ,[measure]
      ,[platform_subset]
      ,[customer_engagement]
      ,[assumption]
      ,[official]
      ,[Units]
      ,[version]
      ,[load_date]
  FROM [IE2_Landing].[dbo].[usage_share_adjust]
"""

usage_share_adjust_records = read_sql_server_to_df(configs) \
    .option("query", usage_share_adjust_query) \
    .load()

# COMMAND ----------

scenario_usage_share_query = """

SELECT [user_name]
      ,[load_date]
      ,[upload_type]
      ,[scenario_name]
      ,[geography_grain]
      ,[geography]
      ,[platform_subset]
      ,[customer_engagement]
      ,[base_product_number]
      ,[measure]
      ,[min_sys_date]
      ,[month_num]
      ,[value]
  FROM [IE2_Prod].[dbo].[scenario_usage_share]
"""

scenario_usage_share_records = read_sql_server_to_df(configs) \
    .option("query", scenario_usage_share_query) \
    .load()

# COMMAND ----------

cartridge_mix_override_query = """

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

cartridge_mix_override_query_records = read_sql_server_to_df(configs) \
    .option("query", cartridge_mix_override_query) \
    .load()

# COMMAND ----------


write_df_to_redshift(configs, usage_share_country_spin_landing_records, "prod.usage_share_country_spin", "overwrite")
write_df_to_redshift(configs, usage_share_override_mature_landing_records, "prod.usage_share_override_mature", "overwrite")
write_df_to_redshift(configs, usage_share_override_landing_records, "prod.usage_share_override_npi", "overwrite")
write_df_to_redshift(configs, usage_share_adjust_records, "prod.usage_share_adjust", "overwrite")
write_df_to_redshift(configs, scenario_usage_share_records, "prod.scenario_usage_share", "overwrite")
write_df_to_redshift(configs, cartridge_mix_override_query_records, "prod.cartridge_mix_override", "overwrite")

