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

SELECT 
    UPPER(record) AS record
    ,cal_date
    ,UPPER(geography_grain) AS geography_grain
    ,UPPER(geography) AS geography
    ,UPPER(platform_subset) AS platform_subset
    ,UPPER(customer_engagement) AS customer_engagement
    ,UPPER(forecast_process_note) AS forecast_process_note
    ,forecast_created_date
    ,UPPER(data_source) AS data_source
    ,UPPER(version) AS version
    ,UPPER(measure) AS measure
    ,units
    ,UPPER(proxy_used) AS proxy_used
    ,UPPER(ib_version) AS ib_version
    ,load_date
  FROM [IE2_Landing].[dbo].[usage_share_country_spin_landing]
"""

usage_share_country_spin_landing_records = read_sql_server_to_df(configs) \
    .option("query", usage_share_country_spin_landing_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, usage_share_country_spin_landing_records, "prod.usage_share_country_spin", "overwrite")

# COMMAND ----------

usage_share_override_mature_landing_query = """

SELECT 
    UPPER(record) AS record
    ,min_sys_dt
    ,month_num
    ,UPPER(geography_grain) AS geography_grain
    ,UPPER(geography) AS geography
    ,UPPER(platform_subset) AS platform_subset
    ,UPPER(customer_engagement) AS customer_engagement
    ,UPPER(forecast_process_note) AS forecast_process_note
    ,UPPER(post_processing_note) AS post_processing_note
    ,forecast_created_date
    ,UPPER(data_source) AS data_source
    ,UPPER(version) AS version
    ,UPPER(measure) AS measure
    ,units
    ,UPPER(proxy_used) AS proxy_used
    ,UPPER(ib_version) AS ib_version
    ,load_date
  FROM [IE2_Landing].[dbo].[usage_share_override_mature_landing]
"""

usage_share_override_mature_landing_records = read_sql_server_to_df(configs) \
    .option("query", usage_share_override_mature_landing_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, usage_share_override_mature_landing_records, "prod.usage_share_override_mature", "overwrite")

# COMMAND ----------

usage_share_override_landing_query = """

SELECT 
    UPPER(record) AS record
    ,min_sys_dt
    ,month_num
    ,UPPER(geography_grain) AS geography_grain
    ,UPPER(geography) AS geography
    ,UPPER(platform_subset) AS platform_subset
    ,UPPER(customer_engagement) AS customer_engagement
    ,UPPER(forecast_process_note) AS forecast_process_note
    ,UPPER(post_processing_note) AS post_processing_note
    ,forecast_created_date
    ,UPPER(data_source) AS data_source
    ,UPPER(version) AS version
    ,UPPER(measure) AS measure
    ,units
    ,UPPER(proxy_used) AS proxy_used
    ,UPPER(ib_version) AS ib_version
    ,load_date
  FROM [IE2_Landing].[dbo].[usage_share_override_landing]
"""

usage_share_override_landing_records = read_sql_server_to_df(configs) \
    .option("query", usage_share_override_landing_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, usage_share_override_landing_records, "prod.usage_share_override_npi", "overwrite")

# COMMAND ----------

usage_share_adjust_query = """

SELECT 
    UPPER(record) AS record
    ,cal_date
    ,UPPER(geography_grain) AS geography_grain
    ,UPPER(geography) AS geography
    ,UPPER(measure) AS measure
    ,UPPER(platform_subset) AS platform_subset
    ,UPPER(customer_engagement) AS customer_engagement
    ,UPPER(assumption) AS assumption
    ,official
    ,units
    ,UPPER(version) AS version
    ,load_date
  FROM [IE2_Landing].[dbo].[usage_share_adjust]
"""

usage_share_adjust_records = read_sql_server_to_df(configs) \
    .option("query", usage_share_adjust_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, usage_share_adjust_records, "prod.usage_share_adjust", "overwrite")

# COMMAND ----------

scenario_usage_share_query = """

SELECT 
       UPPER(user_name) AS user_name
      ,load_date
      ,UPPER(upload_type) AS upload_type
      ,UPPER(scenario_name) AS scenario_name
      ,UPPER(geography_grain) AS geography_grain
      ,UPPER(geography) AS geography
      ,UPPER(platform_subset) AS platform_subset
      ,UPPER(customer_engagement) AS customer_engagement
      ,UPPER(base_product_number) AS base_product_number
      ,UPPER(measure) AS measure
      ,[min_sys_date]
      ,[month_num]
      ,[value]
  FROM [IE2_Prod].[dbo].[scenario_usage_share]
"""

scenario_usage_share_records = read_sql_server_to_df(configs) \
    .option("query", scenario_usage_share_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, scenario_usage_share_records, "prod.scenario_usage_share", "overwrite")

# COMMAND ----------

cartridge_mix_override_query = """

SELECT 
       UPPER([record]) AS record
      ,UPPER([platform_subset]) AS platform_subset
      ,UPPER([Crg_Base_Prod_Number]) AS Crg_Base_Prod_Number
      ,UPPER([geography]) AS geography
      ,UPPER([geography_grain]) AS geography_grain
      ,[cal_date]
      ,[mix_pct]
      ,UPPER([product_lifecycle_status]) AS product_lifecycle_status
      ,UPPER([customer_engagement]) AS customer_engagement
      ,[load_date]
      ,[official]
  FROM ie2_prod.[dbo].[cartridge_mix_override]
"""

cartridge_mix_override_query_records = read_sql_server_to_df(configs) \
    .option("query", cartridge_mix_override_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, cartridge_mix_override_query_records, "prod.cartridge_mix_override", "overwrite")
