# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

## Old Table (SFAI - SQL Server)	                          New Table (Phoenix - Redshift)
## [IE2_Landing].[dbo].[dbo].[usage_share_override_landing]	  prod.usage_share_override_npi
## [IE2_Prod].dbo.scenario_usage_share	                      prod.scenario_usage_share
## [IE2_Prod].[dbo].[working_forecast_channel_fill]           scen.working_forecast_channel_fill
## [IE2_Prod].[dbo].[working_forecast_mix_rate]               scen.working_forecast_mix_rate
## [IE2_Prod].[dbo].[working_forecast_supplies_spares]        scen.working_forecast_supplies_spares
## [IE2_Prod].[dbo].[working_forecast_usage_share]            scen.working_forecast_usage_share
## [IE2_Prod].[dbo].[working_forecast_vtc_override]           scen.working_forecast_vtc_override
## [IE2_Prod].[dbo].[working_forecast_yield]                  scen.working_forecast_yield


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

working_forecast_channel_fill_query = """

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
  FROM [IE2_Prod].[dbo].[working_forecast_channel_fill]
"""

working_forecast_channel_fill_records = read_sql_server_to_df(configs) \
    .option("query", working_forecast_channel_fill_query) \
    .load()


# COMMAND ----------

# working_forecast_channel_fill_records.display()

# COMMAND ----------

write_df_to_redshift(configs, working_forecast_channel_fill_records, "scen.working_forecast_channel_fill", "overwrite")

# COMMAND ----------

working_forecast_mix_rate_query = """

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
  FROM [IE2_Prod].[dbo].[working_forecast_mix_rate]
"""

working_forecast_mix_rate_records = read_sql_server_to_df(configs) \
    .option("query", working_forecast_mix_rate_query) \
    .load()

# COMMAND ----------

# working_forecast_mix_rate_records.display()

# COMMAND ----------

write_df_to_redshift(configs, working_forecast_mix_rate_records, "scen.working_forecast_mix_rate", "overwrite")

# COMMAND ----------

working_forecast_supplies_spares_query = """

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
  FROM [IE2_Prod].[dbo].[working_forecast_supplies_spares]
"""

working_forecast_supplies_spares_records = read_sql_server_to_df(configs) \
    .option("query", working_forecast_supplies_spares_query) \
    .load()

# COMMAND ----------

# working_forecast_supplies_spares_records.display()

# COMMAND ----------

write_df_to_redshift(configs, working_forecast_supplies_spares_records, "scen.working_forecast_supplies_spares", "overwrite")

# COMMAND ----------

working_forecast_usage_share_query = """

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
  FROM [IE2_Prod].[dbo].[working_forecast_usage_share]
"""

working_forecast_usage_share_records = read_sql_server_to_df(configs) \
    .option("query", working_forecast_usage_share_query) \
    .load()

# COMMAND ----------

# working_forecast_usage_share_records.display()

# COMMAND ----------

write_df_to_redshift(configs, working_forecast_usage_share_records, "scen.working_forecast_usage_share", "overwrite")

# COMMAND ----------

working_forecast_vtc_override_query = """

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
  FROM [IE2_Prod].[dbo].[working_forecast_vtc_override]
"""

working_forecast_vtc_override_records = read_sql_server_to_df(configs) \
    .option("query", working_forecast_vtc_override_query) \
    .load()

# COMMAND ----------

# working_forecast_vtc_override_records.display()

# COMMAND ----------

write_df_to_redshift(configs, working_forecast_vtc_override_records, "scen.working_forecast_vtc_override", "overwrite")

# COMMAND ----------

working_forecast_yield_query = """

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
  FROM [IE2_Prod].[dbo].[working_forecast_yield]
"""

working_forecast_yield_records = read_sql_server_to_df(configs) \
    .option("query", working_forecast_yield_query) \
    .load()

# COMMAND ----------

# working_forecast_yield_records.display()

# COMMAND ----------

write_df_to_redshift(configs, working_forecast_yield_records, "scen.working_forecast_yield", "overwrite")
