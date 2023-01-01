# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

## Old Table (SFAI - SQL Server)	                          New Table (Phoenix - Redshift)
## [IE2_Landing].[dbo].[usage_share_override_landing]	      prod.usage_share_override_npi



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
