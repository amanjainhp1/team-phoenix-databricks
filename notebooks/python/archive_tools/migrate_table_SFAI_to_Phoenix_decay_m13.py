# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

## Old Table (SFAI)	New Table (Phoenix)
## [IE2_Scenario].[dbo].[aul_upload_rev] - mdm.aul_upload_rev
## IE2_Scenario.dbo.ink_toner_combined_decay_curves_final - stage.ink_toner_combined_decay_curves_stage
## IE2_Prod.dbo.decay_m13 - stage.decay_m13_stage


# COMMAND ----------

decay_m13_query = """

SELECT [record]
      ,[geography]
      ,[year]
      ,[product]
      ,[split_name]
      ,CAST([value] as decimal(38,37)) as [value]
      ,[technology]
      ,[version]
      ,[insert_ts]
FROM [IE2_Scenario].[dbo].[aul_upload_rev]
"""

decay_m13_records = read_sql_server_to_df(configs) \
    .option("query", decay_m13_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, decay_m13_records, "mdm.aul_upload_rev", "overwrite")

# COMMAND ----------

decay_m13a_query = """

SELECT [record]
      ,[technology]
      ,[brand]
      ,[product_lifecycle_status]
      ,[platform_subset]
      ,[market10]
      ,[EM_DM]
      ,[market13]
      ,[year]
      ,[split_name]
      ,[value]
      ,[load_date]
  FROM IE2_Scenario.[dbo].[ink_toner_combined_decay_curves_final]
"""

decay_m13a_records = read_sql_server_to_df(configs) \
    .option("query", decay_m13a_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, decay_m13a_records, "stage.ink_toner_combined_decay_curves_stage", "overwrite")

# COMMAND ----------

decay_m13b_query = """

SELECT *
FROM IE2_prod.[dbo].[decay_m13]
"""

decay_m13b_records = read_sql_server_to_df(configs) \
    .option("query", decay_m13b_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, decay_m13b_records, "stage.decay_m13_stage", "overwrite")

# COMMAND ----------

submit_remote_query(configs, "TRUNCATE stage.ink_toner_combined_decay_curves_stage")
