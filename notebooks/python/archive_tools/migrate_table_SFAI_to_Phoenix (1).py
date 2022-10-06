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

decay_m13_query = """

SELECT * FROM [IE2_prod].[dbo].[decay_m13]
"""

decay_m13_records = read_sql_server_to_df(configs) \
    .option("query", decay_m13_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, decay_m13_records, "prod.decay_m13", "overwrite")
