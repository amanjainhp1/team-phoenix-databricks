# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

currency_hedge_query = """

SELECT *
FROM [ie2_prod].[dbo].[currency_hedge]
"""

currency_hedge_records = read_sql_server_to_df(configs) \
    .option("query", currency_hedge_query) \
    .load()

# COMMAND ----------



# COMMAND ----------

write_df_to_redshift(configs, currency_hedge_records, "fin_stage.currency_hedge", "overwrite")
