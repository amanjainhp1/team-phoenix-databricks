# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

decay_m13_query = """

SELECT *
FROM [IE2_PROD].[dbo].[decay_m13]

"""

decay_13_records = read_sql_server_to_df(configs) \
    .option("query", decay_m13_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, decay_13_records, "prod.decay_m13", "overwrite")
