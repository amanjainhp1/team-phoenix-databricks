# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

budget_query = """

SELECT [record]
      ,[forecast_name]
      ,[cal_date]
      ,[country_alpha2]
      ,[base_product_number]
      ,[units]
      ,[official]
      ,[load_date]
      ,[version]
FROM [ie2_prod].[dbo].[budget]
WHERE version = (SELECT MAX(version) FROM [ie2_prod].[dbo].[budget])
"""

budget_records = read_sql_server_to_df(configs) \
    .option("query", budget_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, budget_records, "prod.budget", "append")
