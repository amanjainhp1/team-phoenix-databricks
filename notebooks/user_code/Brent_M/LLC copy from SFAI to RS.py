# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

actuals_llc_query = """

SELECT *
FROM [IE2_prod].[dbo].[actuals_llc]
WHERE cal_date = '2023-01-01'  
"""

actuals_llc_records = read_sql_server_to_df(configs) \
    .option("query", actuals_llc_query) \
    .load()

# COMMAND ----------

wd3_llc_query = """

SELECT *
FROM [IE2_prod].[dbo].[wd3_llc]
WHERE version = '2023.02.09.1' AND record = 'WD3-LLC' 
"""

wd3_llc_records = read_sql_server_to_df(configs) \
    .option("query", wd3_llc_query) \
    .load()

# COMMAND ----------

# write actuals_llc records to Redshift
write_df_to_redshift(configs, actuals_llc_records, "prod.actuals_llc", "append")

# COMMAND ----------

# write wd3_llc records to Redshift
write_df_to_redshift(configs, wd3_llc_records, "prod.wd3_llc", "append")
