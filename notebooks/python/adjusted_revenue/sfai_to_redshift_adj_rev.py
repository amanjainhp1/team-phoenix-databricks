# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Finance Tables from SFAI to Redshift

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

#--------- fin_prod ---------
    
adjusted_revenue_salesprod = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.dbo.adjusted_revenue_salesprod") \
    .load()

supplies_finance_flash = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.dbo.supplies_finance_flash") \
    .load()
    
adjusted_revenue_flash = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.dbo.adjusted_revenue_flash") \
    .load()

# COMMAND ----------

tables = [
    ['fin_prod.adjusted_revenue_salesprod', adjusted_revenue_salesprod, "overwrite"],
    ['fin_prod.supplies_finance_flash', supplies_finance_flash, "overwrite"],
    ['fin_prod.adjusted_revenue_flash', adjusted_revenue_flash, "append"],
]

# COMMAND ----------

for t_name, df, mode in tables:
    write_df_to_redshift(configs, df, t_name, mode)
