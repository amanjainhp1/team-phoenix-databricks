# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Transfer Tables from Redshift to SFAI

# COMMAND ----------

# MAGIC %run ../common/configs 

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Dataframes for Each Table

# COMMAND ----------

# fin_prod
adjusted_revenue_salesprod = read_redshift_to_df(configs) \
    .option("query", "SELECT * FROM fin_prod.adjusted_revenue_salesprod WHERE version = (SELECT MAX(version) FROM fin_prod.adjusted_revenue_salesprod)") \
    .load()

adjusted_revenue_flash = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.adjusted_revenue_flash") \
    .load()

# COMMAND ----------

adjusted_revenue_salesprod.createOrReplaceTempView("adjusted_revenue_salesprod")
adjusted_revenue_flash.createOrReplaceTempView("adjusted_revenue_flash")

# COMMAND ----------

query_version = spark.sql("""
SELECT MAX(version) FROM adjusted_revenue_salesprod
""")

query_version.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SFAI Table Names

# COMMAND ----------

tables = [
    ['IE2_Financials.dbo.adjusted_revenue_salesprod', adjusted_revenue_salesprod, "append"],
    ['IE2_Financials.dbo.adjusted_revenue_flash', adjusted_revenue_flash, "overwrite"]
]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write to SFAI

# COMMAND ----------

for t_name, df, mode in tables:
    write_df_to_sqlserver(configs, df, t_name, mode)
