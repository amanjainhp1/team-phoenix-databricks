# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

redshift_ib_staging_records = read_redshift_to_df(configs) \
    .option('dbtable', 'stage.ib_staging') \
    .load()

# COMMAND ----------

write_df_to_sqlserver(configs, redshift_ib_staging_records, "IE2_Scenario.aul.ib_staging_2022_09_16", "overwrite")

# COMMAND ----------


