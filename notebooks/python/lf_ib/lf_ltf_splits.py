# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# if table exists, truncate, else print exception message
try:
    row_count = read_redshift_to_df(configs).option("dbtable", "prod.lf_ltf_splits").load().count()
    if row_count > 0:
        submit_remote_query(configs, "TRUNCATE prod.lf_ltf_splits")
except Exception as error:
    print ("An exception has occured:", error)

# COMMAND ----------

df_lf_ltf_splits = spark.read.format('csv').options(header='true', inferSchema='true').load('{}product/norm_ships/fcst/ltf/large_format/lf_ltp_splits.csv'.format(constants['S3_BASE_BUCKET'][stack]))

# COMMAND ----------

write_df_to_redshift(configs, df_lf_ltf_splits, "prod.lf_ltf_splits", "append")
