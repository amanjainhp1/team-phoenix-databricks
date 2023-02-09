# Databricks notebook source
# MAGIC %run ../common/configs 

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

base_forecast = read_redshift_to_df(configs) \
    .option("dbtable", "stage.vtc") \
    .load()

working_forecast = read_redshift_to_df(configs) \
    .option("dbtable", "scen.toner_working_fcst") \
    .load()

# COMMAND ----------

tables = [
    ['IE2_Scenario.toner.c2c_cartridges_w_vtc_rs_2023_02_08', base_forecast, "overwrite"],
    ['IE2_Scenario.toner.toner_working_fcst_rs_2023_02_08', working_forecast, "overwrite"]
]

# COMMAND ----------

for t_name, df, mode in tables:
    write_df_to_sqlserver(configs, df, t_name, mode)

# COMMAND ----------


