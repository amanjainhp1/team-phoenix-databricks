# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Load Redshift Inputs to Delta Lake for Forecasting Engine

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ./config_forecasting_engine

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Fin Prod

# COMMAND ----------

actuals_plus_forecast_financials = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_plus_forecast_financials") \
    .load()

stf_dollarization_df = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.stf_dollarization") \
    .load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## MDM Tables

# COMMAND ----------

calendar_df = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.calendar") \
    .load()

hw_xref_df = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.hardware_xref") \
    .load()

iso_cc_rollup_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_cc_rollup_xref") \
    .load()

supplies_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.supplies_xref") \
    .load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Prod Tables

# COMMAND ----------

actuals_supplies = read_redshift_to_df(configs) \
    .option("dbtable", "prod.actuals_supplies") \
    .load()

demand = read_redshift_to_df(configs) \
    .option("dbtable", "prod.demand") \
    .load()

installed_base = read_redshift_to_df(configs) \
    .option("query", "select * from prod.ib where version='{}'".format(ib_version)) \
    .load()

ink_working_fcst = read_redshift_to_df(configs) \
    .option("query", "select * from prod.working_forecast where version = '{}' and record = 'WORKING_FORECAST_INK'".format(ink_wf_version)) \
    .load()

norm_shipments = read_redshift_to_df(configs) \
    .option("query", "select * from  prod.norm_shipments where version='{}'".format(ib_version)) \
    .load()

toner_working_fcst = read_redshift_to_df(configs) \
    .option("query", "select * from prod.working_forecast where version = '{}' and record = 'IE2-WORKING-FORECAST'".format(toner_wf_version)) \
    .load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Scen

# COMMAND ----------

toner_us = read_redshift_to_df(configs) \
    .option("dbtable", "scen.toner_03_usage_share") \
    .load()

toner_06_mix_rate_final = read_redshift_to_df(configs) \
    .option("dbtable", "scen.toner_06_mix_rate_final") \
    .load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Stage

# COMMAND ----------

shm_base_helper = read_redshift_to_df(configs) \
    .option("dbtable", "stage.shm_base_helper") \
    .load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Supplies Fcst

# COMMAND ----------

ms4_v_canon_units_prelim = read_redshift_to_df(configs)\
    .option("dbtable", "supplies_fcst.odw_canon_units_prelim_vw")\
    .load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Table List

# COMMAND ----------

tables = [
    ["fin_prod.actuals_plus_forecast_financials", actuals_plus_forecast_financials, "overwrite"],
    ["fin_prod.stf_dollarization", stf_dollarization_df, "overwrite"],
    ["mdm.calendar", calendar_df, "overwrite"],
    ["mdm.iso_cc_rollup_xref", iso_cc_rollup_xref, "overwrite"],
    ["mdm.hardware_xref", hw_xref_df, "overwrite"],
    ["mdm.supplies_xref", supplies_xref, "overwrite"],
    ["prod.actuals_supplies", actuals_supplies, "overwrite"],
    ["prod.demand", demand, "overwrite"],
    ["prod.ib", installed_base, "append"],
    ["prod.norm_shipments", norm_shipments, "append"],
    #["prod.working_forecast", ink_working_fcst, "append"],
    #["prod.working_forecast", toner_working_fcst, "append"],
    ["scen.toner_03_usage_share", toner_us, "overwrite"],
    ["scen.toner_06_mix_rate_final", toner_06_mix_rate_final, "overwrite"],
    ["stage.shm_base_helper", shm_base_helper, "overwrite"],
    ["supplies_fcst.ms4_v_canon_units_prelim", ms4_v_canon_units_prelim, "overwrite"],
]

# COMMAND ----------

# MAGIC %run "../common/delta_lake_load_with_params" $tables=tables

# COMMAND ----------


