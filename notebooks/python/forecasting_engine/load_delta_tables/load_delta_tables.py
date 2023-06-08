# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Load Redshift Inputs to Delta Lake for Forecasting Engine

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Library Scripts

# COMMAND ----------

# MAGIC %run ../config_forecasting_engine

# COMMAND ----------

working_forecast_version = dbutils.jobs.taskValues.get(taskKey="working_forecast_promo", key="working_forecast_version")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Data Check Function

# COMMAND ----------

if dbutils.widgets.get('load_delta_tables').lower().strip() != 'true':
    dbutils.notebook.exit('EXIT: step skipped due to load_delta_tables parameter not equal to "true"')

# COMMAND ----------

# inner function to check if delta table exists
def delta_table_exists(table_name: str) -> bool:
    delta_exists = False
    try:
        delta_df = spark.read.table(table_name)
        delta_exists = True
    except Exception as error:
        print(table_name + " delta table does not exist.")
    return delta_exists

# compare Redshift table to Delta table
# if row counts do not match,
def check_against_redshift(test_tables: list) -> list:
    tables = []
    for table in test_tables:
        table_name = table[0]
        df = table[1]
        mode = table[2]
        version = table[3]

        if not delta_table_exists(table_name):
            tables.append(table)
            continue

        # check if loading full table or single version (i.e. YYYY.MM.DD.N or False) 
        if not version:                
            if spark.read.table(table_name).count() == df.count():
                print(table_name + " row counts match.")
            else:
                spark.sql("DROP TABLE IF EXISTS " + table[0])
                print(table_name + " row counts do not match. Table dropped.")
                tables.append(table)
        else:
            version_list = spark.read.table(table_name) \
                .select('version') \
                .rdd.map(lambda row: row[0]) \
                .collect()
            if version in version_list:
                print(table_name + " version exists.")
            else:
                print(table_name + " version does not exist.")
                tables.append(table)
    
    return tables

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

working_fcst = read_redshift_to_df(configs) \
    .option("query", "select * from prod.working_forecast where version = '{}' and record = '{}-WORKING-FORECAST'".format(working_forecast_version, technology_label.upper())) \
    .load()

norm_shipments = read_redshift_to_df(configs) \
    .option("query", "select * from  prod.norm_shipments where version='{}'".format(ib_version)) \
    .load()

version = read_redshift_to_df(configs) \
    .option("dbtable", "prod.version") \
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

shm_base_helper = read_redshift_to_df(configs) \
    .option("dbtable", f"scen.{technology_label}_shm_base_helper") \
    .load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Table List

# COMMAND ----------

test_tables = [
    ["fin_prod.actuals_plus_forecast_financials", actuals_plus_forecast_financials, "overwrite", False],
    ["fin_prod.stf_dollarization", stf_dollarization_df, "overwrite", False],
    ["mdm.calendar", calendar_df, "overwrite", False],
    ["mdm.iso_cc_rollup_xref", iso_cc_rollup_xref, "overwrite", False],
    ["mdm.hardware_xref", hw_xref_df, "overwrite", False],
    ["mdm.supplies_xref", supplies_xref, "overwrite", False],
    ["prod.actuals_supplies", actuals_supplies, "overwrite", False],
    ["prod.demand", demand, "overwrite", False],
    ["prod.ib", installed_base, "append", ib_version],
    ["prod.norm_shipments", norm_shipments, "append", ib_version],
    ["prod.working_forecast", working_fcst, "append", working_forecast_version],
    [f"scen.{technology_label}_shm_base_helper", shm_base_helper, "overwrite", False],
    ["prod.version", version, "overwrite", False]
]

if technology_label == 'toner':
    test_tables += [
        ["scen.toner_03_usage_share", toner_us, "overwrite", False],
        ["scen.toner_06_mix_rate_final", toner_06_mix_rate_final, "overwrite", False]
    ]

# COMMAND ----------

tables = check_against_redshift(test_tables)

# COMMAND ----------

for table in tables:
    print(table[0] + " ")

# COMMAND ----------

# MAGIC %run "../common/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

end_table_check = check_against_redshift(tables)
