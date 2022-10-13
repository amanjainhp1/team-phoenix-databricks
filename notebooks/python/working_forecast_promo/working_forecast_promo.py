# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

def add_version_and_update_prod_sproc(record: str) -> str:
  f"""
  CALL prod.addversion_sproc('{record}', 'SYSTEM BUILD');

  UPDATE prod.{record.lower()}
  SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = '{record}'),
  version = (SELECT MAX(version) FROM prod.version WHERE record = '{record}');
  """

def read_stage_write_prod(inputs: List):
  for input in inputs:
    df = read_redshift_to_df(configs) \
      .option("query", input[1]) \
      .load()
    
    write_df_to_redshift(configs=configs, df=df, destination=f"prod.{input[0].lower()}", mode="append", postactions=add_version_and_update_table_sproc(f"{input[0].upper()}"))

# COMMAND ----------

inputs = []

working_forecast_query = """
SELECT record, CAST(cal_date AS DATE) cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, cartridges, 
channel_fill, supplies_spares_cartridges, 0 host_cartridges, expected_cartridges, vtc, adjusted_cartridges, CAST(NULL AS DATE) load_date, 
CAST(NULL AS varchar(64)) version
FROM scen.toner_working_fcst
UNION
SELECT record, CAST(cal_date AS DATE) cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, cartridges, 
channel_fill, supplies_spares_cartridges, 0 host_cartridges, expected_cartridges, vtc, adjusted_cartridges, CAST(NULL AS DATE) load_date, 
CAST(NULL as varchar(64)) version
FROM scen.ink_working_fcst
"""

inputs.append(["working_forecast", working_forecast_query])

read_stage_write_prod(inputs)