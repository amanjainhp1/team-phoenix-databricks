# Databricks notebook source
from typing import List

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# create empty widgets for interactive sessions
dbutils.widgets.text('technology', '')

# COMMAND ----------

# retrieve widget values and assign to variables
technology = dbutils.widgets.get('technology').lower()

# for labelling tables, laser/toner = toner, ink = ink
technology_label = ''
if technology == 'ink':
    technology_label = 'ink'
elif technology == 'laser':
    technology_label = 'toner'

# COMMAND ----------

def add_version_and_update_table_sproc(record: str) -> str:
  f"""
  CALL prod.addversion_sproc('{record}', 'SYSTEM BUILD');

  UPDATE prod.{record.lower()}
  SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = '{record}'),
  version = (SELECT MAX(version) FROM prod.version WHERE record = '{record}');
  """

def read_stage_write_prod(inputs: List[List[str]]):
  for input in inputs:
    df = read_redshift_to_df(configs) \
      .option("query", input[1]) \
      .load()
    write_df_to_redshift(configs=configs, df=df, destination=f"prod.{input[0].lower()}", mode="append", postactions=add_version_and_update_table_sproc(f"{input[0]}_{technology_label}".upper()))

# COMMAND ----------

inputs = []

working_forecast_query = f"""
SELECT 
    record
    , CAST(cal_date AS DATE) cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , cartridges
    , channel_fill
    , supplies_spares_cartridges
    ,{'0 AS' if technology == 'laser' else ''} host_cartridges
    ,{'0 AS' if technology == 'laser' else ''} welcome_kits
    , expected_cartridges
    , vtc
    , adjusted_cartridges
    , CAST(NULL AS DATE) load_date
    , CAST(NULL AS varchar(64)) version
FROM scen.{technology_label}_working_fcst
"""

inputs.append(["working_forecast", working_forecast_query])

read_stage_write_prod(inputs)
