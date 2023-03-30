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

page_cc_mix_query = """
SELECT "type", cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, mix_rate, cast(NULL as date) load_date, 
cast(NULL as varchar(64)) version
FROM stage.page_cc_mix;
"""

demand_query = """SELECT 'DEMAND' record, cal_date, 'MARKET10' geography_grain, geography, platform_subset, customer_engagement, measure, units,cast(NULL as date) load_date, cast(NULL as varchar(64)) version
FROM stage.demand
"""

cartridge_demand_volumes_query = """select "source", cal_date, geography_grain, geography,base_product_number, k_color, crg_chrome, consumable_type,cast(null as varchar(255)) as capacity, cartridge_volume,cast(NULL as date) load_date, cast(NULL as varchar(64)) version
from stage.cartridge_units
"""

vtc_query = """
SELECT record, cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, cartridges, channel_fill, supplies_spares_crgs, host_crgs, welcome_kits, expected_crgs, mvtc, mvtc_adjusted_crgs, cast(NULL as date) load_date, cast(NULL as varchar(64)) version
FROM stage.vtc
"""

inputs.append(["page_cc_mix", page_cc_mix_query])
inputs.append(["demand", demand_query])
inputs.append(["cartridge_demand_volumes", cartridge_demand_volumes_query])
inputs.append(["cartridge_demand_cartridges", vtc_query])

read_stage_write_prod(inputs)
