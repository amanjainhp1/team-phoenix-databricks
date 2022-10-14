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
SELECT "type", cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, mix_rate, composite_key,cast(NULL as date) load_date, 
cast(NULL as varchar(64)) version
FROM stage.page_cc_mix;
"""

page_cc_cartridges_query = """
SELECT "type", cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, page_cc_mix, demand, yield, page_demand, cartridges, cartridge_volume, demand_scalar, imp, imp_corrected_cartridges, composite_key,cast(NULL as date) load_date, cast(NULL as varchar(64)) version
FROM stage.page_cc_cartridges
"""

vtc_query = """
SELECT record, cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, cartridges, vol_rate, volume, channel_fill, supplies_spares_crgs, host_crgs, welcome_kits, expected_crgs, vtc, vtc_adjusted_crgs, mvtc, mvtc_adjusted_crgs, vol_count, ma_vol, ma_exp, load_date, version
FROM stage.vtc
"""

inputs.append(["page_cc_mix", page_cc_mix_query])
inputs.append(["page_cc_cartridges", page_cc_cartridges_query])
inputs.append(["vtc", vtc_query])

read_stage_write_prod(inputs)
