# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

add_version_sproc_cc_mix = """
call prod.addversion_sproc('PAGE_CC_MIX','SYSTEM BUILD');  
"""
page_cc_mix = """
SELECT "type", cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, mix_rate, composite_key,cast(NULL as date) load_date, 
cast(NULL as varchar(64)) version
FROM stage.page_cc_mix
"""

# COMMAND ----------

final_page_cc_mix = read_redshift_to_df(configs) \
  .option("query",page_cc_mix) \
  .load()

# COMMAND ----------

write_df_to_redshift(configs,final_page_cc_mix, "prod.page_cc_mix", "append", add_version_sproc_cc_mix)

# COMMAND ----------

submit_remote_query(configs, """
UPDATE prod.page_cc_mix
SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'PAGE_CC_MIX'),
version = (SELECT MAX(version) FROM prod.version WHERE record = 'PAGE_CC_MIX');
""")

# COMMAND ----------

add_version_sproc_cc_cartridges = """
call prod.addversion_sproc('PAGE_CC_CARTRIDGES','SYSTEM BUILD');  
"""
page_cc_cartridges = """
SELECT "type", cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, page_cc_mix, demand, yield, page_demand, cartridges, cartridge_volume, demand_scalar, imp, imp_corrected_cartridges, composite_key,cast(NULL as date) load_date, cast(NULL as varchar(64)) version
FROM stage.page_cc_cartridges
"""

# COMMAND ----------

final_page_cc_cartridges = read_redshift_to_df(configs) \
  .option("query",page_cc_cartridges) \
  .load()

# COMMAND ----------

write_df_to_redshift(configs,final_page_cc_cartridges, "prod.page_cc_cartridges", "append", add_version_sproc_cc_cartridges)

# COMMAND ----------

submit_remote_query(configs, """
UPDATE prod.page_cc_cartridges
SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'PAGE_CC_CARTRIDGES'),
version = (SELECT MAX(version) FROM prod.version WHERE record = 'PAGE_CC_CARTRIDGES');
""")

# COMMAND ----------

add_version_sproc_vtc = """
call prod.addversion_sproc('VTC_CARTRIDGES','SYSTEM BUILD');  
"""
vtc = """
SELECT record, cal_date, geography_grain, geography, platform_subset, base_product_number, customer_engagement, cartridges, vol_rate, volume, channel_fill, supplies_spares_crgs, host_crgs, welcome_kits, expected_crgs, vtc, vtc_adjusted_crgs, mvtc, mvtc_adjusted_crgs, vol_count, ma_vol, ma_exp, load_date, version
FROM stage.vtc
"""

# COMMAND ----------

final_vtc = read_redshift_to_df(configs) \
  .option("query",vtc) \
  .load()

# COMMAND ----------

write_df_to_redshift(configs,final_vtc, "prod.vtc_cartridges", "append", add_version_sproc_vtc)

# COMMAND ----------

submit_remote_query(configs, """
UPDATE prod.vtc_cartridges
SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'VTC_CARTRIDGES'),
version = (SELECT MAX(version) FROM prod.version WHERE record = 'VTC_CARTRIDGES');
""")
