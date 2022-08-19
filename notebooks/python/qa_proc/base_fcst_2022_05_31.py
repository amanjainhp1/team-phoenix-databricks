# Databricks notebook source
# MAGIC %md
# MAGIC # Base Forecast QC development 5/31/2022
# MAGIC 
# MAGIC **OBJECTIVE:** Initial development in RS-DEV - count analysis and code improvement to "match" magnitude in SFAI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation
# MAGIC 
# MAGIC Mapping of SFAI processes (with current counts) to RS processes (with RS-DEV counts)
# MAGIC + ie2_prod.dbo.usage_share (14,983,001) --> stage.usage_share_staging (17,185,043)
# MAGIC + cartridge_volumes (598,840) --> stage.cartridge_units (616,499)
# MAGIC + shm_base_helper (106,503) --> stage.shm_base_helper (106,389)
# MAGIC + pages_ccs_mix (10,576,529) --> stage.page_cc_mix (1,496,452)
# MAGIC 
# MAGIC *First pass:*
# MAGIC + page_cc_mix is off; do counts on the individual steps
# MAGIC 
# MAGIC *Second pass:*
# MAGIC + steps
# MAGIC   + page_mix_engine (3,027,816)
# MAGIC   + cc_mix_engine (4,356,786)
# MAGIC   + page_cc_mix_override (756,208)
# MAGIC   + page_mix_complete (748,226)
# MAGIC   + cc_mix_complete (748,226)
# MAGIC   + page_cc_mix (1,496,452)
# MAGIC + issues
# MAGIC   + page_mix_complete and cc_mix_complete look incorrect
# MAGIC + code review, databricks build, counts
# MAGIC   + iter-1
# MAGIC     + refactor of page_mix_engine (3,027,816) :: upper case on model types
# MAGIC     + refactor of page_mix_complete (3,263,969) :: expected magnitude
# MAGIC   + iter-2
# MAGIC     + refactor of cc_mix_engine (4,356,786) :: upper case on model types
# MAGIC     + refactor of cc_mix_complete (4,691,445) :: expected magnitude
# MAGIC   + iter-3
# MAGIC     + page_cc_mix rebuild (7,955,414)
# MAGIC 
# MAGIC *Third pass:*
# MAGIC + review page_cc_cartridge counts
# MAGIC   + before rebuild: 180,084
# MAGIC   + after rebuild (no refactor, only based on update to page_cc_mix rebuild): 6,639,045
# MAGIC   + review source code to determine what is removing records from page_cc_mix
# MAGIC     + refactor SQL - added UPPER and CAST to all join conditions
# MAGIC     + rebuild count - 6,639,045
# MAGIC     
# MAGIC *Fourth pass:*
# MAGIC + review sub-class before / after rebuild
# MAGIC   + analytic: 180,084 / 6,867,391
# MAGIC   + channel_fill: 1,598 / 19,747
# MAGIC   + supplies_spares: 3,720 / 2,039,067
# MAGIC   + host_cartridges: 458,153 / 458,153
# MAGIC   + welcome_kits: 21,328 / 194,338
# MAGIC   + vtc: 561,947 / 7,233,338

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook setup

# COMMAND ----------

# MAGIC %run "../common/configs"

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## First Pass - RS input/output counts

# COMMAND ----------

us_counts = """select count(*) from stage.usage_share_staging"""
crg_units_counts = """select count(*) from stage.cartridge_units"""
shm_counts = """select count(*) from stage.shm_base_helper"""
page_mix_counts = """select count(*) from stage.page_cc_mix"""


# COMMAND ----------

# usage/share counts
us_counts_df = read_redshift_to_df(configs) \
  .option("query", us_counts) \
  .load()

us_counts_df.show()

# COMMAND ----------

# cartridge units counts
crg_units_counts_df = read_redshift_to_df(configs) \
  .option("query", crg_units_counts) \
  .load()

crg_units_counts_df.show()

# COMMAND ----------

shm_counts_df = read_redshift_to_df(configs) \
  .option("query", shm_counts) \
  .load()

shm_counts_df.show()

# COMMAND ----------

page_mix_counts_df = read_redshift_to_df(configs) \
  .option("query", page_mix_counts) \
  .load()

page_mix_counts_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second Pass - RS input/output counts

# COMMAND ----------

pm_eng_counts = """select count(*) from stage.page_mix_engine"""
cc_eng_counts = """select count(*) from stage.cc_mix_engine"""
ovr_counts = """select count(*) from stage.page_cc_mix_override"""
pm_com_counts = """select count(*) from stage.page_mix_complete"""
cc_com_counts = """select count(*) from stage.cc_mix_complete"""

# COMMAND ----------

# page_mix_engine
pm_eng_counts_df = read_redshift_to_df(configs) \
  .option("query", pm_eng_counts) \
  .load()

pm_eng_counts_df.show()

# COMMAND ----------

# cc_mix_engine
cc_eng_counts_df = read_redshift_to_df(configs) \
  .option("query", cc_eng_counts) \
  .load()

cc_eng_counts_df.show()

# COMMAND ----------

# page_cc_mix_override
ovr_counts_df = read_redshift_to_df(configs) \
  .option("query", ovr_counts) \
  .load()

ovr_counts_df.show()

# COMMAND ----------

# page_mix_complete
pm_com_counts_df = read_redshift_to_df(configs) \
  .option("query", pm_com_counts) \
  .load()

pm_com_counts_df.show()

# COMMAND ----------

# cc_mix_complete
cc_com_counts_df = read_redshift_to_df(configs) \
  .option("query", cc_com_counts) \
  .load()

cc_com_counts_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Third Pass - page_cc_cartridges counts

# COMMAND ----------

pc_crgs_counts = """select count(*) from stage.page_cc_cartridges"""

# COMMAND ----------

# page_cc_cartridges
pc_crgs_counts_df = read_redshift_to_df(configs) \
  .option("query", pc_crgs_counts) \
  .load()

pc_crgs_counts_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fourth Pass - page_cc_catridges_adjusts

# COMMAND ----------

ana_counts = """select count(*) from stage.analytic"""
cf_counts = """select count(*) from stage.channel_fill"""
ss_counts = """select count(*) from stage.supplies_spares"""
host_counts = """select count(*) from stage.host_cartridges"""
wk_counts = """select count(*) from stage.welcome_kits"""
vtc_counts = """select count(*) from stage.vtc"""

# COMMAND ----------

# analytic
ana_counts_df = read_redshift_to_df(configs) \
  .option("query", ana_counts) \
  .load()

ana_counts_df.show()

# COMMAND ----------

# channel_fill
cf_counts_df = read_redshift_to_df(configs) \
  .option("query", cf_counts) \
  .load()

cf_counts_df.show()

# COMMAND ----------

# supplies_spares
ss_counts_df = read_redshift_to_df(configs) \
  .option("query", ss_counts) \
  .load()

ss_counts_df.show()

# COMMAND ----------

# host_cartridges
host_counts_df = read_redshift_to_df(configs) \
  .option("query", host_counts) \
  .load()

host_counts_df.show()

# COMMAND ----------

# welcome_kits
wk_counts_df = read_redshift_to_df(configs) \
  .option("query", wk_counts) \
  .load()

wk_counts_df.show()

# COMMAND ----------

# vtc
vtc_counts_df = read_redshift_to_df(configs) \
  .option("query", vtc_counts) \
  .load()

vtc_counts_df.show()
