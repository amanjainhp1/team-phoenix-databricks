# Databricks notebook source
# MAGIC %run ../common/configs 

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

toner_pivots = read_redshift_to_df(configs) \
    .option("query", "select  record_type\
        , record\
        , cycle\
        , begin_cycle_date\
        , period_dt\
        , month\
        , fiscal_year_qtr\
        , fiscal_yr\
        , calendar_yr_qtr\
        , calendar_yr\
        , market10\
        , region_5\
        , platform_subset\
        , base_prod_name\
        , base_prod_number\
        , customer_engagement\
        , yield\
        , hw_pl\
        , business_feature\
        , hw_product_family\
        , sf_mf\
        , format\
        , mono_color_devices\
        , product_structure\
        , vc_category\
        , supplies_pl\
        , crg_pl_name\
        , crg_category\
        , crg_business\
        , cartridge_alias\
        , cartridge_type\
        , cartridge_size\
        , single_multi\
        , crg_chrome\
        , crg_intro_dt\
        , trans_vs_contract\
        , p2j_identifier\
        , hw_fc_units\
        , ib_units\
        , trd_units_w\
        , pmf_units\
        , pmf_dollars\
        , expected_crgs_w\
        , spares_w\
        , channel_fill_w\
        , equiv_units_w\
        , vtc_w\
        , rev_units_nt\
        , equiv_units_nt\
        , pgswmktshr_blackonly\
        , pgswomktshr_blackonly\
        , pgswmktshr_color\
        , pgswomktshr_color\
        , hp_crg_sz\
        , fiji_usd\
        , discount_pcnt\
        , gross_rev_w\
        , net_rev_w\
        , pgswmktshr\
        , pgswomktshr\
        , fiji_k_mpv\
        , fiji_mkt_shr\
        , supplies_base_qty\
        , supplies_equivalent_units\
        , wampv_k_mpv\
        , wampv_ib_units\
        , load_date\
        , version\
        , adjusted_pages\
        , expected_pages\
        , hp_sell_in_pages_kcmy\
        , hp_sell_in_pages_k_only\
        , net_rev_trade\
      from prod.toner_pivots where version = '2023.04.13.1'")\
    .load()

toner_pivots.createOrReplaceTempView("toner_pivots")

# COMMAND ----------

tables = [
    ['IE2_Prod.dbo.pivots_toner', toner_pivots, "append"]
]

# COMMAND ----------

for t_name, df, mode in tables:
    write_df_to_sqlserver(configs, df, t_name, mode)

# COMMAND ----------


