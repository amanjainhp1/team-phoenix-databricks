# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Trade Forecast Promotion

# COMMAND ----------

# MAGIC %run ../config_forecasting_engine

# COMMAND ----------

record = 'TONER_PIVOTS'
addversion_info = call_redshift_addversion_sproc(configs, record, 'SYSTEM BUILD')

# COMMAND ----------

toner_pivots_promo = f"""
SELECT 
    '{record}' AS version_record
    , tp.record_type
    , tp.record
    , tp.cycle
    , tp.begin_cycle_date
    , tp.period_dt
    , tp.month
    , tp.fiscal_year_qtr
    , tp.fiscal_yr
    , tp.calendar_yr_qtr
    , tp.calendar_yr
    , tp.market10
    , tp.region_5
    , tp.platform_subset
    , tp.base_prod_name
    , tp.base_prod_number
    , tp.customer_engagement
    , tp.yield
    , tp.hw_pl
    , tp.business_feature
    , tp.hw_product_family
    , tp.sf_mf
    , tp.format
    , tp.mono_color_devices
    , tp.product_structure
    , tp.vc_category
    , tp.supplies_pl
    , tp.crg_pl_name
    , tp.crg_category
    , tp.crg_business
    , tp.cartridge_alias
    , tp.cartridge_type
    , tp.cartridge_size
    , tp.single_multi
    , tp.crg_chrome
    , tp.crg_intro_dt
    , tp.trans_vs_contract
    , tp.p2j_identifier
    , tp.hw_fc_units
    , tp.ib_units
    , tp.trd_units_w
    , tp.pmf_units
    , tp.pmf_dollars
    , tp.expected_crgs_w
    , tp.spares_w
    , tp.channel_fill_w
    , tp.equiv_units_w
    , tp.vtc_w
    , tp.pgswmktshr_blackonly
    , tp.pgswomktshr_blackonly
    , tp.pgswmktshr_color
    , tp.pgswomktshr_color
    , tp.hp_crg_sz
    , tp.fiji_usd
    , tp.discount_pcnt
    , tp.gross_rev_w
    , tp.net_rev_w
    , tp.pgswmktshr
    , tp.pgswomktshr
    , tp.fiji_k_mpv
    , tp.fiji_mkt_shr
    , tp.supplies_base_qty
    , tp.supplies_equivalent_units
    , tp.wampv_k_mpv
    , tp.wampv_ib_units
    , '{addversion_info[1]}' AS load_date
    , '{addversion_info[0]}' AS version
    , tp.rev_units_nt
    , tp.equiv_units_nt
    , tp.adjusted_pages
    , tp.expected_pages
    , tp.hp_sell_in_pages_kcmy
    , tp.hp_sell_in_pages_k_only
    , tp.net_rev_trade
    , tp.revenue_nt
FROM stage.toner_pivots_data_source AS tp
"""

df_toner_pivots_promo = read_redshift_to_df(configs).option("query", toner_pivots_promo).load()

# COMMAND ----------

write_df_to_redshift(configs, df_toner_pivots_promo, "prod.toner_pivots", "append")
