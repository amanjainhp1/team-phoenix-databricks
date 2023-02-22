# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Trade Forecast Promotion

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

# COMMAND ----------

add_version_inputs = [
    ['TONER_PIVOTS', 'TONER PIVOTS']
]

for input in add_version_inputs:
    call_redshift_addversion_sproc(configs, input[0], input[1])

# COMMAND ----------

toner_pivots_data_source = read_redshift_to_df(configs) \
    .option("dbtable", "stage.toner_pivots_data_source") \
    .load()

version = read_redshift_to_df(configs) \
    .option("dbtable", "prod.version") \
    .load()

tables = [
    ['stage.toner_pivots_data_source', toner_pivots_data_source, "overwrite"],
    ['prod.version', version, "overwrite"]
]

# COMMAND ----------

# MAGIC %run "../../finance_etl/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

toner_pivots_promo = spark.sql("""

with pivots_promo_01_filter_vars as (
    SELECT record
        , version
        , source_name
        , load_date
        , official
    FROM prod.version
    WHERE record in ('TONER_PIVOTS')
        AND version = (SELECT MAX(version) FROM prod.version WHERE record IN ('TONER_PIVOTS'))
)SELECT vars.record AS version_record
    -- filters
    , record_type
    , tp.record
    , cycle
    , begin_cycle_date
    , period_dt

    -- dates
    , month
    , fiscal_year_qtr
    , fiscal_yr
    , calendar_yr_qtr
    , calendar_yr

    -- geography
    , market10
    , region_5

    -- product
    , platform_subset
    , base_prod_name
    , base_prod_number
    , customer_engagement
    , yield

    -- hardware dimensions
    , hw_pl
    , business_feature
    , hw_product_family
    , sf_mf
    , format
    , mono_color_devices
    , product_structure
    , vc_category

    -- supplies dimensions
    , supplies_pl
    , crg_pl_name
    , crg_category
    , crg_business
    , cartridge_alias
    , cartridge_type
    , cartridge_size
    , single_multi
    , crg_chrome
    , crg_intro_dt
    , trans_vs_contract
    , p2j_identifier

    -- calculations
    , hw_fc_units
    , ib_units
    , trd_units_w
    , pmf_units
    , pmf_dollars
    , expected_crgs_w
    , spares_w
    , channel_fill_w
    , equiv_units_w
    , vtc_w
    , pgswmktshr_blackonly
    , pgswomktshr_blackonly
    , pgswmktshr_color
    , pgswomktshr_color
    , hp_crg_sz
    , fiji_usd
    , discount_pcnt
    , rpp_gross_rev
    , rpp_net_rev_w
    , pgswmktshr
    , pgswomktshr
    , fiji_k_mpv
    , fiji_mkt_shr
    , supplies_base_qty
    , supplies_equivalent_units
    , wampv_k_mpv
    , wampv_ib_units

    , vars.load_date
    , vars.version

FROM stage.toner_pivots_data_source AS tp
CROSS JOIN pivots_promo_01_filter_vars AS vars
WHERE 1=1
    AND vars.record = 'TONER_PIVOTS'
""")

write_df_to_redshift(configs, toner_pivots_promo, "prod.toner_pivots", "append")

# COMMAND ----------


