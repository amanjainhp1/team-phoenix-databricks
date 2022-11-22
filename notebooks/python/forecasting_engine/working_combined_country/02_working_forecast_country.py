# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Working Forecast Country

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## GLobal Variables

# COMMAND ----------

ib_version = '2022.11.17.3'

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Bring in Delta Tables for Inputs

# COMMAND ----------

# tables to stream line:
    # prod.actuals_supplies

actuals_supplies = read_redshift_to_df(configs) \
    .option("dbtable", "prod.actuals_supplies") \
    .load()

iso_cc_rollup_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_cc_rollup_xref") \
    .load()

norm_shipments = read_redshift_to_df(configs) \
    .option("dbtable", "prod.norm_shipments") \
    .load()

shm_base_helper = read_redshift_to_df(configs) \
    .option("dbtable", "stage.shm_base_helper") \
    .load()

supplies_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.supplies_xref") \
    .load()

working_forecast_combined = read_redshift_to_df(configs) \
    .option("dbtable", "scen.working_forecast_combined") \
    .load()

# COMMAND ----------

tables = [
    ["prod.actuals_supplies", actuals_supplies, "overwrite"],
    ["mdm.iso_cc_rollup_xref", iso_cc_rollup_xref, "overwrite"],
    ["prod.norm_shipments", norm_shipments, "overwrite"],
    ["stage.shm_base_helper", shm_base_helper, "overwrite"],
    ["mdm.supplies_xref", supplies_xref, "overwrite"],
    ["scen.working_forecast_combined", working_forecast_combined, "overwrite"]
]

# COMMAND ----------

# MAGIC %run "../../finance_etl/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Country Forecast

# COMMAND ----------

# originally ctrypf_07_c2c_adj_agg
ctry_01 = spark.sql("""
    SELECT geography
        , cal_date
        , platform_subset
        , base_product_number
        , customer_engagement
        , SUM(cartridges) AS cartridges
        , SUM(adjusted_cartridges) AS mvtc_adjusted_crgs
    FROM scen.working_forecast_combined
    WHERE 1=1
        AND geography_grain = 'MARKET10'
    GROUP BY geography
        , cal_date
        , platform_subset
        , base_product_number
        , customer_engagement
""")

ctry_01.createOrReplaceTempView("ctry_01_c2c_adj_agg")

# COMMAND ----------

ctry_02 = spark.sql("""

    with vol_2_crg_mapping as (

        SELECT DISTINCT base_product_number
            , geography
        FROM stage.shm_base_helper
        WHERE 1=1
    )-- negative values in source table
        SELECT DISTINCT acts.market10
            , acts.country_alpha2
            , acts.cal_date
            , acts.platform_subset
            , acts.base_product_number
            , SUM(acts.base_quantity) OVER (PARTITION BY acts.cal_date, acts.country_alpha2, acts.base_product_number) AS base_qty
            -- used for history
            , SUM(acts.base_quantity) OVER (PARTITION BY acts.cal_date, acts.country_alpha2, acts.base_product_number) * 1.0 /
                NULLIF(ROUND(SUM(acts.base_quantity) OVER (PARTITION BY acts.cal_date, acts.market10, acts.base_product_number), 7), 0.0) AS ctry_bpn_qty_mix
            -- used for forecast
            , SUM(acts.base_quantity) OVER (PARTITION BY acts.cal_date, acts.country_alpha2, acts.platform_subset, acts.base_product_number) * 1.0 /
                NULLIF(ROUND(SUM(acts.base_quantity) OVER (PARTITION BY acts.cal_date, acts.market10, acts.platform_subset, acts.base_product_number), 7), 0.0) AS ctry_pfs_bpn_qty_mix
        FROM prod.actuals_supplies AS acts
        JOIN vol_2_crg_mapping AS map
            ON map.base_product_number = acts.base_product_number
        JOIN mdm.supplies_xref AS xref
            ON xref.base_product_number = acts.base_product_number
        JOIN mdm.iso_cc_rollup_xref AS cref
            ON cref.country_alpha2 = acts.country_alpha2
            AND cref.country_scenario = 'MARKET10'
        WHERE 1=1
            AND acts.customer_engagement IN ('EST_INDIRECT_FULFILLMENT', 'I-INK', 'TRAD')
            AND xref.active = 1
            AND NOT xref.Crg_Chrome IN ('HEAD', 'UNK')
    """)

ctry_02.createOrReplaceTempView("ctry_02_crg_mapping")

# COMMAND ----------

# originally ctrypf_09_adj_hist_mix_1
ctry_03 = spark.sql("""
with c2c_vtc_02_forecast_months as (SELECT DATEADD(MONTH, 1, MAX(hw.cal_date)) AS hw_forecast_start
                                                     , MAX(sup.supplies_forecast_start)    AS supplies_forecast_start
                                                FROM prod.norm_shipments AS hw
                                                         CROSS JOIN (SELECT DATEADD(MONTH, 1, MAX(sup.cal_date)) AS supplies_forecast_start
                                                                     FROM mdm.actuals_supplies AS sup
                                                                     WHERE 1 = 1
                                                                       AND sup.official = 1) AS sup
                                                WHERE 1 = 1
                                                  AND hw.record = 'ACTUALS - HW'
                                                  AND hw.version = '{ib_version}'),
     ctrypf_08_c2c_adj_mixes as (SELECT geography
                                                  , cal_date
                                                  , platform_subset
                                                  , base_product_number
                                                  , customer_engagement
                                                  , cartridges
                                                  , mvtc_adjusted_crgs
                                                  , SUM(cartridges)
                                                    OVER (PARTITION BY geography, cal_date, platform_subset, base_product_number, customer_engagement) *
                                                    1.0 /
                                                    NULLIF(SUM(cartridges)
                                                           OVER (PARTITION BY geography, cal_date, platform_subset, base_product_number),
                                                           0.0) AS geo_pfs_bpn_ce_crg_mix
                                                  , SUM(mvtc_adjusted_crgs)
                                                    OVER (PARTITION BY geography, cal_date, platform_subset, base_product_number, customer_engagement) *
                                                    1.0 /
                                                    NULLIF(SUM(mvtc_adjusted_crgs)
                                                           OVER (PARTITION BY geography, cal_date, platform_subset, base_product_number),
                                                           0.0) AS geo_pfs_bpn_ce_mvtc_mix
                                             FROM ctry_01_c2c_adj_agg
                                             WHERE 1 = 1)
SELECT DISTINCT c2c.geography
              , acts.country_alpha2
              , c2c.cal_date
              , c2c.platform_subset
              , c2c.base_product_number
              , c2c.customer_engagement
              , c2c.cartridges
              , c2c.mvtc_adjusted_crgs
              -- mixes
              , acts.ctry_bpn_qty_mix
              , mix.geo_pfs_bpn_ce_crg_mix
              , mix.geo_pfs_bpn_ce_mvtc_mix
              -- calculated fields
              , c2c.cartridges * acts.ctry_bpn_qty_mix         as ctry_crgs
              , c2c.mvtc_adjusted_crgs * acts.ctry_bpn_qty_mix as ctry_mvtc_crgs
FROM ctry_01_c2c_adj_agg AS c2c
         CROSS JOIN c2c_vtc_02_forecast_months AS fcst
         JOIN ctry_02_crg_mapping AS acts
              ON acts.market10 = c2c.geography
                  AND acts.cal_date = c2c.cal_date
                  AND acts.base_product_number = c2c.base_product_number
         JOIN ctrypf_08_c2c_adj_mixes as mix
              ON mix.geography = c2c.geography
                  AND mix.cal_date = c2c.cal_date
                  AND mix.platform_subset = c2c.platform_subset
                  AND mix.base_product_number = c2c.base_product_number
                  AND mix.customer_engagement = c2c.customer_engagement
WHERE 1 = 1
  AND c2c.cal_date < fcst.supplies_forecast_start
""")

ctry_03.createOrReplaceTempView("ctry_03_adj_hist_mix_1")

# COMMAND ----------

ctry_04 = spark.sql("""
SELECT DISTINCT hist.geography
    , hist.country_alpha2
    , hist.cal_date
    , hist.platform_subset
    , hist.base_product_number
    , hist.customer_engagement
    , hist.cartridges
    , hist.mvtc_adjusted_crgs
    -- mixes
    , hist.ctry_bpn_qty_mix
    , hist.geo_pfs_bpn_ce_crg_mix
    , hist.geo_pfs_bpn_ce_mvtc_mix
    -- calculated fields
    , hist.ctry_crgs
    , hist.ctry_mvtc_crgs
    , hist.ctry_crgs * hist.geo_pfs_bpn_ce_crg_mix AS ctry_ce_crgs
    , hist.ctry_mvtc_crgs * hist.geo_pfs_bpn_ce_mvtc_mix AS ctry_ce_mvtc_crgs
FROM ctry_03_adj_hist_mix_1 AS hist
""")

ctry_04.createOrReplaceTempView("ctry_04_adj_hist_mix_2")

# COMMAND ----------

ctry_05 = spark.sql("""

    with c2c_vtc_02_forecast_months as (


        SELECT DATEADD(MONTH, 1, MAX(hw.cal_date)) AS hw_forecast_start
            , MAX(sup.supplies_forecast_start) AS supplies_forecast_start
        FROM "IE2_Prod"."dbo"."norm_shipments" AS hw
        CROSS JOIN (
            SELECT DATEADD(MONTH, 1, MAX(sup.cal_date)) AS supplies_forecast_start
            FROM mdm.actuals_supplies AS sup
            WHERE 1=1
                AND sup.official = 1
        ) AS sup
        WHERE 1=1
            AND hw.record = 'ACTUALS - HW'
            AND hw.version = '{ib_version}'
        ),  ctrypf_03_historic_ctry_mix_prep as (

            SELECT acts.market10 AS geography
                , acts.country_alpha2
                , acts.platform_subset
                , SUM(acts.base_qty) AS sum_base_qty
            FROM "IE2_Staging"."dbt"."ctrypf_02_acts_ctry_mix" AS acts
            CROSS JOIN c2c_vtc_02_forecast_months AS vtc
            WHERE 1=1
                AND acts.cal_date > DATEADD(MONTH, -12, vtc.supplies_forecast_start)
            GROUP BY acts.market10
                , acts.country_alpha2
                , acts.platform_subset
        )
            SELECT acts.geography
                , acts.country_alpha2
                , acts.platform_subset
                , acts.sum_base_qty * 1.0 /
                    NULLIF(SUM(acts.sum_base_qty) OVER (PARTITION BY acts.platform_subset, acts.geography), 0) AS ctry_pfs_qty_mix
            FROM ctrypf_03_historic_ctry_mix_prep AS acts
""")

ctry_05.createOrReplaceTempView("ctry_05_adj_hist_country_mix")
