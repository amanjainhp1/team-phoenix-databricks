# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Working Forecast Country

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Global Libraries

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

# COMMAND ----------

# MAGIC %run ../config_forecasting_engine

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Bring in Delta Tables for Inputs

# COMMAND ----------

working_forecast_combined = read_redshift_to_df(configs) \
    .option("dbtable", "scen.working_forecast_combined") \
    .load()

# COMMAND ----------

tables = [
    ["scen.working_forecast_combined", working_forecast_combined, "overwrite"]
]

# COMMAND ----------

# MAGIC %run "../common/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Country Forecast

# COMMAND ----------

vtc_02 = spark.sql("""

SELECT add_months(MAX(hw.cal_date), 1) AS hw_forecast_start
    , MAX(sup.supplies_forecast_start) AS supplies_forecast_start
FROM prod.norm_shipments AS hw
CROSS JOIN (
    SELECT add_months(MAX(sup.cal_date), 1) AS supplies_forecast_start
    FROM prod.actuals_supplies AS sup
    WHERE 1=1
        AND sup.official = 1
) AS sup
WHERE 1=1
    AND hw.record = 'ACTUALS - HW'
    AND hw.version = '{}'
""".format(ib_version))

vtc_02.createOrReplaceTempView("c2c_vtc_02_forecast_months")

# COMMAND ----------

spark.sql("""select * from c2c_vtc_02_forecast_months""").show()

# COMMAND ----------

# originally ctrypf_07_c2c_adj_agg
ctry_01 = spark.sql("""
    SELECT geography
        , cal_date
        , platform_subset
        , base_product_number
        , customer_engagement
        , SUM(vtc) as vtc
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
        , vtc
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
            AND xref.official = 1
            AND NOT xref.Crg_Chrome IN ('HEAD', 'UNK')
    """)

ctry_02.createOrReplaceTempView("ctry_02_crg_mapping")

# COMMAND ----------

ctry_lib_01 = spark.sql("""
SELECT geography
  , cal_date
  , platform_subset
  , base_product_number
  , customer_engagement
  , cartridges
  , vtc
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
WHERE 1 = 1
""")

ctry_lib_01.createOrReplaceTempView("ctry_lib_01_c2c_adj_mixes")

# COMMAND ----------

# originally ctrypf_09_adj_hist_mix_1
ctry_03 = spark.sql("""

SELECT DISTINCT c2c.geography
              , acts.country_alpha2
              , c2c.cal_date
              , c2c.platform_subset
              , c2c.base_product_number
              , c2c.customer_engagement
              , c2c.vtc
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
         JOIN ctry_lib_01_c2c_adj_mixes as mix
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
      , hist.vtc
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

with ctrypf_03_historic_ctry_mix_prep as (

        SELECT acts.market10 AS geography
            , acts.country_alpha2
            , acts.platform_subset
            , SUM(acts.base_qty) AS sum_base_qty
        FROM ctry_02_crg_mapping AS acts
        CROSS JOIN c2c_vtc_02_forecast_months AS vtc
        WHERE 1=1
            AND acts.cal_date > add_months(vtc.supplies_forecast_start, -12)
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

ctry_05.createOrReplaceTempView("ctry_05_hist_country_mix")

# COMMAND ----------

ctry_06 = spark.sql("""
with ctrypf_01_filter_vars as (
        SELECT record
            , version
        FROM prod.version
        WHERE 1=1
            AND record = 'IB'
            AND version = '{}'
        UNION ALL
        SELECT DISTINCT record
            , version
        FROM prod.actuals_supplies
        WHERE record = 'ACTUALS - SUPPLIES'
            AND official = 1
    ),  ctrypf_05_ib_ctry_mix_prep as (
    SELECT mk.country_level_2 AS geography
        , ib.country_alpha2
        , ib.cal_date
        , ib.platform_subset
        , ib.units
    FROM prod.ib AS ib
    CROSS JOIN c2c_vtc_02_forecast_months AS fcst
    JOIN ctrypf_01_filter_vars AS vars
        ON vars.version = ib.version
        AND vars.record = 'IB'
    JOIN mdm.iso_cc_rollup_xref AS mk
        ON mk.country_alpha2 = ib.country_alpha2
        AND mk.country_scenario = 'MARKET10'
    WHERE 1=1
        AND ib.measure = 'IB'
        AND ib.cal_date >= fcst.supplies_forecast_start
    )SELECT ib.geography
        , ib.country_alpha2
        , ib.cal_date
        , ib.platform_subset
        , SUM(ib.units) OVER (PARTITION BY ib.geography, ib.country_alpha2, ib.cal_date, ib.platform_subset) * 1.0 /
            NULLIF(SUM(ib.units) OVER (PARTITION BY ib.geography, ib.cal_date, ib.platform_subset), 0) AS ctry_pfs_ib_mix
    FROM ctrypf_05_ib_ctry_mix_prep AS ib
    
""".format(ib_version))

ctry_06.createOrReplaceTempView("ctry_06_ib_country_mix")

# COMMAND ----------

ctry_lib_02 = spark.sql("""

SELECT DISTINCT c2c.geography
    , c2c.platform_subset
    , CASE WHEN NOT (hist.platform_subset IS NULL OR hist2.platform_subset IS NULL OR ib.platform_subset IS NULL) THEN 1
           WHEN NOT (hist.platform_subset IS NULL OR ib.platform_subset IS NULL) THEN 1
           WHEN (hist.platform_subset IS NULL AND NOT hist2.platform_subset IS NULL) THEN 2
           WHEN (hist.platform_subset IS NULL AND hist2.platform_subset IS NULL) THEN 3 ELSE 0 END AS combo_mapping
FROM ctry_01_c2c_adj_agg AS c2c
CROSS JOIN c2c_vtc_02_forecast_months AS fcst
JOIN mdm.hardware_xref AS hw
    ON hw.platform_subset = c2c.platform_subset
LEFT JOIN (SELECT DISTINCT geography, platform_subset FROM ctry_05_hist_country_mix) AS hist
    ON hist.geography = c2c.geography
    AND hist.platform_subset = c2c.platform_subset
LEFT JOIN (SELECT DISTINCT geography, platform_subset FROM ctry_05_hist_country_mix) AS hist2
    ON hist2.geography = c2c.geography
    AND hist2.platform_subset = hw.predecessor
LEFT JOIN (SELECT DISTINCT geography, platform_subset FROM ctry_06_ib_country_mix) AS ib
    ON ib.geography = c2c.geography
    AND ib.platform_subset = c2c.platform_subset
WHERE 1=1
    AND c2c.cal_date >= fcst.supplies_forecast_start
    AND hw.technology IN ('LASER','INK')
""")

ctry_lib_02.createOrReplaceTempView("ctry_lib_02_adj_combo_mappings")

# COMMAND ----------

ctry_07 = spark.sql("""

SELECT 1 AS mix_type
    , c2c.geography
    , hist.country_alpha2 AS country_alpha2
    , c2c.cal_date
    , c2c.platform_subset
    , c2c.base_product_number
    , c2c.customer_engagement
    , c2c.vtc
    , c2c.cartridges
    , c2c.mvtc_adjusted_crgs
    -- mixes
    , hist.ctry_pfs_qty_mix AS ctry_pfs_qty_mix
FROM ctry_01_c2c_adj_agg AS c2c
CROSS JOIN c2c_vtc_02_forecast_months AS fcst
JOIN ctry_lib_02_adj_combo_mappings AS combo
    ON combo.geography = c2c.geography
    AND combo.platform_subset = c2c.platform_subset
    AND combo.combo_mapping = 1
JOIN ctry_05_hist_country_mix AS hist
    ON hist.geography = c2c.geography
    AND hist.platform_subset = c2c.platform_subset
WHERE 1=1
    AND c2c.cal_date >= fcst.supplies_forecast_start
""")

ctry_07.createOrReplaceTempView("ctry_07_adj_fcst_mix_type_1")

# COMMAND ----------

ctry_08 = spark.sql("""

SELECT 2 AS mix_type
    , c2c.geography
    , hist.country_alpha2 AS country_alpha2
    , c2c.cal_date
    , c2c.platform_subset
    , c2c.base_product_number
    , c2c.customer_engagement
    , c2c.vtc
    , c2c.cartridges
    , c2c.mvtc_adjusted_crgs
    -- mixes
    , hist.ctry_pfs_qty_mix AS ctry_pfs_qty_mix
FROM ctry_01_c2c_adj_agg AS c2c
CROSS JOIN c2c_vtc_02_forecast_months AS fcst
JOIN ctry_lib_02_adj_combo_mappings AS combo
    ON combo.geography = c2c.geography
    AND combo.platform_subset = c2c.platform_subset
    AND combo.combo_mapping = 2
JOIN mdm.hardware_xref AS hw
    ON hw.platform_subset = c2c.platform_subset
JOIN ctry_05_hist_country_mix AS hist
    ON hist.geography = c2c.geography
    AND hist.platform_subset = hw.predecessor
WHERE 1=1
    AND c2c.cal_date >= fcst.supplies_forecast_start
    AND hw.technology IN ('LASER','INK','PWA')
""")

ctry_08.createOrReplaceTempView("ctry_08_adj_fcst_mix_type_2")

# COMMAND ----------

ctry_09 = spark.sql("""

SELECT 3 AS model_number
    , c2c.geography
    , ib.country_alpha2 AS country_alpha2
    , c2c.cal_date
    , c2c.platform_subset
    , c2c.base_product_number
    , c2c.customer_engagement
    , c2c.vtc
    , c2c.cartridges
    , c2c.mvtc_adjusted_crgs
    -- mixes
    , ib.ctry_pfs_ib_mix AS ctry_pfs_qty_mix
FROM ctry_01_c2c_adj_agg AS c2c
CROSS JOIN c2c_vtc_02_forecast_months AS fcst
JOIN ctry_lib_02_adj_combo_mappings AS combo
    ON combo.geography = c2c.geography
    AND combo.platform_subset = c2c.platform_subset
    AND combo.combo_mapping = 3
JOIN ctry_06_ib_country_mix AS ib
    ON ib.geography = c2c.geography
    AND ib.platform_subset = c2c.platform_subset
    AND ib.cal_date = c2c.cal_date
WHERE 1=1
    AND c2c.cal_date >= fcst.supplies_forecast_start
""")

ctry_09.createOrReplaceTempView("ctry_09_adj_fcst_mix_type_3")

# COMMAND ----------

ctry_10 = spark.sql("""

SELECT c2c.geography
    , c2c.country_alpha2
    , c2c.cal_date
    , c2c.platform_subset
    , c2c.base_product_number
    , c2c.customer_engagement
    , c2c.vtc
    , c2c.cartridges
    , c2c.mvtc_adjusted_crgs
    -- mixes
    , c2c.ctry_pfs_qty_mix
    , COALESCE(mix.geo_pfs_bpn_ce_crg_mix, 1) AS geo_pfs_bpn_ce_crg_mix
    , COALESCE(mix.geo_pfs_bpn_ce_mvtc_mix, 1) AS geo_pfs_bpn_ce_mvtc_mix
    -- calculated fields
    , SUM(c2c.cartridges * c2c.ctry_pfs_qty_mix) OVER
        (PARTITION BY c2c.country_alpha2, c2c.cal_date, c2c.platform_subset, c2c.base_product_number) AS ctry_crgs
    , SUM(c2c.mvtc_adjusted_crgs * c2c.ctry_pfs_qty_mix) OVER
        (PARTITION BY c2c.country_alpha2, c2c.cal_date, c2c.platform_subset, c2c.base_product_number) AS ctry_mvtc_crgs
FROM ctry_07_adj_fcst_mix_type_1 AS c2c
JOIN ctry_lib_01_c2c_adj_mixes as mix
    ON mix.geography = c2c.geography
    AND mix.cal_date = c2c.cal_date
    AND mix.platform_subset = c2c.platform_subset
    AND mix.base_product_number = c2c.base_product_number
    AND mix.customer_engagement = c2c.customer_engagement
WHERE 1=1
""")

ctry_10.createOrReplaceTempView("ctry_10_adj_fcst_ctry_mix_type_1_prep")

# COMMAND ----------

ctry_11 = spark.sql("""

SELECT c2c.geography
    , c2c.country_alpha2
    , c2c.cal_date
    , c2c.platform_subset
    , c2c.base_product_number
    , c2c.customer_engagement
    , c2c.vtc
    , c2c.cartridges
    , c2c.mvtc_adjusted_crgs
    -- mixes
    , c2c.ctry_pfs_qty_mix
    , COALESCE(mix.geo_pfs_bpn_ce_crg_mix, 1) AS geo_pfs_bpn_ce_crg_mix
    , COALESCE(mix.geo_pfs_bpn_ce_mvtc_mix, 1) AS geo_pfs_bpn_ce_mvtc_mix
    -- calculated fields
    , SUM(c2c.cartridges * c2c.ctry_pfs_qty_mix) OVER
        (PARTITION BY c2c.country_alpha2, c2c.cal_date, c2c.platform_subset, c2c.base_product_number) AS ctry_crgs
    , SUM(c2c.mvtc_adjusted_crgs * c2c.ctry_pfs_qty_mix) OVER
        (PARTITION BY c2c.country_alpha2, c2c.cal_date, c2c.platform_subset, c2c.base_product_number) AS ctry_mvtc_crgs
FROM ctry_08_adj_fcst_mix_type_2 AS c2c
JOIN ctry_lib_01_c2c_adj_mixes as mix
    ON mix.geography = c2c.geography
    AND mix.cal_date = c2c.cal_date
    AND mix.platform_subset = c2c.platform_subset
    AND mix.base_product_number = c2c.base_product_number
    AND mix.customer_engagement = c2c.customer_engagement
WHERE 1=1
""")

ctry_11.createOrReplaceTempView("ctry_11_adj_fcst_ctry_mix_type_2_prep")

# COMMAND ----------

ctry_12 = spark.sql("""
    SELECT c2c.geography
        , c2c.country_alpha2
        , c2c.cal_date
        , c2c.platform_subset
        , c2c.base_product_number
        , c2c.customer_engagement
        , c2c.vtc
        , c2c.cartridges
        , c2c.mvtc_adjusted_crgs
        -- mixes
        , c2c.ctry_pfs_qty_mix
        , COALESCE(mix.geo_pfs_bpn_ce_crg_mix, 1) AS geo_pfs_bpn_ce_crg_mix
        , COALESCE(mix.geo_pfs_bpn_ce_mvtc_mix, 1) AS geo_pfs_bpn_ce_mvtc_mix
        -- calculated fields
        , SUM(c2c.cartridges * c2c.ctry_pfs_qty_mix) OVER
            (PARTITION BY c2c.country_alpha2, c2c.cal_date, c2c.platform_subset, c2c.base_product_number) AS ctry_crgs
        , SUM(c2c.mvtc_adjusted_crgs * c2c.ctry_pfs_qty_mix) OVER
            (PARTITION BY c2c.country_alpha2, c2c.cal_date, c2c.platform_subset, c2c.base_product_number) AS ctry_mvtc_crgs
    FROM ctry_09_adj_fcst_mix_type_3 AS c2c
    JOIN ctry_lib_01_c2c_adj_mixes as mix
        ON mix.geography = c2c.geography
        AND mix.cal_date = c2c.cal_date
        AND mix.platform_subset = c2c.platform_subset
        AND mix.base_product_number = c2c.base_product_number
        AND mix.customer_engagement = c2c.customer_engagement
    WHERE 1=1
""")

ctry_12.createOrReplaceTempView("ctry_12_adj_fcst_ctry_mix_type_3_prep")

# COMMAND ----------

ctry_13 = spark.sql("""

SELECT 1 AS combo_mapping
    , fcst.geography
    , fcst.country_alpha2
    , fcst.cal_date
    , fcst.platform_subset
    , fcst.base_product_number
    , fcst.customer_engagement
    , fcst.vtc
    , fcst.cartridges
    , fcst.mvtc_adjusted_crgs
    -- mixes
    , fcst.ctry_pfs_qty_mix
    , fcst.geo_pfs_bpn_ce_crg_mix
    , fcst.geo_pfs_bpn_ce_mvtc_mix
    -- calculated fields
    , fcst.ctry_crgs
    , fcst.ctry_mvtc_crgs
    , fcst.ctry_crgs * fcst.geo_pfs_bpn_ce_crg_mix AS ctry_ce_crgs
    , fcst.ctry_mvtc_crgs * fcst.geo_pfs_bpn_ce_mvtc_mix AS ctry_ce_mvtc_crgs
FROM ctry_10_adj_fcst_ctry_mix_type_1_prep AS fcst

UNION ALL

SELECT 2 AS combo_mapping
    , fcst.geography
    , fcst.country_alpha2
    , fcst.cal_date
    , fcst.platform_subset
    , fcst.base_product_number
    , fcst.customer_engagement
    , fcst.vtc
    , fcst.cartridges
    , fcst.mvtc_adjusted_crgs
    -- mixes
    , fcst.ctry_pfs_qty_mix
    , fcst.geo_pfs_bpn_ce_crg_mix
    , fcst.geo_pfs_bpn_ce_mvtc_mix
    -- calculated fields
    , fcst.ctry_crgs
    , fcst.ctry_mvtc_crgs
    , fcst.ctry_crgs * fcst.geo_pfs_bpn_ce_crg_mix AS ctry_ce_crgs
    , fcst.ctry_mvtc_crgs * fcst.geo_pfs_bpn_ce_mvtc_mix AS ctry_ce_mvtc_crgs
FROM ctry_11_adj_fcst_ctry_mix_type_2_prep AS fcst

UNION ALL

SELECT 3 AS combo_mapping
    , fcst.geography
    , fcst.country_alpha2
    , fcst.cal_date
    , fcst.platform_subset
    , fcst.base_product_number
    , fcst.customer_engagement
    , fcst.vtc
    , fcst.cartridges
    , fcst.mvtc_adjusted_crgs
    -- mixes
    , fcst.ctry_pfs_qty_mix
    , fcst.geo_pfs_bpn_ce_crg_mix
    , fcst.geo_pfs_bpn_ce_mvtc_mix
    -- calculated fields
    , fcst.ctry_crgs
    , fcst.ctry_mvtc_crgs
    , fcst.ctry_crgs * fcst.geo_pfs_bpn_ce_crg_mix AS ctry_ce_crgs
    , fcst.ctry_mvtc_crgs * fcst.geo_pfs_bpn_ce_mvtc_mix AS ctry_ce_mvtc_crgs
FROM ctry_12_adj_fcst_ctry_mix_type_3_prep AS fcst
""")

ctry_13.createOrReplaceTempView("ctry_13_adj_fcst_ctry_mix")

# COMMAND ----------

ctry_14 = spark.sql("""

SELECT acts.geography
    , acts.country_alpha2
    , acts.cal_date
    , acts.platform_subset
    , acts.base_product_number
    , acts.customer_engagement
    , acts.vtc
    , acts.ctry_ce_crgs AS cartridges
    , acts.ctry_ce_mvtc_crgs AS mvtc_adjusted_crgs
FROM ctry_04_adj_hist_mix_2 AS acts

UNION ALL

SELECT fcst.geography
    , fcst.country_alpha2
    , fcst.cal_date
    , fcst.platform_subset
    , fcst.base_product_number
    , fcst.customer_engagement
    , fcst.vtc
    , fcst.ctry_ce_crgs AS cartridges
    , fcst.ctry_ce_mvtc_crgs AS mvtc_adjusted_crgs
FROM ctry_13_adj_fcst_ctry_mix AS fcst
""")

ctry_14.createOrReplaceTempView("c2c_adj_country_pf_split")

# COMMAND ----------

spark.sql("""select count(*) from c2c_adj_country_pf_split""").show()

# COMMAND ----------

write_df_to_redshift(configs, ctry_14, "scen.working_forecast_country", "overwrite")

# COMMAND ----------


