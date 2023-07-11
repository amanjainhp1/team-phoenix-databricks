# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Toner Pivots

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Common Libraries

# COMMAND ----------

# MAGIC %run ../config_forecasting_engine

# COMMAND ----------

query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Toner Pivots Library/Common Models

# COMMAND ----------

filter_vars = """
SELECT record
    , MAX(version) AS version
FROM prod.trade_forecast
WHERE 1=1
    AND record = 'TRADE_FORECAST'
GROUP BY record

UNION ALL

-- just to track inputs; don't use as a filter in subsequent models 5/10/2022
SELECT 'USAGE_SHARE' AS record
    , version AS version
    FROM prod.version 
    WHERE version = (SELECT MAX(version) FROM prod.version where record = 'USAGE_SHARE')

UNION ALL

SELECT record
    , MAX(version) AS version
FROM prod.norm_shipments
WHERE 1=1
    AND record IN ('ACTUALS - HW', 'HW_FCST', 'HW_STF_FCST')
    AND version = (SELECT MAX(version) FROM prod.version where record = 'NORM_SHIPMENTS')
GROUP BY record

UNION ALL

SELECT record
    , MAX(version) AS version
FROM prod.working_forecast
WHERE 1=1
    AND record = 'TONER-WORKING-FORECAST'
GROUP BY record

UNION ALL

SELECT record_type AS record
    , MAX(version) AS version
FROM fin_prod.actuals_plus_forecast_financials
WHERE 1=1
    AND record_type IN ('ACTUALS', 'FORECAST')
GROUP BY record_type

UNION ALL

SELECT record
    , MAX(version) AS version
FROM prod.ib
WHERE 1=1
    AND record = 'IB'
    AND version = (SELECT MAX(version) FROM prod.version where record = 'IB')
GROUP BY record
"""
query_list.append(["stage.pivots_lib_01_filter_vars", filter_vars, "overwrite"])

# COMMAND ----------

geo_mapping = """
SELECT 'CENTRAL EUROPE' AS market_10, 'EU' AS region_5 UNION ALL
SELECT 'GREATER ASIA' AS market_10, 'AP' AS region_5 UNION ALL
SELECT 'INDIA SL & BL' AS market_10, 'AP' AS region_5 UNION ALL
SELECT 'ISE' AS market_10, 'EU' AS region_5 UNION ALL
SELECT 'LATIN AMERICA' AS market_10, 'LA' AS region_5 UNION ALL
SELECT 'NORTH AMERICA' AS market_10, 'NA' AS region_5 UNION ALL
SELECT 'NORTHERN EUROPE' AS market_10, 'EU' AS region_5 UNION ALL
SELECT 'SOUTHERN EUROPE' AS market_10, 'EU' AS region_5 UNION ALL
SELECT 'UK&I' AS market_10, 'EU' AS region_5 UNION ALL
SELECT 'GREATER CHINA' AS market_10, 'AP' AS region_5
"""
query_list.append(["stage.pivots_lib_02_geo_mapping", geo_mapping, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Toner Pivots Models

# COMMAND ----------

pivots_01_demand = f"""
with pivots_t_19_hw_xref as (
    SELECT hw.platform_subset
        , hw.pl
        , hw.format
        , hw.sf_mf
        , hw.mono_color
        , hw.business_feature
        , hw.product_structure
        , hw.vc_category
        --, NULL AS market_function
        --, NULL AS market_category
        --, NULL AS market_group
        , hw.hw_product_family
        , NULL AS supplies_product_family
        , hw.supplies_mkt_cat
        , hw.epa_family
        , NULL AS crg_pl_category
        , pl.L6_Description AS crg_pl_name
        , pl.L5_Description AS crg_category
        , pl.business_division AS crg_business
    FROM mdm.hardware_xref AS hw
    LEFT JOIN mdm.product_line_xref AS pl
        ON hw.pl = pl.pl
    WHERE 1=1
        AND hw.technology = 'LASER'
)
SELECT DISTINCT d.record
    , d.cal_date
    , d.geography AS market10
    , d.platform_subset
    , d.customer_engagement
    , d.measure
    , d.units
    , d.version
FROM prod.demand AS d
JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = d.platform_subset
WHERE 1=1
    AND d.measure IN ('HP_K_PAGES', 'HP_COLOR_PAGES', 'NON_HP_COLOR_PAGES', 'NON_HP_K_PAGES')
    AND d.cal_date BETWEEN '{pivots_start}' AND '{pivots_end}'
    AND d.version = (select MAX(version) from prod.demand)
"""

query_list.append(["stage.pivots_01_demand", pivots_01_demand, "overwrite"])

# COMMAND ----------

cartridge_mix = f"""

with pivots_t_19_hw_xref as (
    SELECT hw.platform_subset
        , hw.pl
        , hw.format
        , hw.sf_mf
        , hw.mono_color
        , hw.business_feature
        , hw.product_structure
        , hw.vc_category
        --, NULL AS market_function
        --, NULL AS market_category
        --, NULL AS market_group
        , hw.hw_product_family
        , NULL AS supplies_product_family
        , hw.supplies_mkt_cat
        , hw.epa_family
        , NULL AS crg_pl_category
        , pl.L6_Description AS crg_pl_name
        , pl.L5_Description AS crg_category
        , pl.business_division AS crg_business
    FROM mdm.hardware_xref AS hw
    LEFT JOIN mdm.product_line_xref AS pl
        ON hw.pl = pl.pl
    WHERE 1=1
        AND hw.technology = 'LASER'
),  pivots_t_18_supplies_xref as (
    SELECT s.base_product_number
        , s.type
        , s.size
        , s.single_multi
        , s.crg_chrome
        , s.cartridge_alias
        , s.toner_category
        , s.k_color
        , s.crg_intro_dt
        , COALESCE (s.equivalents_multiplier, 1) AS equivalents_multiplier
        , rdma.pl AS supplies_pl
        , rdma.base_prod_name
    FROM mdm.supplies_xref AS s
    LEFT JOIN mdm.rdma AS rdma
        ON rdma.base_prod_number = s.base_product_number
    WHERE 1=1
        AND s.official = 'true'
        AND s.technology = 'LASER'
)
SELECT DISTINCT 'WORKING_FORECAST_MIX' AS record
    , mr.cal_date
    , mr.geography
    , mr.platform_subset
    , mr.base_product_number
    , mr.customer_engagement
    , s.k_color
    , mr.mix_rate
FROM scen.toner_06_mix_rate_final AS mr 
JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = mr.platform_subset
JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = mr.base_product_number
WHERE 1=1
    AND mr.cal_date BETWEEN '{pivots_start}' AND '{pivots_end}'
"""

query_list.append(["stage.pivots_02_cartridge_mix", cartridge_mix, "overwrite"])

# COMMAND ----------

yield_pivots = f"""

with pivots_t_04_yield_step_1 as (
    SELECT cast('YIELD' as varchar(32)) AS record
        , c.Date AS cal_date
        , cast('N/A' as varchar(8)) AS market10
        , CASE WHEN y.geography = 'JP' THEN 'AP'
               WHEN y.geography = 'US' THEN 'NA'
               ELSE y.geography END AS region_5
        , cast('N/A' as varchar(8)) AS platform_subset
        , y.base_product_number
        , y.effective_date
        , y.value AS yield
         , cast('Y' as varchar(8)) AS official_flag
    FROM mdm.yield AS y
    JOIN mdm.calendar AS c
        ON 1=1
    WHERE 1=1
        AND y.official =1
        AND y.geography_grain = 'REGION_5'
        AND c.day_of_month = 1
        AND c.Date BETWEEN '{pivots_start}' AND '{pivots_end}'
),  pivots_t_04b_yield_step_2 as (
    SELECT record
        , cal_date
        , market10
        , region_5
        , platform_subset
        , base_product_number
        , effective_date
        , COALESCE(LEAD(effective_date) OVER (PARTITION BY base_product_number, region_5 ORDER BY effective_date)
            , CAST('2119-08-30' AS DATE)) AS next_effective_date
        , yield
        , official_flag
    FROM pivots_t_04_yield_step_1
    WHERE 1=1
)
SELECT DISTINCT y1.record
    , y2.cal_date
    , y1.market10
    , y1.region_5
    , y1.platform_subset
    , y1.base_product_number
    , y1.effective_date
    , y1.next_effective_date
    , y1.yield
    , y1.official_flag
FROM pivots_t_04b_yield_step_2 AS y1
JOIN pivots_t_04b_yield_step_2 AS y2
    ON y1.effective_date <= y2.cal_date
    AND y1.next_effective_date > y2.cal_date
    AND y1.region_5 = y2.region_5
    AND y1.base_product_number = y2.base_product_number
"""

query_list.append(["stage.pivots_03_yield", yield_pivots, "overwrite"])

# COMMAND ----------

usage = f"""
SELECT 'USAGE_SHARE' AS record
    , us.cal_date
    , us.geography AS market10
    , geo.region_5
    , us.platform_subset
    , 'N/A' AS base_product_number
    , us.customer_engagement
    , us.measure
    , us.units
    , 'Y' AS official_flag
FROM scen.toner_03_usage_share AS us
JOIN mdm.hardware_xref AS hw
    ON hw.platform_subset = us.platform_subset
JOIN stage.pivots_lib_02_geo_mapping AS geo
    ON geo.market_10 = us.geography
WHERE 1=1
    AND us.measure IN ('COLOR_USAGE', 'K_USAGE')
    AND us.geography_grain = 'MARKET10'
    AND NOT hw.product_lifecycle_status = 'E'
    AND hw.technology IN ('LASER')
    AND us.cal_date BETWEEN '{pivots_start}' AND '{pivots_end}'
"""
query_list.append(["stage.pivots_04_usage", usage, "overwrite"])

# COMMAND ----------

pages_wo_mktshr = """
with pivots_t_06_pages_wo_mktshr as (
    SELECT DISTINCT 'PGS WO MKTSHR' AS record
        , cal_date
        , market10
        , platform_subset
        , customer_engagement
        , COALESCE(COALESCE(NON_HP_COLOR_PAGES, 0) + COALESCE(NON_HP_K_PAGES, 0) +
                            COALESCE(HP_COLOR_PAGES, 0) + COALESCE(HP_K_PAGES, 0), 0) As total_units
        , COALESCE(COALESCE(NON_HP_K_PAGES, 0) + COALESCE(HP_K_PAGES, 0), 0) As k_units
        , COALESCE(COALESCE(NON_HP_COLOR_PAGES, 0) + COALESCE(HP_COLOR_PAGES, 0), 0) As color_units
        , 'Y' AS official_flag
    FROM stage.pivots_01_demand
    PIVOT
        ( 
        SUM(units) FOR measure IN ('HP_COLOR_PAGES' as HP_COLOR_PAGES, 'HP_K_PAGES' as HP_K_PAGES, 'NON_HP_COLOR_PAGES' as NON_HP_COLOR_PAGES, 'NON_HP_K_PAGES' as NON_HP_K_PAGES)
    )
)
SELECT p.record
    , p.cal_date
    , p.market10
    , geo.region_5
    , p.platform_subset
    , crg.base_product_number
    , p.customer_engagement
    , crg.k_color
    , SUM(p.k_units * crg.mix_rate) AS units
    , p.official_flag
FROM pivots_t_06_pages_wo_mktshr AS p
JOIN stage.pivots_lib_02_geo_mapping AS geo
    ON geo.market_10 = p.market10
JOIN stage.pivots_02_cartridge_mix AS crg
    ON crg.platform_subset = p.platform_subset
    AND crg.customer_engagement = p.customer_engagement
    AND crg.cal_date = p.cal_date
    AND crg.geography = p.market10
WHERE 1=1 and crg.k_color = 'BLACK'
GROUP BY p.record
    , p.cal_date
    , p.market10
    , geo.region_5
    , p.platform_subset
    , crg.base_product_number
    , p.customer_engagement
    , crg.k_color
    , p.official_flag

UNION

SELECT p.record
    , p.cal_date
    , p.market10
    , geo.region_5
    , p.platform_subset
    , crg.base_product_number
    , p.customer_engagement
    , crg.k_color
    , SUM(p.color_units * crg.mix_rate) AS units
    , p.official_flag
FROM pivots_t_06_pages_wo_mktshr AS p
JOIN stage.pivots_lib_02_geo_mapping AS geo
    ON geo.market_10 = p.market10
JOIN stage.pivots_02_cartridge_mix AS crg
    ON crg.platform_subset = p.platform_subset
    AND crg.customer_engagement = p.customer_engagement
    AND crg.cal_date = p.cal_date
    AND crg.geography = p.market10
WHERE 1=1 and crg.k_color = 'COLOR'
GROUP BY p.record
    , p.cal_date
    , p.market10
    , geo.region_5
    , p.platform_subset
    , crg.base_product_number
    , p.customer_engagement
    , crg.k_color
    , p.official_flag

"""
query_list.append(["stage.pivots_05_pages_wo_mktshr_split", pages_wo_mktshr, "overwrite"])

# COMMAND ----------

pages_w_mktshr = """

with pivots_t_08_pages_w_mktshr as (
    SELECT DISTINCT 'PGS W MKTSHR' AS record
        , cal_date
        , market10
        , platform_subset
        , customer_engagement
        , COALESCE(HP_COLOR_PAGES + HP_K_PAGES, 0) AS total_units
        , COALESCE(HP_COLOR_PAGES,0) color_units
        , COALESCE(HP_K_PAGES,0) k_units
        , 'Y' AS official_flag
    FROM stage.pivots_01_demand
    PIVOT
    (
        SUM(units) FOR measure IN ('HP_COLOR_PAGES' as HP_COLOR_PAGES, 'HP_K_PAGES' as HP_K_PAGES, 'NON_HP_COLOR_PAGES' as NON_HP_COLOR_PAGES, 'NON_HP_K_PAGES' as NON_HP_K_PAGES)
    )
)
SELECT p.record
    , p.cal_date
    , p.market10
    , geo.region_5
    , p.platform_subset
    , crg.base_product_number
    , p.customer_engagement
    , crg.k_color
    , SUM(p.k_units * crg.mix_rate) AS units
    , p.official_flag
FROM pivots_t_08_pages_w_mktshr AS p
JOIN stage.pivots_lib_02_geo_mapping AS geo
    ON geo.market_10 = p.market10
JOIN stage.pivots_02_cartridge_mix AS crg
    ON crg.platform_subset = p.platform_subset
    AND crg.customer_engagement = p.customer_engagement
    AND crg.cal_date = p.cal_date
    AND crg.geography = p.market10
WHERE 1=1 and crg.k_color = 'BLACK'
GROUP BY p.record
    , p.cal_date
    , p.market10
    , geo.region_5
    , p.platform_subset
    , crg.base_product_number
    , p.customer_engagement
    , crg.k_color
    , p.official_flag

UNION

SELECT p.record
    , p.cal_date
    , p.market10
    , geo.region_5
    , p.platform_subset
    , crg.base_product_number
    , p.customer_engagement
    , crg.k_color
    , SUM(p.color_units * crg.mix_rate) AS units
    , p.official_flag
FROM pivots_t_08_pages_w_mktshr AS p
JOIN stage.pivots_lib_02_geo_mapping AS geo
    ON geo.market_10 = p.market10
JOIN stage.pivots_02_cartridge_mix AS crg
    ON crg.platform_subset = p.platform_subset
    AND crg.customer_engagement = p.customer_engagement
    AND crg.cal_date = p.cal_date
    AND crg.geography = p.market10
WHERE 1=1 and crg.k_color = 'COLOR'
GROUP BY p.record
    , p.cal_date
    , p.market10
    , geo.region_5
    , p.platform_subset
    , crg.base_product_number
    , p.customer_engagement
    , crg.k_color
    , p.official_flag
"""

query_list.append(["stage.pivots_06_pages_w_mktshr_split", pages_w_mktshr, "overwrite"])

# COMMAND ----------

crg_size = """

SELECT 'HP CRG SZ' AS record
    , p.cal_date
    , p.market10
    , p.region_5
    , p.platform_subset
    , p.base_product_number
    , p.customer_engagement
    , COALESCE(p.units, 0) * 1.0 /
        NULLIF(COALESCE(y.yield, 0), 0) AS units
    , y.yield
    , p.official_flag
FROM stage.pivots_05_pages_wo_mktshr_split AS p
LEFT JOIN stage.pivots_03_yield AS y
    ON p.base_product_number = y.base_product_number
    AND p.cal_date = y.cal_date
    AND p.region_5 = y.region_5
WHERE 1=1
"""
query_list.append(["stage.pivots_07_crg_size", crg_size, "overwrite"])

# COMMAND ----------

pages_wo_mktshr_k = """

Select 'PGS WO MKTSHR-BLK' as record
    , p.cal_date
    , p.market10
    , p.region_5
    , p.platform_subset
    , p.base_product_number
    , p.customer_engagement
    , SUM(p.units) as units
    , p.official_flag
FROM stage.pivots_05_pages_wo_mktshr_split AS p
WHERE 1=1
    AND p.k_color = 'BLACK'
GROUP BY  p.cal_date
    , p.market10
    , p.region_5
    , p.platform_subset
    , p.base_product_number
    , p.customer_engagement
    , p.official_flag
"""

query_list.append(["stage.pivots_08_pages_wo_mktshr_k", pages_wo_mktshr_k, "overwrite"])

# COMMAND ----------

pages_wo_mktshr_kcmy = """

Select 'PGS WO MKTSHR-KCMY' as record
    , p.cal_date
    , p.market10
    , p.region_5
    , p.platform_subset
    , p.base_product_number
    , p.customer_engagement
    , SUM(p.units) as units
    , p.official_flag
FROM stage.pivots_05_pages_wo_mktshr_split AS p
WHERE 1=1
    AND p.k_color IN ('BLACK', 'COLOR')
GROUP BY p.cal_date
    , p.market10
    , p.region_5
    , p.platform_subset
    , p.base_product_number
    , p.customer_engagement
    , p.official_flag
"""

query_list.append(["stage.pivots_09_pages_wo_mktshr_kcmy", pages_wo_mktshr_kcmy, "overwrite"])

# COMMAND ----------

pages_w_mktshr_k = """

Select 'PGS W MKTSHR-BLK' as record
    , p.cal_date
    , p.market10
    , p.region_5
    , p.platform_subset
    , p.base_product_number
    , p.customer_engagement
    , SUM(p.units) as units
    , p.official_flag
FROM stage.pivots_06_pages_w_mktshr_split AS p
WHERE 1=1
    AND p.k_color = 'BLACK'
GROUP BY  p.cal_date
    , p.market10
    , p.region_5
    , p.platform_subset
    , p.base_product_number
    , p.customer_engagement
    , p.official_flag
"""

query_list.append(["stage.pivots_10_pages_w_mktshr_k", pages_w_mktshr_k, "overwrite"])

# COMMAND ----------

pages_w_mktshr_kcmy = """

Select 'PGS W MKTSHR-KCMY' as record
    , p.cal_date
    , p.market10
    , p.region_5
    , p.platform_subset
    , p.base_product_number
    , p.customer_engagement
    , SUM(p.units) as units
    , p.official_flag
FROM stage.pivots_06_pages_w_mktshr_split AS p
WHERE 1=1
    AND p.k_color IN ('BLACK', 'COLOR')
GROUP BY  p.cal_date
    , p.market10
    , p.region_5
    , p.platform_subset
    , p.base_product_number
    , p.customer_engagement
    , p.official_flag
"""

query_list.append(["stage.pivots_11_pages_w_mktshr_kcmy", pages_w_mktshr_kcmy, "overwrite"])

# COMMAND ----------

supplies_pmf = f"""

SELECT 'SUPPLIES PMF' AS record
    , t.cal_date
    , t.market10
    , t.region_5
    , COALESCE(t.platform_subset, 'UNKNOWN') AS platform_subset
    , t.base_product_number
    , COALESCE(t.customer_engagement, 'UNKNOWN') AS customer_engagement
    , t.cartridges as units
    , 'Y' as official_flag
FROM prod.trade_forecast AS t
JOIN stage.pivots_lib_01_filter_vars AS fv
    ON fv.record = t.record
    AND fv.version = t.version
WHERE 1=1
    AND t.cal_date BETWEEN '{pivots_start}' AND '{pivots_end}'
"""

query_list.append(["stage.pivots_12_supplies_pmf", supplies_pmf, "overwrite"])

# COMMAND ----------

supplies_pmf_equivalent = """

SELECT 'PMF EQUIV' AS record
    , p.cal_date
    , p.market10
    , p.region_5
    , p.platform_subset
    , p.base_product_number
    , p.customer_engagement
    , p.units AS base_units
    , p.units * COALESCE(s.equivalents_multiplier, 1) AS units
    , p.official_flag
FROM stage.pivots_12_supplies_pmf AS p
LEFT JOIN mdm.supplies_xref AS s
    ON s.base_product_number = p.base_product_number
WHERE 1=1
    AND s.official = 1
"""

query_list.append(["stage.pivots_13_supplies_pmf_equivalent", supplies_pmf_equivalent, "overwrite"])

# COMMAND ----------

units_pivot_prep = """


SELECT 'COLOR USAGE' AS record
    , cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , units
    , 0 as yield
    , official_flag
FROM stage.pivots_04_usage
WHERE 1=1
    AND measure = 'COLOR_USAGE'

UNION ALL

SELECT 'K USAGE' AS record
    , cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , units
    , 0 as yield
    , official_flag
FROM stage.pivots_04_usage
WHERE 1=1
    AND measure = 'K_USAGE'

UNION ALL

SELECT record
    , cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , units
    , 0 as yield
    , official_flag
FROM stage.pivots_05_pages_wo_mktshr_split
WHERE 1=1

UNION ALL

SELECT record
    , cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , units
    , 0 as yield
    , official_flag
FROM stage.pivots_06_pages_w_mktshr_split
WHERE 1=1

UNION ALL

SELECT record
    , cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , units
    , yield
    , official_flag
FROM stage.pivots_07_crg_size
WHERE 1=1

UNION ALL

SELECT record
    , cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , units
    , 0 as yield
    , official_flag
FROM stage.pivots_08_pages_wo_mktshr_k
WHERE 1=1

UNION ALL

SELECT record
    , cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , units
    , 0 as yield
    , official_flag
FROM stage.pivots_09_pages_wo_mktshr_kcmy
WHERE 1=1

UNION ALL

SELECT record
    , cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , units
    , 0 as yield
    , official_flag
FROM stage.pivots_10_pages_w_mktshr_k
WHERE 1=1

UNION ALL

SELECT record
    , cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , units
    , 0 as yield
    , official_flag
FROM stage.pivots_11_pages_w_mktshr_kcmy
WHERE 1=1

UNION ALL

SELECT record
    , cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , units
    , 0 as yield
    , official_flag
FROM stage.pivots_12_supplies_pmf
WHERE 1=1

UNION ALL

SELECT record
    , cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , units
    , 0 as yield
    , official_flag
FROM stage.pivots_13_supplies_pmf_equivalent
WHERE 1=1
"""

query_list.append(["stage.pivots_14_units_pivot_prep", units_pivot_prep, "overwrite"])

# COMMAND ----------

units_pivot = """

SELECT cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , pgs_w_mktshr
    , pgs_wo_mktshr
    , pgs_w_mktshr_blk
    , pgs_wo_mktshr_blk
    , pgs_w_mktshr_kcmy
    , pgs_wo_mktshr_kcmy
    , hp_crg_sz
    , supplies_pmf
    , pmf_equiv
    , color_usage
    , k_usage
    , yield
    , official_flag
FROM
(
    SELECT record
        , cal_date
        , market10
        , region_5
        , platform_subset
        , base_product_number
        , customer_engagement
        , units
        , yield
        , official_flag
    FROM stage.pivots_14_units_pivot_prep
) AS TP
PIVOT
(
    SUM(units)
    FOR record
    IN ('PGS W MKTSHR' AS pgs_w_mktshr, 'PGS WO MKTSHR' AS pgs_wo_mktshr, 'PGS W MKTSHR-BLK' AS pgs_w_mktshr_blk,
        'PGS WO MKTSHR-BLK' AS pgs_wo_mktshr_blk, 'PGS W MKTSHR-KCMY' AS pgs_w_mktshr_kcmy, 'PGS WO MKTSHR-KCMY' AS pgs_wo_mktshr_kcmy,
        'HP CRG SZ' AS hp_crg_sz, 'SUPPLIES PMF' AS supplies_pmf, 'PMF EQUIV' AS pmf_equiv, 'COLOR USAGE' AS color_usage, 'K USAGE' AS k_usage)
) 
"""

query_list.append(["stage.pivots_15_units_pivot", units_pivot, "overwrite"])

# COMMAND ----------

working_forecast = f"""

with pivots_t_17_fiscal_calendar as (
    SELECT cal.date
        , cal.fiscal_year_qtr
        , cal.fiscal_yr
        , cal.calendar_yr_qtr
        , cal.calendar_yr
    FROM mdm.calendar AS cal
    WHERE 1=1
        AND cal.day_of_month = 1
        AND cal.fiscal_yr BETWEEN {pivots_start[0:4]} AND {pivots_end[0:4]}
),  pivots_t_19_hw_xref as (

    SELECT hw.platform_subset
        , hw.pl
        , hw.format
        , hw.sf_mf
        , hw.mono_color
        , hw.business_feature
        , hw.product_structure
        , hw.vc_category
        --, NULL AS market_function
        --, NULL AS market_category
        --, NULL AS market_group
        , hw.hw_product_family
        , NULL AS supplies_product_family
        , hw.supplies_mkt_cat
        , hw.epa_family
        , NULL AS crg_pl_category
        , pl.L6_Description AS crg_pl_name
        , pl.L5_Description AS crg_category
        , pl.business_division AS crg_business
    FROM mdm.hardware_xref AS hw
    LEFT JOIN mdm.product_line_xref AS pl
        ON hw.pl = pl.pl
    WHERE 1=1
        AND hw.technology = 'LASER'
),  pivots_t_18_supplies_xref as (

SELECT s.base_product_number
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.cartridge_alias
    , s.toner_category
    , s.k_color
    , s.crg_intro_dt
    , COALESCE (s.equivalents_multiplier, 1) AS equivalents_multiplier
    , rdma.pl AS supplies_pl
    , rdma.base_prod_name
FROM mdm.supplies_xref AS s
LEFT JOIN mdm.rdma AS rdma
    ON rdma.base_prod_number = s.base_product_number
WHERE 1=1
    AND s.official = 'true'
    AND s.Technology = 'LASER'
)
SELECT wf.record
    , wf.cal_date
    , wf.geography AS market10
    , geo.region_5
    , wf.platform_subset
    , wf.base_product_number
    , wf.customer_engagement
    , wf.cartridges
    , wf.channel_fill
    , wf.supplies_spares_cartridges
    , wf.expected_cartridges
    , wf.vtc
    , wf.adjusted_cartridges
    , COALESCE(s.equivalents_multiplier, 1) AS equivalents_multiplier
FROM prod.working_forecast AS wf
JOIN stage.pivots_lib_01_filter_vars AS fv 
    ON fv.record = wf.record
    AND fv.version = wf.version
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = wf.cal_date
JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = wf.platform_subset
LEFT JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = wf.base_product_number
JOIN stage.pivots_lib_02_geo_mapping AS geo
    ON geo.market_10 = wf.geography
WHERE 1=1
"""

query_list.append(["stage.pivots_16_working_forecast", working_forecast, "overwrite"])

# COMMAND ----------

wampv_prep = """
SELECT 'HW - IB' AS record_type
    , 'WAMPV_PREP' AS record
    , ib.cal_date
    , ccx.market10 AS market10
    , ib.platform_subset AS platform_subset
    , ib.customer_engagement
    , SUM(ib.units) AS wampv_ib_units
FROM prod.ib AS ib
JOIN stage.pivots_lib_01_filter_vars AS fv
    ON fv.record = ib.record
    AND fv.version = ib.version
LEFT JOIN mdm.iso_country_code_xref AS ccx
    ON ccx.country_alpha2 = ib.country_alpha2
WHERE 1=1
    AND measure = 'IB'
GROUP BY ib.cal_date
    , ccx.market10
    , ib.platform_subset
    , ib.customer_engagement
"""

query_list.append(["stage.pivots_17_wampv_prep", wampv_prep, "overwrite"])

# COMMAND ----------

combined = f"""
with pivots_t_17_fiscal_calendar as (
    
    SELECT cal.date
        , cal.fiscal_year_qtr
        , cal.fiscal_yr
        , cal.calendar_yr_qtr
        , cal.calendar_yr
    FROM mdm.calendar AS cal
    WHERE 1=1
        AND cal.day_of_month = 1
        AND cal.fiscal_yr BETWEEN {pivots_start[0:4]} AND {pivots_end[0:4]}
),  pivots_t_19_hw_xref as (

    SELECT hw.platform_subset
        , hw.pl
        , hw.format
        , hw.sf_mf
        , hw.mono_color
        , hw.business_feature
        , hw.product_structure
        , hw.vc_category
        --, NULL AS market_function
        --, NULL AS market_category
        --, NULL AS market_group
        , hw.hw_product_family
        , NULL AS supplies_product_family
        , hw.supplies_mkt_cat
        , hw.epa_family
        , NULL AS crg_pl_category
        , pl.L6_Description AS crg_pl_name
        , pl.L5_Description AS crg_category
        , pl.business_division AS crg_business
    FROM mdm.hardware_xref AS hw
    LEFT JOIN mdm.product_line_xref AS pl
        ON hw.pl = pl.pl
    WHERE 1=1
        AND hw.technology = 'LASER'
),  pivots_t_20_norm_ships as (

    SELECT ns.record
        , ns.cal_date
        , ns.region_5
        , ns.country_alpha2
        , ns.platform_subset
        , ns.units
        , ns.version
        , ns.load_date
    FROM prod.norm_shipments AS ns
    JOIN stage.pivots_lib_01_filter_vars AS fv
        ON fv.record = ns.record  -- 3 record categories
        AND fv.version = ns.version
    JOIN pivots_t_17_fiscal_calendar AS f
        ON f.Date = ns.cal_date
    JOIN pivots_t_19_hw_xref AS hw
        ON hw.platform_subset = ns.platform_subset
    WHERE 1=1
        AND ns.record IN ('ACTUALS - HW', 'HW_FCST', 'HW_STF_FCST')
),  pivots_t_18_supplies_xref as (

    SELECT s.base_product_number
        , s.type
        , s.size
        , s.single_multi
        , s.crg_chrome
        , s.cartridge_alias
        , s.toner_category
        , s.k_color
        , s.crg_intro_dt
        , COALESCE (s.equivalents_multiplier, 1) AS equivalents_multiplier
        , rdma.pl AS supplies_pl
        , rdma.base_prod_name
    FROM mdm.supplies_xref AS s
    LEFT JOIN mdm.rdma AS rdma
        ON rdma.base_prod_number = s.base_product_number
    WHERE 1=1
        AND s.official = 'true'
        AND s.Technology = 'LASER'
),  pivots_t_22_net_rev_per_unit as (

SELECT apf.cal_date
    , apf.platform_subset
    , apf.base_product_number
    , apf.customer_engagement
    , apf.base_product_line_code
    , apf.market10
    , apf.region_5
    --, apf.country_alpha2
    , SUM(apf.net_revenue * 1.0) / NULLIF(SUM(apf.units), 0) AS net_revenue_per_unit
FROM fin_prod.actuals_plus_forecast_financials AS apf
JOIN stage.pivots_lib_01_filter_vars AS fv
    ON fv.record = apf.record_type  -- 2 record categories
    AND fv.version = apf.version
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.Date = apf.cal_date
WHERE 1=1
    AND apf.technology = 'LASER'
    AND apf.units <> 0
GROUP BY apf.cal_date
    , apf.platform_subset
    , apf.base_product_number
    , apf.customer_engagement
    , apf.base_product_line_code
    , apf.market10
    , apf.region_5
    --, apf.country_alpha2
)SELECT 'HW - IB' AS record_type
    , 'IB' AS record

    , f.date AS month
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , ccx.market10 AS market10
    , ccx.region_5 AS region_5

    , ib.platform_subset AS platform_subset

    , 'N/A' AS base_prod_name
    , 'N/A' AS base_prod_number

    , ib.customer_engagement
    , 0 as yield

    , hw.pl AS hw_pl
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category

    , 'N/A' AS supplies_pl
    , 'N/A' AS crg_pl_name
    , 'N/A' AS crg_category
    , 'N/A' AS crg_business
    , 'N/A' AS cartridge_alias
    , 'N/A' AS cartridge_type
    , 'N/A' AS cartridge_size
    , 'N/A' AS single_multi
    , 'N/A' AS crg_chrome
    , null AS crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier

    , SUM(0) AS hw_fc_units
    , SUM(ib.units) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units
    , SUM(0) AS hp_sell_in_pages_kcmy
    , SUM(0) AS hp_sell_in_pages_k_only

FROM prod.ib AS ib
JOIN stage.pivots_lib_01_filter_vars AS fv
    ON fv.record = ib.record
    AND fv.version = ib.version
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = ib.cal_date
JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = ib.platform_subset
LEFT JOIN mdm.iso_country_code_xref AS ccx
    ON ccx.country_alpha2 = ib.country_alpha2
WHERE 1=1
    AND measure = 'IB'

GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , ccx.market10
    , ccx.region_5

    , ib.platform_subset
    , ib.customer_engagement

    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category

UNION ALL

SELECT 'HW SHIPS' AS record_type
    , 'PRINTER SHIPMENTS' AS record


    , f.date AS month
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , ccx.market10 AS market10
    , ccx.region_5 AS region_5

    , ns.platform_subset AS platform_subset

    , 'N/A' AS base_prod_name
    , 'N/A' AS base_prod_number

    , 'N/A' AS customer_engagement
    , 0 as yield

    , hw.pl AS hw_pl
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category

    , 'N/A' AS supplies_pl
    , 'N/A' AS crg_pl_name
    , 'N/A' AS crg_category
    , 'N/A' AS crg_business
    , 'N/A' AS cartridge_alias
    , 'N/A' AS cartridge_type
    , 'N/A' AS cartridge_size
    , 'N/A' AS single_multi
    , 'N/A' AS crg_chrome
    , null AS crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier

    , SUM(ns.units) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units
    , SUM(0) AS hp_sell_in_pages_kcmy
    , SUM(0) AS hp_sell_in_pages_k_only

FROM pivots_t_20_norm_ships AS ns
JOIN pivots_t_17_fiscal_calendar AS f
    ON ns.cal_date = f.date
JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = ns.platform_subset
LEFT JOIN mdm.iso_country_code_xref AS ccx
    ON ccx.country_alpha2 = ns.country_alpha2

GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , ccx.market10
    , ccx.region_5

    , ns.platform_subset

    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category

UNION ALL

SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'GROSS REV, FIJI$, DISCOUNT %, PMF $' AS record
    , f.date AS month
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr
    , apf.market10 AS market10
    , apf.region_5 AS region_5
    , apf.platform_subset
    , s.base_prod_name AS base_prod_name
    , apf.base_product_number AS base_prod_number
    , apf.customer_engagement
    , 0 as yield
    , hw.pl AS hw_pf
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category
    , s.supplies_pl AS supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type AS cartridge_type
    , s.size AS cartridge_size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier
    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(apf.shipment_net_revenue) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(apf.net_revenue) AS fiji_usd  -- rename fiji field names to better descriptions
    , COALESCE(SUM(apf.contra) * 1.0 / NULLIF(SUM(apf.gross_revenue), 0), 0) AS discount_pcnt  -- not used; candidate to remove
    , SUM(apf.gross_revenue) AS gross_rev_w  
    , SUM(apf.net_revenue) AS net_rev_w    
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units
    , SUM(0) AS hp_sell_in_pages_kcmy
    , SUM(0) AS hp_sell_in_pages_k_only
FROM fin_prod.actuals_plus_forecast_financials AS apf  -- code review with Priya --> Noelle
JOIN stage.pivots_lib_01_filter_vars AS fv
    ON fv.record = apf.record_type  -- 2 record categories
    AND fv.version = apf.version
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = apf.cal_date
JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = apf.base_product_number
JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = apf.platform_subset
WHERE 1=1
GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr
    , apf.market10
    , apf.region_5
    , apf.platform_subset
    , s.base_prod_name
    , apf.base_product_number
    , apf.customer_engagement
    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category
    , s.supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt

UNION ALL

SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'HP SELL-IN PAGES' AS record


    , f.date AS month
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , apf.market10 AS market10
    , apf.region_5 AS region_5

    , apf.platform_subset

    , s.base_prod_name AS base_prod_name
    , apf.base_product_number AS base_prod_number

    , apf.customer_engagement
    , 0 as yield

    , hw.pl AS hw_pf
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category

    , s.supplies_pl AS supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type AS cartridge_type
    , s.size AS cartridge_size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier

    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd 
    , SUM(0) AS discount_pcnt 
    , SUM(0) AS gross_rev_w  
    , SUM(0) AS net_rev_w    
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units
    , COALESCE(SUM(yield_x_units), 0) AS hp_sell_in_pages_kcmy
    , COALESCE(SUM(yield_x_units_black_only), 0) AS hp_sell_in_pages_k_only

FROM fin_prod.actuals_plus_forecast_financials AS apf
JOIN stage.pivots_lib_01_filter_vars AS fv
    ON fv.record = apf.record_type  -- 2 record categories
    AND fv.version = apf.version
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = apf.cal_date
JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = apf.base_product_number
JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = apf.platform_subset
WHERE 1=1
    AND apf.record_type = 'ACTUALS'

GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr
    , apf.market10
    , apf.region_5
    , apf.platform_subset
    , s.base_prod_name
    , apf.base_product_number
    , apf.customer_engagement

    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category

    , s.supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    
UNION ALL

SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'WORKING: TRADE UNITS, SPARES, CHANNEL FILL, EXPECTED CRGS' AS record

    , f.date AS month
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , wf.market10 AS market_10
    , wf.region_5 AS region_5

    , wf.platform_subset AS platform_subset

    , s.base_prod_name AS base_prod_name
    , wf.base_product_number AS base_prod_number

    , wf.customer_engagement
    , 0 as yield

    , hw.pl AS hw_pl
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category

    , s.supplies_pl AS supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type AS cartridge_type
    , s.size AS cartridge_size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier

    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(wf.adjusted_cartridges) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(wf.expected_cartridges) AS expected_crgs_w  -- to remove? this is the consumption forecast
    , SUM(wf.supplies_spares_cartridges) AS spares_w
    , SUM(wf.channel_fill) AS channel_fill_w
    , SUM(s.equivalents_multiplier * wf.adjusted_cartridges) AS equiv_units_w  -- singles
    , SUM(wf.vtc) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units
    , SUM(0) AS hp_sell_in_pages_kcmy 
    , SUM(0) AS hp_sell_in_pages_k_only

FROM stage.pivots_16_working_forecast AS wf
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = wf.cal_date
JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = wf.platform_subset
JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = wf.base_product_number

GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , wf.market10
    , wf.region_5

    , wf.platform_subset
    , wf.customer_engagement
    , s.base_prod_name
    , wf.base_product_number

    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category

    , s.supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt

UNION ALL

SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'HP CRG SZ, PGS W MKTSHR-BLK, PGS WO MKTSHR-BLK, PGS W MKTSHR, PGS WO MKTSHR' AS record

    , f.date AS month
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , p.market10
    , p.region_5

    , p.platform_subset

    , s.base_prod_name AS base_prod_name
    , p.base_product_number AS base_prod_number

    , p.customer_engagement
    , sum(p.yield) as yield

    , hw.pl AS hw_pl
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category

    , s.supplies_pl AS supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type AS cartridge_type
    , s.size AS cartridge_size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier

    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(p.pgs_w_mktshr_blk) AS pgswmktshr_blackonly
    , SUM(p.pgs_wo_mktshr_blk) AS pgswomktshr_blackonly
    , SUM(p.pgs_w_mktshr_kcmy) AS pgswmktshr_color
    , SUM(p.pgs_wo_mktshr_kcmy) AS pgswomktshr_color
    , SUM(p.hp_crg_sz) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(0) AS net_rev_trade
    , SUM(p.pgs_w_mktshr) AS pgswmktshr
    , SUM(p.pgs_wo_mktshr) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units
    , SUM(0) AS hp_sell_in_pages_kcmy
    , SUM(0) AS hp_sell_in_pages_k_only

FROM stage.pivots_15_units_pivot AS p
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = p.cal_date
LEFT JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = p.platform_subset
LEFT JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = p.base_product_number

GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , p.market10
    , p.region_5

    , p.platform_subset
    , s.base_prod_name
    , p.base_product_number
    , p.customer_engagement

    , hw.pl 
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category

    , s.supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt

UNION ALL

SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'PMF UNITS, PMF EQUI' AS record

    , f.date AS month
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , p.market10
    , p.region_5

    , p.platform_subset

    , s.base_prod_name AS base_prod_name
    , p.base_product_number AS base_prod_number

    , p.customer_engagement
    , sum(p.yield) as yield

    , hw.pl AS hw_pl
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category

    , s.supplies_pl AS supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type AS cartridge_type
    , s.size AS cartridge_size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier

    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(p.supplies_pmf) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(p.pmf_equiv) AS equiv_units_w  -- this is trade, but it is being mapped to working; incorrect
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units
    , SUM(0) AS hp_sell_in_pages_kcmy
    , SUM(0) AS hp_sell_in_pages_k_only

FROM stage.pivots_15_units_pivot AS p  -- TODO locate VIEW IN Redshift 
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = p.cal_date
LEFT JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = p.platform_subset
LEFT JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = p.base_product_number
WHERE 1=1

GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , p.market10
    , p.region_5

    , p.platform_subset
    , s.base_prod_name
    , p.base_product_number
    , p.customer_engagement

    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category

    , s.supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt

UNION ALL

SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'HP SELL-IN PAGES' AS record
    , f.date AS month
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , p.market10
    , p.region_5

    , p.platform_subset

    , s.base_prod_name AS base_prod_name
    , p.base_product_number AS base_prod_number

    , p.customer_engagement
    , 0 as yield

    , hw.pl AS hw_pl
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category

    , s.supplies_pl AS supplies_pl

    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business

    , s.cartridge_alias
    , s.type AS cartridge_type
    , s.size AS cartridge_size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier

    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w  -- this is trade, but it is being mapped to working; incorrect
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units
    , SUM(p.supplies_pmf * p.yield) AS hp_sell_in_pages_kcmy
    , CASE WHEN s.k_color <> 'BLACK' THEN SUM(0) ELSE SUM(p.supplies_pmf * p.yield) END AS hp_sell_in_pages_k_only
FROM stage.pivots_15_units_pivot AS p  -- TODO locate VIEW IN Redshift 
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = p.cal_date
LEFT JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = p.platform_subset
LEFT JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = p.base_product_number
WHERE 1=1
    AND p.cal_date > (select min(cal_date) from fin_prod.actuals_plus_forecast_financials where record_type  != 'ACTUALS') 
GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr
    , p.market10
    , p.region_5
    , p.platform_subset
    , s.base_prod_name
    , p.base_product_number
    , p.customer_engagement
    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category
    , s.supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , s.k_color

UNION ALL

SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'K_USAGE, COLOR_USAGE' AS record

    , f.date AS month
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , p.market10
    , p.region_5

    , p.platform_subset

    , s.base_prod_name AS base_prod_name
    , p.base_product_number AS base_prod_number

    , p.customer_engagement
    , sum(p.yield) as yield

    , hw.pl AS hw_pl
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category

    , s.supplies_pl AS supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type AS cartridge_type
    , s.size AS cartridge_size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier

    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(p.color_usage) AS fiji_color_mpv
    , SUM(p.k_usage) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units
    , SUM(0) AS hp_sell_in_pages_kcmy
    , SUM(0) AS hp_sell_in_pages_k_only

FROM stage.pivots_15_units_pivot AS p  
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = p.cal_date
LEFT JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = p.platform_subset
LEFT JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = p.base_product_number
WHERE 1=1

GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , p.market10
    , p.region_5

    , p.platform_subset
    , s.base_prod_name
    , p.base_product_number
    , p.customer_engagement

    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category

    , s.supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt

UNION ALL

SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'WAMPV_PREP' AS record


    , f.date AS month
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , p.market10
    , p.region_5

    , p.platform_subset

    , s.base_prod_name AS base_prod_name
    , p.base_product_number AS base_prod_number

    , p.customer_engagement
    , sum(p.yield) as yield

    , hw.pl AS hw_pl
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category

    , s.supplies_pl AS supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type AS cartridge_type
    , s.size AS cartridge_size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier

    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(p.k_usage) AS wampv_k_mpv
    , SUM(w.wampv_ib_units) AS wampv_ib_units    
    , SUM(0) AS hp_sell_in_pages_kcmy 
    , SUM(0) AS hp_sell_in_pages_k_only

FROM stage.pivots_15_units_pivot AS p 
LEFT JOIN stage.pivots_17_wampv_prep as w
    ON w.cal_date = p.cal_date
    AND w.market10 = p.market10
    AND w.platform_subset = p.platform_subset
    AND w.customer_engagement = p.customer_engagement
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = p.cal_date
JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = p.platform_subset
LEFT JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = p.base_product_number
WHERE 1=1
    AND NOT p.k_usage IS NULL  -- this filters out base product numbers

GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , p.market10
    , p.region_5

    , p.platform_subset
    , s.base_prod_name
    , p.base_product_number
    , p.customer_engagement

    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category

    , s.supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt

UNION ALL

SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'NET REVENUE WORKING' AS record


    , f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , nrpu.market10 AS market_10
    , nrpu.region_5 AS region_5
    , nrpu.platform_subset
    , s.base_prod_name AS base_prod_name
    , nrpu.base_product_number AS base_prod_number
    , nrpu.customer_engagement    
    , 0 as yield

    , hw.pl AS hw_pl
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category

    , s.supplies_pl AS supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business

    , s.cartridge_alias
    , s.type AS cartridge_type
    , s.size AS cartridge_size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier
    
    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(nrpu.net_revenue_per_unit * t.supplies_pmf) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units    
    , SUM(0) AS hp_sell_in_pages_kcmy 
    , SUM(0) AS hp_sell_in_pages_k_only
FROM pivots_t_22_net_rev_per_unit AS nrpu
JOIN stage.pivots_15_units_pivot AS t 
    ON nrpu.base_product_number = t.base_product_number
    AND nrpu.platform_subset = t.platform_subset
    AND nrpu.cal_date = t.cal_date
    AND nrpu.market10 = t.market10
    AND nrpu.customer_engagement = t.customer_engagement
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = nrpu.cal_date
JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = nrpu.platform_subset
JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = nrpu.base_product_number
GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr
    , nrpu.market10
    , nrpu.region_5
    , nrpu.platform_subset
    , s.base_prod_name
    , nrpu.base_product_number
    , nrpu.customer_engagement
    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category
    , s.supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt

UNION ALL


SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'NET REVENUE WORKING' AS record


    , f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , 'N/A' AS market_10
    , c.region_5

    , 'N/A' AS platform_subset

    , c.base_product_name AS base_prod_name
    , c.base_product_number AS base_prod_number

    , 'N/A' AS customer_engagement

    , 0 as yield

    , '' AS hw_pl
    , '' AS business_feature
    , '' AS supplies_product_family
    , '' AS sf_mf
    , '' AS format
    , '' AS mono_color_devices
    , '' AS product_structure
    , '' AS vc_category

    , s.supplies_pl AS supplies_pl
    , '' AS crg_pl_name
    , '' AS crg_category
    , '' AS crg_business
    , s.cartridge_alias
    , s.type AS cartridge_type
    , s.size AS cartridge_size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier

    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(base_quantity) AS supplies_base_qty
    , SUM(c.base_quantity * COALESCE(s.equivalents_multiplier, 1)) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units
    , SUM(0) AS hp_sell_in_pages_kcmy
    , SUM(0) AS hp_sell_in_pages_k_only

FROM fin_prod.odw_canon_units_prelim_vw AS c  -- TODO locate VIEW IN Redshift 
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = c.cal_date
LEFT JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = c.base_product_number

GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , c.region_5

    , c.base_product_name
    , c.base_product_number

    , s.supplies_pl
    , s.cartridge_alias
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    
UNION ALL

SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'NON-TRADE UNITS' AS record

    , f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , asb.market10 AS market_10
    , geo.region_5

    , asb.platform_subset

    , s.base_prod_name AS base_prod_name
    , asb.base_product_number AS base_prod_number

    , asb.customer_engagement

    , 0 as yield

    , hw.pl AS hw_pl
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category

    , s.supplies_pl AS supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type AS cartridge_type
    , s.size AS cartridge_size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier

    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w
    , SUM(0) AS vtc_w
    , SUM(asb.revenue_units) AS rev_units_nt
    , SUM(asb.equivalent_units) AS equiv_units_nt
    , SUM(0) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units    
    , SUM(0) AS hp_sell_in_pages_kcmy 
    , SUM(0) AS hp_sell_in_pages_k_only

FROM fin_prod.actuals_supplies_baseprod asb
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = asb.cal_date
JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = asb.platform_subset
JOIN pivots_t_18_supplies_xref AS s
    ON s.base_product_number = asb.base_product_number
JOIN stage.pivots_lib_02_geo_mapping AS geo
    ON geo.market_10 = asb.market10
WHERE customer_engagement = 'EST_DIRECT_FULFILLMENT'
GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr

    , asb.market10
    , geo.region_5

    , asb.platform_subset
    , s.base_prod_name
    , asb.base_product_number
    , asb.customer_engagement

    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category

    , s.supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , s.cartridge_alias
    , s.type
    , s.size
    , s.single_multi
    , s.crg_chrome
    , s.crg_intro_dt
    , asb.revenue_units
    , asb.equivalent_units

UNION 

SELECT 'SUPPLIES FC/ACTUALS' AS record_type
    , 'NON-TRADE REVENUE' AS record

    , f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr
    , asb.market10 AS market_10
    , geo.region_5
    , asb.platform_subset
    , null AS base_prod_name
    , asb.base_product_number AS base_prod_number
    , asb.customer_engagement
    , 0 as yield
    , hw.pl AS hw_pl
    , hw.business_feature AS business_feature
    , hw.hw_product_family
    , hw.sf_mf AS sf_mf
    , hw.format AS format
    , hw.mono_color AS mono_color_devices
    , hw.product_structure AS product_structure
    , hw.vc_category
    , asb.pl AS supplies_pl
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , null as cartridge_alias
    , null AS cartridge_type
    , null AS cartridge_size
    , null as single_multi
    , null as crg_chrome
    , null as crg_intro_dt
    , '' AS trans_vs_contract
    , '' AS p2j_identifier
    , SUM(0) AS hw_fc_units
    , SUM(0) AS ib_units
    , SUM(0) AS trd_units_w
    , SUM(0) AS pmf_units
    , SUM(0) AS pmf_dollars
    , SUM(0) AS expected_crgs_w
    , SUM(0) AS spares_w
    , SUM(0) AS channel_fill_w
    , SUM(0) AS equiv_units_w
    , SUM(0) AS vtc_w
    , SUM(0) AS rev_units_nt
    , SUM(0) AS equiv_units_nt
    , SUM(asb.net_revenue) as revenue_nt
    , SUM(0) AS pgswmktshr_blackonly
    , SUM(0) AS pgswomktshr_blackonly
    , SUM(0) AS pgswmktshr_color
    , SUM(0) AS pgswomktshr_color
    , SUM(0) AS hp_crg_sz
    , SUM(0) AS fiji_usd
    , SUM(0) AS discount_pcnt
    , SUM(0) AS gross_rev_w
    , SUM(0) AS net_rev_w
    , SUM(0) AS net_rev_trade
    , SUM(0) AS pgswmktshr
    , SUM(0) AS pgswomktshr
    , SUM(0) AS fiji_color_mpv
    , SUM(0) AS fiji_k_mpv
    , SUM(0) AS fiji_mkt_shr
    , SUM(0) AS supplies_base_qty
    , SUM(0) AS supplies_equivalent_units
    , SUM(0) AS wampv_k_mpv
    , SUM(0) AS wampv_ib_units    
    , SUM(0) AS hp_sell_in_pages_kcmy 
    , SUM(0) AS hp_sell_in_pages_k_only
FROM fin_prod.actuals_supplies_baseprod asb
JOIN pivots_t_17_fiscal_calendar AS f
    ON f.date = asb.cal_date
LEFT JOIN pivots_t_19_hw_xref AS hw
    ON hw.platform_subset = asb.platform_subset
JOIN stage.pivots_lib_02_geo_mapping AS geo
    ON geo.market_10 = asb.market10
WHERE asb.base_product_number IN ('EST_MPS_REVENUE_JV', 'CTSS', 'BIRDS', 'CISS')
GROUP BY f.date
    , f.fiscal_year_qtr
    , f.fiscal_yr
    , f.calendar_yr_qtr
    , f.calendar_yr
    , asb.market10
    , geo.region_5
    , asb.platform_subset
    , asb.base_product_number
    , asb.customer_engagement
    , hw.pl
    , hw.business_feature
    , hw.hw_product_family
    , hw.sf_mf
    , hw.format
    , hw.mono_color
    , hw.product_structure
    , hw.vc_category
    , hw.crg_pl_name
    , hw.crg_category
    , hw.crg_business
    , asb.revenue_units
    , asb.equivalent_units
    , asb.pl

"""

query_list.append(["stage.pivots_18_combined", combined, "overwrite"])

# COMMAND ----------

toner_pivots_data_source = f"""

SELECT
    -- filters
    record_type
    , record
    , cast('{cycle_date[:-3]}' as varchar(64)) "cycle" -- e.g. 2023-05
    , cast('{cycle_date}' as varchar(64)) begin_cycle_date -- e.g. 2023-05-07
    , cast('{cycle_date}' as date) period_dt
    -- dates
    , month
    , fiscal_year_qtr
    , fiscal_yr
    , calendar_yr_qtr
    , calendar_yr

    -- geography
    , upper(market10) as market10
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
    --, market_function
    --, market_category
    --, market_group

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
    , trd_units_w * yield as adjusted_pages 
    , pmf_units
    , pmf_dollars
    , expected_crgs_w
    , expected_crgs_w * yield as expected_pages
    , spares_w
    , channel_fill_w
    , equiv_units_w
    , vtc_w
    , rev_units_nt
    , equiv_units_nt
    , revenue_nt
    , pgswmktshr_blackonly
    , pgswomktshr_blackonly
    , pgswmktshr_color
    , pgswomktshr_color
    , hp_crg_sz
    , fiji_usd
    , discount_pcnt
    , gross_rev_w
    , net_rev_w
    , net_rev_trade -- new
    , pgswmktshr
    , pgswomktshr
    -- , fiji_color_mpv
    , fiji_k_mpv
    , fiji_mkt_shr
    , supplies_base_qty
    , supplies_equivalent_units
    , wampv_k_mpv
    , wampv_ib_units
    , hp_sell_in_pages_kcmy -- new
    , hp_sell_in_pages_k_only -- new
FROM stage.pivots_18_combined p 
"""

query_list.append(["stage.toner_pivots_data_source", toner_pivots_data_source, "overwrite"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
