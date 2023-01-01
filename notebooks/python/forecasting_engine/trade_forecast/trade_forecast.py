# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Trade Forecast

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

# COMMAND ----------

stf_dollarization = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.stf_dollarization") \
    .load()

tables = [
    ['fin_prod.stf_dollarization', stf_dollarization, "overwrite"]
]

# COMMAND ----------

# MAGIC %run "../../finance_etl/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

c2c_02_geography_mapping = spark.sql("""
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
""")
    
c2c_02_geography_mapping.createOrReplaceTempView("c2c_02_geography_mapping")

# COMMAND ----------

trade_01_common = spark.sql("""
    SELECT record
        , MAX(version) AS version
    FROM fin_prod.stf_dollarization
    WHERE 1=1
        AND record = 'SUPPLIES_STF'
    GROUP BY record
    
    UNION ALL
    
    SELECT record
        , MAX(version) AS version
    FROM prod.working_forecast
    WHERE 1=1
        AND record = 'WORKING_FORECAST_TONER'
    GROUP BY record
    
    UNION ALL
    
    SELECT record
        , MAX(version) AS version
    FROM prod.working_forecast_country
    WHERE 1=1
        AND record = 'WORKING_FORECAST_COUNTRY'
    GROUP BY record
""")
    
trade_01_common.createOrReplaceTempView("trade_01_filter_vars")

# COMMAND ----------

trade_01 = spark.sql("""

with trade_05_supplies_stf_agg as (
    SELECT stf.geography AS region_5  -- region_5
        , stf.cal_date
        , stf.base_product_number
        -- this catches upload errors / dupes in stf_dollarization
        , SUM(stf.units) AS total_units
    FROM fin_prod.stf_dollarization AS stf
    JOIN trade_01_filter_vars as fv
        ON fv.version = stf.version
        AND fv.record = stf.record
    WHERE 1=1
    GROUP BY stf.geography
        , stf.cal_date
        , stf.base_product_number
    
),  trade_03a_country_mix_numerator as (
    SELECT wfc.base_product_number
        , wfc.cal_date
        , geo.region_5
        , wfc.country AS country_alpha2
        , SUM(wfc.imp_corrected_cartridges) AS country_numer
    FROM prod.working_forecast_country AS wfc
    JOIN trade_01_filter_vars AS fv
        ON fv.version = wfc.version
        AND fv.record = wfc.record
    JOIN mdm.hardware_xref AS hw
        ON hw.platform_subset = wfc.platform_subset
    JOIN c2c_02_geography_mapping AS geo
        ON geo.market_10 = wfc.geography
    WHERE 1=1
        AND hw.technology = 'LASER'
    GROUP BY wfc.base_product_number
        , wfc.cal_date
        , geo.region_5
        , wfc.country
    
),  trade_03b_country_mix_denom as (
    SELECT wfc.base_product_number
        , wfc.cal_date
        , geo.region_5
        , SUM(wfc.imp_corrected_cartridges) AS country_denom
    FROM prod.working_forecast_country AS wfc
    JOIN trade_01_filter_vars AS fv
        ON fv.version = wfc.version
        AND fv.record = wfc.record
    JOIN mdm.hardware_xref AS hw
        ON hw.platform_subset = wfc.platform_subset
    JOIN c2c_02_geography_mapping AS geo
        ON geo.market_10 = wfc.geography
    WHERE 1=1
        AND hw.technology = 'LASER'
    GROUP BY wfc.base_product_number
        , wfc.cal_date
        , geo.region_5
    
),  trade_03_work_fcst_ctry_mix as (
    SELECT cmn.base_product_number
        , cmn.cal_date
        , cmn.region_5
        , cmn.country_alpha2
        , cmn.country_numer
        , cmd.country_denom
        , CASE WHEN cmn.country_numer * 1.0 / NULLIF(cmd.country_denom, 0) IS NULL
               THEN 1.0 / NULLIF(COUNT(cmn.country_alpha2) OVER (PARTITION BY cmn.cal_date, cmn.region_5, cmn.base_product_number), 0)
               END AS country_r5_even_split
        , cmn.country_numer * 1.0 / NULLIF(cmd.country_denom, 0) AS country_r5_split
    FROM trade_03a_country_mix_numerator AS cmn
    JOIN trade_03b_country_mix_denom AS cmd
        ON cmn.base_product_number = cmd.base_product_number
        AND cmn.cal_date = cmd.cal_date
        AND cmn.region_5 = cmd.region_5
    WHERE 1=1
    
)SELECT stf.region_5
    , wfc.country_alpha2
    , stf.cal_date
    , stf.base_product_number
    , wfc.country_r5_split AS country_split
    , wfc.country_r5_even_split AS country_even_split
    , stf.total_units AS stf_geo_units
    , stf.total_units * COALESCE(wfc.country_r5_split, wfc.country_r5_even_split) AS stf_country_units
FROM trade_05_supplies_stf_agg AS stf
LEFT JOIN trade_03_work_fcst_ctry_mix AS wfc
    ON wfc.base_product_number = stf.base_product_number
    AND wfc.cal_date = stf.cal_date
    AND wfc.region_5 = stf.region_5
WHERE 1=1
""")

trade_01.createOrReplaceTempView("trade_01_supplies_stf_country_mix")

# COMMAND ----------

trade_02 = spark.sql("""

with trade_08_pfs_ce_split_1 as (
    SELECT wf.cal_date
        , wf.geography
        , wf.platform_subset
        , wf.base_product_number
        , wf.customer_engagement
        , SUM(wf.adjusted_cartridges) OVER (PARTITION BY wf.cal_date, wf.geography, wf.base_product_number, wf.platform_subset, wf.customer_engagement) * 1.0 /
            NULLIF(SUM(wf.adjusted_cartridges) OVER (PARTITION BY wf.cal_date, wf.geography, wf.base_product_number), 0) AS pfs_ce_split
    FROM prod.working_forecast AS wf
    JOIN trade_01_filter_vars as fv
        ON fv.version = wf.version
        AND fv.record = wf.record
    JOIN mdm.hardware_xref AS hw
        ON hw.platform_subset = wf.platform_subset
    WHERE 1=1
        AND hw.technology = 'LASER'
    
),  trade_09_pfs_ce_split_2 as (
    select cal_date
        , geography
        , platform_subset
        , base_product_number
        , customer_engagement
        , COUNT(1) OVER (PARTITION BY cal_date, geography, platform_subset, base_product_number, customer_engagement) * 1.0 /
            NULLIF(COUNT(1) over (PARTITION BY cal_date, geography, base_product_number), 0) AS pfs_ce_even_split
    FROM trade_08_pfs_ce_split_1
    WHERE 1=1
        AND pfs_ce_split IS NULL
    
),  trade_10_pfs_ce_split_3 as (
    SELECT cal_date
        , geography
        , platform_subset
        , base_product_number
        , customer_engagement
        , pfs_ce_split AS pfs_ce_split
    FROM trade_08_pfs_ce_split_1
    WHERE 1=1
        AND NOT pfs_ce_split IS NULL

    UNION ALL

    SELECT s2.cal_date
        , s2.geography
        , s2.platform_subset
        , s2.base_product_number
        , s2.customer_engagement
        , s2.pfs_ce_even_split AS pfs_ce_split
    FROM trade_09_pfs_ce_split_2 AS s2
    JOIN trade_08_pfs_ce_split_1 AS s1
        ON s1.cal_date = s2.cal_date
        AND s1.geography = s2.geography
        AND s1.platform_subset = s2.platform_subset
        AND s1.base_product_number = s2.base_product_number
        AND s1.customer_engagement = s2.customer_engagement
    WHERE 1=1
        AND s1.pfs_ce_split IS NULL
    
),  trade_07_supplies_stf_market10 as (
    SELECT stf.cal_date
        , geo.market10
        , stf.base_product_number
        , SUM(stf.stf_country_units) AS units
    FROM trade_01_supplies_stf_country_mix AS stf
    JOIN mdm.iso_country_code_xref AS geo
        ON geo.country_alpha2 = stf.country_alpha2
    WHERE 1=1
        AND NOT stf.country_alpha2 IS NULL  -- this will drop records where WF and CO forecast do not match
    GROUP BY stf.cal_date
        , geo.market10
        , stf.base_product_number
    
)-- stf split to working forecast grain
-- WF MATCH
    SELECT pfs.cal_date
        , pfs.geography
        , pfs.platform_subset
        , pfs.base_product_number
        , pfs.customer_engagement
        , pfs.pfs_ce_split
        , stf.units
        , stf.units * pfs.pfs_ce_split AS units_mix
    FROM trade_10_pfs_ce_split_3 AS pfs
    JOIN trade_07_supplies_stf_market10 AS stf
        ON stf.cal_date = pfs.cal_date
        AND stf.market10 = pfs.geography
        AND stf.base_product_number = pfs.base_product_number
""")

trade_02.createOrReplaceTempView("trade_02_pfs_ce_mix")

# COMMAND ----------

trade_03 = spark.sql("""
    SELECT trade.cal_date
        , trade.geography AS market10
        , g.region_5
        , trade.platform_subset
        , trade.base_product_number
        , trade.customer_engagement
        , trade.units_mix AS cartridges
    FROM trade_02_pfs_ce_mix AS trade
    JOIN c2c_02_geography_mapping AS g
        ON g.market_10 = trade.geography
""")

trade_03.createOrReplaceTempView("trade_03_supplies_stf_transformed")

# COMMAND ----------

trade_04 = spark.sql("""

with trade_02_stf_boundaries as (
    SELECT MIN(stf.cal_date) AS min_cal_date
        , MAX(stf.cal_date) AS max_cal_date
    FROM fin_prod.stf_dollarization AS stf
    JOIN trade_01_filter_vars as fv
        ON fv.version = stf.version
        AND fv.record = stf.record
    WHERE 1=1
    
),  trade_04_calendar as (
SELECT DISTINCT cal.date
    , stf.min_cal_date
    , stf.max_cal_date
FROM mdm.calendar AS cal
CROSS JOIN trade_02_stf_boundaries AS stf
WHERE 1=1
    AND cal.Day_of_Month = 1
    AND cal.date BETWEEN stf.min_cal_date AND stf.max_cal_date
    
),  trade_13_stf_horizon_fill_prep as (
    SELECT stf.market10
        , stf.region_5
        , stf.platform_subset
        , stf.base_product_number
        , stf.customer_engagement
        , MAX(stf.cal_date) AS max_cal_date
    FROM trade_03_supplies_stf_transformed AS stf
    GROUP BY stf.market10
        , stf.region_5
        , stf.platform_subset
        , stf.base_product_number
        , stf.customer_engagement
    
) SELECT c.date AS cal_date
    , stf.market10
    , stf.region_5
    , stf.platform_subset
    , stf.base_product_number
    , stf.customer_engagement
    , 0 as cartridges
FROM trade_04_calendar AS c
CROSS JOIN trade_02_stf_boundaries AS ub
CROSS JOIN trade_13_stf_horizon_fill_prep AS stf
WHERE 1=1
    AND DATEDIFF(stf.max_cal_date, ub.max_cal_date) > 0  -- every record that ends before max date of STF
    AND c.date > stf.max_cal_date  -- no overlap
""")

trade_04.createOrReplaceTempView("trade_04_stf_horizon_fill")

# COMMAND ----------

trade_05 = spark.sql("""
    SELECT wf.cal_date
        , wf.geography AS market10
        , g.region_5
        , wf.platform_subset
        , wf.base_product_number
        , wf.customer_engagement
        , wf.adjusted_cartridges AS cartridges
    FROM prod.working_forecast AS wf
    JOIN trade_01_filter_vars as fv
        ON fv.version = wf.version
        AND fv.record = wf.record
    JOIN mdm.hardware_xref AS hw
        ON hw.platform_subset = wf.platform_subset
    JOIN c2c_02_geography_mapping AS g
        ON g.market_10 = wf.geography
    WHERE 1=1
        AND hw.technology = 'LASER'
""")

trade_05.createOrReplaceTempView("trade_05_working_forecast")

# COMMAND ----------

trade_06 = spark.sql("""

with trade_02_stf_boundaries as (
    SELECT MIN(stf.cal_date) AS min_cal_date
        , MAX(stf.cal_date) AS max_cal_date
    FROM fin_prod.stf_dollarization AS stf
    JOIN trade_01_filter_vars as fv
        ON fv.version = stf.version
        AND fv.record = stf.record
    WHERE 1=1
    
)SELECT 'ALL-MATCHING-STF-RECORDS' AS type
    , stf.cal_date
    , stf.market10
    , stf.region_5
    , stf.platform_subset
    , stf.base_product_number
    , stf.customer_engagement
    , stf.cartridges
FROM trade_03_supplies_stf_transformed AS stf

UNION ALL

SELECT 'ALL-NON-MATCHING-STF-RECORDS' AS type
    , stf.cal_date
    , NULL AS market10
    , stf.region_5
    , NULL AS platform_subset
    , stf.base_product_number
    , NULL AS customer_engagement
    , stf.stf_geo_units AS cartridges
FROM trade_01_supplies_stf_country_mix AS stf
WHERE 1=1
    AND stf.country_alpha2 IS NULL

UNION ALL

SELECT 'HORIZON-FILL-STF-RECORDS' AS type
    , stf.cal_date
    , stf.market10
    , stf.region_5
    , stf.platform_subset
    , stf.base_product_number
    , stf.customer_engagement
    , stf.cartridges
FROM trade_04_stf_horizon_fill AS stf

UNION ALL

SELECT 'WF-NOT-IN-STF-RANGE' AS type
    , wf.cal_date
    , wf.market10
    , wf.region_5
    , wf.platform_subset
    , wf.base_product_number
    , wf.customer_engagement
    , wf.cartridges
FROM trade_05_working_forecast AS wf
CROSS JOIN trade_02_stf_boundaries AS ub
WHERE 1=1
    AND (wf.cal_date < ub.min_cal_date OR wf.cal_date > ub.max_cal_date)

UNION ALL

SELECT 'WF-IN-STF-RANGE' AS type
    , wf.cal_date
    , wf.market10
    , wf.region_5
    , wf.platform_subset
    , wf.base_product_number
    , wf.customer_engagement
    , wf.cartridges
FROM trade_05_working_forecast AS wf
CROSS JOIN trade_02_stf_boundaries AS ub
LEFT JOIN trade_03_supplies_stf_transformed AS stf
    ON stf.market10 = wf.market10
    AND stf.region_5 = wf.region_5
    AND stf.base_product_number = wf.base_product_number
WHERE 1=1
    AND wf.cal_date BETWEEN ub.min_cal_date AND ub.max_cal_date
    AND stf.market10 IS NULL
    AND stf.region_5 IS NULL
    AND stf.base_product_number IS NULL
""")

trade_06.createOrReplaceTempView("trade_06_trade_forecast")

# COMMAND ----------

trade_07 = spark.sql("""

SELECT cal_date
    , market10
    , region_5
    , platform_subset
    , base_product_number
    , customer_engagement
    , cartridges
    , CAST(cal_date AS string) + ' ' + COALESCE(market10, 'blank') + ' ' + COALESCE(region_5, 'blank') + ' ' +
        COALESCE(platform_subset, 'blank') + ' ' + base_product_number + ' ' + COALESCE(customer_engagement, 'blank') AS composite_key
FROM trade_06_trade_forecast

""")

write_df_to_redshift(configs, trade_07, "stage.trade_forecast_staging", "overwrite")

# COMMAND ----------


