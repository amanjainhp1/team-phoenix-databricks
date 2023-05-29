# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # page_cc_cartridges

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation
# MAGIC *Note well:* mdm, prod schema tables listed in alphabetical order, stage schema tables listed in build order
# MAGIC
# MAGIC Stepwise process:
# MAGIC   1. page_cc_cartridges
# MAGIC   
# MAGIC Detail:
# MAGIC + page_cc_cartridges
# MAGIC   + pages / ccs are distributed to platform_subset / base_product_number combinations

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_cc_cartridges

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_cartridges_actuals_toner

# COMMAND ----------

page_cc_cartridges_actuals_toner = """
WITH crg_months AS
    (SELECT date_key
          , [date] AS cal_date
     FROM mdm.calendar
     WHERE 1 = 1
       AND day_of_month = 1)

    , geography_mapping AS
    (SELECT DISTINCT market10 AS market_10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND NOT region_5 IS NULL
       AND NOT region_5 = 'JP'
       AND NOT region_5 LIKE 'X%')

   , yield AS
    (SELECT y.base_product_number
          , map.market_10
          -- note: assumes effective_date is in yyyymm format. multiplying by 100 and adding 1 to get to yyyymmdd
          , y.effective_date
          , COALESCE(LEAD(effective_date)
                     OVER (PARTITION BY y.base_product_number, map.market_10 ORDER BY y.effective_date)
            , CAST('2119-08-30' AS date)) AS next_effective_date
          , y.value                       AS yield
     FROM mdm.yield AS y
              JOIN geography_mapping AS map
                   ON map.region_5 = y.geography
     WHERE 1 = 1
       AND y.official = 1
       AND UPPER(y.geography_grain) = 'REGION_5')

   , pen_fills AS
    (SELECT y.base_product_number
          , m.cal_date
          , y.market_10
          , y.yield
     FROM yield AS y
              JOIN crg_months AS m
                   ON y.effective_date <= m.cal_date
                       AND y.next_effective_date > m.cal_date)

   , pcm_02_hp_demand AS
    (SELECT d.cal_date
          , d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_K_PAGES' THEN units END)     AS black_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_COLOR_PAGES' THEN units END)     AS color_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_COLOR_PAGES' THEN units END * 3) AS cmy_demand
     FROM stage.demand AS d
     LEFT JOIN mdm.hardware_xref AS hw
        ON UPPER(hw.platform_subset) = UPPER(d.platform_subset)
     WHERE 1 = 1
        AND UPPER(hw.technology) IN ('LASER') 
        AND d.cal_date <= (SELECT MAX(cal_date) FROM stage.cartridge_units)
     GROUP BY d.cal_date
            , d.geography
            , d.platform_subset
            , d.customer_engagement)

    , actuals_page_mix_black as 
(
    SELECT  ac.cal_date
          , ac.geography_grain
          , ac.geography
          , ac.platform_subset
          , ac.base_product_number
          , ac.customer_engagement
          , ac.k_color
          , ac.crg_chrome
          , ac.cartridge_volume
          , SUM(cartridge_volume)
                   OVER (PARTITION BY ac.cal_date, ac.geography, ac.platform_subset, ac.base_product_number, ac.crg_chrome, ac.customer_engagement)/
            SUM(cartridge_volume)
                   OVER (PARTITION BY ac.cal_date, ac.geography, ac.platform_subset, ac.crg_chrome, ac.customer_engagement) AS page_mix
     FROM stage.cartridge_units AS ac
     LEFT JOIN mdm.hardware_xref hx ON hx.platform_subset = ac.platform_subset
     WHERE 1 = 1 
     AND UPPER(hx.technology) IN ('LASER')
     AND UPPER(ac.k_color) = 'BLACK' -- AND UPPER(p.crg_chrome) = 'BLK'  Do we need to include DRUM
),

    actuals_page_mix_color as 
(
    SELECT  ac.cal_date
    	  , ac.geography_grain
          , ac.geography
          , ac.platform_subset
          , ac.base_product_number
          , ac.customer_engagement
          , ac.k_color
          , ac.crg_chrome
          , ac.cartridge_volume
          , SUM(cartridge_volume)
                   OVER (PARTITION BY  ac.geography, ac.platform_subset, ac.base_product_number, ac.customer_engagement)/
            SUM(cartridge_volume)
                   OVER (PARTITION BY  ac.geography, ac.platform_subset, ac.crg_chrome, ac.customer_engagement) AS page_mix
     FROM stage.cartridge_units AS ac
     LEFT JOIN mdm.hardware_xref hx ON hx.platform_subset = ac.platform_subset
     WHERE 1 = 1 
     AND UPPER(hx.technology) IN ('LASER')
     AND UPPER(ac.k_color) = 'COLOR' 
     AND UPPER(ac.crg_chrome) IS NOT NULL
),

 pcrg_04_k_actuals AS
    (SELECT pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , acb.page_mix AS page_cc_mix
          , dmd.black_demand
          , pf.yield
          , acb.page_mix * dmd.black_demand         AS page_demand
          , COALESCE(acb.page_mix * dmd.black_demand /
            NULLIF(pf.yield, 0),pcm.cartridge_volume)                                            AS cartridges
          , pcm.cartridge_volume          AS  adjusted_cartridges
     FROM stage.cartridge_units AS pcm
     		  LEFT JOIN actuals_page_mix_black acb on acb.cal_date = pcm.cal_date and acb.geography = pcm.geography and acb.platform_subset = pcm.platform_subset and acb.base_product_number = pcm.base_product_number
     		  										and acb.customer_engagement = pcm.customer_engagement
              LEFT JOIN mdm.supplies_xref AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              LEFT JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
              LEFT JOIN pcm_02_hp_demand AS dmd
                   ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(dmd.geography) = UPPER(pcm.geography)
                       AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
                       AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
              LEFT JOIN pen_fills AS pf
                   ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
                       AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND UPPER(s.k_color) = 'BLACK'
       AND UPPER(hw.technology) IN ('LASER'))

, pcrg_05_color_actuals AS
    (SELECT pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , acb.page_mix AS page_cc_mix
          , dmd.color_demand
          , pf.yield
          , acb.page_mix * dmd.color_demand                             AS page_demand
          , COALESCE(acb.page_mix * dmd.color_demand /
            NULLIF(pf.yield, 0),pcm.cartridge_volume)                                            AS cartridges
          , pcm.cartridge_volume          AS  adjusted_cartridges
     FROM stage.cartridge_units AS pcm
     		  LEFT JOIN actuals_page_mix_color acb on acb.cal_date = pcm.cal_date and acb.geography = pcm.geography and acb.platform_subset = pcm.platform_subset and acb.base_product_number = pcm.base_product_number
     		  										and acb.customer_engagement = pcm.customer_engagement
              LEFT JOIN mdm.supplies_xref AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              LEFT JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
              LEFT JOIN pcm_02_hp_demand AS dmd
                   ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(dmd.geography) = UPPER(pcm.geography)
                       AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
                       AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
              LEFT JOIN pen_fills AS pf
                   ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
                       AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND UPPER(s.k_color) = 'COLOR'
       AND UPPER(hw.technology) IN ('LASER'))


SELECT cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , black_demand AS demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM pcrg_04_k_actuals

UNION ALL

SELECT cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , color_demand AS demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM pcrg_05_color_actuals
"""

query_list.append(["stage.page_cc_cartridges_actuals_toner", page_cc_cartridges_actuals_toner, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_cartridges_actuals_ink

# COMMAND ----------

page_cc_cartridges_actuals_ink = """
WITH crg_months AS
    (SELECT date_key
          , [date] AS cal_date
     FROM mdm.calendar
     WHERE 1 = 1
       AND day_of_month = 1)

    , geography_mapping AS
    (SELECT DISTINCT market10 AS market_10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND NOT region_5 IS NULL
       AND NOT region_5 = 'JP'
       AND NOT region_5 LIKE 'X%')

   , yield AS
    (SELECT y.base_product_number
          , map.market_10
          -- note: assumes effective_date is in yyyymm format. multiplying by 100 and adding 1 to get to yyyymmdd
          , y.effective_date
          , COALESCE(LEAD(effective_date)
                     OVER (PARTITION BY y.base_product_number, map.market_10 ORDER BY y.effective_date)
            , CAST('2119-08-30' AS date)) AS next_effective_date
          , y.value                       AS yield
     FROM mdm.yield AS y
              JOIN geography_mapping AS map
                   ON map.region_5 = y.geography
     WHERE 1 = 1
       AND y.official = 1
       AND UPPER(y.geography_grain) = 'REGION_5')

   , pen_fills AS
    (SELECT y.base_product_number
          , m.cal_date
          , y.market_10
          , y.yield
     FROM yield AS y
              JOIN crg_months AS m
                   ON y.effective_date <= m.cal_date
                       AND y.next_effective_date > m.cal_date)

   , pcm_02_hp_demand AS
    (SELECT d.cal_date
          , d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_K_PAGES' THEN units END)     AS black_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_COLOR_PAGES' THEN units END)     AS color_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_COLOR_PAGES' THEN units END * 3) AS cmy_demand
     FROM stage.demand AS d
     LEFT JOIN mdm.hardware_xref AS hw
        ON UPPER(hw.platform_subset) = UPPER(d.platform_subset)
     WHERE 1 = 1
        AND UPPER(hw.technology) IN ('INK','PWA') 
        AND d.cal_date <= (SELECT MAX(cal_date) FROM stage.cartridge_units)
     GROUP BY d.cal_date
            , d.geography
            , d.platform_subset
            , d.customer_engagement)

    , actuals_page_mix_black as 
(
    SELECT  ac.cal_date
          , ac.geography_grain
          , ac.geography
          , ac.platform_subset
          , ac.base_product_number
          , ac.customer_engagement
          , ac.k_color
          , ac.crg_chrome
          , ac.cartridge_volume
          , SUM(cartridge_volume)
                   OVER (PARTITION BY ac.cal_date, ac.geography, ac.platform_subset, ac.base_product_number, ac.crg_chrome, ac.customer_engagement)/
            SUM(cartridge_volume)
                   OVER (PARTITION BY ac.cal_date, ac.geography, ac.platform_subset, ac.crg_chrome, ac.customer_engagement) AS page_mix
     FROM stage.cartridge_units AS ac
     LEFT JOIN mdm.hardware_xref hx ON hx.platform_subset = ac.platform_subset
     WHERE 1 = 1 
     AND UPPER(hx.technology) IN ('INK','PWA')
     AND UPPER(ac.k_color) = 'BLACK' -- AND UPPER(p.crg_chrome) = 'BLK'  Do we need to include DRUM
),

    actuals_page_mix_color as 
(
    SELECT  ac.cal_date
    	  , ac.geography_grain
          , ac.geography
          , ac.platform_subset
          , ac.base_product_number
          , ac.customer_engagement
          , ac.k_color
          , ac.crg_chrome
          , ac.cartridge_volume
          , SUM(cartridge_volume)
                   OVER (PARTITION BY  ac.geography, ac.platform_subset, ac.base_product_number, ac.customer_engagement)/
            SUM(cartridge_volume)
                   OVER (PARTITION BY  ac.geography, ac.platform_subset, ac.crg_chrome, ac.customer_engagement) AS page_mix
     FROM stage.cartridge_units AS ac
     LEFT JOIN mdm.hardware_xref hx ON hx.platform_subset = ac.platform_subset
     WHERE 1 = 1 
     AND UPPER(hx.technology) IN ('INK','PWA')
     AND UPPER(ac.k_color) = 'COLOR' 
     AND UPPER(ac.crg_chrome) IS NOT NULL
),

 pcrg_04_k_actuals AS
    (SELECT pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , acb.page_mix AS page_cc_mix
          , dmd.black_demand
          , pf.yield
          , acb.page_mix * dmd.black_demand         AS page_demand
          , COALESCE(acb.page_mix * dmd.black_demand /
            NULLIF(pf.yield, 0),pcm.cartridge_volume)                                            AS cartridges
          , pcm.cartridge_volume          AS  adjusted_cartridges
     FROM stage.cartridge_units AS pcm
     		  LEFT JOIN actuals_page_mix_black acb on acb.cal_date = pcm.cal_date and acb.geography = pcm.geography and acb.platform_subset = pcm.platform_subset and acb.base_product_number = pcm.base_product_number
     		  										and acb.customer_engagement = pcm.customer_engagement
              LEFT JOIN mdm.supplies_xref AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              LEFT JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
              LEFT JOIN pcm_02_hp_demand AS dmd
                   ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(dmd.geography) = UPPER(pcm.geography)
                       AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
                       AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
              LEFT JOIN pen_fills AS pf
                   ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
                       AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND UPPER(s.k_color) = 'BLACK'
       AND UPPER(hw.technology) IN ('INK','PWA'))

, pcrg_05_color_actuals AS
    (SELECT pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , acb.page_mix AS page_cc_mix
          , dmd.color_demand
          , pf.yield
          , acb.page_mix * dmd.color_demand                             AS page_demand
          , COALESCE(acb.page_mix * dmd.color_demand /
            NULLIF(pf.yield, 0),pcm.cartridge_volume)                                            AS cartridges
          , pcm.cartridge_volume          AS  adjusted_cartridges
     FROM stage.cartridge_units AS pcm
     		  LEFT JOIN actuals_page_mix_color acb on acb.cal_date = pcm.cal_date and acb.geography = pcm.geography and acb.platform_subset = pcm.platform_subset and acb.base_product_number = pcm.base_product_number
     		  										and acb.customer_engagement = pcm.customer_engagement
              LEFT JOIN mdm.supplies_xref AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              LEFT JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
              LEFT JOIN pcm_02_hp_demand AS dmd
                   ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(dmd.geography) = UPPER(pcm.geography)
                       AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
                       AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
              LEFT JOIN pen_fills AS pf
                   ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
                       AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND UPPER(s.k_color) = 'COLOR'
       AND UPPER(hw.technology) IN ('INK','PWA'))


SELECT cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , black_demand AS demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM pcrg_04_k_actuals

UNION ALL

SELECT cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , color_demand AS demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM pcrg_05_color_actuals
"""

query_list.append(["stage.page_cc_cartridges_actuals_ink", page_cc_cartridges_actuals_ink, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_cartridges_forecast_toner

# COMMAND ----------

page_cc_cartridges_forecast_toner = """
WITH crg_months AS
    (SELECT date_key
          , [date] AS cal_date
     FROM mdm.calendar
     WHERE 1 = 1
       AND day_of_month = 1)

    , geography_mapping AS
    (SELECT DISTINCT market10 AS market_10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND NOT region_5 IS NULL
       AND NOT region_5 = 'JP'
       AND NOT region_5 LIKE 'X%')

   , yield AS
    (SELECT y.base_product_number
          , map.market_10
          -- note: assumes effective_date is in yyyymm format. multiplying by 100 and adding 1 to get to yyyymmdd
          , y.effective_date
          , COALESCE(LEAD(effective_date)
                     OVER (PARTITION BY y.base_product_number, map.market_10 ORDER BY y.effective_date)
            , CAST('2119-08-30' AS date)) AS next_effective_date
          , y.value                       AS yield
     FROM mdm.yield AS y
              JOIN geography_mapping AS map
                   ON map.region_5 = y.geography
     WHERE 1 = 1
       AND y.official = 1
       AND UPPER(y.geography_grain) = 'REGION_5')

   , pen_fills AS
    (SELECT y.base_product_number
          , m.cal_date
          , y.market_10
          , y.yield
     FROM yield AS y
              JOIN crg_months AS m
                   ON y.effective_date <= m.cal_date
                       AND y.next_effective_date > m.cal_date)

   , pcm_02_hp_demand AS
    (SELECT d.cal_date
          , d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_K_PAGES' THEN units END)     AS black_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_COLOR_PAGES' THEN units END)     AS color_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_COLOR_PAGES' THEN units END * 3) AS cmy_demand
     FROM stage.demand AS d
     JOIN mdm.hardware_xref AS hw
        ON UPPER(hw.platform_subset) = UPPER(d.platform_subset)
     WHERE 1 = 1
        AND UPPER(hw.technology) IN ('LASER') 
        AND d.cal_date > (SELECT MAX(cal_date) FROM stage.cartridge_units)
     GROUP BY d.cal_date
            , d.geography
            , d.platform_subset
            , d.customer_engagement)

, pcrg_04_k_fcst AS
    (SELECT pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.black_demand
          , pf.yield
          , pcm.mix_rate * dmd.black_demand                             AS page_demand
          , pcm.mix_rate * dmd.black_demand /
            NULLIF(pf.yield, 0)                                            AS cartridges
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)          AS  adjusted_cartridges
     FROM stage.page_cc_mix AS pcm
              LEFT JOIN mdm.supplies_xref AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              LEFT JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
              LEFT JOIN pcm_02_hp_demand AS dmd
                   ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(dmd.geography) = UPPER(pcm.geography)
                       AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
                       AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
              LEFT JOIN pen_fills AS pf
                   ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
                       AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND UPPER(s.k_color) = 'BLACK'
       AND UPPER(hw.technology) IN ('LASER') 
       AND pcm.cal_date > (SELECT MAX(cal_date) FROM stage.cartridge_units)
       )

, pcrg_05_color_fcst AS
    (SELECT pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.color_demand                                              
          , pf.yield
          , pcm.mix_rate * dmd.color_demand                             AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)                                            AS cartridges
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)          AS adjusted_cartridges
     FROM stage.page_cc_mix AS pcm
              LEFT JOIN mdm.supplies_xref AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              LEFT JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
              LEFT JOIN pcm_02_hp_demand AS dmd
                   ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(dmd.geography) = UPPER(pcm.geography)
                       AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
                       AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
              LEFT JOIN pen_fills AS pf
                   ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
                       AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND UPPER(s.k_color) = 'COLOR'
       AND UPPER(hw.technology) IN ('LASER') 
       AND pcm.cal_date > (SELECT MAX(cal_date) FROM stage.cartridge_units)
     )


SELECT cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , black_demand AS demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM pcrg_04_k_fcst

UNION ALL

SELECT cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , color_demand as demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM pcrg_05_color_fcst
"""

query_list.append(["stage.page_cc_cartridges_forecast_toner", page_cc_cartridges_forecast_toner, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_cartridges_forecast_ink

# COMMAND ----------

page_cc_cartridges_forecast_ink = """
WITH crg_months AS
    (SELECT date_key
          , [date] AS cal_date
     FROM mdm.calendar
     WHERE 1 = 1
       AND day_of_month = 1)

    , geography_mapping AS
    (SELECT DISTINCT market10 AS market_10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND NOT region_5 IS NULL
       AND NOT region_5 = 'JP'
       AND NOT region_5 LIKE 'X%')

   , yield AS
    (SELECT y.base_product_number
          , map.market_10
          -- note: assumes effective_date is in yyyymm format. multiplying by 100 and adding 1 to get to yyyymmdd
          , y.effective_date
          , COALESCE(LEAD(effective_date)
                     OVER (PARTITION BY y.base_product_number, map.market_10 ORDER BY y.effective_date)
            , CAST('2119-08-30' AS date)) AS next_effective_date
          , y.value                       AS yield
     FROM mdm.yield AS y
              JOIN geography_mapping AS map
                   ON map.region_5 = y.geography
     WHERE 1 = 1
       AND y.official = 1
       AND UPPER(y.geography_grain) = 'REGION_5')

   , pen_fills AS
    (SELECT y.base_product_number
          , m.cal_date
          , y.market_10
          , y.yield
     FROM yield AS y
              JOIN crg_months AS m
                   ON y.effective_date <= m.cal_date
                       AND y.next_effective_date > m.cal_date)

   , pcm_02_hp_demand AS
    (SELECT d.cal_date
          , d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_K_PAGES' THEN units END)     AS black_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_COLOR_PAGES' THEN units END)     AS color_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_COLOR_PAGES' THEN units END * 3) AS cmy_demand
     FROM stage.demand AS d
     LEFT JOIN mdm.hardware_xref AS hw
        ON UPPER(hw.platform_subset) = UPPER(d.platform_subset)
     WHERE 1 = 1
        AND UPPER(hw.technology) IN ('INK','PWA')
        AND d.cal_date > (SELECT MAX(cal_date) FROM stage.cartridge_units)
     GROUP BY d.cal_date
            , d.geography
            , d.platform_subset
            , d.customer_engagement)

, pcrg_04_k_fcst AS
    (SELECT pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.black_demand
          , pf.yield
          , pcm.mix_rate * dmd.black_demand                             AS page_demand
          , pcm.mix_rate * dmd.black_demand /
            NULLIF(pf.yield, 0)                                            AS cartridges
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)          AS  adjusted_cartridges
     FROM stage.page_cc_mix AS pcm
              LEFT JOIN mdm.supplies_xref AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              LEFT JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
              LEFT JOIN pcm_02_hp_demand AS dmd
                   ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(dmd.geography) = UPPER(pcm.geography)
                       AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
                       AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
              LEFT JOIN pen_fills AS pf
                   ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
                       AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND UPPER(s.k_color) = 'BLACK'
       AND UPPER(hw.technology) IN ('INK','PWA')
       AND pcm.cal_date > (SELECT MAX(cal_date) FROM stage.cartridge_units)
       )

, pcrg_05_color_fcst AS
    (SELECT pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.color_demand                                          
          , pf.yield
          , pcm.mix_rate * dmd.color_demand                             AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)                                            AS cartridges
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)          AS adjusted_cartridges
     FROM stage.page_cc_mix AS pcm
              LEFT JOIN mdm.supplies_xref AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              LEFT JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
              LEFT JOIN pcm_02_hp_demand AS dmd
                   ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(dmd.geography) = UPPER(pcm.geography)
                       AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
                       AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
              LEFT JOIN pen_fills AS pf
                   ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
                       AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
                       AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND UPPER(s.k_color) = 'COLOR'
       AND UPPER(hw.technology) IN ('INK','PWA')
       AND pcm.cal_date > (SELECT MAX(cal_date) FROM stage.cartridge_units)
     )


SELECT cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , black_demand AS demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM pcrg_04_k_fcst

UNION ALL

SELECT cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , color_demand as demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM pcrg_05_color_fcst
"""

query_list.append(["stage.page_cc_cartridges_forecast_ink", page_cc_cartridges_forecast_ink, "overwrite"])

# COMMAND ----------

page_cc_cartridges_final = """

SELECT 
      cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM stage.page_cc_cartridges_actuals_toner

UNION 

SELECT 
      cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM stage.page_cc_cartridges_actuals_ink

UNION

SELECT 
      cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM stage.page_cc_cartridges_forecast_toner

UNION

SELECT 
      cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , page_cc_mix
    , demand
    , yield
    , page_demand
    , cartridges
    , adjusted_cartridges
FROM stage.page_cc_cartridges_forecast_ink

"""

query_list.append(["stage.page_cc_cartridges", page_cc_cartridges_final, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Redshift

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
