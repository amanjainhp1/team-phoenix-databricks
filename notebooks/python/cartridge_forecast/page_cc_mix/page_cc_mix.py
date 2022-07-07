# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # page_cc_mix

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation
# MAGIC *Note well:* mdm, prod schema tables listed in alphabetical order, stage schema tables listed in build order
# MAGIC 
# MAGIC Stepwise process:
# MAGIC   1. shm_base_helper
# MAGIC   2. cartridge_units
# MAGIC   3. page_mix_engine
# MAGIC   4. cc_mix_engine
# MAGIC   5. page_cc_mix_override
# MAGIC   6. page_mix_complete
# MAGIC   7. cc_mix_complete
# MAGIC   8. page_cc_mix
# MAGIC 
# MAGIC Detail:
# MAGIC + shm_base_helper 
# MAGIC   + scrubbed data from supplies_hw_mapping
# MAGIC   + sub-process inputs
# MAGIC     + mdm.hardware_xref
# MAGIC     + mdm.iso_country_code_xref
# MAGIC     + mdm.supplies_xref
# MAGIC + cartridge_units 
# MAGIC   + historical/actuals time period
# MAGIC   + sub-process inputs
# MAGIC     + mdm.iso_cc_rollup_xref
# MAGIC     + mdm.supplies_xref
# MAGIC     + prod.actual_supplies 
# MAGIC     + stage.shm_base_helper
# MAGIC + page_mix_engine - toner
# MAGIC   + historical/actuals time period + projection through forecast time period
# MAGIC   + aggregate page_mix to 1 across crg_chrome (K, C, M, Y)
# MAGIC   + sub-process inputs
# MAGIC     + mdm.calendar
# MAGIC     + mdm.hardware_xref
# MAGIC     + mdm.iso_country_code_xref
# MAGIC     + mdm.supplies_xref
# MAGIC     + mdm.yield
# MAGIC     + prod.actuals_supplies (to get forecast month start)
# MAGIC     + stage.usage_share_staging
# MAGIC     + stage.shm_base_helper
# MAGIC     + stage.cartridge_units
# MAGIC + cc mix - ink/pwa
# MAGIC   + historical/actuals time period + projection through forecast time period
# MAGIC   + aggregate page_mix to 1 across k_color (black, color)
# MAGIC   + sub-process inputs
# MAGIC     + mdm.calendar
# MAGIC     + mdm.hardware_xref
# MAGIC     + mdm.iso_country_code_xref
# MAGIC     + mdm.supplies_xref
# MAGIC     + mdm.yield
# MAGIC     + prod.actuals_supplies (to get forecast month start)
# MAGIC     + stage.usage_share_staging
# MAGIC     + stage.shm_base_helper
# MAGIC     + stage.cartridge_units
# MAGIC + page_cc_mix_override - ink, laser, pwa
# MAGIC   + NPI printer overrides provided by GBU forecasters
# MAGIC   + sub-process inputs
# MAGIC     + mdm.calendar
# MAGIC     + mdm.hardware_xref
# MAGIC     + mdm.iso_country_code_xref
# MAGIC     + mdm.supplies_xref
# MAGIC     + mdm.yield
# MAGIC     + prod.cartridge_mix_override
# MAGIC + page_mix_complete
# MAGIC   + combined stage.page_mix_engine with page_cc_mix_override
# MAGIC   + sub-process inputs
# MAGIC     + stage.page_mix_engine
# MAGIC     + stage.page_cc_mix_override
# MAGIC + cc_mix_complete
# MAGIC   + combined stage.cc_mix_engine with page_cc_mix_override
# MAGIC   + sub-process inputs
# MAGIC     + stage.cc_mix_engine
# MAGIC     + stage.page_cc_mix_override
# MAGIC + page_cc_mix
# MAGIC   + final output
# MAGIC   + sub-process inputs
# MAGIC     + stage.page_mix_complete
# MAGIC     + stage.cc_mix_complete

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs to page_cc_mix

# COMMAND ----------

# MAGIC %md
# MAGIC ### supplies_hw_mapping modified helper

# COMMAND ----------

shm_base_helper = """
WITH shm_01_iso AS
    (SELECT DISTINCT market10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND region_5 NOT IN ('XU', 'XW'))

   , shm_02_geo AS
    (SELECT DISTINCT shm.platform_subset
                   , shm.base_product_number
                   , shm.geography
                   , CASE
                         WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%STND%' THEN 'STD'
                         WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%YET2%' THEN 'HP+'
                         WHEN hw.technology = 'LASER' THEN 'TRAD'
                         ELSE shm.customer_engagement END AS customer_engagement
     FROM mdm.supplies_hw_mapping AS shm
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = shm.platform_subset
     WHERE 1 = 1
       AND shm.official = 1
       AND shm.geography_grain = 'MARKET10'

     UNION ALL

     SELECT DISTINCT shm.platform_subset
                   , shm.base_product_number
                   , iso.market10                         AS geography
                   , CASE
                         WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%STND%' THEN 'STD'
                         WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%YET2%' THEN 'HP+'
                         WHEN hw.technology = 'LASER' THEN 'TRAD'
                         ELSE shm.customer_engagement END AS customer_engagement
     FROM mdm.supplies_hw_mapping AS shm
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = shm.platform_subset
              JOIN shm_01_iso AS iso
                   ON iso.region_5 = shm.geography -- changed geography_grain to geography
     WHERE 1 = 1
       AND shm.official = 1
       AND shm.geography_grain = 'REGION_5'

     UNION ALL

     SELECT DISTINCT shm.platform_subset
                   , shm.base_product_number
                   , iso.market10                         AS geography
                   , CASE
                         WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%STND%' THEN 'STD'
                         WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%YET2%' THEN 'HP+'
                         WHEN hw.technology = 'LASER' THEN 'TRAD'
                         ELSE shm.customer_engagement END AS customer_engagement
     FROM mdm.supplies_hw_mapping AS shm
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = shm.platform_subset
              JOIN mdm.iso_cc_rollup_xref AS cc
                   ON cc.country_level_1 = shm.geography -- gives us cc.country_alpha2
              JOIN mdm.iso_country_code_xref AS iso
                   ON iso.country_alpha2 = cc.country_alpha2 -- changed geography_grain to geography
     WHERE 1 = 1
       AND shm.official = 1
       AND shm.geography_grain = 'REGION_8'
       AND cc.country_scenario = 'HOST_REGION_8')

SELECT DISTINCT platform_subset
              , base_product_number
              , geography
              , customer_engagement
              , platform_subset + ' ' + base_product_number + ' ' +
                geography + ' ' + customer_engagement AS composite_key
FROM shm_02_geo
"""

query_list.append(["stage.shm_base_helper", shm_base_helper, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### cartridge_units (formerly cartridge_volumes)

# COMMAND ----------

cartridge_units = """
SELECT 'ACTUALS'                AS source
     , acts.cal_date
     , acts.base_product_number
     , cref.country_scenario    AS geography_grain
     , cref.country_level_2     AS geography
     , xref.k_color
     , xref.crg_chrome
     , CASE
           WHEN xref.crg_chrome IN ('DRUM') THEN 'DRUM'
           ELSE 'CARTRIDGE' END AS consumable_type
     , SUM(acts.base_quantity)  AS cartridge_volume
FROM prod.actuals_supplies AS acts
         JOIN mdm.supplies_xref AS xref
              ON xref.base_product_number = acts.base_product_number
         JOIN mdm.iso_cc_rollup_xref AS cref
              ON cref.country_alpha2 = acts.country_alpha2
                  AND cref.country_scenario = 'MARKET10'
WHERE 1 = 1
  AND acts.customer_engagement IN ('EST_INDIRECT_FULFILLMENT', 'I-INK', 'TRAD')
  AND NOT xref.crg_chrome IN ('HEAD', 'UNK')
GROUP BY acts.cal_date
       , acts.base_product_number
       , cref.country_scenario 
       , cref.country_level_2
       , xref.k_color
       , xref.crg_chrome
       , CASE
             WHEN xref.crg_chrome IN ('DRUM') THEN 'DRUM'
             ELSE 'CARTRIDGE' END
"""

query_list.append(["stage.cartridge_units", cartridge_units, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_mix_engine - toner

# COMMAND ----------

page_mix_engine = """
WITH pcm_02_hp_demand AS
    (SELECT d.cal_date
          , d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(CASE
                    WHEN UPPER(d.measure) = 'HP_K_PAGES'
                        THEN units END)                                       AS black_demand
          , MAX(CASE
                    WHEN UPPER(d.measure) = 'HP_C_PAGES'
                        THEN units END)                                       AS color_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_C_PAGES' THEN units END *
                3)                                                            AS cmy_demand
     FROM stage.usage_share_staging AS d
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = d.platform_subset
     WHERE 1 = 1
       AND hw.technology = 'LASER'
     GROUP BY d.cal_date
            , d.geography
            , d.platform_subset
            , d.customer_engagement)

   , pcm_03_dmd_date_boundary AS
    (SELECT d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(d.cal_date) AS max_dmd_date
     FROM pcm_02_hp_demand AS d
     GROUP BY d.geography
            , d.platform_subset
            , d.customer_engagement)

   , pcm_04_crg_actuals AS
    (SELECT DISTINCT v.cal_date
                   , v.geography_grain
                   , v.geography
                   , shm.platform_subset
                   , v.base_product_number
                   , v.k_color
                   , v.crg_chrome
                   , v.consumable_type
                   , v.cartridge_volume
     FROM stage.cartridge_units AS v
              JOIN stage.shm_base_helper AS shm
                   ON shm.base_product_number = v.base_product_number
                       AND shm.geography = v.geography
     WHERE 1 = 1
       AND v.cartridge_volume > 0)

   , crg_months AS
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

   , pcm_05_pages AS
    (SELECT DISTINCT v.cal_date
                   , v.geography_grain
                   , v.geography
                   , v.platform_subset
                   , v.base_product_number
                   , dmd.customer_engagement
                   , CASE
                         WHEN UPPER(s.single_multi) = 'TRI-PACK' THEN 'MULTI'
                         ELSE 'SINGLE' END         AS single_multi
                   , v.k_color
                   , v.crg_chrome
                   , v.consumable_type
                   , v.cartridge_volume
                   , pf.yield
                   , v.cartridge_volume * pf.yield AS pages
     FROM pcm_04_crg_actuals AS v
              JOIN mdm.supplies_xref AS s
                   ON s.base_product_number = v.base_product_number
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = v.platform_subset
              JOIN pcm_02_hp_demand AS dmd
                   ON dmd.platform_subset = v.platform_subset
                       AND dmd.geography = v.geography
                       AND dmd.cal_date = v.cal_date
              JOIN pen_fills AS pf
                   ON pf.base_product_number = v.base_product_number
                       AND pf.cal_date = v.cal_date
                       AND pf.market_10 = v.geography
              JOIN stage.shm_base_helper AS shm
                   ON shm.base_product_number = v.base_product_number
                       AND shm.platform_subset = v.platform_subset
                       AND shm.customer_engagement = dmd.customer_engagement
     WHERE 1 = 1
       AND hw.technology = 'LASER')

   , pcm_06_mix_step_1_k AS
    (SELECT p.cal_date
          , p.geography_grain
          , p.geography
          , p.platform_subset
          , p.base_product_number
          , p.customer_engagement
          , p.single_multi
          , p.k_color
          , p.crg_chrome
          , p.consumable_type
          , p.cartridge_volume
          , p.yield
          , p.pages
          , p.pages /
            NULLIF(SUM(pages)
                   OVER (PARTITION BY p.cal_date, p.geography, p.platform_subset, p.crg_chrome, p.customer_engagement),
                   0) AS mix_step_1
     FROM pcm_05_pages AS p
     WHERE 1 = 1
       AND UPPER(p.k_color) = 'BLACK')

   , pcm_07_mix_step_1_color_single AS
    (SELECT p.cal_date
          , p.geography_grain
          , p.geography
          , p.platform_subset
          , p.base_product_number
          , p.customer_engagement
          , p.single_multi
          , p.k_color
          , p.crg_chrome
          , p.consumable_type
          , p.cartridge_volume
          , p.yield
          , p.pages
          , p.pages /
            NULLIF(SUM(pages)
                   OVER (PARTITION BY p.cal_date, p.geography, p.platform_subset, p.crg_chrome, p.customer_engagement),
                   0) AS mix_step_1
     FROM pcm_05_pages AS p
     WHERE 1 = 1
       AND UPPER(p.k_color) = 'COLOR'
       AND UPPER(p.single_multi) = 'SINGLE')

   , pcm_08_mix_step_1_color_multi AS
    (SELECT p.cal_date
          , p.geography_grain
          , p.geography
          , p.platform_subset
          , p.base_product_number
          , p.customer_engagement
          , p.single_multi
          , p.k_color
          , p.crg_chrome
          , p.consumable_type
          , p.cartridge_volume
          , p.yield
          , p.pages

          -- individual tri-pack ... pcm_10
          , CASE
                WHEN UPPER(p.single_multi) <> 'SINGLE'
                    THEN SUM(p.pages)
                         OVER (PARTITION BY p.cal_date, p.geography, p.platform_subset, p.base_product_number, p.customer_engagement, p.single_multi) *
                         1.0 /
                         NULLIF(SUM(p.pages)
                                OVER (PARTITION BY p.cal_date, p.geography, p.platform_subset, p.customer_engagement, p.single_multi),
                                0)
                ELSE NULL END AS mix_step_1

          -- more than 1 tri-pack per pfs ... pcm_09
          , SUM(p.pages)
            OVER (PARTITION BY p.cal_date, p.geography, p.platform_subset, p.single_multi, p.customer_engagement) *
            1.0 /
            NULLIF(SUM(p.pages)
                   OVER (PARTITION BY p.cal_date, p.geography, p.platform_subset, p.customer_engagement),
                   0)         AS mix_step_1_all
     FROM pcm_05_pages AS p
     WHERE 1 = 1
       AND UPPER(p.k_color) = 'COLOR' -- need all color crgs in the denominator
    )

   , pcm_09_mix_step_2_multi_fix AS
    (
        -- 1 to M possible if there are more than 1 tri-pack; distinct removes dupes
        SELECT DISTINCT m7.cal_date
                      , m7.geography_grain
                      , m7.geography
                      , m7.platform_subset
                      , m7.base_product_number
                      , m7.customer_engagement
                      , m7.single_multi
                      , m7.k_color
                      , m7.crg_chrome
                      , m7.consumable_type
                      , m7.cartridge_volume
                      , m7.yield
                      , m7.pages
                      , m7.mix_step_1
                      -- cks w/o a multipack have to be accounted for
                      , CASE
                            WHEN NOT m8.platform_subset IS NULL
                                THEN m7.mix_step_1 * -1.0 * m8.mix_step_1_all
                            ELSE 0.0 END AS mix_step_2
        FROM pcm_07_mix_step_1_color_single AS m7
                 LEFT JOIN pcm_08_mix_step_1_color_multi AS m8
                           ON m8.cal_date = m7.cal_date
                               AND m8.geography = m7.geography
                               AND m8.platform_subset = m7.platform_subset
                               AND
                              m8.customer_engagement = m7.customer_engagement
                               AND UPPER(m8.single_multi) <> 'SINGLE'
        WHERE 1 = 1)

   , pcm_10_mix_step_3_mix_acts AS
    (SELECT m4.cal_date
          , m4.geography_grain
          , m4.geography
          , m4.platform_subset
          , m4.base_product_number
          , m4.customer_engagement
          , m4.single_multi
          , m4.k_color
          , m4.crg_chrome
          , m4.consumable_type
          , m4.cartridge_volume
          , m4.yield
          , m4.pages
          , m4.mix_step_1
          , NULL          AS mix_step_2
          , m4.mix_step_1 AS page_mix
     FROM pcm_06_mix_step_1_k AS m4
     WHERE 1 = 1

     UNION ALL

     SELECT m9.cal_date
          , m9.geography_grain
          , m9.geography
          , m9.platform_subset
          , m9.base_product_number
          , m9.customer_engagement
          , m9.single_multi
          , m9.k_color
          , m9.crg_chrome
          , m9.consumable_type
          , m9.cartridge_volume
          , m9.yield
          , m9.pages
          , m9.mix_step_1
          , m9.mix_step_2
          , m9.mix_step_1 + m9.mix_step_2 AS pgs_ccs_mix
     FROM pcm_09_mix_step_2_multi_fix AS m9 -- singles only
     WHERE 1 = 1

     UNION ALL

     SELECT m8.cal_date
          , m8.geography_grain
          , m8.geography
          , m8.platform_subset
          , m8.base_product_number
          , m8.customer_engagement
          , m8.single_multi
          , m8.k_color
          , m8.crg_chrome
          , m8.consumable_type
          , m8.cartridge_volume
          , m8.yield
          , m8.pages
          , m8.mix_step_1
          , NULL                           AS mix_step_2
          , m8.mix_step_1_all * mix_step_1 AS pgs_ccs_mix
     FROM pcm_08_mix_step_1_color_multi AS m8
     WHERE 1 = 1
       AND UPPER(m8.single_multi) <> 'SINGLE')

   , pcm_11_mix_dmd_spread AS
    (SELECT m10.cal_date
          , m10.geography_grain
          , m10.geography
          , m10.platform_subset
          , m10.base_product_number
          , m10.customer_engagement
          , m10.single_multi
          , m10.k_color
          , m10.crg_chrome
          , m10.consumable_type
          , m10.cartridge_volume
          , m10.yield
          , m10.pages
          , m10.mix_step_1
          , m10.mix_step_2
          , m10.page_mix
          , CASE
                WHEN UPPER(m10.k_color) = 'BLACK'
                    THEN m10.page_mix * m2.black_demand
                WHEN UPPER(m10.single_multi) <> 'SINGLE' AND
                     m10.k_color = 'COLOR' THEN m10.page_mix * m2.cmy_demand
                WHEN UPPER(m10.single_multi) = 'SINGLE' AND
                     m10.k_color = 'COLOR' THEN m10.page_mix * m2.color_demand
                ELSE NULL END AS device_spread
     FROM pcm_10_mix_step_3_mix_acts AS m10
              JOIN pcm_02_hp_demand AS m2
                   ON m10.cal_date = m2.cal_date
                       AND m10.geography = m2.geography
                       AND m10.platform_subset = m2.platform_subset
                       AND m10.customer_engagement = m2.customer_engagement)

   , c2c_vtc_02_forecast_months AS
    (SELECT DATEADD(MONTH, 1, MAX(sup.cal_date)) AS supplies_forecast_start
     FROM prod.actuals_supplies AS sup
     WHERE 1 = 1
       AND sup.official = 1)

   , pcm_12_mix_weighted_avg_step_1 AS
    (SELECT m11.geography_grain
          , m11.geography
          , m11.platform_subset
          , m11.base_product_number
          , m11.customer_engagement
          , m11.single_multi
          , m11.k_color
          , m11.crg_chrome
          , SUM(m11.device_spread) AS device_spread
     FROM pcm_11_mix_dmd_spread AS m11
              CROSS JOIN c2c_vtc_02_forecast_months AS fm
     WHERE 1 = 1
       AND m11.cal_date BETWEEN DATEADD(MONTH, -10,
                                        fm.supplies_forecast_start) AND DATEADD(
             MONTH, -1, fm.supplies_forecast_start) -- last nine months
     GROUP BY m11.geography_grain
            , m11.geography
            , m11.platform_subset
            , m11.base_product_number
            , m11.customer_engagement
            , m11.single_multi
            , m11.k_color
            , m11.crg_chrome)

   , pcm_13_mix_weighted_avg_step_2_k AS
    (SELECT m12.geography_grain
          , m12.geography
          , m12.platform_subset
          , m12.base_product_number
          , m12.customer_engagement
          , m12.single_multi
          , m12.k_color
          , m12.crg_chrome
          , SUM(m12.device_spread)
            OVER (PARTITION BY m12.geography, m12.platform_subset, m12.base_product_number, m12.customer_engagement) /
            NULLIF(SUM(m12.device_spread)
                   OVER (PARTITION BY m12.geography, m12.platform_subset, m12.customer_engagement, m12.crg_chrome),
                   0) AS weighted_avg
     FROM pcm_12_mix_weighted_avg_step_1 AS m12
     WHERE 1 = 1
       AND UPPER(m12.k_color) = 'BLACK')

   , pcm_14_mix_weighted_avg_step_3_color_single AS
    (SELECT m12.geography_grain
          , m12.geography
          , m12.platform_subset
          , m12.base_product_number
          , m12.customer_engagement
          , m12.single_multi
          , m12.k_color
          , m12.crg_chrome
          , SUM(m12.device_spread)
            OVER (PARTITION BY m12.geography, m12.platform_subset, m12.customer_engagement) AS color_device_spread
          , SUM(m12.device_spread)
            OVER (PARTITION BY m12.geography, m12.platform_subset, m12.base_product_number, m12.customer_engagement) /
            NULLIF(SUM(m12.device_spread)
                   OVER (PARTITION BY m12.geography, m12.platform_subset, m12.customer_engagement, m12.crg_chrome),
                   0)                                                                       AS weighted_avg
     FROM pcm_12_mix_weighted_avg_step_1 AS m12
     WHERE 1 = 1
       AND UPPER(m12.k_color) = 'COLOR')

   , pcm_15_mix_weighted_avg_step_4_color_multi AS
    (SELECT DISTINCT m12.geography_grain
                   , m12.geography
                   , m12.platform_subset
                   , m12.base_product_number
                   , m12.customer_engagement
                   , m12.single_multi
                   , m12.k_color
                   , m12.crg_chrome
                   , SUM(m12.device_spread)
                     OVER (PARTITION BY m12.geography, m12.platform_subset, m12.base_product_number, m12.customer_engagement) /
                     NULLIF(SUM(m14.color_device_spread)
                            OVER (PARTITION BY m12.geography, m12.platform_subset, m12.customer_engagement),
                            0) AS weighted_avg
     FROM pcm_12_mix_weighted_avg_step_1 AS m12
              LEFT JOIN pcm_14_mix_weighted_avg_step_3_color_single AS m14
                        ON m14.geography = m12.geography
                            AND m14.platform_subset = m12.platform_subset
                            AND
                           m14.customer_engagement = m12.customer_engagement
     WHERE 1 = 1
       AND UPPER(m12.k_color) = 'COLOR'
       AND UPPER(m12.single_multi) <> 'SINGLE')

   , pcm_16_mix_weighted_avg_step_5_color_single_fix AS
    (SELECT m14.geography_grain
          , m14.geography
          , m14.platform_subset
          , m14.base_product_number
          , m14.customer_engagement
          , m14.single_multi
          , m14.k_color
          , m14.crg_chrome
          , m14.weighted_avg
          , CASE
                WHEN m15.weighted_avg IS NULL THEN m14.weighted_avg
                ELSE COALESCE(m15.weighted_avg, 0) * (
                        m14.weighted_avg /
                        NULLIF(SUM(m14.weighted_avg)
                               OVER (PARTITION BY m14.geography, m14.platform_subset, m14.customer_engagement, m14.crg_chrome),
                               0)
                    )
            END AS multi_adjust
     FROM pcm_14_mix_weighted_avg_step_3_color_single AS m14
              LEFT JOIN
          (SELECT geography
                , platform_subset
                , customer_engagement
                , 1.0 - SUM(weighted_avg) AS weighted_avg -- updated rate logic
           FROM pcm_15_mix_weighted_avg_step_4_color_multi
           GROUP BY geography
                  , platform_subset
                  , customer_engagement) AS m15
          ON m15.geography = m14.geography
              AND m15.platform_subset = m14.platform_subset
              AND m15.customer_engagement = m14.customer_engagement
     WHERE 1 = 1
       AND UPPER(m14.k_color) = 'COLOR'
       AND UPPER(m14.single_multi) = 'SINGLE')

   , pcm_17_mix_weighted_avg AS
    (SELECT m13.geography_grain
          , m13.geography
          , m13.platform_subset
          , m13.base_product_number
          , m13.customer_engagement
          , m13.single_multi
          , m13.k_color
          , m13.crg_chrome
          , m13.weighted_avg
     FROM pcm_13_mix_weighted_avg_step_2_k AS m13

     UNION ALL

     SELECT m15.geography_grain
          , m15.geography
          , m15.platform_subset
          , m15.base_product_number
          , m15.customer_engagement
          , m15.single_multi
          , m15.k_color
          , m15.crg_chrome
          , m15.weighted_avg
     FROM pcm_15_mix_weighted_avg_step_4_color_multi AS m15

     UNION ALL

     SELECT m16.geography_grain
          , m16.geography
          , m16.platform_subset
          , m16.base_product_number
          , m16.customer_engagement
          , m16.single_multi
          , m16.k_color
          , m16.crg_chrome
          , m16.multi_adjust AS weighted_avg
     FROM pcm_16_mix_weighted_avg_step_5_color_single_fix AS m16)

   , pcm_18_projection AS
    (SELECT c.date           AS cal_date
          , m17.geography_grain
          , m17.geography
          , m17.platform_subset
          , m17.base_product_number
          , m17.customer_engagement
          , m17.single_multi
          , m17.weighted_avg AS page_mix
     FROM pcm_17_mix_weighted_avg AS m17
              CROSS JOIN c2c_vtc_02_forecast_months AS fm
              CROSS JOIN mdm.calendar AS c
              JOIN pcm_03_dmd_date_boundary AS ddb
                   ON ddb.geography = m17.geography
                       AND ddb.platform_subset = m17.platform_subset
                       AND ddb.customer_engagement = m17.customer_engagement
     WHERE 1 = 1
       AND c.day_of_month = 1
       AND c.date BETWEEN fm.supplies_forecast_start AND ddb.max_dmd_date)

SELECT 'PCM_ENGINE_ACTS' AS type
     , m10.cal_date
     , m10.geography_grain
     , m10.geography
     , m10.platform_subset
     , m10.base_product_number
     , m10.customer_engagement
     , m10.single_multi
     , m10.page_mix
     , CAST(cal_date AS varchar) + ' ' + geography + ' ' + platform_subset +
       ' ' +
       base_product_number + ' ' + customer_engagement + ' ' +
       'PCM_ENGINE_ACTS' AS composite_key
FROM pcm_10_mix_step_3_mix_acts AS m10

UNION ALL

SELECT 'PCM_ENGINE_PROJECTION'                                          AS type
     , m18.cal_date
     , m18.geography_grain
     , m18.geography
     , m18.platform_subset
     , m18.base_product_number
     , m18.customer_engagement
     , m18.single_multi
     , m18.page_mix
     , CAST(cal_date AS varchar) + ' ' + geography + ' ' + platform_subset +
       ' ' +
       base_product_number + ' ' + customer_engagement + ' ' +
       'PCM_ENGINE_PROJECTION'                                                         AS composite_key
FROM pcm_18_projection AS m18
"""

query_list.append(["stage.page_mix_engine", page_mix_engine, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## cc_mix_engine - ink/pwa

# COMMAND ----------

cc_mix_engine = """
WITH pcm_02_hp_demand AS
    (SELECT d.cal_date
          , d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_K_PAGES' THEN units END)     AS black_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_C_PAGES' THEN units END)     AS color_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_C_PAGES' THEN units END * 3) AS cmy_demand
     FROM stage.usage_share_staging AS d
     JOIN mdm.hardware_xref AS hw
        ON hw.platform_subset = d.platform_subset
     WHERE 1 = 1
        AND hw.technology IN ('INK', 'PWA')
     GROUP BY d.cal_date
            , d.geography
            , d.platform_subset
            , d.customer_engagement)

   , pcm_03_dmd_date_boundary AS
    (SELECT d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(d.cal_date) AS max_dmd_date
     FROM pcm_02_hp_demand AS d
     GROUP BY d.geography
            , d.platform_subset
            , d.customer_engagement)

   , pcm_04_crg_actuals AS
    (SELECT DISTINCT v.cal_date
                   , v.geography_grain
                   , v.geography
                   , shm.platform_subset
                   , v.base_product_number
                   , v.k_color
                   , v.crg_chrome
                   , v.consumable_type
                   , v.cartridge_volume
     FROM stage.cartridge_units AS v
              JOIN stage.shm_base_helper AS shm
                   ON shm.base_product_number = v.base_product_number
                       AND shm.geography = v.geography
     WHERE 1 = 1
       AND v.cartridge_volume > 0)

   , crg_months AS
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

   , pcm_05_ccs AS
    (SELECT DISTINCT v.cal_date
                   , v.geography_grain
                   , v.geography
                   , v.platform_subset
                   , v.base_product_number
                   , dmd.customer_engagement
                   , v.k_color
                   , v.crg_chrome
                   , v.consumable_type
                   , v.cartridge_volume
                   , pf.yield
                   , CASE
                         WHEN hw.technology = 'INK' AND v.k_color = 'BLACK'
                             THEN v.cartridge_volume * pf.yield *
                                  SUM(dmd.black_demand)
                                  OVER (PARTITION BY dmd.platform_subset, dmd.geography, dmd.cal_date, dmd.customer_engagement) *
                                  1.0 /
                                  NULLIF(SUM(dmd.black_demand)
                                         OVER (PARTITION BY dmd.platform_subset, dmd.geography, dmd.cal_date), 0)
                         WHEN hw.technology = 'INK' AND v.k_color = 'COLOR'
                             THEN v.cartridge_volume * pf.yield *
                                  SUM(dmd.color_demand)
                                  OVER (PARTITION BY dmd.platform_subset, dmd.geography, dmd.cal_date, dmd.customer_engagement) *
                                  1.0 /
                                  NULLIF(SUM(dmd.color_demand)
                                         OVER (PARTITION BY dmd.platform_subset, dmd.geography, dmd.cal_date), 0)
            END                            AS ccs
     FROM pcm_04_crg_actuals AS v
              JOIN mdm.supplies_xref AS s
                   ON s.base_product_number = v.base_product_number
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = v.platform_subset
              JOIN pcm_02_hp_demand AS dmd
                   ON dmd.platform_subset = v.platform_subset
                       AND dmd.geography = v.geography
                       AND dmd.cal_date = v.cal_date
              JOIN pen_fills AS pf
                   ON pf.base_product_number = v.base_product_number
                       AND pf.cal_date = v.cal_date
                       AND pf.market_10 = v.geography
              JOIN stage.shm_base_helper AS shm
                   ON shm.base_product_number = v.base_product_number
                       AND shm.platform_subset = v.platform_subset
                       AND shm.customer_engagement = dmd.customer_engagement
     WHERE 1=1 AND hw.technology IN ('INK', 'PWA'))

   , pcm_06_mix_step_1_k AS
    (SELECT p.cal_date
          , p.geography_grain
          , p.geography
          , p.platform_subset
          , p.base_product_number
          , p.customer_engagement
          , p.k_color
          , p.crg_chrome
          , p.consumable_type
          , p.cartridge_volume
          , p.yield
          , p.ccs
          , p.ccs /
            NULLIF(SUM(ccs)
                   OVER (PARTITION BY p.cal_date, p.geography, p.platform_subset, p.k_color, p.customer_engagement),
                   0) AS mix_step_1
     FROM pcm_05_ccs AS p
     WHERE 1 = 1
       AND UPPER(p.k_color) = 'BLACK')

   , pcm_07_mix_step_2_color AS
    (SELECT p.cal_date
          , p.geography_grain
          , p.geography
          , p.platform_subset
          , p.base_product_number
          , p.customer_engagement
          , p.k_color
          , p.crg_chrome
          , p.consumable_type
          , p.cartridge_volume
          , p.yield
          , p.ccs
          , p.ccs /
            NULLIF(SUM(ccs)
                   OVER (PARTITION BY p.cal_date, p.geography, p.platform_subset, p.k_color, p.customer_engagement),
                   0) AS mix_step_1
     FROM pcm_05_ccs AS p
     WHERE 1 = 1
       AND UPPER(p.k_color) = 'COLOR')

   , pcm_08_mix_step_3_acts AS
    (SELECT m6.cal_date
          , m6.geography_grain
          , m6.geography
          , m6.platform_subset
          , m6.base_product_number
          , m6.customer_engagement
          , m6.k_color
          , m6.crg_chrome
          , m6.consumable_type
          , m6.cartridge_volume
          , m6.yield
          , m6.ccs
          , m6.mix_step_1
          , m6.mix_step_1 AS cc_mix
     FROM pcm_06_mix_step_1_k AS m6
     WHERE 1 = 1

     UNION ALL

     SELECT m7.cal_date
          , m7.geography_grain
          , m7.geography
          , m7.platform_subset
          , m7.base_product_number
          , m7.customer_engagement
          , m7.k_color
          , m7.crg_chrome
          , m7.consumable_type
          , m7.cartridge_volume
          , m7.yield
          , m7.ccs
          , m7.mix_step_1
          , m7.mix_step_1 AS cc_mix
     FROM pcm_07_mix_step_2_color AS m7
     WHERE 1 = 1)

   , pcm_09_mix_dmd_spread AS
    (SELECT m8.cal_date
          , m8.geography_grain
          , m8.geography
          , m8.platform_subset
          , m8.base_product_number
          , m8.customer_engagement
          , m8.k_color
          , m8.crg_chrome
          , m8.consumable_type
          , m8.cartridge_volume
          , m8.yield
          , m8.ccs
          , m8.mix_step_1
          , m8.cc_mix
          , CASE
                WHEN UPPER(m8.k_color) = 'BLACK' THEN m8.cc_mix * m2.black_demand
                WHEN UPPER(m8.k_color) = 'COLOR' THEN m8.cc_mix * m2.color_demand
                ELSE NULL END AS device_spread
     FROM pcm_08_mix_step_3_acts AS m8
              JOIN pcm_02_hp_demand AS m2
                   ON m8.cal_date = m2.cal_date
                       AND m8.geography = m2.geography
                       AND m8.platform_subset = m2.platform_subset
                       AND m8.customer_engagement = m2.customer_engagement)

   , supplies_forecast_months AS
    (SELECT DATEADD(MONTH, 1, MAX(sup.cal_date)) AS supplies_forecast_start
     FROM prod.actuals_supplies AS sup
     WHERE 1 = 1
       AND sup.official = 1)

   , pcm_10_mix_weighted_avg_step_1 AS
    (SELECT m9.geography_grain
          , m9.geography
          , m9.platform_subset
          , m9.base_product_number
          , m9.customer_engagement
          , m9.k_color
          , m9.crg_chrome
          , SUM(m9.device_spread) AS device_spread
     FROM pcm_09_mix_dmd_spread AS m9
              CROSS JOIN supplies_forecast_months AS fm
     WHERE 1 = 1
       AND m9.cal_date BETWEEN DATEADD(MONTH, -10, fm.supplies_forecast_start) AND DATEADD(MONTH, -1, fm.supplies_forecast_start) -- last nine months
     GROUP BY m9.geography_grain
            , m9.geography
            , m9.platform_subset
            , m9.base_product_number
            , m9.customer_engagement
            , m9.k_color
            , m9.crg_chrome)

   , pcm_11_mix_weighted_avg_step_2_k AS
    (SELECT m10.geography_grain
          , m10.geography
          , m10.platform_subset
          , m10.base_product_number
          , m10.customer_engagement
          , m10.k_color
          , m10.crg_chrome
          , SUM(m10.device_spread)
            OVER (PARTITION BY m10.geography, m10.platform_subset, m10.base_product_number, m10.customer_engagement) /
            NULLIF(SUM(m10.device_spread)
                   OVER (PARTITION BY m10.geography, m10.platform_subset, m10.customer_engagement, m10.k_color),
                   0) AS weighted_avg
     FROM pcm_10_mix_weighted_avg_step_1 AS m10
     WHERE 1 = 1
       AND UPPER(m10.k_color) = 'BLACK')

   , pcm_12_mix_weighted_avg_step_3_color AS
    (SELECT m10.geography_grain
          , m10.geography
          , m10.platform_subset
          , m10.base_product_number
          , m10.customer_engagement
          , m10.k_color
          , m10.crg_chrome
          , SUM(m10.device_spread)
            OVER (PARTITION BY m10.geography, m10.platform_subset, m10.customer_engagement) AS color_device_spread
          , SUM(m10.device_spread)
            OVER (PARTITION BY m10.geography, m10.platform_subset, m10.base_product_number, m10.customer_engagement) /
            NULLIF(SUM(m10.device_spread)
                   OVER (PARTITION BY m10.geography, m10.platform_subset, m10.customer_engagement, m10.k_color),
                   0)                                                                       AS weighted_avg
     FROM pcm_10_mix_weighted_avg_step_1 AS m10
     WHERE 1 = 1
       AND UPPER(m10.k_color) = 'COLOR')

   , pcm_13_mix_weighted_avg AS
    (SELECT m11.geography_grain
          , m11.geography
          , m11.platform_subset
          , m11.base_product_number
          , m11.customer_engagement
          , m11.k_color
          , m11.crg_chrome
          , m11.weighted_avg
     FROM pcm_11_mix_weighted_avg_step_2_k AS m11

     UNION ALL

     SELECT m12.geography_grain
          , m12.geography
          , m12.platform_subset
          , m12.base_product_number
          , m12.customer_engagement
          , m12.k_color
          , m12.crg_chrome
          , m12.weighted_avg
     FROM pcm_12_mix_weighted_avg_step_3_color AS m12)

   , pcm_14_projection AS
    (SELECT c.date           AS cal_date
          , m17.geography_grain
          , m17.geography
          , m17.platform_subset
          , m17.base_product_number
          , m17.customer_engagement
          , m17.weighted_avg AS cc_mix
     FROM pcm_13_mix_weighted_avg AS m17
              CROSS JOIN supplies_forecast_months AS fm
              CROSS JOIN mdm.calendar AS c
              JOIN pcm_03_dmd_date_boundary AS ddb
                   ON ddb.geography = m17.geography
                       AND ddb.platform_subset = m17.platform_subset
                       AND ddb.customer_engagement = m17.customer_engagement
     WHERE 1 = 1
       AND c.day_of_month = 1
       AND c.date BETWEEN fm.supplies_forecast_start AND ddb.max_dmd_date)

SELECT 'PCM_ENGINE_ACTS' AS type
     , m8.cal_date
     , m8.geography_grain
     , m8.geography
     , m8.platform_subset
     , m8.base_product_number
     , m8.customer_engagement
     , m8.cc_mix
     , CAST(cal_date AS varchar) + ' ' + geography + ' ' + platform_subset +
       ' ' +
       base_product_number + ' ' + customer_engagement + ' ' +
       'PCM_ENGINE_ACTS' AS composite_key
FROM pcm_08_mix_step_3_acts AS m8

UNION ALL

SELECT 'PCM_ENGINE_PROJECTION'                                          AS type
     , m14.cal_date
     , m14.geography_grain
     , m14.geography
     , m14.platform_subset
     , m14.base_product_number
     , m14.customer_engagement
     , m14.cc_mix
     , CAST(cal_date AS varchar) + ' ' + geography + ' ' + platform_subset +
       ' ' +
       base_product_number + ' ' + customer_engagement + ' ' +
       'PCM_ENGINE_PROJECTION'                                                         AS composite_key
FROM pcm_14_projection AS m14
"""

query_list.append(["stage.cc_mix_engine", cc_mix_engine, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_cc_mix_override

# COMMAND ----------

page_cc_mix_override = """
WITH crg_months AS
    (SELECT date_key
          , Date AS cal_date
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
          -- NOTE: assumes effective_date is in YYYYMM format. Multiplying by 100 and adding 1 to get to YYYYMMDD
          , y.effective_date
          , COALESCE(LEAD(effective_date)
                     OVER (PARTITION BY y.base_product_number, map.market_10 ORDER BY y.effective_date)
            , CAST('2119-08-30' AS DATE)) AS next_effective_date
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

    , c2c_vtc_02_forecast_months AS
    (SELECT DATEADD(MONTH, 1, MAX(sup.cal_date)) AS supplies_forecast_start
     FROM prod.actuals_supplies AS sup
     WHERE 1 = 1
       AND sup.official = 1)

   , prod_crg_mix_region5 AS
    (SELECT cmo.cal_date
          , map.market_10               AS market10
          , cmo.platform_subset
          , cmo.Crg_Base_Prod_Number
          , cmo.customer_engagement
          , cmo.mix_pct
          , CASE
                WHEN hw.technology = 'LASER' AND CAST(cmo.load_date AS DATE) > '2021-11-15' THEN 'PAGE_MIX'
                WHEN hw.technology <> 'LASER' THEN 'PAGE_MIX'
                ELSE 'CRG_MIX' END      AS upload_type  -- legacy
          , CASE
                WHEN NOT sup.k_color IS NULL THEN sup.k_color
                WHEN sup.k_color IS NULL AND sup.Crg_Chrome IN ('K', 'BLK') THEN 'BLACK'
                WHEN sup.k_color IS NULL AND sup.Crg_Chrome IN ('C', 'CYN', 'M', 'MAG', 'Y', 'YEL') THEN 'COLOR'
                ELSE NULL END           AS k_color
          , CASE
                WHEN sup.Crg_Chrome = 'C' THEN 'CYN'
                WHEN sup.Crg_Chrome = 'M' THEN 'MAG'
                WHEN sup.Crg_Chrome = 'Y' THEN 'YEL'
                WHEN sup.Crg_Chrome = 'K' THEN 'BLK'
                ELSE sup.Crg_Chrome END AS Crg_Chrome
          , CASE
                WHEN sup.Crg_Chrome IN ('DRUM') THEN 'DRUM'
                ELSE 'CARTRIDGE' END    AS consumable_type
          , cmo.load_date
     FROM prod.cartridge_mix_override AS cmo
              JOIN mdm.supplies_xref AS sup
                   ON cmo.Crg_base_prod_number = sup.base_product_number
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = cmo.platform_subset
              JOIN geography_mapping AS map
                   ON map.region_5 = cmo.geography
     WHERE 1 = 1
       AND cmo.official = 1
       AND UPPER(cmo.geography_grain) = 'REGION_5'
       AND NOT UPPER(sup.Crg_Chrome) IN ('HEAD', 'UNK')
       AND UPPER(hw.product_lifecycle_status) = 'N'
       AND UPPER(hw.technology) IN ('LASER', 'INK', 'PWA')
       AND CAST(cmo.load_date AS DATE) > '2021-11-15')

   , prod_crg_mix_market10 AS
    (SELECT cmo.cal_date
          , cmo.geography               AS market10
          , cmo.platform_subset
          , cmo.Crg_Base_Prod_Number
          , cmo.customer_engagement
          , cmo.mix_pct
          , CASE
                WHEN hw.technology = 'LASER' AND CAST(cmo.load_date AS DATE) > '2021-11-15' THEN 'PAGE_MIX'
                WHEN hw.technology <> 'LASER' THEN 'PAGE_MIX'
                ELSE 'crg_mix' END      AS upload_type -- HARD-CODED cut-line from cartridge mix to page/ccs mix; page_mix is page/ccs mix
          , CASE
                WHEN NOT sup.k_color IS NULL THEN sup.k_color
                WHEN sup.k_color IS NULL AND sup.Crg_Chrome IN ('K', 'BLK') THEN 'BLACK'
                WHEN sup.k_color IS NULL AND sup.Crg_Chrome IN ('C', 'CYN', 'M', 'MAG', 'Y', 'YEL') THEN 'COLOR'
                ELSE NULL END           AS k_color
          , CASE
                WHEN sup.Crg_Chrome = 'C' THEN 'CYN'
                WHEN sup.Crg_Chrome = 'M' THEN 'MAG'
                WHEN sup.Crg_Chrome = 'Y' THEN 'YEL'
                WHEN sup.Crg_Chrome = 'K' THEN 'BLK'
                ELSE sup.Crg_Chrome END AS Crg_Chrome
          , CASE
                WHEN sup.Crg_Chrome IN ('DRUM') THEN 'DRUM'
                ELSE 'CARTRIDGE' END    AS consumable_type
          , cmo.load_date
     FROM prod.cartridge_mix_override AS cmo
              JOIN mdm.supplies_xref AS sup
                   ON cmo.Crg_base_prod_number = sup.base_product_number
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = cmo.platform_subset
     WHERE 1 = 1
       AND cmo.official = 1
       AND UPPER(cmo.geography_grain) = 'MARKET10'
       AND NOT UPPER(sup.Crg_Chrome) IN ('HEAD', 'UNK')
       AND UPPER(hw.product_lifecycle_status) = 'N'
       AND UPPER(hw.technology) IN ('LASER', 'INK', 'PWA')
       AND CAST(cmo.load_date AS DATE) > '2021-11-15')

   , prod_crg_mix AS
    (SELECT cmo.cal_date
          , cmo.market10
          , cmo.platform_subset
          , cmo.Crg_Base_Prod_Number
          , cmo.customer_engagement
          , cmo.mix_pct
          , cmo.upload_type
          , cmo.k_color
          , cmo.Crg_Chrome
          , cmo.consumable_type
          , cmo.load_date
     FROM prod_crg_mix_region5 AS cmo

     UNION ALL

     SELECT cmo.cal_date
          , cmo.market10
          , cmo.platform_subset
          , cmo.Crg_Base_Prod_Number
          , cmo.customer_engagement
          , cmo.mix_pct
          , cmo.upload_type
          , cmo.k_color
          , cmo.Crg_Chrome
          , cmo.consumable_type
          , cmo.load_date
     FROM prod_crg_mix_market10 AS cmo)

   , prod_crg_mix_filter AS
    (SELECT cmo.cal_date
          , cmo.market10
          , cmo.platform_subset
          , cmo.customer_engagement
          , cmo.k_color
          , MAX(cmo.load_date) AS filter
     FROM prod_crg_mix AS cmo
     GROUP BY cmo.cal_date
            , cmo.market10
            , cmo.platform_subset
            , cmo.customer_engagement
            , cmo.k_color)

   , add_type_and_yield AS
    (SELECT cmo.cal_date
          , cmo.market10
          , cmo.k_color
          , cmo.Crg_Chrome
          , cmo.consumable_type
          , cmo.platform_subset
          , cmo.Crg_Base_Prod_Number                                                                                                  AS base_product_number
          , cmo.customer_engagement
          , COUNT(cmo.Crg_Base_Prod_Number) OVER
            (PARTITION BY cmo.platform_subset, cmo.customer_engagement, cmo.market10, cmo.cal_date, cmo.k_color, cmo.consumable_type) AS product_count
          , cmo.mix_pct
          , cmo.upload_type
          , pf.yield
     FROM prod_crg_mix AS cmo
              JOIN prod_crg_mix_filter AS filter
                   ON filter.cal_date = cmo.cal_date
                       AND filter.market10 = cmo.market10
                       AND filter.platform_subset = cmo.platform_subset
                       AND filter.customer_engagement = cmo.customer_engagement
                       AND filter.k_color =
                           cmo.k_color -- key condition for crg mix logic (does it impact future uploads of page_mix?)
                       AND filter.filter =
                           cmo.load_date -- filter to most recent upload by JOIN conditions; if not used then pens can drop/add throwing off mix_rate
              JOIN pen_fills AS pf
                   ON cmo.Crg_Base_Prod_Number = pf.base_product_number
                       AND cmo.market10 = pf.market_10
                       AND cmo.cal_date = pf.cal_date
     WHERE 1 = 1)

   , transform_pages_ccs AS
    (SELECT cmo.cal_date
          , cmo.geography_grain
          , cmo.geography
          , cmo.platform_subset
          , cmo.base_product_number
          , CASE WHEN s.single_multi = 'TRI-PACK' THEN 'MULTI'
                                                  ELSE 'SINGLE' END AS single_multi
          , UPPER(cmo.k_color)                                      AS k_color
          , cmo.crg_chrome
          , cmo.consumable_type
          , cmo.customer_engagement
          , cmo.cartridge_mix
          , pf.yield
          , cmo.cartridge_mix * pf.yield                            AS pages_ccs
     FROM prod.cartridge_mix_override_transform AS cmo
     JOIN mdm.supplies_xref AS s
         ON s.base_product_number = cmo.base_product_number
     JOIN pen_fills AS pf
         ON pf.base_product_number = cmo.base_product_number
         AND pf.cal_date = cmo.cal_date
         AND pf.market_10 = cmo.geography)

   , transform_mix_step_1_k AS
    (SELECT t.cal_date
          , t.geography_grain
          , t.geography
          , t.platform_subset
          , t.base_product_number
          , t.customer_engagement
          , t.single_multi
          , t.k_color
          , t.crg_chrome
          , t.consumable_type
          , t.yield
          , t.pages_ccs
          , t.pages_ccs / NULLIF(SUM(pages_ccs)
                                 OVER (PARTITION BY t.cal_date, t.geography, t.platform_subset, t.customer_engagement, t.crg_chrome),
                                 0) AS mix_step_1
     FROM transform_pages_ccs AS t
     WHERE 1 = 1
       AND t.k_color = 'BLACK')

   , transform_mix_step_2_color_single AS
    (SELECT t.cal_date
          , t.geography_grain
          , t.geography
          , t.platform_subset
          , t.base_product_number
          , t.customer_engagement
          , t.single_multi
          , t.k_color
          , t.crg_chrome
          , t.consumable_type
          , t.yield
          , t.pages_ccs
          , t.pages_ccs / NULLIF(SUM(pages_ccs)
                                 OVER (PARTITION BY t.cal_date, t.geography, t.platform_subset, t.customer_engagement, t.crg_chrome),
                                 0) AS mix_step_1
     FROM transform_pages_ccs AS t
     WHERE 1 = 1
       AND t.k_color = 'COLOR'
       AND t.single_multi = 'SINGLE')

   , transform_mix_step_3_color_multi  AS
    (SELECT t.cal_date
          , t.geography_grain
          , t.geography
          , t.platform_subset
          , t.base_product_number
          , t.customer_engagement
          , t.single_multi
          , t.k_color
          , t.crg_chrome
          , t.consumable_type
          , t.yield
          , t.pages_ccs
          -- individual tri-pack ... pcm_20
          , SUM(t.pages_ccs)
            OVER (PARTITION BY t.cal_date, t.geography, t.platform_subset) *
            1.0 /
            NULLIF(SUM(pages_ccs)
                   OVER (PARTITION BY t.cal_date, t.geography, t.platform_subset),
                   0) AS mix_step_1
          -- more than 1 tri-pack per pfs ... pcm_19
          , SUM(t.pages_ccs)
            OVER (PARTITION BY t.cal_date, t.geography, t.platform_subset, t.single_multi) *
            1.0 /
            NULLIF(SUM(pages_ccs)
                   OVER (PARTITION BY t.cal_date, t.geography, t.platform_subset),
                   0) AS mix_step_1_all
     FROM transform_pages_ccs AS t
     WHERE 1 = 1
       AND t.k_color = 'COLOR') -- need all color crgs in the denominator

   , transform_mix_step_4_multi_fix AS
    (SELECT t1.cal_date
          , t1.geography_grain
          , t1.geography
          , t1.platform_subset
          , t1.base_product_number
          , t1.customer_engagement
          , t1.single_multi
          , t1.k_color
          , t1.crg_chrome
          , t1.consumable_type
          , t1.yield
          , t1.pages_ccs
          , t1.mix_step_1
          -- CKs w/o a multipack have to be accounted for
          , CASE WHEN NOT t2.platform_subset IS NULL
                     THEN t1.mix_step_1 * -1.0 * t2.mix_step_1_all
                     ELSE 0.0 END AS mix_step_2
     FROM transform_mix_step_2_color_single AS t1
     LEFT JOIN transform_mix_step_3_color_multi AS t2
         ON t2.cal_date = t1.cal_date
         AND t2.geography = t1.geography
         AND t2.platform_subset = t1.platform_subset
         AND t2.single_multi <> 'SINGLE'
     WHERE 1 = 1)

   , transform_combined AS
    (SELECT t1.cal_date
          , t1.geography_grain
          , t1.geography
          , t1.platform_subset
          , t1.base_product_number
          , t1.customer_engagement
          , t1.single_multi
          , t1.k_color
          , t1.crg_chrome
          , t1.consumable_type
          , t1.yield
          , t1.pages_ccs
          , t1.mix_step_1
          , NULL          AS mix_step_2
          , t1.mix_step_1 AS pgs_ccs_mix
     FROM transform_mix_step_1_k AS t1
     CROSS JOIN c2c_vtc_02_forecast_months AS fm
     WHERE 1 = 1
       AND t1.cal_date >= fm.supplies_forecast_start

     UNION ALL

     SELECT t4.cal_date
          , t4.geography_grain
          , t4.geography
          , t4.platform_subset
          , t4.base_product_number
          , t4.customer_engagement
          , t4.single_multi
          , t4.k_color
          , t4.crg_chrome
          , t4.consumable_type
          , t4.yield
          , t4.pages_ccs
          , t4.mix_step_1
          , t4.mix_step_2
          , t4.mix_step_1 + t4.mix_step_2 AS pgs_ccs_mix
     FROM transform_mix_step_4_multi_fix AS t4 -- singles only
     CROSS JOIN c2c_vtc_02_forecast_months AS fm
     WHERE 1 = 1
       AND t4.cal_date >= fm.supplies_forecast_start

     UNION ALL

     SELECT t3.cal_date
          , t3.geography_grain
          , t3.geography
          , t3.platform_subset
          , t3.base_product_number
          , t3.customer_engagement
          , t3.single_multi
          , t3.k_color
          , t3.crg_chrome
          , t3.consumable_type
          , t3.yield
          , t3.pages_ccs
          , t3.mix_step_1
          , NULL          AS mix_step_2
          , t3.mix_step_1 AS pgs_ccs_mix
     FROM transform_mix_step_3_color_multi AS t3
     CROSS JOIN c2c_vtc_02_forecast_months AS fm
     WHERE 1 = 1
       AND t3.cal_date >= fm.supplies_forecast_start
       AND t3.single_multi <> 'SINGLE')

   , output AS
    (SELECT 'UPLOADS' AS type
          , cal_date
          , market10
          , platform_subset
          , base_product_number
          , customer_engagement
          , mix_pct   AS mix_pct
     FROM add_type_and_yield

     UNION ALL

     SELECT 'TRANSFORMED_UPLOADS' AS type
          , t.cal_date
          , t.geography           AS market10
          , t.platform_subset
          , t.base_product_number
          , t.customer_engagement
          , t.pgs_ccs_mix         AS mix_pct
     FROM transform_combined AS t
     LEFT JOIN stage.page_cc_mix_override AS p
         ON p.cal_date = t.cal_date
         AND p.market10 = t.geography
         AND p.platform_subset = t.platform_subset
         AND p.customer_engagement = t.customer_engagement
     WHERE 1 = 1
       AND p.cal_date IS NULL
       AND p.market10 IS NULL
       AND p.platform_subset IS NULL
       AND p.customer_engagement IS NULL)

SELECT type
    , cal_date
    , market10
    , platform_subset
    , base_product_number
    , customer_engagement
    , mix_pct
FROM output
"""

query_list.append(["stage.page_cc_mix_override", page_cc_mix_override, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_mix_complete

# COMMAND ----------

page_mix_complete = """
WITH pcm_27_pages_mix_prep AS
    (
        -- engine mix; precedence to pcm_26 (forecaster overrides)
        SELECT m19.type
             , m19.cal_date
             , m19.geography_grain
             , m19.geography
             , m19.platform_subset
             , m19.base_product_number
             , m19.customer_engagement
             , m19.page_mix
        FROM stage.page_mix_engine AS m19
                 LEFT OUTER JOIN stage.page_cc_mix_override AS m26
                                 ON m26.cal_date = m19.cal_date
                                     AND UPPER(m26.market10) = UPPER(m19.geography)
                                     AND UPPER(m26.platform_subset) = UPPER(m19.platform_subset)
                                     AND UPPER(m26.customer_engagement) = UPPER(m19.customer_engagement)
        WHERE 1 = 1
          AND m26.cal_date IS NULL
          AND m26.market10 IS NULL
          AND m26.platform_subset IS NULL
          AND m26.customer_engagement IS NULL

        UNION ALL

        -- page mix uploads and transforms
        SELECT 'PCM_FORECASTER_OVERRIDE' AS type
             , m26.cal_date
             , 'MARKET10' AS geography_grain
             , m26.market10 AS geography
             , m26.platform_subset
             , m26.base_product_number
             , m26.customer_engagement
             , m26.mix_pct AS page_mix
        FROM stage.page_cc_mix_override AS m26
        JOIN mdm.hardware_xref AS hw
            ON hw.platform_subset = m26.platform_subset
        WHERE 1=1
          AND hw.technology = 'LASER')

   , pcm_28_pages_ccs_mix_filter AS
    (SELECT m27.type
          , m27.cal_date
          , m27.geography_grain
          , m27.geography
          , m27.platform_subset
          , m27.base_product_number
          , m27.customer_engagement
          , m27.page_mix
     FROM pcm_27_pages_mix_prep AS m27
     WHERE 1 = 1
       AND UPPER(m27.type) = 'PCM_ENGINE_ACTS' -- all actuals

     UNION ALL

     SELECT m27.type
          , m27.cal_date
          , m27.geography_grain
          , m27.geography
          , m27.platform_subset
          , m27.base_product_number
          , m27.customer_engagement
          , m27.page_mix
     FROM pcm_27_pages_mix_prep AS m27
              JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(m27.platform_subset)
     WHERE 1 = 1
       AND UPPER(m27.type) = 'PCM_ENGINE_PROJECTION'
       AND UPPER(hw.product_lifecycle_status) <> 'N' -- all forecast NOT NPIs

     UNION ALL

     SELECT m27.type
          , m27.cal_date
          , m27.geography_grain
          , m27.geography
          , m27.platform_subset
          , m27.base_product_number
          , m27.customer_engagement
          , m27.page_mix
     FROM pcm_27_pages_mix_prep AS m27
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = m27.platform_subset
     WHERE 1 = 1
       AND UPPER(m27.type) IN ('PCM_FORECASTER_OVERRIDE')
       AND UPPER(hw.product_lifecycle_status) = 'N' -- all forecast NPIs
    )

SELECT m28.type
     , m28.cal_date
     , m28.geography_grain
     , m28.geography
     , m28.platform_subset
     , m28.base_product_number
     , m28.customer_engagement
     , m28.page_mix
     , CAST(m28.cal_date AS VARCHAR) + ' ' + m28.geography + ' ' + m28.platform_subset + ' ' +
       m28.base_product_number + ' ' + m28.customer_engagement AS composite_key
FROM pcm_28_pages_ccs_mix_filter AS m28
         JOIN stage.shm_base_helper AS shm
              ON UPPER(shm.geography) = UPPER(m28.geography)
                  AND UPPER(shm.platform_subset) = UPPER(m28.platform_subset)
                  AND UPPER(shm.base_product_number) = UPPER(m28.base_product_number)
                  AND UPPER(shm.customer_engagement) = UPPER(m28.customer_engagement)
"""

query_list.append(["stage.page_mix_complete", page_mix_complete, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## cc_mix_complete

# COMMAND ----------

cc_mix_complete = """
WITH pcm_27_pages_ccs_mix_prep AS
    (
        -- engine mix; precedence to pcm_26 (forecaster overrides)
        SELECT m19.type
             , m19.cal_date
             , m19.geography_grain
             , m19.geography
             , m19.platform_subset
             , m19.base_product_number
             , m19.customer_engagement
             , m19.cc_mix
        FROM stage.cc_mix_engine AS m19
                 LEFT OUTER JOIN stage.page_cc_mix_override AS m26
                                 ON m26.cal_date = m19.cal_date
                                     AND UPPER(m26.market10) = UPPER(m19.geography)
                                     AND UPPER(m26.platform_subset) = UPPER(m19.platform_subset)
                                     AND UPPER(m26.customer_engagement) = UPPER(m19.customer_engagement)
        WHERE 1 = 1
          AND m26.cal_date IS NULL
          AND m26.market10 IS NULL
          AND m26.platform_subset IS NULL
          AND m26.customer_engagement IS NULL

        UNION ALL

        -- cc mix uploads
        SELECT 'PCM_FORECASTER_OVERRIDE' AS type
             , m26.cal_date
             , 'MARKET10' AS geography_grain
             , m26.market10 AS geography
             , m26.platform_subset
             , m26.base_product_number
             , m26.customer_engagement
             , m26.mix_pct AS cc_mix
        FROM stage.page_cc_mix_override AS m26
        JOIN mdm.hardware_xref AS hw
            ON hw.platform_subset = m26.platform_subset
        WHERE 1=1
          AND m26.type = 'UPLOADS'  -- no transformed mix required for ink
          AND hw.technology IN ('INK', 'PWA'))

   , pcm_28_pages_ccs_mix_filter AS
    (SELECT m27.type
          , m27.cal_date
          , m27.geography_grain
          , m27.geography
          , m27.platform_subset
          , m27.base_product_number
          , m27.customer_engagement
          , m27.cc_mix
     FROM pcm_27_pages_ccs_mix_prep AS m27
     WHERE 1 = 1
       AND UPPER(m27.type) = 'PCM_ENGINE_ACTS' -- all actuals

     UNION ALL

     SELECT m27.type
          , m27.cal_date
          , m27.geography_grain
          , m27.geography
          , m27.platform_subset
          , m27.base_product_number
          , m27.customer_engagement
          , m27.cc_mix
     FROM pcm_27_pages_ccs_mix_prep AS m27
              JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(m27.platform_subset)
     WHERE 1 = 1
       AND UPPER(m27.type) = 'PCM_ENGINE_PROJECTION'
       AND UPPER(hw.product_lifecycle_status) <> 'N' -- all forecast NOT NPIs

     UNION ALL

     SELECT m27.type
          , m27.cal_date
          , m27.geography_grain
          , m27.geography
          , m27.platform_subset
          , m27.base_product_number
          , m27.customer_engagement
          , m27.cc_mix
     FROM pcm_27_pages_ccs_mix_prep AS m27
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = m27.platform_subset
     WHERE 1 = 1
       AND m27.type IN ('PCM_FORECASTER_OVERRIDE')
       AND UPPER(hw.product_lifecycle_status) = 'N' -- all forecast NPIs
    )

SELECT m28.type
     , m28.cal_date
     , m28.geography_grain
     , m28.geography
     , m28.platform_subset
     , m28.base_product_number
     , m28.customer_engagement
     , m28.cc_mix
     , CAST(m28.cal_date AS VARCHAR) + ' ' + m28.geography + ' ' + m28.platform_subset + ' ' +
       m28.base_product_number + ' ' + m28.customer_engagement AS composite_key
FROM pcm_28_pages_ccs_mix_filter AS m28
         JOIN stage.shm_base_helper AS shm
              ON UPPER(shm.geography) = UPPER(m28.geography)
                  AND UPPER(shm.platform_subset) = UPPER(m28.platform_subset)
                  AND UPPER(shm.base_product_number) = UPPER(m28.base_product_number)
                  AND UPPER(shm.customer_engagement) = UPPER(m28.customer_engagement)
"""

query_list.append(["stage.cc_mix_complete", cc_mix_complete, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_cc_mix

# COMMAND ----------

page_cc_mix = """
SELECT type
     , cal_date
     , geography_grain
     , geography
     , platform_subset
     , base_product_number
     , customer_engagement
     , cc_mix AS mix_rate
     , composite_key
FROM stage.cc_mix_complete

UNION ALL

SELECT type
     , cal_date
     , geography_grain
     , geography
     , platform_subset
     , base_product_number
     , customer_engagement
     , page_mix AS mix_rate
     , composite_key
FROM stage.page_mix_complete
"""

query_list.append(["stage.page_cc_mix", page_cc_mix, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Redshift

# COMMAND ----------

# MAGIC %run "../../common/output_to_redshift" $query_list=query_list
