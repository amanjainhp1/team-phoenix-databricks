# Databricks notebook source
# MAGIC %md
# MAGIC # Ink Working Forecast

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 1 :: select correct u/s override records, blow out to market10 for region_5 records

# COMMAND ----------

ink_01_us_uploads = """
WITH geography_mapping AS
    (SELECT DISTINCT market10 AS market_10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND NOT region_5 IS NULL
       AND NOT region_5 = 'JP'
       AND NOT region_5 LIKE 'X%')

    , override_filters AS
     (SELECT DISTINCT 'SCENARIO_USAGE_SHARE'                 AS record
                    , us_scen.user_name                      AS user_name
                    , us_scen.load_date                      AS load_date
                    , CAST(us_scen.load_date AS VARCHAR(50)) AS version
      FROM scen.working_forecast_usage_share AS us_scen
      WHERE 1 = 1
        AND us_scen.upload_type = 'WORKING-FORECAST'
        AND us_scen.user_name IN ('SAIMANK', 'SONSEEAHRAYR', 'ZACP'))

   , ink_us_prep AS
    (SELECT fv.user_name
          , MAX(fv.load_date) AS max_load_date
     FROM override_filters AS fv
     WHERE 1 = 1
       AND fv.record = 'SCENARIO_USAGE_SHARE'
     GROUP BY fv.user_name)

   , ink_us_subset AS
    (SELECT us.geography_grain
          , us.geography
          , DATEADD(MONTH, us.month_num, us.min_sys_date) AS cal_date
          , us.customer_engagement
          , us.platform_subset
          , us.measure
          , us.value                                      AS units
          , us.user_name
          , us.load_date
          , CAST(us.load_date AS VARCHAR(50))             AS version -- needed to work with base model
     FROM scen.working_forecast_usage_share AS us
     JOIN ink_us_prep AS usp
         ON usp.user_name = us.user_name
         AND usp.max_load_date = us.load_date
     WHERE 1 = 1
       AND us.upload_type = 'WORKING-FORECAST'
       AND us.geography_grain = 'MARKET10'

     UNION ALL

     SELECT 'MARKET10'                                    AS geography_grain
          , geo.market_10                                 AS geography
          , DATEADD(MONTH, us.month_num, us.min_sys_date) AS cal_date
          , us.customer_engagement
          , us.platform_subset
          , us.measure
          , us.value                                      AS units
          , us.user_name
          , us.load_date
          , CAST(us.load_date AS VARCHAR(50))             AS version -- needed to work with base model
     FROM scen.working_forecast_usage_share AS us
     JOIN ink_us_prep AS usp
         ON usp.user_name = us.user_name
         AND usp.max_load_date = us.load_date
     JOIN geography_mapping AS geo
         ON geo.region_5 = us.geography
     WHERE 1 = 1
       AND us.upload_type = 'WORKING-FORECAST'
       AND us.geography_grain = 'REGION_5')
       
 select * from ink_us_subset
"""

query_list.append(["scen.ink_01_us_uploads", ink_01_us_uploads, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 2 :: tranform u/s uploads to demand measures

# COMMAND ----------

ink_02_us_dmd = """
WITH dmd_01_ib_load AS
    (SELECT ib.cal_date
          , ib.platform_subset
          , ib.customer_engagement
          , ccx.market10 AS geography
          , ib.measure
          , ib.units
          , ib.version
     FROM prod.ib AS ib
              JOIN mdm.iso_country_code_xref AS ccx
                   ON ccx.country_alpha2 = ib.country_alpha2
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = ib.platform_subset
     WHERE 1 = 1
       AND ib.version = (SELECT MAX(version) FROM prod.ib)
       AND NOT UPPER(hw.product_lifecycle_status) = 'E'
       AND UPPER(hw.technology) IN ('INK', 'PWA')
       AND ib.cal_date > CAST('2015-10-01' AS DATE))

   , dmd_02_ib AS
    (SELECT ib.cal_date
          , ib.platform_subset
          , ib.customer_engagement
          , ib.geography
          , SUM(CASE WHEN UPPER(ib.measure) = 'IB' THEN ib.units END) AS ib
          , ib.version
     FROM dmd_01_ib_load AS ib
     GROUP BY ib.cal_date
            , ib.platform_subset
            , ib.customer_engagement
            , ib.geography
            , ib.version)

   , dmd_03_us_load AS
    (SELECT us.geography
          , us.cal_date                         AS year_month_start
          , CASE
            WHEN hw.technology = 'LASER' AND
                 us.platform_subset LIKE '%STND%'
                THEN 'STD'
            WHEN hw.technology = 'LASER' AND
                 us.platform_subset LIKE '%YET2%'
                THEN 'HP+'
                ELSE us.customer_engagement END AS customer_engagement
          , us.platform_subset
          , us.measure
          , us.units
     FROM scen.ink_01_us_uploads AS us
     JOIN mdm.hardware_xref AS hw
         ON hw.platform_subset = us.platform_subset
     WHERE 1 = 1
       AND UPPER(us.measure) IN
           ('USAGE', 'COLOR_USAGE', 'K_USAGE', 'HP_SHARE')
       AND UPPER(us.geography_grain) = 'MARKET10'
       AND NOT UPPER(hw.product_lifecycle_status) = 'E'
       AND UPPER(hw.technology) IN ('INK', 'PWA')
       AND us.cal_date > CAST('2015-10-01' AS DATE))

   , dmd_04_us_agg AS
    (SELECT us.geography
          , us.year_month_start
          , us.customer_engagement
          , us.platform_subset
          , MAX(CASE
                    WHEN UPPER(us.measure) = 'USAGE' THEN us.units
                    ELSE NULL END) AS usage
          , MAX(CASE
                    WHEN UPPER(us.measure) = 'COLOR_USAGE' THEN us.units
                    ELSE NULL END) AS color_usage
          , MAX(CASE
                    WHEN UPPER(us.measure) = 'K_USAGE' THEN us.units
                    ELSE NULL END) AS k_usage
          , MAX(CASE
                    WHEN UPPER(us.measure) = 'HP_SHARE' THEN us.units
                    ELSE NULL END) AS hp_share
     FROM dmd_03_us_load AS us
     GROUP BY us.geography
            , us.year_month_start
            , us.customer_engagement
            , us.platform_subset)

   , dmd_05_us AS
    (SELECT us.geography
          , us.year_month_start
          , us.customer_engagement
          , us.platform_subset
          , CASE
                WHEN us.color_usage IS NULL AND us.k_usage IS NULL
                    THEN usage
                ELSE us.k_usage END AS k_usage
          , us.color_usage
          , us.hp_share
          , 0                       AS fraction_host
     FROM dmd_04_us_agg AS us)

   , dmd_06_us_ib AS
    (SELECT ib.cal_date
          , ib.platform_subset
          , ib.customer_engagement
          , ib.geography
          , ib.ib
          , ib.version
          , us.color_usage
          , us.k_usage
          , us.hp_share
          , us.fraction_host
     FROM dmd_05_us AS us
              JOIN dmd_02_ib AS ib
                   ON us.geography = ib.geography
                       AND us.year_month_start = ib.cal_date
                       AND
                      us.platform_subset = ib.platform_subset
                       AND us.customer_engagement =
                           ib.customer_engagement
     GROUP BY ib.version
            , ib.cal_date
            , ib.platform_subset
            , ib.customer_engagement
            , ib.geography
            , ib.ib
            , us.color_usage
            , us.k_usage
            , us.hp_share
            , us.fraction_host)

   , dmd_07_calcs AS
    (SELECT cal_date
          , geography
          , platform_subset
          , customer_engagement
          , version
          , SUM(ib)                            AS IB
          , SUM(k_usage * hp_share * ib)       AS HP_K_PAGES
          , SUM(k_usage * (1 - hp_share) * ib) AS NON_HP_K_PAGES
          , SUM(color_usage * hp_share * ib)   AS HP_C_PAGES
          , SUM(color_usage *
                (1 - hp_share) *
                ib)                            AS NON_HP_C_PAGES
          , SUM(color_usage * ib)              AS TOTAL_C_PAGES
          , SUM(k_usage * ib)                  AS TOTAL_K_PAGES
     FROM dmd_06_us_ib
     GROUP BY cal_date
            , geography
            , platform_subset
            , customer_engagement
            , version)

   , dmd_08_unpivot_measure AS
    (SELECT cal_date
          , geography
          , platform_subset
          , customer_engagement
          , measure
          , units
          , version
     FROM dmd_07_calcs UNPIVOT (
                                units FOR measure IN (IB, HP_K_PAGES, NON_HP_K_PAGES, HP_C_PAGES, NON_HP_C_PAGES, TOTAL_C_PAGES, TOTAL_K_PAGES)
         ) AS final_unpivot)

SELECT 'DEMAND'           AS record
     , dmd.cal_date
     , 'MARKET10'         AS geography_grain
     , dmd.geography
     , dmd.platform_subset
     , dmd.customer_engagement
     , dmd.measure
     , dmd.units
     , dmd.version
     , 'PAGES'            AS source
     , CAST(NULL AS DATE) AS load_date
FROM dmd_08_unpivot_measure AS dmd
"""

query_list.append(["scen.ink_02_us_dmd", ink_02_us_dmd, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 2b :: transform u/s to demand measures
# MAGIC Deprecate after these measures exist in prod.usage_share

# COMMAND ----------

# create spectrum schema var based on stack/env so this query works in all our envs
spectrum_schema = phoenix_spectrum
if stack == 'itg' or stack == 'prod':
    spectrum_schema = spectrum_schema + "_" + stack

ink_demand = f"""
WITH dbd_01_ib_load AS
    (SELECT ib.cal_date
          , ib.platform_subset
          , ib.customer_engagement
          , ccx.market10 AS geography
          , ib.measure
          , ib.units
     FROM prod.ib AS ib
              JOIN mdm.iso_country_code_xref AS ccx
                   ON ccx.country_alpha2 = ib.country_alpha2
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = ib.platform_subset
     WHERE 1 = 1
       AND ib.version = (SELECT MAX(version) FROM prod.ib)
       AND NOT UPPER(hw.product_lifecycle_status) = 'E'
       AND UPPER(hw.technology) IN ('LASER', 'INK', 'PWA')
       AND ib.cal_date > CAST('2015-10-01' AS DATE))

   , dmd_02_ib AS
    (SELECT ib.cal_date
          , ib.platform_subset
          , ib.customer_engagement
          , ib.geography
          , SUM(CASE WHEN UPPER(ib.measure) = 'IB' THEN ib.units END) AS ib
     FROM dbd_01_ib_load AS ib
     GROUP BY ib.cal_date
            , ib.platform_subset
            , ib.customer_engagement
            , ib.geography)

   , dmd_03_us_load AS
    (SELECT us.geography
          , us.cal_date                         AS year_month_start
          , CASE
                WHEN hw.technology = 'LASER' AND
                     us.platform_subset LIKE '%STND%'
                    THEN 'STD'
                WHEN hw.technology = 'LASER' AND
                     us.platform_subset LIKE '%YET2%'
                    THEN 'HP+'
                ELSE us.customer_engagement END AS customer_engagement
          , us.platform_subset
          , us.measure
          , us.units
     FROM {spectrum_schema}.usage_share AS us
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = us.platform_subset
     WHERE 1 = 1
       AND us.version = (SELECT MAX(version) FROM {spectrum_schema}.usage_share)
       AND UPPER(us.measure) IN
           ('USAGE', 'COLOR_USAGE', 'K_USAGE', 'HP_SHARE')
       AND UPPER(us.geography_grain) = 'MARKET10'
       AND NOT UPPER(hw.product_lifecycle_status) = 'E'
       AND UPPER(hw.technology) IN ('LASER', 'INK', 'PWA')
       AND us.cal_date > CAST('2015-10-01' AS DATE))

   , dmd_04_us_agg AS
    (SELECT us.geography
          , us.year_month_start
          , us.customer_engagement
          , us.platform_subset
          , MAX(CASE
                    WHEN UPPER(us.measure) = 'USAGE' THEN us.units
                    ELSE NULL END) AS usage
          , MAX(CASE
                    WHEN UPPER(us.measure) = 'COLOR_USAGE' THEN us.units
                    ELSE NULL END) AS color_usage
          , MAX(CASE
                    WHEN UPPER(us.measure) = 'K_USAGE' THEN us.units
                    ELSE NULL END) AS k_usage
          , MAX(CASE
                    WHEN UPPER(us.measure) = 'HP_SHARE' THEN us.units
                    ELSE NULL END) AS hp_share
     FROM dmd_03_us_load AS us
     GROUP BY us.geography
            , us.year_month_start
            , us.customer_engagement
            , us.platform_subset)

   , dmd_05_us AS
    (SELECT us.geography
          , us.year_month_start
          , us.customer_engagement
          , us.platform_subset
          , CASE
                WHEN us.color_usage IS NULL AND us.k_usage IS NULL
                    THEN usage
                ELSE us.k_usage END AS k_usage
          , us.color_usage
          , us.hp_share
          , 0                       AS fraction_host
     FROM dmd_04_us_agg AS us)

   , dmd_06_us_ib AS
    (SELECT ib.cal_date
          , ib.platform_subset
          , ib.customer_engagement
          , ib.geography
          , ib.ib
          , us.color_usage
          , us.k_usage
          , us.hp_share
          , us.fraction_host
     FROM dmd_05_us AS us
              JOIN dmd_02_ib AS ib
                   ON us.geography = ib.geography
                       AND us.year_month_start = ib.cal_date
                       AND
                      us.platform_subset = ib.platform_subset
                       AND us.customer_engagement =
                           ib.customer_engagement)

   , dmd_07_calcs AS
    (SELECT cal_date
          , geography
          , platform_subset
          , customer_engagement
          , SUM(ib)                            AS IB
          , SUM(k_usage * hp_share * ib)       AS HP_K_PAGES
          , SUM(k_usage * (1 - hp_share) * ib) AS NON_HP_K_PAGES
          , SUM(color_usage * hp_share * ib)   AS HP_C_PAGES
          , SUM(color_usage *
                (1 - hp_share) *
                ib)                            AS NON_HP_C_PAGES
          , SUM(color_usage * ib)              AS TOTAL_C_PAGES
          , SUM(k_usage * ib)                  AS TOTAL_K_PAGES
     FROM dmd_06_us_ib
     GROUP BY cal_date
            , geography
            , platform_subset
            , customer_engagement)

   , dmd_09_unpivot_measure AS
    (SELECT cal_date
          , geography
          , platform_subset
          , customer_engagement
          , measure
          , units
     FROM dmd_07_calcs UNPIVOT (
                                units FOR measure IN (IB, HP_K_PAGES, NON_HP_K_PAGES, HP_C_PAGES, NON_HP_C_PAGES, TOTAL_C_PAGES, TOTAL_K_PAGES)
         ) AS final_unpivot)

SELECT dmd.cal_date
      , dmd.geography
      , dmd.platform_subset
      , dmd.customer_engagement
      , dmd.measure
      , dmd.units
 FROM dmd_09_unpivot_measure AS dmd
"""

query_list.append(["scen.ink_demand", ink_demand, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 3 :: combine uploads with production data (concat + left outer join)

# COMMAND ----------

ink_03_usage_share = """
WITH override_filters  AS
    (SELECT MAX(ib.version)    AS ib_version
          , MAX(us.us_version) AS us_version
     FROM prod.ib
     CROSS JOIN (SELECT MAX(version) AS us_version
                 FROM prod.usage_share) AS us)

   , ink_usage_share AS
    (SELECT uss.geography_grain
          , uss.geography
          , uss.cal_date
          , uss.customer_engagement
          , uss.platform_subset
          , uss.measure
          , uss.units
          , uss.version
          , fv.us_version
          , fv.ib_version
     FROM scen.ink_02_us_dmd AS uss
     CROSS JOIN override_filters AS fv
     WHERE 1 = 1

     UNION ALL

     SELECT 'MARKET10' AS geography_grain
          , us.geography
          , us.cal_date
          , us.customer_engagement
          , us.platform_subset
          , us.measure
          , us.units
          , fv.us_version AS version
          , fv.us_version
          , fv.ib_version
     FROM scen.ink_demand AS us
     CROSS JOIN override_filters AS fv
     JOIN mdm.hardware_xref AS hw
         ON us.platform_subset = hw.platform_subset
     LEFT OUTER JOIN scen.ink_02_us_dmd AS us_scen
         ON us.geography = us_scen.geography
         AND us.cal_date = us_scen.cal_date
         AND
            us.platform_subset = us_scen.platform_subset
         AND us.customer_engagement =
             us_scen.customer_engagement
         AND us.measure = us_scen.measure
     WHERE 1 = 1
       AND hw.technology IN ('INK', 'PWA')
       AND us_scen.platform_subset IS NULL
       AND us_scen.geography IS NULL
       AND us_scen.customer_engagement IS NULL
       AND us_scen.cal_date IS NULL)
       
       SELECT * FROM ink_usage_share
"""

query_list.append(["scen.ink_03_usage_share", ink_03_usage_share, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 4 :: run base forecast page mix process

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4.1: shm_base_helper

# COMMAND ----------

ink_shm_base_helper = """
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
            WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%STND%'
                                         THEN 'STD'
            WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%YET2%'
                                         THEN 'HP+'
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
                   , iso.market10                                                                            AS geography
                   , CASE
         WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%STND%'
                                                                            THEN 'STD'
         WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%YET2%'
                                                                            THEN 'HP+'
         WHEN hw.technology = 'LASER'
                                                                            THEN 'TRAD'
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
                   , iso.market10                                                                            AS geography
                   , CASE
         WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%STND%'
                                                                            THEN 'STD'
         WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%YET2%'
                                                                            THEN 'HP+'
         WHEN hw.technology = 'LASER'
                                                                            THEN 'TRAD'
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

query_list.append(["scen.ink_shm_base_helper",ink_shm_base_helper, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4.2: cartridge_units

# COMMAND ----------

ink_cartridge_units = """
SELECT 'ACTUALS'                                          AS source
     , acts.cal_date
     , acts.base_product_number
     , cref.country_scenario                              AS geography_grain
     , cref.country_level_2                               AS geography
     , xref.k_color
     , xref.crg_chrome
     , CASE
    WHEN xref.crg_chrome IN ('DRUM') THEN 'DRUM'
                                     ELSE 'CARTRIDGE' END AS consumable_type
     , SUM(acts.base_quantity)                            AS cartridge_volume
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

query_list.append(["scen.ink_cartridge_units", ink_cartridge_units, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4.3: page_mix_engine

# COMMAND ----------

ink_page_mix_engine = """
WITH pcm_02_hp_demand AS
    (SELECT d.cal_date
          , d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_K_PAGES' THEN units END)     AS black_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_C_PAGES' THEN units END)     AS color_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_C_PAGES' THEN units END * 3) AS cmy_demand
     FROM scen.ink_03_usage_share AS d
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
     FROM scen.ink_cartridge_units AS v
              JOIN scen.ink_shm_base_helper AS shm
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
              JOIN scen.ink_shm_base_helper AS shm
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

SELECT 'PCM_ENGINE_PROJECTION'  AS type
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
       'PCM_ENGINE_PROJECTION'   AS composite_key
FROM pcm_14_projection AS m14
"""

query_list.append(["scen.ink_page_mix_engine", ink_page_mix_engine, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4.4: page_mix_override

# COMMAND ----------

ink_page_cc_mix_override = """
WITH crg_months            AS
    (SELECT date_key
          , date AS cal_date
     FROM mdm.calendar
     WHERE 1 = 1
       AND day_of_month = 1)

   , geography_mapping     AS
    (SELECT DISTINCT market10 AS market_10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND NOT region_5 IS NULL
       AND NOT region_5 = 'JP'
       AND NOT region_5 LIKE 'X%')

   , yield                 AS
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

   , pen_fills             AS
    (SELECT y.base_product_number
          , m.cal_date
          , y.market_10
          , y.yield
     FROM yield AS y
     JOIN crg_months AS m
         ON y.effective_date <= m.cal_date
         AND y.next_effective_date > m.cal_date)

   , prod_crg_mix_region5  AS
    (SELECT cmo.cal_date
          , map.market_10                                                             AS market10
          , cmo.platform_subset
          , cmo.crg_base_prod_number
          , cmo.customer_engagement
          , cmo.mix_pct
          , CASE
            WHEN hw.technology = 'LASER' AND
                 CAST(cmo.load_date AS DATE) > '2021-11-15' THEN 'PAGE_MIX'
            WHEN hw.technology <> 'LASER'                   THEN 'PAGE_MIX'
                                                            ELSE 'CRG_MIX' END        AS upload_type -- legacy
          , CASE
            WHEN NOT sup.k_color IS NULL                                THEN sup.k_color
            WHEN sup.k_color IS NULL AND sup.crg_chrome IN ('K', 'BLK')
                                                                        THEN 'BLACK'
            WHEN sup.k_color IS NULL AND
                 sup.crg_chrome IN ('C', 'CYN', 'M', 'MAG', 'Y', 'YEL')
                                                                        THEN 'COLOR'
                                                                        ELSE NULL END AS k_color
          , CASE
            WHEN sup.crg_chrome = 'C' THEN 'CYN'
            WHEN sup.crg_chrome = 'M' THEN 'MAG'
            WHEN sup.crg_chrome = 'Y' THEN 'YEL'
            WHEN sup.crg_chrome = 'K' THEN 'BLK'
                                      ELSE sup.crg_chrome END                         AS crg_chrome
          , CASE
            WHEN sup.crg_chrome IN ('DRUM') THEN 'DRUM'
                                            ELSE 'CARTRIDGE' END                      AS consumable_type
          , cmo.load_date
     FROM prod.cartridge_mix_override AS cmo
     JOIN mdm.supplies_xref AS sup
         ON cmo.crg_base_prod_number = sup.base_product_number
     JOIN mdm.hardware_xref AS hw
         ON hw.platform_subset = cmo.platform_subset
     JOIN geography_mapping AS map
         ON map.region_5 = cmo.geography
     WHERE 1 = 1
       AND cmo.official = 1
       AND UPPER(cmo.geography_grain) = 'REGION_5'
       AND NOT UPPER(sup.crg_chrome) IN ('HEAD', 'UNK')
       AND UPPER(hw.product_lifecycle_status) = 'N'
       AND UPPER(hw.technology) IN ('LASER', 'INK', 'PWA')
       AND CAST(cmo.load_date AS DATE) > '2021-11-15')

   , prod_crg_mix_market10 AS
    (SELECT cmo.cal_date
          , cmo.geography                                                             AS market10
          , cmo.platform_subset
          , cmo.crg_base_prod_number
          , cmo.customer_engagement
          , cmo.mix_pct
          , CASE
            WHEN hw.technology = 'LASER' AND
                 CAST(cmo.load_date AS DATE) > '2021-11-15' THEN 'PAGE_MIX'
            WHEN hw.technology <> 'LASER'                   THEN 'PAGE_MIX'
                                                            ELSE 'crg_mix' END        AS upload_type -- HARD-CODED cut-line from cartridge mix to page/ccs mix; page_mix is page/ccs mix
          , CASE
            WHEN NOT sup.k_color IS NULL                                THEN sup.k_color
            WHEN sup.k_color IS NULL AND sup.crg_chrome IN ('K', 'BLK')
                                                                        THEN 'BLACK'
            WHEN sup.k_color IS NULL AND
                 sup.crg_chrome IN ('C', 'CYN', 'M', 'MAG', 'Y', 'YEL')
                                                                        THEN 'COLOR'
                                                                        ELSE NULL END AS k_color
          , CASE
            WHEN sup.crg_chrome = 'C' THEN 'CYN'
            WHEN sup.crg_chrome = 'M' THEN 'MAG'
            WHEN sup.crg_chrome = 'Y' THEN 'YEL'
            WHEN sup.crg_chrome = 'K' THEN 'BLK'
                                      ELSE sup.crg_chrome END                         AS crg_chrome
          , CASE
            WHEN sup.crg_chrome IN ('DRUM') THEN 'DRUM'
                                            ELSE 'CARTRIDGE' END                      AS consumable_type
          , cmo.load_date
     FROM prod.cartridge_mix_override AS cmo
     JOIN mdm.supplies_xref AS sup
         ON cmo.crg_base_prod_number = sup.base_product_number
     JOIN mdm.hardware_xref AS hw
         ON hw.platform_subset = cmo.platform_subset
     WHERE 1 = 1
       AND cmo.official = 1
       AND UPPER(cmo.geography_grain) = 'MARKET10'
       AND NOT UPPER(sup.crg_chrome) IN ('HEAD', 'UNK')
       AND UPPER(hw.product_lifecycle_status) = 'N'
       AND UPPER(hw.technology) IN ('LASER', 'INK', 'PWA')
       AND CAST(cmo.load_date AS DATE) > '2021-11-15')

   , prod_crg_mix          AS
    (SELECT cmo.cal_date
          , cmo.market10
          , cmo.platform_subset
          , cmo.crg_base_prod_number
          , cmo.customer_engagement
          , cmo.mix_pct
          , cmo.upload_type
          , cmo.k_color
          , cmo.crg_chrome
          , cmo.consumable_type
          , cmo.load_date
     FROM prod_crg_mix_region5 AS cmo

     UNION ALL

     SELECT cmo.cal_date
          , cmo.market10
          , cmo.platform_subset
          , cmo.crg_base_prod_number
          , cmo.customer_engagement
          , cmo.mix_pct
          , cmo.upload_type
          , cmo.k_color
          , cmo.crg_chrome
          , cmo.consumable_type
          , cmo.load_date
     FROM prod_crg_mix_market10 AS cmo)

   , prod_crg_mix_filter   AS
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

   , add_type_and_yield    AS
    (SELECT cmo.cal_date
          , cmo.market10
          , cmo.k_color
          , cmo.crg_chrome
          , cmo.consumable_type
          , cmo.platform_subset
          , cmo.crg_base_prod_number                                                                                                  AS base_product_number
          , cmo.customer_engagement
          , COUNT(cmo.crg_base_prod_number) OVER
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
         ON cmo.crg_base_prod_number = pf.base_product_number
         AND cmo.market10 = pf.market_10
         AND cmo.cal_date = pf.cal_date
     WHERE 1 = 1)

SELECT cal_date
     , market10
     , platform_subset
     , base_product_number
     , customer_engagement
     , k_color
     , crg_chrome
     , consumable_type
     , product_count
     , mix_pct                                         AS mix_pct
     , yield
     , CAST(cal_date AS VARCHAR) + ' ' + market10 + ' ' + platform_subset +
       ' ' +
       base_product_number + ' ' + customer_engagement AS composite_key
FROM add_type_and_yield
"""

query_list.append(["scen.ink_page_cc_mix_override",ink_page_cc_mix_override, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### cc_mix_complete

# COMMAND ----------

ink_cc_mix_complete = """
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
        FROM scen.ink_page_mix_engine AS m19
                 LEFT OUTER JOIN scen.ink_page_cc_mix_override AS m26
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
        FROM scen.ink_page_cc_mix_override AS m26
        JOIN mdm.hardware_xref AS hw
            ON hw.platform_subset = m26.platform_subset
        WHERE 1=1
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
         JOIN scen.ink_shm_base_helper AS shm
              ON UPPER(shm.geography) = UPPER(m28.geography)
                  AND UPPER(shm.platform_subset) = UPPER(m28.platform_subset)
                  AND UPPER(shm.base_product_number) = UPPER(m28.base_product_number)
                  AND UPPER(shm.customer_engagement) = UPPER(m28.customer_engagement)
"""

query_list.append(["scen.ink_cc_mix_complete", ink_cc_mix_complete, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### cc_mix

# COMMAND ----------

ink_04_cc_mix = """
SELECT type
     , cal_date
     , geography_grain
     , geography
     , platform_subset
     , base_product_number
     , customer_engagement
     , cc_mix AS mix_rate
     , composite_key
FROM scen.ink_cc_mix_complete
"""

query_list.append(["scen.ink_04_cc_mix", ink_04_cc_mix, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 5 :: select correct cc mix override records, blow out to market10 for region_5 records

# COMMAND ----------

ink_05_mix_uploads = """
WITH geography_mapping     AS
    (SELECT DISTINCT market10 AS market_10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND NOT region_5 IS NULL
       AND NOT region_5 = 'JP'
       AND NOT region_5 LIKE 'X%')

   , override_filters      AS
    (SELECT DISTINCT 'SCENARIO_MIX_RATE'                AS record
                   , smr.user_name                      AS user_name
                   , smr.load_date                      AS load_date
                   , CAST(smr.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_mix_rate AS smr
     WHERE 1 = 1
       AND smr.upload_type = 'WORKING-FORECAST'
       AND smr.user_name IN ('SAIMANK', 'SONSEEAHRAYR', 'ZACP'))

   , ink_mix_rate_prep   AS
    (SELECT fv.user_name
          , MAX(fv.load_date) AS max_load_date
     FROM override_filters AS fv
     WHERE 1 = 1
       AND fv.record = 'SCENARIO_MIX_RATE'
     GROUP BY fv.user_name)

   , ink_mix_rate_subset AS
    (SELECT smr.month_num
          , smr.min_sys_date
          , smr.geography
          , smr.platform_subset
          , smr.base_product_number
          , smr.customer_engagement
          , smr.value
     FROM scen.working_forecast_mix_rate AS smr
     JOIN ink_mix_rate_prep AS mrp
         ON mrp.user_name = smr.user_name
         AND mrp.max_load_date = smr.load_date
     WHERE 1 = 1
       AND smr.upload_type = 'WORKING-FORECAST'
       AND smr.geography_grain = 'MARKET10'

     UNION ALL

     SELECT smr.month_num
          , smr.min_sys_date
          , geo.market_10 AS geography
          , smr.platform_subset
          , smr.base_product_number
          , smr.customer_engagement
          , smr.value
     FROM scen.working_forecast_mix_rate AS smr
     JOIN ink_mix_rate_prep AS mrp
         ON mrp.user_name = smr.user_name
         AND mrp.max_load_date = smr.load_date
     JOIN geography_mapping AS geo
         ON geo.region_5 = smr.geography
     WHERE 1 = 1
       AND smr.upload_type = 'WORKING-FORECAST'
       AND smr.geography_grain = 'REGION_5')
       
SELECT *
FROM ink_mix_rate_subset
"""

query_list.append(["scen.ink_05_mix_uploads", ink_05_mix_uploads, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 6: combine uploads with transformed data (concat + left outer join) 

# COMMAND ----------

ink_06_mix_rate_final = """
WITH ink_mix_rate_final AS
         (SELECT DATEADD(MONTH, smr.month_num, smr.min_sys_date)    AS cal_date
               , 'MARKET10'                                         AS geography_grain
               , smr.geography
               , smr.platform_subset
               , smr.base_product_number
               , smr.customer_engagement
               , smr.value                                          AS mix_rate
               , 'pcm_21_2'                                         AS type
               , CASE
                 WHEN s.single_multi = 'Tri-pack' THEN 'Multi'
                                                  ELSE 'Single' END AS single_multi
          FROM scen.ink_05_mix_uploads AS smr
          JOIN mdm.supplies_xref AS s
              ON s.base_product_number = smr.base_product_number
          WHERE 1 = 1

          UNION ALL

          SELECT mix.cal_date
               , mix.geography_grain
               , mix.geography
               , mix.platform_subset
               , mix.base_product_number
               , mix.customer_engagement
               , mix.mix_rate
               , mix.type
               , CASE
              WHEN s.single_multi = 'Tri-pack' THEN 'Multi'
                                               ELSE 'Single' END AS single_multi
          FROM scen.ink_04_cc_mix AS mix
          JOIN mdm.supplies_xref AS s
              ON s.base_product_number = mix.base_product_number
          LEFT OUTER JOIN scen.ink_05_mix_uploads AS smr
              ON DATEADD(MONTH, smr.month_num,
                         smr.min_sys_date) = mix.cal_date
              AND
                 smr.platform_subset = mix.platform_subset
              AND smr.base_product_number =
                  mix.base_product_number
              AND smr.geography = mix.geography
              AND smr.customer_engagement =
                  mix.customer_engagement
          WHERE 1 = 1
            AND DATEADD(MONTH, smr.month_num, smr.min_sys_date) IS NULL
            AND smr.platform_subset IS NULL
            AND smr.base_product_number IS NULL
            AND smr.geography IS NULL
            AND smr.customer_engagement IS NULL)
            
SELECT *
FROM ink_mix_rate_final
"""

query_list.append(["scen.ink_06_mix_rate_final", ink_06_mix_rate_final, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 7: run page_cc_cartridges process with new input dataset

# COMMAND ----------

ink_07_page_cc_cartridges = """
WITH crg_months               AS
    (SELECT date_key
          , [date] AS cal_date
     FROM mdm.calendar
     WHERE 1 = 1
       AND day_of_month = 1)

   , geography_mapping        AS
    (SELECT DISTINCT market10 AS market_10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND NOT region_5 IS NULL
       AND NOT region_5 = 'JP'
       AND NOT region_5 LIKE 'X%')

   , yield                    AS
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

   , pen_fills                AS
    (SELECT y.base_product_number
          , m.cal_date
          , y.market_10
          , y.yield
     FROM yield AS y
     JOIN crg_months AS m
         ON y.effective_date <= m.cal_date
         AND y.next_effective_date > m.cal_date)

   , pcm_02_hp_demand         AS
    (SELECT d.cal_date
          , d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_K_PAGES'
                         THEN units END)                                      AS black_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_C_PAGES'
                         THEN units END)                                      AS color_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_C_PAGES' THEN units END *
                3)                                                            AS cmy_demand
     FROM scen.ink_03_usage_share AS d
     JOIN mdm.hardware_xref AS hw
         ON UPPER(hw.platform_subset) = UPPER(d.platform_subset)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('INK', 'PWA')
     GROUP BY d.cal_date
            , d.geography
            , d.platform_subset
            , d.customer_engagement)

   , pcm_04_crg_actuals       AS
    (SELECT DISTINCT v.cal_date
                   , v.geography_grain
                   , v.geography
                   , shm.platform_subset
                   , v.base_product_number
                   , v.k_color
                   , v.crg_chrome
                   , v.consumable_type
                   , v.cartridge_volume
     FROM scen.ink_cartridge_units AS v
     JOIN scen.ink_shm_base_helper AS shm
         ON UPPER(shm.base_product_number) = UPPER(v.base_product_number)
         AND UPPER(shm.geography) = UPPER(v.geography)
     WHERE 1 = 1
       AND v.cartridge_volume > 0)

   , pcm_05_pages             AS
    (SELECT DISTINCT v.cal_date
                   , v.geography_grain
                   , v.geography
                   , v.platform_subset
                   , v.base_product_number
                   , dmd.customer_engagement
                   , CASE
            WHEN UPPER(s.single_multi) = 'TRI-PACK' THEN 'MULTI'
                                                    ELSE 'SINGLE' END AS single_multi
                   , v.k_color
                   , v.crg_chrome
                   , v.consumable_type
                   , v.cartridge_volume
                   , pf.yield
                   , v.cartridge_volume * pf.yield                    AS pages
     FROM pcm_04_crg_actuals AS v
     JOIN mdm.supplies_xref AS s
         ON UPPER(s.base_product_number) = UPPER(v.base_product_number)
     JOIN mdm.hardware_xref AS hw
         ON UPPER(hw.platform_subset) = UPPER(v.platform_subset)
     JOIN pcm_02_hp_demand AS dmd
         ON UPPER(dmd.platform_subset) = UPPER(v.platform_subset)
         AND UPPER(dmd.geography) = UPPER(v.geography)
         AND CAST(dmd.cal_date AS DATE) = CAST(v.cal_date AS DATE)
     JOIN pen_fills AS pf
         ON UPPER(pf.base_product_number) = UPPER(v.base_product_number)
         AND CAST(pf.cal_date AS DATE) = CAST(v.cal_date AS DATE)
         AND UPPER(pf.market_10) = UPPER(v.geography)
     JOIN scen.ink_shm_base_helper AS shm
         ON UPPER(shm.base_product_number) = UPPER(v.base_product_number)
         AND UPPER(shm.platform_subset) = UPPER(v.platform_subset)
         AND UPPER(shm.customer_engagement) = UPPER(dmd.customer_engagement)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('INK', 'PWA'))

   , pcrg_01_k_acts           AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate                    AS page_cc_mix
          , dmd.black_demand
          , pf.yield
          , pcm.mix_rate * dmd.black_demand AS page_demand
          , pcm.mix_rate * dmd.black_demand /
            NULLIF(pf.yield, 0)             AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.black_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.black_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                       AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.black_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.black_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                       AS imp
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.black_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.black_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                     AS imp_corrected_cartridges
     FROM scen.ink_06_mix_rate_final AS pcm
     JOIN mdm.supplies_xref AS s
         ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
     JOIN pcm_02_hp_demand AS dmd
         ON dmd.cal_date = pcm.cal_date
         AND UPPER(dmd.geography) = UPPER(pcm.geography)
         AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
         AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
     JOIN pen_fills AS pf
         ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
         AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(pf.market_10) = UPPER(pcm.geography)
     JOIN pcm_05_pages AS pc
         ON pc.cal_date = pcm.cal_date
         AND UPPER(pc.geography) = UPPER(pcm.geography)
         AND UPPER(pc.platform_subset) = UPPER(pcm.platform_subset)
         AND UPPER(pc.base_product_number) = UPPER(pcm.base_product_number)
         AND UPPER(pc.customer_engagement) = UPPER(pcm.customer_engagement)
     WHERE 1 = 1
       AND UPPER(pcm.type) = 'PCM_ENGINE_ACTS'
       AND UPPER(s.k_color) = 'BLACK')

   , pcrg_02_color_acts       AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate                    AS page_cc_mix
          , dmd.color_demand
          , pf.yield
          , pcm.mix_rate * dmd.color_demand AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)             AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.color_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.color_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                       AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.color_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.color_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                       AS imp
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.color_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.color_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                     AS imp_corrected_cartridges
     FROM scen.ink_06_mix_rate_final AS pcm
     JOIN mdm.supplies_xref AS s
         ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
     JOIN pcm_02_hp_demand AS dmd
         ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(dmd.geography) = UPPER(pcm.geography)
         AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
         AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
     JOIN pen_fills AS pf
         ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
         AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(pf.market_10) = UPPER(pcm.geography)
     JOIN pcm_05_pages AS pc
         ON CAST(pc.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(pc.geography) = UPPER(pcm.geography)
         AND UPPER(pc.platform_subset) = UPPER(pcm.platform_subset)
         AND UPPER(pc.base_product_number) = UPPER(pcm.base_product_number)
         AND UPPER(pc.customer_engagement) = UPPER(pcm.customer_engagement)
     WHERE 1 = 1
       AND UPPER(pcm.type) = 'PCM_ENGINE_ACTS'
       AND CASE WHEN UPPER(s.single_multi) = 'TRI-PACK' THEN 'MULTI'
                                                        ELSE 'SINGLE' END =
           'SINGLE'
       AND UPPER(s.k_color) = 'COLOR')

   , pcrg_03_color_multi_acts AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate                                        AS page_cc_mix
          , dmd.cmy_demand
          , pf.yield
          , pcm.mix_rate * dmd.cmy_demand                       AS page_demand
          , pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0) AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.cmy_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.cmy_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                           AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.cmy_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.cmy_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                           AS imp
          , pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.cmy_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.cmy_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                                         AS imp_corrected_cartridges
     FROM scen.ink_06_mix_rate_final AS pcm
     JOIN mdm.supplies_xref AS s
         ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
     JOIN pcm_02_hp_demand AS dmd
         ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(dmd.geography) = UPPER(pcm.geography)
         AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
         AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
     JOIN pen_fills AS pf
         ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
         AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(pf.market_10) = UPPER(pcm.geography)
     JOIN pcm_05_pages AS pc
         ON CAST(pc.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(pc.geography) = UPPER(pcm.geography)
         AND UPPER(pc.platform_subset) = UPPER(pcm.platform_subset)
         AND UPPER(pc.base_product_number) = UPPER(pcm.base_product_number)
         AND UPPER(pc.customer_engagement) = UPPER(pcm.customer_engagement)
     WHERE 1 = 1
       AND UPPER(pcm.type) = 'PCM_ENGINE_ACTS'
       AND CASE WHEN UPPER(s.single_multi) = 'TRI-PACK' THEN 'MULTI'
                                                        ELSE 'SINGLE' END <>
           'SINGLE'
       AND UPPER(s.k_color) = 'COLOR')

   , pcrg_04_k_fcst           AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate                    AS page_cc_mix
          , dmd.black_demand
          , pf.yield
          , pcm.mix_rate * dmd.black_demand AS page_demand
          , pcm.mix_rate * dmd.black_demand /
            NULLIF(pf.yield, 0)             AS cartridges
          , 0.0                             AS cartridge_volume
          , 1.0                             AS demand_scalar
          , 1.0                             AS imp
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0) *
            1.0                             AS imp_corrected_cartridges
     FROM scen.ink_06_mix_rate_final AS pcm
     JOIN mdm.supplies_xref AS s
         ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
     JOIN mdm.hardware_xref AS hw
         ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
     JOIN pcm_02_hp_demand AS dmd
         ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(dmd.geography) = UPPER(pcm.geography)
         AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
         AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
     JOIN pen_fills AS pf
         ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
         AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND UPPER(s.k_color) = 'BLACK'
       AND UPPER(pcm.type) <> 'PCM_ENGINE_ACTS')

   , pcrg_05_color_fcst       AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate                    AS page_cc_mix
          , dmd.color_demand                AS demand
          , pf.yield
          , pcm.mix_rate * dmd.color_demand AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)             AS cartridges
          , 0.0                             AS cartridge_volume
          , 1.0                             AS demand_scalar
          , 1.0                             AS imp
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0) *
            1.0                             AS imp_corrected_cartridges
     FROM scen.ink_06_mix_rate_final AS pcm
     JOIN mdm.supplies_xref AS s
         ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
     JOIN mdm.hardware_xref AS hw
         ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
     JOIN pcm_02_hp_demand AS dmd
         ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(dmd.geography) = UPPER(pcm.geography)
         AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
         AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
     JOIN pen_fills AS pf
         ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
         AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND CASE WHEN UPPER(s.single_multi) = 'TRI-PACK' THEN 'MULTI'
                                                        ELSE 'SINGLE' END =
           'SINGLE'
       AND UPPER(pcm.type) <> 'PCM_ENGINE_ACTS'
       AND UPPER(s.k_color) = 'COLOR'

     UNION ALL

     SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate                  AS page_cc_mix
          , dmd.cmy_demand                AS demand
          , pf.yield
          , pcm.mix_rate * dmd.cmy_demand AS page_demand
          , pcm.mix_rate * dmd.cmy_demand /
            NULLIF(pf.yield, 0)           AS cartridges
          , 0.0                           AS cartridge_volume
          , 1.0                           AS demand_scalar
          , 1.0                           AS imp
          , pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0) *
            1.0                           AS imp_corrected_cartridges
     FROM scen.ink_06_mix_rate_final AS pcm
     JOIN mdm.supplies_xref AS s
         ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
     JOIN mdm.hardware_xref AS hw
         ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
     JOIN pcm_02_hp_demand AS dmd
         ON CAST(dmd.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(dmd.geography) = UPPER(pcm.geography)
         AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
         AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
     JOIN pen_fills AS pf
         ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
         AND CAST(pf.cal_date AS DATE) = CAST(pcm.cal_date AS DATE)
         AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND CASE WHEN UPPER(s.single_multi) = 'TRI-PACK' THEN 'MULTI'
                                                        ELSE 'SINGLE' END <>
           'SINGLE'
       AND UPPER(pcm.type) <> 'PCM_ENGINE_ACTS'
       AND UPPER(s.k_color) = 'COLOR')

SELECT type
     , cal_date
     , geography_grain
     , geography
     , platform_subset
     , base_product_number
     , customer_engagement
     , page_cc_mix
     , black_demand                                    AS demand
     , yield
     , page_demand
     , cartridges
     , cartridge_volume
     , demand_scalar
     , imp
     , imp_corrected_cartridges
     , CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset +
       ' ' +
       base_product_number + ' ' + customer_engagement AS composite_key
FROM pcrg_01_k_acts

UNION ALL

SELECT type
     , cal_date
     , geography_grain
     , geography
     , platform_subset
     , base_product_number
     , customer_engagement
     , page_cc_mix
     , color_demand                                    AS demand
     , yield
     , page_demand
     , cartridges
     , cartridge_volume
     , demand_scalar
     , imp
     , imp_corrected_cartridges
     , CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset +
       ' ' +
       base_product_number + ' ' + customer_engagement AS composite_key
FROM pcrg_02_color_acts

UNION ALL

SELECT type
     , cal_date
     , geography_grain
     , geography
     , platform_subset
     , base_product_number
     , customer_engagement
     , page_cc_mix
     , cmy_demand                                      AS demand
     , yield
     , page_demand
     , cartridges
     , cartridge_volume
     , demand_scalar
     , imp
     , imp_corrected_cartridges
     , CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset +
       ' ' +
       base_product_number + ' ' + customer_engagement AS composite_key
FROM pcrg_03_color_multi_acts

UNION ALL

SELECT type
     , cal_date
     , geography_grain
     , geography
     , platform_subset
     , base_product_number
     , customer_engagement
     , page_cc_mix
     , black_demand                                    AS demand
     , yield
     , page_demand
     , cartridges
     , cartridge_volume
     , demand_scalar
     , imp
     , imp_corrected_cartridges
     , CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset +
       ' ' +
       base_product_number + ' ' + customer_engagement AS composite_key
FROM pcrg_04_k_fcst

UNION ALL

SELECT type
     , cal_date
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
     , cartridge_volume
     , demand_scalar
     , imp
     , imp_corrected_cartridges
     , CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset +
       ' ' +
       base_product_number + ' ' + customer_engagement AS composite_key
FROM pcrg_05_color_fcst
"""

query_list.append(["scen.ink_07_page_cc_cartridges", ink_07_page_cc_cartridges, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 8: run base forecast cartridge adjust processes

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 8.1: analytic

# COMMAND ----------

ink_08_analytic = """
WITH date_helper           AS
    (SELECT date_key
          , date AS cal_date
     FROM mdm.calendar
     WHERE 1 = 1
       AND day_of_month = 1)

   , ana_02_c2c_setup      AS
    (SELECT c2c.cal_date
          , c2c.geography_grain
          , c2c.geography
          , c2c.platform_subset
          , c2c.base_product_number
          , c2c.customer_engagement
          , hw.pl
          , hw.hw_product_family
          , sup.crg_chrome
          , c2c.cartridges
          , c2c.imp_corrected_cartridges
          , MIN(c2c.cal_date)
            OVER (PARTITION BY c2c.geography, c2c.platform_subset, c2c.base_product_number, c2c.customer_engagement) AS min_cal_date
          , MAX(c2c.cal_date)
            OVER (PARTITION BY c2c.geography, c2c.platform_subset, c2c.base_product_number, c2c.customer_engagement) AS max_cal_date
     FROM scen.ink_07_page_cc_cartridges AS c2c
     JOIN mdm.hardware_xref AS hw
         ON UPPER(hw.platform_subset) = UPPER(c2c.platform_subset)
     JOIN mdm.supplies_xref AS sup
         ON UPPER(sup.base_product_number) =
            UPPER(c2c.base_product_number)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('INK', 'PWA'))

   , ana_03_c2c_fill_gap_1 AS
    (SELECT DISTINCT d.cal_date AS cal_date
                   , c2c.geography_grain
                   , c2c.geography
                   , c2c.platform_subset
                   , c2c.base_product_number
                   , c2c.customer_engagement
                   , c2c.pl
                   , c2c.hw_product_family
                   , c2c.crg_chrome
     FROM date_helper AS d
     CROSS JOIN ana_02_c2c_setup AS c2c
     WHERE 1 = 1
       AND d.cal_date BETWEEN c2c.min_cal_date AND c2c.max_cal_date)

   , ana_04_c2c_fill_gap_2 AS
    (SELECT h.cal_date
          , COALESCE(h.geography_grain, c2c.geography_grain) AS geography_grain
          , COALESCE(h.geography, c2c.geography)             AS geography
          , COALESCE(h.platform_subset,
                     c2c.platform_subset)                    AS platform_subset
          , COALESCE(h.base_product_number,
                     c2c.base_product_number)                AS base_product_number
          , COALESCE(h.customer_engagement,
                     c2c.customer_engagement)                AS customer_engagement
          , COALESCE(h.pl, c2c.pl)                           AS pl
          , COALESCE(h.hw_product_family,
                     c2c.hw_product_family)                  AS hw_product_family
          , COALESCE(h.crg_chrome, c2c.crg_chrome)           AS crg_chrome
          , COALESCE(c2c.cartridges, 0)                      AS cartridges
          , COALESCE(c2c.imp_corrected_cartridges, 0)        AS imp_corrected_cartridges
     FROM ana_03_c2c_fill_gap_1 AS h
     LEFT JOIN ana_02_c2c_setup AS c2c
         ON c2c.cal_date = h.cal_date
         AND UPPER(c2c.geography) = UPPER(h.geography)
         AND
            UPPER(c2c.platform_subset) = UPPER(h.platform_subset)
         AND UPPER(c2c.base_product_number) =
             UPPER(h.base_product_number)
         AND UPPER(c2c.customer_engagement) =
             UPPER(h.customer_engagement))

   , ana_05_c2c_fill_gap_3 AS
    (SELECT cal_date
          , geography_grain
          , geography
          , platform_subset
          , base_product_number
          , customer_engagement
          , pl
          , hw_product_family
          , crg_chrome
          , cartridges
          , imp_corrected_cartridges
          , MIN(cal_date)
            OVER (PARTITION BY geography, platform_subset, base_product_number, customer_engagement) AS min_cal_date
     FROM ana_04_c2c_fill_gap_2)

SELECT cal_date
     , min_cal_date
     , geography_grain
     , geography
     , platform_subset
     , base_product_number
     , customer_engagement
     , pl
     , hw_product_family
     , crg_chrome
     , cartridges
     , imp_corrected_cartridges
     , CASE
    WHEN min_cal_date < CAST(DATEADD(MONTH, 1,
                                     DATE_TRUNC('MONTH', CAST(GETDATE() AS DATE))) AS DATE)
        THEN 1
        ELSE 0 END                                                             AS actuals_flag
     , COUNT(cal_date)
       OVER (PARTITION BY geography, platform_subset, base_product_number, customer_engagement
           ORDER BY cal_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
FROM ana_05_c2c_fill_gap_3
"""

query_list.append(["scen.ink_08_analytic", ink_08_analytic, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 8.2: channel_fill

# COMMAND ----------

ink_09_channel_fill = """
WITH cfadj_01_c2c                AS
    (SELECT c2c.cal_date
          , hw.intro_date                                                                                            AS hw_intro_date
          , MIN(c2c.cal_date)
            OVER (PARTITION BY c2c.geography, c2c.platform_subset, c2c.base_product_number, c2c.customer_engagement) AS sys_crg_intro_date
          , c2c.geography_grain
          , c2c.geography
          , c2c.platform_subset
          , c2c.base_product_number
          , c2c.customer_engagement
          , c2c.cartridges
     FROM scen.ink_08_analytic AS c2c
     JOIN mdm.hardware_xref AS hw
         ON UPPER(hw.platform_subset) = UPPER(c2c.platform_subset)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('INK', 'PWA'))

   , cfadj_04_c2c_avg_ship       AS
    (SELECT c2c.geography_grain
          , c2c.geography
          , c2c.platform_subset
          , c2c.base_product_number
          , c2c.customer_engagement
          , c2c.sys_crg_intro_date
          , AVG(c2c.cartridges) AS avg_shipments
          , 0                   AS month_offset
     FROM cfadj_01_c2c AS c2c
     WHERE 1 = 1
       AND c2c.cal_date BETWEEN (DATEADD(MONTH, 7, c2c.sys_crg_intro_date)) AND (DATEADD(MONTH, 9, c2c.sys_crg_intro_date))
     GROUP BY c2c.geography_grain
            , c2c.geography
            , c2c.platform_subset
            , c2c.base_product_number
            , c2c.customer_engagement
            , c2c.sys_crg_intro_date)

   , cfadj_02_norm_shipments     AS
    (SELECT DISTINCT cc.country_level_2 AS geography
                   , ns.platform_subset
                   , MIN(ns.cal_date)   AS min_acts_date
     FROM prod.norm_shipments AS ns
     JOIN mdm.iso_cc_rollup_xref AS cc
         ON cc.country_alpha2 = ns.country_alpha2
     WHERE 1 = 1
       AND UPPER(cc.country_scenario) = 'MARKET10'
       AND ns.version = (SELECT MAX(version) FROM prod.norm_shipments)
     GROUP BY cc.country_level_2
            , ns.platform_subset)

   , cfadj_03_hw_intro           AS
    (SELECT DISTINCT c2c.geography
                   , c2c.platform_subset
                   , COALESCE(ns.min_acts_date,
                              c2c.hw_intro_date) AS sys_hw_intro_date
     FROM cfadj_01_c2c AS c2c
     LEFT JOIN cfadj_02_norm_shipments AS ns
         ON UPPER(ns.geography) = UPPER(c2c.geography)
         AND UPPER(ns.platform_subset) =
             UPPER(c2c.platform_subset))

   , cfadj_05_valid_crgs         AS
    (SELECT c2c.geography_grain
          , c2c.geography
          , c2c.platform_subset
          , c2c.base_product_number
          , c2c.customer_engagement
          , c2c.sys_crg_intro_date
          , hw.sys_hw_intro_date
          , c2c.avg_shipments
          , 0 AS month_offset
     FROM cfadj_04_c2c_avg_ship AS c2c
     JOIN cfadj_03_hw_intro AS hw
         ON UPPER(hw.geography) = UPPER(c2c.geography)
         AND
            UPPER(hw.platform_subset) = UPPER(c2c.platform_subset)
     WHERE 1 = 1
       AND c2c.sys_crg_intro_date BETWEEN hw.sys_hw_intro_date AND DATEADD(MONTH, 6, hw.sys_hw_intro_date))

   , cfadj_06_valid_months       AS
    (SELECT c2c.geography_grain
          , c2c.geography
          , c2c.platform_subset
          , c2c.base_product_number
          , c2c.customer_engagement
          , c2c.sys_crg_intro_date
          , c2c.sys_hw_intro_date
          , c2c.month_offset
          , c2c.avg_shipments
          , cal.date AS cal_date
     FROM cfadj_05_valid_crgs AS c2c
     JOIN mdm.calendar AS cal
         ON (
                    cal.date BETWEEN c2c.sys_hw_intro_date AND DATEADD(MONTH, 6, c2c.sys_hw_intro_date) OR
                    cal.date = DATEADD(MONTH, -1 * c2c.month_offset,
                                       c2c.sys_hw_intro_date)
                )
         AND cal.day_of_month = 1)

   , cfadj_07_power_args         AS
    (SELECT geography_grain
          , geography
          , platform_subset
          , base_product_number
          , customer_engagement
          , sys_crg_intro_date
          , sys_hw_intro_date
          , month_offset
          , avg_shipments
          , cal_date
          , CASE
            WHEN DATEDIFF(MONTH, sys_hw_intro_date, cal_date) = 6 THEN 1
                                                                  ELSE 1 +
                                                                       DATEDIFF(
                                                                               MONTH,
                                                                               cal_date,
                                                                               MAX(
                                                                               cal_date)
                                                                               OVER (PARTITION BY geography, platform_subset, base_product_number, customer_engagement))
            END AS power_arg
     FROM cfadj_06_valid_months AS c2c)

   , cfadj_08_channel_fill_setup AS
    (SELECT pa.geography_grain
          , pa.geography
          , pa.platform_subset
          , pa.base_product_number
          , pa.customer_engagement
          , pa.sys_crg_intro_date
          , pa.sys_hw_intro_date
          , pa.month_offset
          , pa.avg_shipments
          , pa.cal_date
          , pa.power_arg
          , pa.avg_shipments * POWER(0.85, pa.power_arg) -
            c2c.cartridges AS channel_fill
     FROM cfadj_07_power_args AS pa
     JOIN cfadj_01_c2c AS c2c
         ON UPPER(c2c.geography) = UPPER(pa.geography)
         AND
            UPPER(c2c.platform_subset) = UPPER(pa.platform_subset)
         AND UPPER(c2c.base_product_number) =
             UPPER(pa.base_product_number)
         AND c2c.cal_date = pa.cal_date
         AND UPPER(c2c.customer_engagement) =
             UPPER(pa.customer_engagement))

SELECT c2c.geography_grain
     , c2c.geography
     , c2c.platform_subset
     , c2c.base_product_number
     , c2c.customer_engagement
     , c2c.sys_crg_intro_date
     , c2c.sys_hw_intro_date
     , c2c.month_offset
     , c2c.avg_shipments
     , c2c.cal_date
     , c2c.power_arg
     , c2c.channel_fill
FROM cfadj_08_channel_fill_setup AS c2c
WHERE 1 = 1
  AND c2c.channel_fill > 0
"""

query_list.append(["scen.ink_09_channel_fill", ink_09_channel_fill, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 8.3: supplies_spares

# COMMAND ----------

ink_10_supplies_spares = """
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

   , ssadj_08_c2c_setup AS
    (SELECT c2c.cal_date
          , c2c.min_cal_date
          , c2c.geography_grain
          , c2c.geography
          , c2c.platform_subset
          , c2c.base_product_number
          , c2c.customer_engagement
          , c2c.pl
          , c2c.hw_product_family
          , c2c.Crg_Chrome
          , c2c.cartridges
          , c2c.actuals_flag
          , c2c.running_count
     FROM scen.ink_08_analytic AS c2c
     WHERE 1 = 1
       AND NOT (c2c.base_product_number IN
                ('W9014MC', 'W9040MC', 'W9041MC', 'W9042MC', 'W9043MC') AND
                c2c.cal_date >
                CAST(DATEADD(MONTH, 1, DATE_TRUNC('MONTH', CAST(GETDATE() AS DATE))) AS DATE)) -- EOL products
    )

   , c2c_supplies_spares_helper_1 AS
    (SELECT c2c.cal_date
          , c2c.min_cal_date
          , c2c.geography
          , c2c.platform_subset
          , c2c.base_product_number
          , c2c.customer_engagement
          , c2c.pl
          , c2c.hw_product_family
          , c2c.Crg_Chrome
          , c2c.cartridges
          , c2c.actuals_flag
          , c2c.running_count
          , pf.yield
     FROM ssadj_08_c2c_setup AS c2c
              LEFT JOIN pen_fills AS pf
                        ON UPPER(pf.base_product_number) =
                           UPPER(c2c.base_product_number)
                            AND pf.cal_date = c2c.cal_date
                            AND UPPER(pf.market_10) = UPPER(c2c.geography))

   , ssadj_04_hw_ships AS
    (SELECT ns.cal_date
          , cref.country_level_2 AS geography
          , ns.country_alpha2
          , SUM(ns.units)        AS units
     FROM prod.norm_shipments AS ns
              JOIN mdm.iso_cc_rollup_xref AS cref
                   ON UPPER(cref.country_alpha2) = UPPER(ns.country_alpha2)
                       AND UPPER(cref.country_scenario) = 'MARKET10'
     WHERE 1=1
        AND ns.version = (SELECT MAX(version) FROM prod.norm_shipments)
     GROUP BY ns.cal_date
            , cref.country_level_2
            , ns.country_alpha2)

   , ssadj_05_hw_ratio AS
    (SELECT cal_date
          , geography
          , 1 - SUM(CASE
                        WHEN country_alpha2 IN
                             ('IN', 'HK', 'ID', 'MY', 'PH', 'SG', 'TH', 'VN')
                            THEN units
                        ELSE 0 END) /
                NULLIF(SUM(units), 0) AS hw_ratio
     FROM ssadj_04_hw_ships
     GROUP BY cal_date
            , geography)

   , case_statement AS
    (SELECT c2c.cal_date
        , c2c.min_cal_date
        , c2c.geography
        , c2c.platform_subset
        , c2c.base_product_number
        , c2c.customer_engagement
        , c2c.pl
        , c2c.hw_product_family
        , c2c.Crg_Chrome
        , c2c.cartridges
        , c2c.yield
        , CASE WHEN UPPER(c2c.geography) IN ('GREATER ASIA', 'GREATER CHINA', 'INDIA SL & BL') AND UPPER(c2c.pl) = 'G8' THEN

                   CASE WHEN UPPER(c2c.hw_product_family) IN ('TONER LJ TOPAZ') THEN

                            CASE WHEN c2c.cal_date < CAST('2019-04-01' AS DATE) THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date BETWEEN CAST('2019-04-01' AS DATE) AND CAST('2020-06-01' AS DATE) THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                                    * hw.hw_ratio
                                     ELSE 0 END

                                 WHEN c2c.cal_date >= CAST('2020-07-01' AS DATE) THEN 0

                            ELSE 0 END

                        WHEN UPPER(c2c.hw_product_family) IN ('TONER CLJ JADE') THEN

                            CASE WHEN c2c.cal_date < CAST('2019-04-01' AS DATE) THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date BETWEEN CAST('2019-04-01' AS DATE) AND CAST('2022-03-01' AS DATE) THEN  -- first day of previous month

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                                    * hw.hw_ratio
                                     ELSE 0 END

                                 WHEN c2c.cal_date >= CAST('2022-04-01' AS DATE) THEN -- 1st day of current month

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome
                                                                                            ORDER BY c2c.cal_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), 0)
                                                   * hw.hw_ratio
                                     ELSE 0 END

                            ELSE 0 END

                        WHEN UPPER(c2c.hw_product_family) IN ('TONER LJ ONYX') THEN

                            CASE WHEN c2c.cal_date < CAST('2019-05-01' AS DATE) THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date BETWEEN CAST('2019-05-01' AS DATE) AND CAST('2020-06-01' AS DATE) THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                                    * hw.hw_ratio
                                     ELSE 0 END

                                 WHEN c2c.cal_date >= CAST('2020-07-01' AS DATE) THEN 0

                            ELSE 0 END

                        WHEN UPPER(c2c.hw_product_family) IN ('TONER CLJ AGATE') THEN

                            CASE WHEN c2c.cal_date < CAST('2019-05-01' AS DATE) THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date BETWEEN CAST('2019-05-01' AS DATE) AND CAST('2022-03-01' AS DATE) THEN  -- first day of previous month

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                                    * hw.hw_ratio
                                     ELSE 0 END

                                 WHEN c2c.cal_date >= CAST('2022-04-01' AS DATE) THEN -- 1st day of current month

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome
                                                                                            ORDER BY c2c.cal_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), 0)
                                                    * hw.hw_ratio
                                     ELSE 0 END

                            ELSE 0 END

                        WHEN UPPER(c2c.hw_product_family) IN ('TONER LJ DIAMOND', 'TONER CLJ RUBY') THEN

                            CASE WHEN c2c.cal_date < CAST('2022-04-01' AS DATE) THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date >= CAST('2020-09-01' AS DATE) THEN 0

                            ELSE 0 END

                        WHEN UPPER(c2c.hw_product_family) IN ('TONER CLJ AGATE EPA', 'TONER CLJ JADE EPA') THEN

                            CASE WHEN c2c.cal_date < CAST('2022-04-01' AS DATE) THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date >= CAST('2021-01-01' AS DATE) THEN 0

                            ELSE 0 END

                        WHEN UPPER(c2c.hw_product_family) IN ('TONER LJ AMBER') THEN

                            CASE WHEN c2c.cal_date < CAST('2022-04-01' AS DATE) THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date >= CAST('2022-04-01' AS DATE) THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                                NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome
                                                                                             ORDER BY c2c.cal_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), 0)
                                     ELSE 0 END

                            ELSE 0 END

                        WHEN UPPER(c2c.hw_product_family) IN ('TONER LJ MOONSTONE', 'TONER LJ PEARL', 'TONER LJ MORGANITE', 'TONER CLJ JASPER', 'TONER CLJ CITRINE', 'TONER CLJ AMMOLITE') THEN
                            -- nothing added with this block of code
                            CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL') THEN 0
                            ELSE 0 END

                   ELSE 0 END

             WHEN UPPER(c2c.geography) IN ('ISE') AND UPPER(c2c.pl) = 'G8' THEN

                CASE WHEN UPPER(c2c.hw_product_family) IN ('TONER LJ MOONSTONE', 'TONER LJ PEARL', 'TONER LJ MORGANITE', 'TONER CLJ JASPER', 'TONER CLJ CITRINE', 'TONER CLJ AMMOLITE') THEN
                            -- nothing added with this block of code
                            CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL') THEN 0
                            ELSE 0 END

                ELSE 0 END

             WHEN UPPER(c2c.geography) IN ('CENTRAL EUROPE', 'NORTHERN EUROPE', 'SOUTHERN EUROPE', 'UK&I', 'LATIN AMERICA', 'NORTH AMERICA') AND UPPER(c2c.pl) = 'G8' THEN

                 CASE WHEN c2c.cal_date < CAST('2022-04-01' AS DATE) THEN

                          CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                               THEN (c2c.cartridges * c2c.yield) /
                                     NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                          ELSE 0 END

                      WHEN c2c.cal_date >= CAST('2022-04-01' AS DATE) THEN

                          CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                               THEN c2c.yield / NULLIF(SUM(c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                          ELSE 0 END

                 ELSE 0 END

        ELSE 0 END AS supplies_spares

    FROM c2c_supplies_spares_helper_1 AS c2c
    JOIN ssadj_05_hw_ratio AS hw
        ON hw.cal_date = c2c.cal_date
        AND UPPER(hw.geography) = UPPER(c2c.geography))

SELECT cal_date
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , pl
    , hw_product_family
    , Crg_Chrome
    , cartridges
    , yield
    , CAST(supplies_spares AS FLOAT) AS supplies_spares
FROM case_statement
"""

query_list.append(["scen.ink_10_supplies_spares", ink_10_supplies_spares, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 8.4: host

# COMMAND ----------

ink_11_host = """
WITH shm_07_geo_1_host           AS
    (SELECT DISTINCT shm.platform_subset
                   , shm.base_product_number
                   , shm.geography_grain
                   , shm.geography -- could be at several grains
                   , CASE
            WHEN UPPER(hw.technology) = 'LASER' AND
                 UPPER(shm.platform_subset) LIKE '%STND%'
                                                THEN 'STD'
            WHEN UPPER(hw.technology) = 'LASER' AND
                 UPPER(shm.platform_subset) LIKE '%YET2%'
                                                THEN 'HP+'
            WHEN UPPER(hw.technology) = 'LASER' THEN 'TRAD'
                                                ELSE shm.customer_engagement END AS customer_engagement
                   , shm.host_multiplier
     FROM mdm.supplies_hw_mapping AS shm
     JOIN mdm.hardware_xref AS hw
         ON hw.platform_subset = shm.platform_subset
     WHERE 1 = 1
       AND shm.official = 1
       AND NOT shm.host_multiplier IS NULL
       AND shm.host_multiplier > 0
       AND UPPER(shm.geography_grain) IN
           ('REGION_5', 'REGION_8', 'MARKET10') -- ASSUMPTION
       AND UPPER(hw.technology) IN ('INK', 'LASER', 'PWA'))

   , shm_08_map_geo_host         AS
    (SELECT platform_subset
          , base_product_number
          , geography_grain
          , geography
          , customer_engagement
          , host_multiplier
          , platform_subset + ' ' + base_product_number + ' ' +
            geography_grain + ' ' +
            geography + ' ' + customer_engagement AS composite_key
     FROM shm_07_geo_1_host)

   , hostadj_01_shm_host_mult    AS
    (SELECT DISTINCT shm.platform_subset
                   , shm.base_product_number
                   , shm.geography_grain
                   , shm.geography
                   , shm.customer_engagement
                   , shm.host_multiplier
     FROM shm_08_map_geo_host AS shm
     WHERE 1 = 1
       AND shm.customer_engagement = 'TRAD')

   , hostadj_02_norm_ships_r5    AS
    (SELECT ns.cal_date
          , iso.market10
          , ns.platform_subset
          , shm.base_product_number
          , shm.customer_engagement
          , SUM(ns.units)                       AS ns_units
          , shm.host_multiplier
          , SUM(ns.units * shm.host_multiplier) AS host_units
     FROM prod.norm_shipments AS ns
     JOIN mdm.iso_country_code_xref AS iso
         ON UPPER(iso.country_alpha2) = ns.country_alpha2 -- region_5
     JOIN hostadj_01_shm_host_mult AS shm
         ON UPPER(shm.geography) = UPPER(iso.region_5)
         AND UPPER(shm.platform_subset) = UPPER(ns.platform_subset)
     WHERE 1 = 1
       AND ns.version = (SELECT MAX(version) FROM prod.norm_shipments)
       AND ns.units >= 0.0
       AND UPPER(shm.geography_grain) = 'REGION_5'
     GROUP BY ns.cal_date
            , iso.market10
            , ns.platform_subset
            , shm.base_product_number
            , shm.customer_engagement
            , shm.host_multiplier)

   , hostadj_03_norm_ships_r8    AS
    (SELECT ns.cal_date
          , ns.country_alpha2
          , cc.country_level_1                                                                                                        AS region_8
          , iso.market10
          , ns.platform_subset
          , shm.base_product_number
          , shm.customer_engagement
          , ns.units                                                                                                                  AS ns_units
          , shm.host_multiplier
          , SUM(ns.units)
            OVER (PARTITION BY ns.cal_date, cc.country_level_1, ns.platform_subset, shm.base_product_number, shm.customer_engagement) AS ns_units_r8
          , SUM(ns.units * shm.host_multiplier)
            OVER (PARTITION BY ns.cal_date, cc.country_level_1, ns.platform_subset, shm.base_product_number, shm.customer_engagement) AS host_units_r8
     FROM prod.norm_shipments AS ns
     JOIN mdm.iso_cc_rollup_xref AS cc
         ON UPPER(cc.country_alpha2) = UPPER(ns.country_alpha2)
     JOIN mdm.iso_country_code_xref AS iso
         ON UPPER(iso.country_alpha2) = UPPER(ns.country_alpha2)
     JOIN hostadj_01_shm_host_mult AS shm
         ON UPPER(shm.geography) = UPPER(cc.country_level_1) -- region_8
         AND UPPER(shm.platform_subset) = UPPER(ns.platform_subset)
     WHERE 1 = 1
       AND ns.version = (SELECT MAX(version) FROM prod.norm_shipments)
       AND ns.units >= 0.0
       AND UPPER(cc.country_scenario) = 'HOST_REGION_8'
       AND cc.official = 1
       AND UPPER(shm.geography_grain) = 'REGION_8')

   , hostadj_04_norm_ships_m10   AS
    (SELECT cal_date
          , country_alpha2
          , region_8
          , market10
          , platform_subset
          , base_product_number
          , customer_engagement
          , ns_units
          , host_multiplier
          , ns_units_r8
          , host_units_r8
          , SUM(ns_units)
            OVER (PARTITION BY cal_date, market10, platform_subset, base_product_number, customer_engagement) AS ns_units_m10
          , SUM(ns_units * host_multiplier)
            OVER (PARTITION BY cal_date, market10, platform_subset, base_product_number, customer_engagement) AS host_units_m10
     FROM hostadj_03_norm_ships_r8 AS ns)

   , hostadj_05_norm_ships_m10_2 AS
    (SELECT ns.cal_date
          , iso.market10
          , ns.platform_subset
          , shm.base_product_number
          , shm.customer_engagement
          , SUM(ns.units)                       AS ns_units
          , shm.host_multiplier
          , SUM(ns.units * shm.host_multiplier) AS host_units
     FROM prod.norm_shipments AS ns
     JOIN mdm.iso_country_code_xref AS iso
         ON UPPER(iso.country_alpha2) =
            UPPER(ns.country_alpha2) -- to get market10
     JOIN hostadj_01_shm_host_mult AS shm
         ON UPPER(shm.geography) = UPPER(iso.market10)
         AND UPPER(shm.platform_subset) = UPPER(ns.platform_subset)
     WHERE 1 = 1
       AND ns.version = (SELECT MAX(version) FROM prod.norm_shipments)
       AND ns.units >= 0.0
       AND UPPER(shm.geography_grain) = 'MARKET10'
     GROUP BY ns.cal_date
            , iso.market10
            , ns.platform_subset
            , shm.base_product_number
            , shm.customer_engagement
            , shm.host_multiplier)

   , hostadj_06_host_cartridges  AS
    (SELECT cal_date
          , market10                                       AS geography
          , platform_subset
          , base_product_number
          , customer_engagement
          , ns_units
          , host_units
          , CAST(cal_date AS VARCHAR) + ' ' + COALESCE(market10, 'UNKNOWN') +
            ' ' + COALESCE(platform_subset, 'UNKNOWN') + ' ' +
            COALESCE(base_product_number, 'UNKNOWN') +
            ' ' + COALESCE(customer_engagement, 'UNKNOWN') AS composite_key
     FROM hostadj_02_norm_ships_r5
     WHERE 1 = 1

     UNION ALL

     SELECT cal_date
          , market10                                       AS geography
          , platform_subset
          , base_product_number
          , customer_engagement
          , MAX(ns_units_m10)                              AS ns_units
          , MAX(host_units_m10)                            AS host_units
          , CAST(cal_date AS VARCHAR) + ' ' + COALESCE(market10, 'UNKNOWN') +
            ' ' + COALESCE(platform_subset, 'UNKNOWN') + ' ' +
            COALESCE(base_product_number, 'UNKNOWN') +
            ' ' + COALESCE(customer_engagement, 'UNKNOWN') AS composite_key
     FROM hostadj_04_norm_ships_m10
     WHERE 1 = 1
     GROUP BY cal_date
            , market10
            , platform_subset
            , base_product_number
            , customer_engagement

     UNION ALL

     SELECT cal_date
          , market10                                       AS geography
          , platform_subset
          , base_product_number
          , customer_engagement
          , ns_units
          , host_units
          , CAST(cal_date AS VARCHAR) + ' ' + COALESCE(market10, 'UNKNOWN') +
            ' ' + COALESCE(platform_subset, 'UNKNOWN') + ' ' +
            COALESCE(base_product_number, 'UNKNOWN') +
            ' ' + COALESCE(customer_engagement, 'UNKNOWN') AS composite_key
     FROM hostadj_05_norm_ships_m10_2
     WHERE 1 = 1)

SELECT cal_date
     , geography
     , platform_subset
     , base_product_number
     , customer_engagement
     , SUM(host_units) AS host_units
     , composite_key
FROM hostadj_06_host_cartridges
WHERE 1 = 1
GROUP BY cal_date
       , geography
       , platform_subset
       , base_product_number
       , customer_engagement
       , composite_key
"""

query_list.append(["scen.ink_11_host", ink_11_host, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 8.5: welcome_kits

# COMMAND ----------

ink_12_welcome_kits = """
WITH wel_01_stf_enroll    AS
    (SELECT iiel.platform_subset
          , CAST('I-INK' AS VARCHAR(25))       AS customer_engagement
          , iiel.year_month                    AS cal_date
          , cc.market10                        AS geography
          , SUM(iiel.all_enrollments_customer) AS all_enrollments_customer
     FROM prod.instant_ink_enrollees AS iiel
     JOIN mdm.iso_country_code_xref AS cc
         ON cc.country_alpha2 = iiel.country
     WHERE 1 = 1
       AND iiel.official = 1
       AND UPPER(iiel.data_source) = 'FCST'
       AND iiel.all_enrollments_customer <> 0.0
     GROUP BY iiel.platform_subset
            , iiel.year_fiscal
            , iiel.year_month
            , cc.market10)

   , wel_02_ltf_ib_step_1 AS
    (SELECT ib.cal_date
          , ib.platform_subset
          , ib.customer_engagement
          , ib.country_alpha2
          , iso.market10
          , ib.units
          , COALESCE(LAG(ib.units)
                     OVER (PARTITION BY ib.platform_subset, ib.customer_engagement, ib.country_alpha2 ORDER BY ib.cal_date),
                     ib.units)                                                                                     AS lagged_ib
          , ROW_NUMBER()
            OVER (PARTITION BY ib.platform_subset, ib.customer_engagement, ib.country_alpha2 ORDER BY ib.cal_date) AS month_number
     FROM prod.ib AS ib
     LEFT JOIN mdm.iso_country_code_xref AS iso
         ON UPPER(iso.country_alpha2) = UPPER(ib.country_alpha2)
     WHERE 1 = 1
       AND ib.version = (SELECT MAX(version) FROM prod.ib)
       AND ib.cal_date > CAST('2022-10-01' AS DATE)
       AND UPPER(ib.measure) = 'IB'
       AND UPPER(ib.customer_engagement) = 'I-INK')

   , wel_03_ltf_ib_step_2 AS
    (SELECT ib.cal_date
          , ib.platform_subset
          , ib.customer_engagement
          , ib.market10       AS geography
          , SUM(ib.units)     AS units
          , SUM(ib.lagged_ib) AS lagged_ib
     FROM wel_02_ltf_ib_step_1 AS ib
     WHERE 1 = 1
       AND month_number <> 1
     GROUP BY ib.cal_date
            , ib.platform_subset
            , ib.customer_engagement
            , ib.market10)

   , wel_04_ltf_ib_step_3 AS
    (SELECT ib.cal_date
          , ib.platform_subset
          , ib.customer_engagement
          , ib.geography
          , ib.units
          , ib.lagged_ib
          , ib.units - ib.lagged_ib                        AS mom_delta
          , -0.02 * ib.units                               AS cancellations
          , (ib.units - ib.lagged_ib) - (-0.02 * ib.units) AS welcome_kits
     FROM wel_03_ltf_ib_step_2 AS ib)

SELECT c2c.cal_date
     , c2c.geography_grain
     , c2c.geography
     , c2c.platform_subset
     , c2c.base_product_number
     , c2c.customer_engagement
     , ROUND(stf.all_enrollments_customer, 0) AS welcome_kits
FROM scen.ink_08_analytic AS c2c
JOIN wel_01_stf_enroll AS stf
    ON stf.cal_date = c2c.cal_date
    AND UPPER(stf.geography) = UPPER(c2c.geography)
    AND UPPER(stf.platform_subset) = UPPER(c2c.platform_subset)
    AND
       UPPER(stf.customer_engagement) = UPPER(c2c.customer_engagement)

UNION ALL

SELECT c2c.cal_date
     , c2c.geography_grain
     , c2c.geography
     , c2c.platform_subset
     , c2c.base_product_number
     , c2c.customer_engagement
     , ROUND(ltf.welcome_kits, 0) AS welcome_kits
FROM scen.ink_08_analytic AS c2c
JOIN wel_04_ltf_ib_step_3 AS ltf
    ON ltf.cal_date = c2c.cal_date
    AND UPPER(ltf.geography) = UPPER(c2c.geography)
    AND UPPER(ltf.platform_subset) = UPPER(c2c.platform_subset)
    AND
       UPPER(ltf.customer_engagement) = UPPER(c2c.customer_engagement)
"""

query_list.append(["scen.ink_12_welcome_kits", ink_12_welcome_kits, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 8.6: vtc

# COMMAND ----------

ink_13_ink_crgs_w_vtc = """
WITH vtc_01_analytic_cartridges AS
    (SELECT cal_date
          , geography
          , platform_subset
          , base_product_number
          , customer_engagement
          , SUM(cartridges)               AS cartridges
          , SUM(imp_corrected_cartridges) AS imp_corrected_cartridges
     FROM scen.ink_08_analytic
     GROUP BY cal_date
            , geography
            , platform_subset
            , base_product_number
            , customer_engagement)

   , vtc_03_norm_ships          AS
    (SELECT cref.country_level_2 AS geography
          , ns.cal_date
          , ns.platform_subset
          , SUM(ns.units)        AS units
     FROM prod.norm_shipments AS ns
     JOIN mdm.iso_cc_rollup_xref AS cref
         ON UPPER(cref.country_alpha2) = UPPER(ns.country_alpha2)
         AND UPPER(cref.country_scenario) = 'Market10'
     WHERE 1 = 1
       AND ns.version = (SELECT MAX(version) FROM prod.norm_shipments)
     GROUP BY cref.country_level_2
            , ns.cal_date
            , ns.platform_subset)

   , c2c_vtc_04_expected_crgs   AS
    (SELECT cr.cal_date
          , cr.geography
          , cr.platform_subset
          , cr.base_product_number
          , cr.customer_engagement
          , COALESCE(ss.cartridges, 0)                                   AS ss_cartridges
          , COALESCE(ns.units, 0)                                        AS hw_units
          , COALESCE(ss.supplies_spares, 0)                              AS supplies_spares_rate
          , COALESCE(ns.units * ss.supplies_spares, 0)                   AS supplies_spares
          , COALESCE(cf.channel_fill, 0)                                 AS channel_fill
          , COALESCE(h.host_units, 0)                                    AS host_cartridges
          , COALESCE(w.welcome_kits, 0)                                  AS welcome_kits
          , cr.cartridges
          , cr.imp_corrected_cartridges
          , cr.imp_corrected_cartridges * 1.0 / NULLIF(cr.cartridges, 0) AS imp
          , cr.cartridges +
            COALESCE(cf.channel_fill, 0) +
            COALESCE(ns.units * ss.supplies_spares, 0)                   AS expected_crgs
     FROM vtc_01_analytic_cartridges AS cr
     LEFT JOIN scen.ink_10_supplies_spares AS ss
         ON cr.cal_date = ss.cal_date
         AND UPPER(cr.geography) = UPPER(ss.geography)
         AND UPPER(cr.base_product_number) = UPPER(ss.base_product_number)
         AND UPPER(cr.platform_subset) = UPPER(ss.platform_subset)
         AND UPPER(cr.customer_engagement) = UPPER(ss.customer_engagement)
     LEFT JOIN scen.ink_09_channel_fill AS cf
         ON cr.cal_date = cf.cal_date
         AND UPPER(cr.geography) = UPPER(cf.geography)
         AND UPPER(cr.base_product_number) = UPPER(cf.base_product_number)
         AND UPPER(cr.platform_subset) = UPPER(cf.platform_subset)
         AND UPPER(cr.customer_engagement) = UPPER(cf.customer_engagement)
     LEFT JOIN vtc_03_norm_ships AS ns
         ON UPPER(ns.geography) = UPPER(cr.geography)
         AND UPPER(ns.platform_subset) = UPPER(cr.platform_subset)
         AND ns.cal_date = cr.cal_date
     LEFT JOIN scen.ink_11_host AS h
         ON cr.cal_date = h.cal_date
         AND UPPER(cr.geography) = UPPER(h.geography)
         AND UPPER(cr.base_product_number) = UPPER(h.base_product_number)
         AND UPPER(cr.platform_subset) = UPPER(h.platform_subset)
         AND UPPER(cr.customer_engagement) = UPPER(h.customer_engagement)
     LEFT JOIN scen.ink_12_welcome_kits AS w
         ON cr.cal_date = w.cal_date
         AND UPPER(cr.geography) = UPPER(w.geography)
         AND UPPER(cr.base_product_number) = UPPER(w.base_product_number)
         AND UPPER(cr.platform_subset) = UPPER(w.platform_subset)
         AND UPPER(cr.customer_engagement) = UPPER(w.customer_engagement))

   , c2c_vtc_05_vtc_calc        AS
    (SELECT ac.cal_date
          , ac.geography
          , ac.base_product_number
          , ac.platform_subset
          , ac.customer_engagement
          , ac.cartridges
          , ac.imp_corrected_cartridges
          , ac.channel_fill
          , ac.hw_units
          , ac.supplies_spares_rate
          , ac.supplies_spares
          , ac.expected_crgs
          , ac.host_cartridges
          , ac.welcome_kits
          , ac.imp
          , COALESCE(SUM(ac.imp_corrected_cartridges)
                     OVER (PARTITION BY ac.cal_date, ac.geography, ac.base_product_number),
                     0) /
            NULLIF(SUM(ac.expected_crgs)
                   OVER (PARTITION BY ac.cal_date, ac.geography, ac.base_product_number),
                   0) AS vtc
     FROM c2c_vtc_04_expected_crgs AS ac)

   , c2c_vtc_02_forecast_months AS
    (SELECT DATEADD(MONTH, 1, MAX(hw.cal_date)) AS hw_forecast_start
          , MAX(sup.supplies_forecast_start)    AS supplies_forecast_start
     FROM prod.norm_shipments AS hw
     CROSS JOIN (SELECT DATEADD(MONTH, 1, MAX(sup.cal_date)) AS supplies_forecast_start
                 FROM prod.actuals_supplies AS sup
                 WHERE 1 = 1
                   AND sup.official = 1) AS sup
     WHERE 1 = 1
       AND UPPER(hw.record) = 'ACTUALS - HW'
       AND hw.version = (SELECT MAX(version) FROM prod.norm_shipments))

   , c2c_vtc_06_vol_count       AS
    (SELECT DISTINCT geography
                   , platform_subset
                   , base_product_number
                   , customer_engagement
                   , COUNT(cal_date)
                     OVER (PARTITION BY geography, base_product_number) AS vol_count -- count of months with volume
     FROM c2c_vtc_05_vtc_calc
     CROSS JOIN c2c_vtc_02_forecast_months AS fm
     WHERE 1 = 1
       AND imp_corrected_cartridges <> 0
       AND cal_date BETWEEN DATEADD(MONTH, -24,
                                    fm.supplies_forecast_start) AND DATEADD(
             MONTH, -1, fm.supplies_forecast_start) -- 24 month window
    )

   , c2c_vtc_07_ma_vtc_prep     AS
    (SELECT 'ACTUALS'                                                    AS type
          , vtcc.cal_date
          , vtcc.geography
          , vtcc.base_product_number
          , vtcc.platform_subset
          , vtcc.customer_engagement
          , vtcc.cartridges
          , vtcc.imp_corrected_cartridges
          , vtcc.channel_fill
          , vtcc.supplies_spares
          , vtcc.host_cartridges
          , vtcc.welcome_kits
          , vtcc.expected_crgs
          , vtcc.imp
          , vtcc.vtc
          , vol_counts.vol_count
          , MAX(vtcc.cal_date)
            OVER (PARTITION BY vtcc.geography, vtcc.base_product_number) AS max_cal_date
     FROM c2c_vtc_05_vtc_calc AS vtcc
     CROSS JOIN c2c_vtc_02_forecast_months AS fm
     LEFT JOIN c2c_vtc_06_vol_count AS vol_counts
         ON UPPER(vol_counts.geography) =
            UPPER(vtcc.geography)
         AND
            UPPER(vol_counts.platform_subset) =
            UPPER(vtcc.platform_subset)
         AND
            UPPER(vol_counts.base_product_number) =
            UPPER(vtcc.base_product_number)
         AND
            UPPER(vol_counts.customer_engagement) =
            UPPER(vtcc.customer_engagement)
     WHERE 1 = 1
       AND vtcc.cal_date < fm.supplies_forecast_start

     UNION ALL

     SELECT 'FORECAST' AS type
          , vtcc.cal_date
          , vtcc.geography
          , vtcc.base_product_number
          , vtcc.platform_subset
          , vtcc.customer_engagement
          , vtcc.cartridges
          , vtcc.imp_corrected_cartridges
          , vtcc.channel_fill
          , vtcc.supplies_spares
          , vtcc.host_cartridges
          , vtcc.welcome_kits
          , vtcc.expected_crgs
          , vtcc.imp
          , vtcc.vtc
          , NULL       AS vol_count
          , NULL       AS max_cal_date
     FROM c2c_vtc_05_vtc_calc AS vtcc
     CROSS JOIN c2c_vtc_02_forecast_months AS fm
     WHERE 1 = 1
       AND vtcc.cal_date >= fm.supplies_forecast_start)

   , c2c_vtc_08_ma_vtc          AS
    (SELECT vtcc.geography
          , vtcc.base_product_number
          , MAX(vtcc.vol_count)                AS vol_count
          , SUM(vtcc.imp_corrected_cartridges) AS ma_vol -- used for 9 month MA; numerator
          , SUM(vtcc.expected_crgs)            AS ma_exp -- used for 9 month MA; denominator
     FROM c2c_vtc_07_ma_vtc_prep AS vtcc
     WHERE 1 = 1
       AND UPPER(vtcc.type) = 'ACTUALS'
       AND vtcc.cal_date BETWEEN DATEADD(MONTH, -8, vtcc.max_cal_date) AND vtcc.max_cal_date
     GROUP BY vtcc.geography
            , vtcc.base_product_number)

   , c2c_vtc_09_ma_vtc_proj     AS
    (SELECT vtcc.cal_date
          , vtcc.geography
          , vtcc.base_product_number
          , vtcc.platform_subset
          , vtcc.customer_engagement
          , vtcc.cartridges
          , vtcc.expected_crgs
          , vtcc.channel_fill
          , vtcc.supplies_spares
          , vtcc.host_cartridges
          , vtcc.welcome_kits
          , vtcc.imp
          , vtcc.imp_corrected_cartridges
          , vtcc.vtc
          , COALESCE(vtcc.vol_count, f.vol_count) AS vol_count
          , f.ma_vol
          , f.ma_exp
          , CASE
            WHEN vtcc.cal_date < fm.supplies_forecast_start
                THEN vtcc.vtc -- history, use VTC
            WHEN f.vol_count >= 9 AND
                 vtcc.cal_date >=
                 fm.supplies_forecast_start
                THEN f.mvtc -- else use MVTC based on last month of actuals
            WHEN f.vol_count < 9 AND
                 vtcc.cal_date >=
                 fm.supplies_forecast_start
                THEN 1.0 -- if we don't use MA VTC then use VTC or 1.0; problematic for first month of forecast
                ELSE 1.0 END                      AS mvtc -- use 1.0 as a placeholder for anything else; could create issues in the forecast window
     FROM c2c_vtc_07_ma_vtc_prep AS vtcc
     CROSS JOIN c2c_vtc_02_forecast_months AS fm
     LEFT JOIN
     (SELECT DISTINCT geography
                    , base_product_number
                    , vol_count
                    , ma_vol
                    , ma_exp
                    , ma_vol * 1.0 / NULLIF(ma_exp, 0) AS mvtc
      FROM c2c_vtc_08_ma_vtc) AS f
         ON UPPER(f.geography) = UPPER(vtcc.geography)
         AND UPPER(f.base_product_number) =
             UPPER(vtcc.base_product_number))

   , c2c_vtc                    AS
    (SELECT cal_date
          , geography
          , base_product_number
          , platform_subset
          , customer_engagement
          , cartridges
          , expected_crgs
          , channel_fill
          , supplies_spares
          , host_cartridges
          , welcome_kits
          , imp
          , imp_corrected_cartridges
          , vtc
          , mvtc
          , vol_count
          , ma_vol
          , ma_exp
     FROM c2c_vtc_09_ma_vtc_proj)

SELECT 'CONVERT_TO_CARTRIDGE'           AS record
     , vtc.cal_date
     , 'MARKET10'                       AS geography_grain
     , vtc.geography
     , vtc.platform_subset
     , vtc.base_product_number
     , vtc.customer_engagement
     , vtc.cartridges
     , vtc.imp                          AS vol_rate
     , vtc.imp_corrected_cartridges     AS volume
     , COALESCE(vtc.channel_fill, 0)    AS channel_fill
     , COALESCE(vtc.supplies_spares, 0) AS supplies_spares_crgs
     , COALESCE(vtc.host_cartridges, 0) AS host_crgs
     , COALESCE(vtc.welcome_kits, 0)    AS welcome_kits
     , COALESCE(vtc.expected_crgs, 0)   AS expected_crgs
     , COALESCE(vtc.vtc, 0)             AS vtc
     , COALESCE(vtc.vtc, 0) *
       COALESCE(vtc.expected_crgs, 0)   AS vtc_adjusted_crgs
     , COALESCE(vtc.mvtc, 0)            AS mvtc
     , COALESCE(vtc.mvtc, 0) *
       COALESCE(vtc.expected_crgs, 0)   AS mvtc_adjusted_crgs
     , vtc.vol_count
     , vtc.ma_vol
     , vtc.ma_exp
     , NULL                             AS load_date
     , NULL                             AS version
FROM c2c_vtc AS vtc
WHERE 1 = 1

UNION ALL

-- bring in host_cartridges only cartridges from hostadj_06
SELECT 'CONVERT_TO_CARTRIDGE'    AS record
     , h.cal_date
     , 'MARKET10'                AS geography_grain
     , h.geography
     , h.platform_subset
     , h.base_product_number
     , h.customer_engagement -- TRAD only
     , 0.0                       AS cartridges
     , 0.0                       AS vol_rate
     , 0.0                       AS volume
     , 0.0                       AS channel_fill
     , 0.0                       AS supplies_spares_crgs
     , COALESCE(h.host_units, 0) AS host_crgs
     , 0.0                       AS welcome_kits
     , 0.0                       AS expected_crgs
     , 0.0                       AS vtc
     , 0.0                       AS vtc_adjusted_crgs
     , 0.0                       AS mvtc
     , 0.0                       AS mvtc_adjusted_crgs
     , 0.0                       AS vol_count
     , 0.0                       AS ma_vol
     , 0.0                       AS ma_exp
     , NULL                      AS load_date
     , NULL                      AS version
FROM scen.ink_11_host AS h
LEFT OUTER JOIN c2c_vtc AS vtc
    ON vtc.cal_date = h.cal_date
    AND UPPER(vtc.geography) = UPPER(h.geography)
    AND UPPER(vtc.platform_subset) = UPPER(h.platform_subset)
    AND UPPER(vtc.base_product_number) = UPPER(h.base_product_number)
    AND UPPER(vtc.customer_engagement) = UPPER(h.customer_engagement)
WHERE 1 = 1
  AND vtc.cal_date IS NULL
  AND vtc.geography IS NULL
  AND vtc.platform_subset IS NULL
  AND vtc.base_product_number IS NULL
  AND vtc.customer_engagement IS NULL
"""

query_list.append(["scen.ink_13_ink_crgs_w_vtc", ink_13_ink_crgs_w_vtc, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 9: ink_working_forecast

# COMMAND ----------

ink_working_fcst = """
WITH geography_mapping   AS
    (SELECT DISTINCT market10 AS market_10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND NOT region_5 IS NULL
       AND NOT region_5 = 'JP'
       AND NOT region_5 LIKE 'X%')

   , crg_months          AS
    (SELECT date_key
          , [date] AS cal_date
     FROM mdm.calendar
     WHERE 1 = 1
       AND day_of_month = 1)

   , vtc_03_norm_ships   AS
    (SELECT cref.country_level_2 AS geography
          , ns.cal_date
          , ns.platform_subset
          , SUM(ns.units)        AS units
     FROM prod.norm_shipments AS ns
     JOIN mdm.iso_cc_rollup_xref AS cref
         ON UPPER(cref.country_alpha2) = UPPER(ns.country_alpha2)
         AND UPPER(cref.country_scenario) = 'Market10'
     WHERE 1 = 1
       AND ns.version = (SELECT MAX(version) FROM prod.norm_shipments)
     GROUP BY cref.country_level_2
            , ns.cal_date
            , ns.platform_subset)

   , supplies_enrichment AS
    (SELECT DISTINCT xref.platform_subset   AS platform_subset
                   , xref.hw_product_family AS supplies_product_family
                   , xref.brand             AS supplies_family
                   , xref.supplies_mkt_cat  AS supplies_mkt_cat
                   , xref.epa_family        AS epa_family
     FROM mdm.hardware_xref AS xref
     WHERE 1 = 1
       AND xref.technology IN ('INK', 'PWA'))

   , ink_supplies_xref AS
    (SELECT DISTINCT s.base_product_number
                   , CASE WHEN s.crg_chrome = 'DRUM'                THEN 'DRUM'
                          WHEN s.base_product_number IN
                               ('W8004A', 'W8004J', 'W8006J', 'W8007J',
                                'W8007X', 'W8009A', 'W8009J', 'W8000J',
                                'W8001J',
                                'W8002J', 'W8003J', 'CRTGCHERRYITPJ',
                                'EYRIEMLKJDMITP', 'EYRIEMLKXDMITP')
                                                                    THEN 'INSTANT TONER'
                          WHEN s.base_product_number LIKE '%A'      THEN 'A'
                          WHEN s.base_product_number LIKE '%AC'     THEN 'AC'
                          WHEN s.base_product_number LIKE '%MC'     THEN 'MC'
                          WHEN s.base_product_number LIKE '%X'      THEN 'X'
                          WHEN s.base_product_number LIKE '%XC'     THEN 'XC'
                          WHEN s.base_product_number LIKE '%YC' OR
                               s.base_product_number LIKE '%JC'     THEN 'YC/JC'
                                                                    ELSE NULL
            END AS cartridge_type
     FROM mdm.supplies_xref AS s
     WHERE 1 = 1
       AND s.technology IN ('INK', 'PWA'))

   , country_code_xref   AS
    (SELECT DISTINCT market10
                   , region_3
                   , region_4
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE NOT market10 IS NULL
       AND NOT region_3 IS NULL
       AND NOT region_4 IS NULL
       AND NOT region_5 IS NULL
       AND NOT market10 = 'World Wide'
       AND NOT region_4 = 'JP'
       AND NOT region_5 = 'XU')

   , yield               AS
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

   , pen_fills           AS
    (SELECT y.base_product_number
          , m.cal_date
          , y.market_10
          , y.yield
     FROM yield AS y
     JOIN crg_months AS m
         ON y.effective_date <= m.cal_date
         AND y.next_effective_date > m.cal_date)

   , override_filters    AS
    (SELECT 'IB'         AS record
          , 'SYSTEM'     AS user_name
          , NULL         AS load_date
          , MAX(version) AS version
     FROM prod.ib

     UNION ALL

     SELECT 'USAGE_SHARE' AS record
          , 'SYSTEM'      AS user_name
          , NULL          AS load_date
          , MAX(version)  AS version
     FROM prod.usage_share

     UNION ALL

     SELECT DISTINCT 'SCENARIO_USAGE_SHARE'                 AS record
                   , us_scen.user_name                      AS user_name
                   , us_scen.load_date                      AS load_date
                   , CAST(us_scen.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_usage_share AS us_scen
     WHERE 1 = 1
       AND us_scen.upload_type = 'WORKING-FORECAST'
       AND us_scen.user_name IN ('SAIMANK', 'SONSEEAHRAYR', 'ZACP')

     UNION ALL

     SELECT DISTINCT 'SCENARIO_MIX_RATE'                AS record
                   , smr.user_name                      AS user_name
                   , smr.load_date                      AS load_date
                   , CAST(smr.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_mix_rate AS smr
     WHERE 1 = 1
       AND smr.upload_type = 'WORKING-FORECAST'
       AND smr.user_name IN ('SAIMANK', 'SONSEEAHRAYR', 'ZACP')

     UNION ALL

     SELECT DISTINCT 'SCENARIO_YIELD'                      AS record
                   , scen_y.user_name                      AS user_name
                   , scen_y.load_date                      AS load_date
                   , CAST(scen_y.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_yield AS scen_y
     WHERE 1 = 1
       AND scen_y.upload_type = 'WORKING-FORECAST'
       AND scen_y.user_name IN ('SAIMANK', 'SONSEEAHRAYR', 'ZACP')

     UNION ALL

     SELECT DISTINCT 'SCENARIO_CHANNEL_FILL'           AS record
                   , cf.user_name                      AS user_name
                   , cf.load_date                      AS load_date
                   , CAST(cf.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_channel_fill AS cf
     WHERE 1 = 1
       AND cf.upload_type = 'WORKING-FORECAST'
       AND cf.user_name IN ('SAIMANK', 'SONSEEAHRAYR', 'ZACP')

     UNION ALL

     SELECT DISTINCT 'SCENARIO_SUPPLIES_SPARES'         AS record
                   , ssp.user_name                      AS user_name
                   , ssp.load_date                      AS load_date
                   , CAST(ssp.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_supplies_spares AS ssp
     WHERE 1 = 1
       AND ssp.upload_type = 'WORKING-FORECAST'
       AND ssp.user_name IN ('SAIMANK', 'SONSEEAHRAYR', 'ZACP')

     UNION ALL

     SELECT DISTINCT 'SCENARIO_VTC_OVERRIDE'          AS record
                   , v.user_name                      AS user_name
                   , v.load_date                      AS load_date
                   , CAST(v.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_vtc_override AS v
     WHERE 1 = 1
       AND v.upload_type = 'WORKING-FORECAST'
       AND v.user_name IN
           ('SAIMANK', 'SONSEEAHRAYR', 'ZACP'))

   , ink_cf_prep       AS
    (SELECT fv.user_name
          , MAX(fv.load_date) AS max_load_date
     FROM override_filters AS fv
     WHERE 1 = 1
       AND fv.record = 'SCENARIO_CHANNEL_FILL'
     GROUP BY fv.user_name)

   , ink_cf_subset     AS
    (SELECT DISTINCT DATEADD(MONTH, cf.month_num, cf.min_sys_date) AS cal_date
                   , cf.geography
                   , cf.platform_subset
                   , cf.base_product_number
                   , cf.customer_engagement
                   , cf.value                                      AS channel_fill
     FROM scen.working_forecast_channel_fill AS cf
     JOIN ink_cf_prep AS cfp
         ON cfp.user_name = cf.user_name
         AND cfp.max_load_date = cf.load_date
     WHERE 1 = 1
       AND cf.upload_type = 'WORKING-FORECAST'
       AND cf.geography_grain = 'MARKET10'

     UNION ALL

     SELECT DISTINCT DATEADD(MONTH, cf.month_num, cf.min_sys_date)         AS cal_date
                   , geo.market_10                                         AS geography
                   , cf.platform_subset
                   , cf.base_product_number
                   , cf.customer_engagement
                   , CASE WHEN cf.geography = 'AP' THEN cf.value / 3.0
                          WHEN cf.geography = 'EU' THEN cf.value / 5.0
                          WHEN cf.geography = 'LA' THEN cf.value / 1.0
                          WHEN cf.geography = 'NA'
                                                   THEN cf.value / 1.0 END AS channel_fill
     FROM scen.working_forecast_channel_fill AS cf
     JOIN ink_cf_prep AS cfp
         ON cfp.user_name = cf.user_name
         AND cfp.max_load_date = cf.load_date
     JOIN geography_mapping AS geo
         ON geo.region_5 = cf.geography
     WHERE 1 = 1
       AND cf.upload_type = 'WORKING-FORECAST'
       AND cf.geography_grain = 'REGION_5')

   , ink_cf            AS
    (SELECT DISTINCT cfs.cal_date
                   , cfs.geography
                   , cfs.platform_subset
                   , cfs.base_product_number
                   , cfs.customer_engagement
                   , ROUND(cfs.channel_fill, 0) AS channel_fill
     FROM ink_cf_subset AS cfs
     WHERE 1 = 1)

   , ink_ss_prep       AS
    (SELECT fv.user_name
          , MAX(fv.load_date) AS max_load_date
     FROM override_filters AS fv
     WHERE 1 = 1
       AND fv.record = 'SCENARIO_SUPPLIES_SPARES'
     GROUP BY fv.user_name)

   , ink_ss_subset     AS
    (SELECT ssp.geography
          , ssp.platform_subset
          , ssp.base_product_number
          , ssp.customer_engagement
          , ssp.min_sys_date
          , ssp.month_num
          , ssp.value
     FROM scen.working_forecast_supplies_spares AS ssp
     JOIN ink_ss_prep AS sspr
         ON sspr.user_name = ssp.user_name
         AND sspr.max_load_date = ssp.load_date
     WHERE 1 = 1
       AND ssp.upload_type = 'WORKING-FORECAST'
       AND ssp.geography_grain = 'MARKET10'

     UNION ALL

     SELECT geo.market_10                                     AS geography
          , ssp.platform_subset
          , ssp.base_product_number
          , ssp.customer_engagement
          , ssp.min_sys_date
          , ssp.month_num
          , CASE WHEN ssp.geography = 'EU' THEN ssp.value / 4.0
                                           ELSE ssp.value END AS value
     FROM scen.working_forecast_supplies_spares AS ssp
     JOIN ink_ss_prep AS sspr
         ON sspr.user_name = ssp.user_name
         AND sspr.max_load_date = ssp.load_date
     JOIN geography_mapping AS geo
         ON geo.region_5 = ssp.geography
     WHERE 1 = 1
       AND ssp.upload_type = 'WORKING-FORECAST'
       AND ssp.geography_grain = 'REGION_5')

   , ink_ss            AS
    (SELECT DISTINCT DATEADD(MONTH, ssp.month_num, ssp.min_sys_date) AS cal_date
                   , ssp.geography
                   , ssp.platform_subset
                   , ssp.base_product_number
                   , ssp.customer_engagement
                   , ROUND(ssp.value, 0)                             AS supplies_spares -- treat value as units
     FROM ink_ss_subset AS ssp
     LEFT JOIN vtc_03_norm_ships AS c2c
         ON ssp.geography = c2c.geography
         AND DATEADD(MONTH, ssp.month_num, ssp.min_sys_date) = c2c.cal_date
         AND ssp.platform_subset = c2c.platform_subset
     WHERE 1 = 1)

   , ink_vtc_prep      AS
    (SELECT fv.user_name
          , MAX(fv.load_date) AS max_load_date
     FROM override_filters AS fv
     WHERE 1 = 1
       AND fv.record = 'SCENARIO_VTC_OVERRIDE'
     GROUP BY fv.user_name)

   , ink_vtc_subset    AS
    (SELECT v.geography
          , v.base_product_number
          , v.min_sys_date
          , v.month_num
          , v.value
     FROM scen.working_forecast_vtc_override AS v
     JOIN ink_vtc_prep AS vp
         ON vp.user_name = v.user_name
         AND vp.max_load_date = v.load_date
     WHERE 1 = 1
       AND v.upload_type = 'WORKING-FORECAST'
       AND v.geography_grain = 'MARKET10'

     UNION ALL

     SELECT geo.market_10 AS geography
          , v.base_product_number
          , v.min_sys_date
          , v.month_num
          , v.value
     FROM scen.working_forecast_vtc_override AS v
     JOIN ink_vtc_prep AS vp
         ON vp.user_name = v.user_name
         AND vp.max_load_date = v.load_date
     JOIN geography_mapping AS geo
         ON geo.region_5 = v.geography
     WHERE 1 = 1
       AND v.upload_type = 'WORKING-FORECAST'
       AND v.geography_grain = 'REGION_5')

   , ink_vtc           AS
    (SELECT DISTINCT DATEADD(MONTH, v.month_num, v.min_sys_date) AS cal_date
                   , v.geography
                   , v.base_product_number
                   , v.value                                     AS mvtc
     FROM ink_vtc_subset AS v
     WHERE 1 = 1)

   , ink_working_fcst  AS
    (SELECT 'IE2-WORKING-FORECAST'                                 AS record
          , GETDATE()                                              AS build_time
          , vtc.cal_date
          , vtc.geography_grain
          , vtc.geography
          , vtc.platform_subset
          , vtc.base_product_number
          , vtc.customer_engagement
          , vtc.cartridges
          , COALESCE(cf.channel_fill, vtc.channel_fill)            AS channel_fill
          , COALESCE(ss.supplies_spares,
                     vtc.supplies_spares_crgs)                     AS supplies_spares_cartridges
          , vtc.cartridges + COALESCE(cf.channel_fill, vtc.channel_fill) +
            COALESCE(ss.supplies_spares,
                     vtc.supplies_spares_crgs)                     AS expected_cartridges
          , COALESCE(p.mvtc, vtc.mvtc)                             AS vtc
          , COALESCE(COALESCE(p.mvtc, vtc.mvtc) * (vtc.cartridges +
                                                   COALESCE(cf.channel_fill, vtc.channel_fill) +
                                                   COALESCE(ss.supplies_spares,
                                                            vtc.supplies_spares_crgs)),
                     vtc.mvtc_adjusted_crgs)                       AS adjusted_cartridges
          , enr.supplies_product_family
          , enr.supplies_family
          , enr.supplies_mkt_cat
          , enr.epa_family
          , hw.pl                                                  AS hw_pl
          , cc.region_3
          , cc.region_4
          , cc.region_5
          , cc.market10
          , cal.fiscal_year_qtr
          , cal.fiscal_yr
          , CAST(vtc.cal_date AS VARCHAR(25)) + '-' + vtc.platform_subset +
            '-' + vtc.base_product_number + '-' + vtc.geography + '-' +
            vtc.customer_engagement                                AS composite_key
          , NULL                                                   AS cartridge_type
          , pf.yield
     FROM scen.ink_13_ink_crgs_w_vtc AS vtc
     LEFT JOIN ink_cf AS cf
         ON cf.geography = vtc.geography
         AND cf.cal_date = vtc.cal_date
         AND cf.platform_subset = vtc.platform_subset
         AND cf.base_product_number = vtc.base_product_number
         AND cf.customer_engagement = vtc.customer_engagement
     LEFT JOIN ink_ss AS ss
         ON ss.geography = vtc.geography
         AND ss.cal_date = vtc.cal_date
         AND ss.platform_subset = vtc.platform_subset
         AND ss.base_product_number = vtc.base_product_number
         AND ss.customer_engagement = vtc.customer_engagement
     LEFT JOIN ink_vtc AS p
         ON p.geography = vtc.geography
         AND p.cal_date = vtc.cal_date
         AND p.base_product_number = vtc.base_product_number
     LEFT JOIN supplies_enrichment AS enr
         ON enr.platform_subset = vtc.platform_subset
     JOIN mdm.calendar AS cal
         ON vtc.cal_date = cal.date
     JOIN mdm.hardware_xref AS hw
         ON vtc.platform_subset = hw.platform_subset
     JOIN country_code_xref AS cc
         ON vtc.geography = cc.market10
     LEFT JOIN ink_supplies_xref AS supp
         ON supp.base_product_number = vtc.base_product_number
     LEFT JOIN pen_fills AS pf
         ON pf.market_10 = cc.market10
         AND pf.cal_date = vtc.cal_date
         AND pf.base_product_number = vtc.base_product_number
     WHERE 1 = 1
       AND hw.technology IN ('INK', 'PWA')

     UNION ALL

     SELECT 'IE2-WORKING-FORECAST'         AS record
          , GETDATE()                      AS build_time
          , cf.cal_date
          , 'MARKET10'                     AS geography_grain
          , cf.geography
          , cf.platform_subset
          , cf.base_product_number
          , cf.customer_engagement
          , 0.0                            AS cartridges
          , COALESCE(cf.channel_fill, 0.0) AS channel_fill
          , 0.0                            AS supplies_spares_cartridges
          , COALESCE(cf.channel_fill, 0.0) AS expected_cartridges
          , 1.0                            AS vtc
          , COALESCE(cf.channel_fill, 0.0) AS adjusted_cartridges
          , enr.supplies_product_family
          , enr.supplies_family
          , enr.supplies_mkt_cat
          , enr.epa_family
          , hw.pl                          AS hw_pl
          , cc.region_3
          , cc.region_4
          , cc.region_5
          , cc.market10
          , cal.fiscal_year_qtr
          , cal.fiscal_yr
          , CAST(cf.cal_date AS VARCHAR(25)) + '-' + cf.platform_subset + '-' +
            cf.base_product_number + '-' + cf.geography + '-' +
            cf.customer_engagement         AS composite_key
          , NULL                           AS cartridge_type
          , pf.yield
     FROM ink_cf AS cf -- allows forecasters to upload records not in system for cf
     LEFT OUTER JOIN scen.ink_13_ink_crgs_w_vtc AS vtc
         ON cf.geography = vtc.geography
         AND cf.cal_date = vtc.cal_date
         AND cf.platform_subset = vtc.platform_subset
         AND cf.base_product_number = vtc.base_product_number
         AND cf.customer_engagement = vtc.customer_engagement
     LEFT JOIN supplies_enrichment AS enr
         ON enr.platform_subset = cf.platform_subset
     JOIN mdm.calendar AS cal
         ON cf.cal_date = cal.date
     JOIN mdm.hardware_xref AS hw
         ON cf.platform_subset = hw.platform_subset
     JOIN country_code_xref AS cc
         ON cf.geography = cc.market10
     LEFT JOIN ink_supplies_xref AS supp
         ON supp.base_product_number = cf.base_product_number
     LEFT JOIN pen_fills AS pf
         ON pf.market_10 = cc.market10
         AND pf.cal_date = cf.cal_date
         AND pf.base_product_number = cf.base_product_number
     WHERE 1 = 1
       AND hw.technology IN ('INK', 'PWA')
       AND cf.channel_fill > 0
       AND vtc.geography IS NULL
       AND vtc.cal_date IS NULL
       AND vtc.platform_subset IS NULL
       AND vtc.base_product_number IS NULL
       AND vtc.customer_engagement IS NULL)
 
SELECT *
FROM ink_working_fcst
"""

query_list.append(["scen.ink_working_fcst", ink_working_fcst, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Redshift

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
