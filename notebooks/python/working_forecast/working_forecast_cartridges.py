# Databricks notebook source
# MAGIC %md
# MAGIC #Working Forecast

# COMMAND ----------

# create empty widgets for interactive sessions
dbutils.widgets.text('technology', '') # technology i.e. ink, laser/toner, large format (lf)
dbutils.widgets.text('ib_version', '') # installed base version
dbutils.widgets.text('usage_share_version', '') # usage-share version

# COMMAND ----------

# Global Variables
# retrieve widget values and assign to variables
technology = dbutils.widgets.get('technology').lower()
ib_version = dbutils.widgets.get('ib_version')
usage_share_version = dbutils.widgets.get('usage_share_version')

supply_unit = 'cc' if technology == 'ink' else 'page'
supply_units = supply_unit + 's'

# for labelling tables, laser/toner = toner, ink = ink
technology_label = ''
if technology == 'ink':
    technology_label = 'ink'
elif technology == 'laser':
    technology_label = 'toner'

user_list = {}
user_list["ink"] = ['ANAA','ANA.ANAYA@HP.COM', 'SAIMANK', 'SAIMAN.KUSIN@HP.COM', 'SONSEEAHRAY', 'SONSEEAHRAY.RUCKER@HP.COM', 'ZACP', 'ZACHARY.PEAKE@HP.COM']
user_list["laser"] = ['JON.LINDBERG@HP.COM', 'JOHNF', 'JOHN.FLOCK@HP.COM', 'VANB', 'WILLIAM.VAN.BAIN@HP.COM', 'YONGHOONL', 'YONGHOON.LEE@HP.COM']

technologies_list = {}
technologies_list["ink"] = ['INK', 'PWA']
technologies_list["laser"] = ['LASER']

# COMMAND ----------

# join lists used in SQL queries to comma and single quote separated strings 
def join_list(list_to_join: list) -> str:
    return '\'' + '\',\''.join(list_to_join) + '\''

users = join_list(user_list[technology])
technologies_list = join_list(technologies_list[technology])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 1 :: select correct u/s override records, blow out to market10 for region_5 records

# COMMAND ----------

wf_01_us_uploads = f"""
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
        AND UPPER(us_scen.user_name) IN ({users}))

   , wf_us_prep AS
    (SELECT fv.user_name
          , MAX(fv.load_date) AS max_load_date
     FROM override_filters AS fv
     WHERE 1 = 1
       AND fv.record = 'SCENARIO_USAGE_SHARE'
     GROUP BY fv.user_name)

   , wf_us_subset AS
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
     JOIN wf_us_prep AS usp
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
     JOIN wf_us_prep AS usp
         ON usp.user_name = us.user_name
         AND usp.max_load_date = us.load_date
     JOIN geography_mapping AS geo
         ON geo.region_5 = us.geography
     WHERE 1 = 1
       AND us.upload_type = 'WORKING-FORECAST'
       AND us.geography_grain = 'REGION_5')
       
 select * from wf_us_subset
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 2 :: tranform u/s uploads to demand measures

# COMMAND ----------

wf_02_us_dmd = f"""
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
       AND ib.version = '{ib_version}'
       AND NOT UPPER(hw.product_lifecycle_status) = 'E'
       AND UPPER(hw.technology) IN ({technologies_list})
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
     FROM scen.{technology_label}_01_us_uploads AS us
     JOIN mdm.hardware_xref AS hw
         ON hw.platform_subset = us.platform_subset
     WHERE 1 = 1
       AND UPPER(us.measure) IN
           ('USAGE', 'COLOR_USAGE', 'K_USAGE', 'HP_SHARE')
       AND UPPER(us.geography_grain) = 'MARKET10'
       AND NOT UPPER(hw.product_lifecycle_status) = 'E'
       AND UPPER(hw.technology) IN ({technologies_list})
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
          , SUM(color_usage * hp_share * ib)   AS HP_COLOR_PAGES
          , SUM(color_usage *
                (1 - hp_share) *
                ib)                            AS NON_HP_COLOR_PAGES
          , SUM(color_usage * ib)              AS TOTAL_HP_COLOR_PAGES
          , SUM(k_usage * ib)                  AS TOTAL_K_PAGES
          , SUM(color_usage)                   AS COLOR_USAGE
          , SUM(k_usage)                       AS K_USAGE
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
                                units FOR measure IN (IB, HP_K_PAGES, NON_HP_K_PAGES, HP_COLOR_PAGES, NON_HP_COLOR_PAGES, TOTAL_HP_COLOR_PAGES, TOTAL_K_PAGES, COLOR_USAGE, K_USAGE)
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 2b :: transform u/s to demand measures
# MAGIC Deprecate after these measures exist in phoenix_spectrum_prod.usage_share 

# COMMAND ----------

wf_demand = f"""
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
       AND ib.version = '{ib_version}'
       AND NOT UPPER(hw.product_lifecycle_status) = 'E'
       AND UPPER(hw.technology) IN ({technologies_list})
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
     FROM prod.usage_share  AS us
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = us.platform_subset
     WHERE 1 = 1
       AND us.version = '{usage_share_version}'
       AND UPPER(us.measure) IN
           ('USAGE', 'COLOR_USAGE', 'K_USAGE', 'HP_SHARE')
       AND UPPER(us.geography_grain) = 'MARKET10'
       AND NOT UPPER(hw.product_lifecycle_status) = 'E'
       AND UPPER(hw.technology) IN ({technologies_list})
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
          , SUM(color_usage * hp_share * ib)   AS HP_COLOR_PAGES
          , SUM(color_usage *
                (1 - hp_share) *
                ib)                            AS NON_HP_COLOR_PAGES
          , SUM(color_usage * ib)              AS TOTAL_COLOR_PAGES
          , SUM(k_usage * ib)                  AS TOTAL_K_PAGES
          , SUM(color_usage)                   AS COLOR_USAGE
          , SUM(k_usage)                       AS K_USAGE
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
                                units FOR measure IN (IB, HP_K_PAGES, NON_HP_K_PAGES, HP_COLOR_PAGES, NON_HP_COLOR_PAGES, TOTAL_COLOR_PAGES, TOTAL_K_PAGES, COLOR_USAGE, K_USAGE)
         ) AS final_unpivot)

SELECT dmd.cal_date
      , dmd.geography
      , dmd.platform_subset
      , dmd.customer_engagement
      , dmd.measure
      , dmd.units
 FROM dmd_09_unpivot_measure AS dmd
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 3 :: combine uploads with production data (concat + left outer join)

# COMMAND ----------

wf_03_usage_share = f"""
WITH override_filters  AS
    (SELECT '{ib_version}'    AS ib_version
          , '{usage_share_version}' AS us_version
)


   , wf_usage_share AS
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
     FROM scen.{technology_label}_02_us_dmd AS uss
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
     FROM scen.{technology_label}_demand AS us
     CROSS JOIN override_filters AS fv
     JOIN mdm.hardware_xref AS hw
         ON us.platform_subset = hw.platform_subset
     LEFT OUTER JOIN scen.{technology_label}_02_us_dmd AS us_scen
         ON us.geography = us_scen.geography
         AND us.cal_date = us_scen.cal_date
         AND
            us.platform_subset = us_scen.platform_subset
         AND us.customer_engagement =
             us_scen.customer_engagement
         AND us.measure = us_scen.measure
     WHERE 1 = 1
       AND hw.technology IN ({technologies_list})
       AND us_scen.platform_subset IS NULL
       AND us_scen.geography IS NULL
       AND us_scen.customer_engagement IS NULL
       AND us_scen.cal_date IS NULL)
       
       SELECT * FROM wf_usage_share
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 4 :: run base forecast page mix process

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4.1: shm_base_helper

# COMMAND ----------

wf_shm_base_helper = """
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

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4.2: cartridge_units

# COMMAND ----------

wf_cartridge_units = """
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

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4.3: page_mix_engine

# COMMAND ----------

ink_page_mix_engine = f"""
WITH pcm_02_hp_demand AS
    (SELECT d.cal_date
          , d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_K_PAGES' THEN units END)     AS black_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_C_PAGES' THEN units END)     AS color_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_C_PAGES' THEN units END * 3) AS cmy_demand
     FROM scen.{technology_label}_03_usage_share AS d
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
     FROM scen.{technology_label}_cartridge_units AS v
              JOIN scen.{technology_label}_shm_base_helper AS shm
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
                   , CASE WHEN hw.technology <> 'INK' THEN v.cartridge_volume * pf.yield
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
              JOIN scen.{technology_label}_shm_base_helper AS shm
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

# COMMAND ----------

toner_page_mix_engine = f"""
WITH pcm_02_hp_demand                                AS
    (SELECT d.cal_date
          , d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(CASE
            WHEN UPPER(d.measure) = 'HP_K_PAGES'
                THEN units END) AS black_demand
          , MAX(CASE
            WHEN UPPER(d.measure) = 'HP_COLOR_PAGES'
                THEN units END) AS color_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_COLOR_PAGES' THEN units END *
                3)              AS cmy_demand
     FROM scen.{technology_label}_03_usage_share AS d
     JOIN mdm.hardware_xref AS hw
         ON hw.platform_subset = d.platform_subset
     WHERE 1 = 1
       AND hw.technology = 'LASER'
     GROUP BY d.cal_date
            , d.geography
            , d.platform_subset
            , d.customer_engagement)

   , pcm_03_dmd_date_boundary                        AS
    (SELECT d.geography
          , d.platform_subset
          , d.customer_engagement
          , MAX(d.cal_date) AS max_dmd_date
     FROM pcm_02_hp_demand AS d
     GROUP BY d.geography
            , d.platform_subset
            , d.customer_engagement)

   , pcm_04_crg_actuals                              AS
    (SELECT DISTINCT v.cal_date
                   , v.geography_grain
                   , v.geography
                   , shm.platform_subset
                   , v.base_product_number
                   , v.k_color
                   , v.crg_chrome
                   , v.consumable_type
                   , v.cartridge_volume
     FROM scen.{technology_label}_cartridge_units AS v
     JOIN scen.{technology_label}_shm_base_helper AS shm
         ON shm.base_product_number = v.base_product_number
         AND shm.geography = v.geography
     WHERE 1 = 1
       AND v.cartridge_volume > 0)

   , crg_months                                      AS
    (SELECT date_key
          , [date] AS cal_date
     FROM mdm.calendar
     WHERE 1 = 1
       AND day_of_month = 1)

   , geography_mapping                               AS
    (SELECT DISTINCT market10 AS market_10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND NOT region_5 IS NULL
       AND NOT region_5 = 'JP'
       AND NOT region_5 LIKE 'X%')

   , yield                                           AS
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

   , pen_fills                                       AS
    (SELECT y.base_product_number
          , m.cal_date
          , y.market_10
          , y.yield
     FROM yield AS y
     JOIN crg_months AS m
         ON y.effective_date <= m.cal_date
         AND y.next_effective_date > m.cal_date)

   , pcm_05_pages                                    AS
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
     JOIN scen.{technology_label}_shm_base_helper AS shm
         ON shm.base_product_number = v.base_product_number
         AND shm.platform_subset = v.platform_subset
         AND shm.customer_engagement = dmd.customer_engagement
     WHERE 1 = 1
       AND hw.technology = 'LASER')

   , pcm_06_mix_step_1_k                             AS
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

   , pcm_07_mix_step_1_color_single                  AS
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

   , pcm_08_mix_step_1_color_multi                   AS
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

   , pcm_09_mix_step_2_multi_fix                     AS
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

   , pcm_10_mix_step_3_mix_acts                      AS
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

   , pcm_11_mix_dmd_spread                           AS
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

   , c2c_vtc_02_forecast_months                      AS
    (SELECT DATEADD(MONTH, 1, MAX(sup.cal_date)) AS supplies_forecast_start
     FROM prod.actuals_supplies AS sup
     WHERE 1 = 1
       AND sup.official = 1)

   , pcm_12_mix_weighted_avg_step_1                  AS
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

   , pcm_13_mix_weighted_avg_step_2_k                AS
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

   , pcm_14_mix_weighted_avg_step_3_color_single     AS
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

   , pcm_15_mix_weighted_avg_step_4_color_multi      AS
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

   , pcm_17_mix_weighted_avg                         AS
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

   , pcm_18_projection                               AS
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

SELECT 'PCM_ENGINE_PROJECTION' AS type
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
       'PCM_ENGINE_PROJECTION' AS composite_key
FROM pcm_18_projection AS m18
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4.4: page_cc_mix_override

# COMMAND ----------

wf_page_cc_mix_override = f"""
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
       AND UPPER(hw.technology) IN ({technologies_list})
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
                                                            ELSE 'CRG_MIX' END        AS upload_type -- HARD-CODED cut-line from cartridge mix to page/ccs mix; page_mix is page/ccs mix
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
       AND UPPER(hw.technology) IN ({technologies_list})
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
     , CAST(cal_date AS VARCHAR) || ' ' || market10 || ' ' || platform_subset ||
       ' ' ||
       base_product_number || ' ' || customer_engagement AS composite_key
FROM add_type_and_yield
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### page_cc_mix_complete

# COMMAND ----------

wf_page_cc_mix_complete = f"""
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
             , m19.{supply_unit}_mix
        FROM scen.{technology_label}_page_mix_engine AS m19
        LEFT OUTER JOIN scen.{technology_label}_page_cc_mix_override AS m26
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

        -- {supply_unit} mix uploads
        SELECT 'PCM_FORECASTER_OVERRIDE' AS type
             , m26.cal_date
             , 'MARKET10' AS geography_grain
             , m26.market10 AS geography
             , m26.platform_subset
             , m26.base_product_number
             , m26.customer_engagement
             , m26.mix_pct AS {supply_unit}_mix
        FROM scen.{technology_label}_page_cc_mix_override AS m26
        JOIN mdm.hardware_xref AS hw
            ON hw.platform_subset = m26.platform_subset
        WHERE 1=1
        AND hw.technology IN ({technologies_list})
    )

   , pcm_28_pages_ccs_mix_filter AS
    (SELECT m27.type
          , m27.cal_date
          , m27.geography_grain
          , m27.geography
          , m27.platform_subset
          , m27.base_product_number
          , m27.customer_engagement
          , m27.{supply_unit}_mix
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
          , m27.{supply_unit}_mix
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
          , m27.{supply_unit}_mix
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
     , m28.{supply_unit}_mix
     , CAST(m28.cal_date AS VARCHAR) || ' ' || m28.geography || ' ' || m28.platform_subset || ' ' ||
       m28.base_product_number || ' ' || m28.customer_engagement AS composite_key
FROM pcm_28_pages_ccs_mix_filter AS m28
         JOIN scen.{technology_label}_shm_base_helper AS shm
              ON UPPER(shm.geography) = UPPER(m28.geography)
                  AND UPPER(shm.platform_subset) = UPPER(m28.platform_subset)
                  AND UPPER(shm.base_product_number) = UPPER(m28.base_product_number)
                  AND UPPER(shm.customer_engagement) = UPPER(m28.customer_engagement)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### page_cc_mix

# COMMAND ----------

wf_04_page_cc_mix = f"""
SELECT type
     , cal_date
     , geography_grain
     , geography
     , platform_subset
     , base_product_number
     , customer_engagement
     , {supply_unit}_mix AS mix_rate
     , composite_key
FROM scen.{technology_label}_{supply_unit}_mix_complete
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 5 :: select correct page/cc mix override records, blow out to market10 for region_5 records

# COMMAND ----------

wf_05_mix_uploads = f"""
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
       AND UPPER(smr.user_name) IN ({users}))

   , wf_mix_rate_prep   AS
    (SELECT fv.user_name
          , MAX(fv.load_date) AS max_load_date
     FROM override_filters AS fv
     WHERE 1 = 1
       AND fv.record = 'SCENARIO_MIX_RATE'
     GROUP BY fv.user_name)

   , wf_mix_rate_subset AS
    (SELECT smr.month_num
          , smr.min_sys_date
          , smr.geography
          , smr.platform_subset
          , smr.base_product_number
          , smr.customer_engagement
          , smr.value
     FROM scen.working_forecast_mix_rate AS smr
     JOIN wf_mix_rate_prep AS mrp
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
     JOIN wf_mix_rate_prep AS mrp
         ON mrp.user_name = smr.user_name
         AND mrp.max_load_date = smr.load_date
     JOIN geography_mapping AS geo
         ON geo.region_5 = smr.geography
     WHERE 1 = 1
       AND smr.upload_type = 'WORKING-FORECAST'
       AND smr.geography_grain = 'REGION_5')
       
SELECT *
FROM wf_mix_rate_subset
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 6: combine uploads with transformed data (concat + left outer join) 

# COMMAND ----------

wf_06_mix_rate_final = f"""
WITH wf_mix_rate_final AS
         (SELECT DATEADD(MONTH, smr.month_num, smr.min_sys_date)    AS cal_date
               , 'MARKET10'                                         AS geography_grain
               , smr.geography
               , smr.platform_subset
               , smr.base_product_number
               , smr.customer_engagement
               , smr.value                                          AS mix_rate
               , 'PCM_21_2'                                         AS type
               , CASE
                 WHEN s.single_multi = 'TRI-PACK' THEN 'MULTI'
                                                  ELSE 'SINGLE' END AS single_multi
          FROM scen.{technology_label}_05_mix_uploads AS smr
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
              WHEN s.single_multi = 'TRI-PACK' THEN 'MULTI'
                                               ELSE 'SINGLE' END AS single_multi
          FROM scen.{technology_label}_04_{supply_unit}_mix AS mix
          JOIN mdm.supplies_xref AS s
              ON s.base_product_number = mix.base_product_number
          LEFT OUTER JOIN scen.{technology_label}_05_mix_uploads AS smr
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
FROM wf_mix_rate_final
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 7: run page_cc_cartridges process with new input dataset

# COMMAND ----------

wf_07_page_cc_cartridges = f"""
WITH crg_months               AS
    (SELECT date_key
          , date AS cal_date
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
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_COLOR_PAGES'
                         THEN units END)                                      AS color_demand
          , MAX(CASE WHEN UPPER(d.measure) = 'HP_COLOR_PAGES' THEN units END *
                3)                                                            AS cmy_demand
     FROM scen.{technology_label}_03_usage_share AS d
     JOIN mdm.hardware_xref AS hw
         ON UPPER(hw.platform_subset) = UPPER(d.platform_subset)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ({technologies_list})
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
     FROM scen.{technology_label}_cartridge_units AS v
     JOIN scen.{technology_label}_shm_base_helper AS shm
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
     JOIN scen.{technology_label}_shm_base_helper AS shm
         ON UPPER(shm.base_product_number) = UPPER(v.base_product_number)
         AND UPPER(shm.platform_subset) = UPPER(v.platform_subset)
         AND UPPER(shm.customer_engagement) = UPPER(dmd.customer_engagement)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ({technologies_list})
    )

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
     FROM scen.{technology_label}_06_mix_rate_final AS pcm
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
     FROM scen.{technology_label}_06_mix_rate_final AS pcm
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
     FROM scen.{technology_label}_06_mix_rate_final AS pcm
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
     FROM scen.{technology_label}_06_mix_rate_final AS pcm
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
     FROM scen.{technology_label}_06_mix_rate_final AS pcm
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
     FROM scen.{technology_label}_06_mix_rate_final AS pcm
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
     , CAST(cal_date AS VARCHAR) || ' ' || geography || ' ' || platform_subset ||
       ' ' ||
       base_product_number || ' ' || customer_engagement AS composite_key
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
     , CAST(cal_date AS VARCHAR) || ' ' || geography || ' ' || platform_subset ||
       ' ' ||
       base_product_number || ' ' || customer_engagement AS composite_key
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
     , CAST(cal_date AS VARCHAR) || ' ' || geography || ' ' || platform_subset ||
       ' ' ||
       base_product_number || ' ' || customer_engagement AS composite_key
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
     , CAST(cal_date AS VARCHAR) || ' ' || geography || ' ' || platform_subset ||
       ' ' ||
       base_product_number || ' ' || customer_engagement AS composite_key
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
     , CAST(cal_date AS VARCHAR) || ' ' || geography || ' ' || platform_subset ||
       ' ' ||
       base_product_number || ' ' || customer_engagement AS composite_key
FROM pcrg_05_color_fcst
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Redshift

# COMMAND ----------

query_list = []

query_list.append([f"scen.{technology_label}_01_us_uploads", wf_01_us_uploads, "overwrite"])
query_list.append([f"scen.{technology_label}_02_us_dmd", wf_02_us_dmd, "overwrite"])
query_list.append([f"scen.{technology_label}_demand", wf_demand, "overwrite"])
query_list.append([f"scen.{technology_label}_03_usage_share", wf_03_usage_share, "overwrite"])
query_list.append([f"scen.{technology_label}_shm_base_helper", wf_shm_base_helper, "overwrite"])
query_list.append([f"scen.{technology_label}_cartridge_units", wf_cartridge_units, "overwrite"])
query_list.append([f"scen.{technology_label}_page_mix_engine", eval(f"{technology_label}_page_mix_engine"), "overwrite"])
query_list.append([f"scen.{technology_label}_page_cc_mix_override", wf_page_cc_mix_override, "overwrite"])
query_list.append([f"scen.{technology_label}_{supply_unit}_mix_complete", wf_page_cc_mix_complete, "overwrite"])
query_list.append([f"scen.{technology_label}_04_{supply_unit}_mix", wf_04_page_cc_mix, "overwrite"])
query_list.append([f"scen.{technology_label}_05_mix_uploads", wf_05_mix_uploads, "overwrite"])
query_list.append([f"scen.{technology_label}_06_mix_rate_final", wf_06_mix_rate_final, "overwrite"])
query_list.append([f"scen.{technology_label}_07_page_cc_cartridges", wf_07_page_cc_cartridges, "overwrite"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list

# COMMAND ----------

# MAGIC %md
# MAGIC ##Update tables in Redshift

# COMMAND ----------

# both updates utilize same ctes, so we define them here
update_page_cc_cartridges_table_ctes = """
WITH ac_ce AS (
    SELECT 
        ac.cal_date
        ,ac.market10
        ,ac.country_alpha2
        ,ac.platform_subset
        ,ac.base_product_number
        ,CASE 
            WHEN hw.technology = 'LASER' AND ac.platform_subset LIKE '%STND%' THEN 'STD'
            WHEN hw.technology = 'LASER' AND ac.platform_subset LIKE '%YET2%' THEN 'HP+'
            ELSE ac.customer_engagement 
        END AS customer_engagement
        ,ac.base_quantity
    FROM prod.actuals_supplies ac 
    LEFT JOIN mdm.hardware_xref hw
        ON hw.platform_subset = ac.platform_subset
    WHERE ac.official = 1
),

act_m10 AS (
    SELECT
        cal_date
        ,market10
        ,platform_subset
        ,base_product_number
        ,customer_engagement
        ,SUM(base_quantity) base_quantity
    FROM ac_ce
    GROUP BY cal_date, market10, platform_subset, base_product_number, customer_engagement
)
"""

# COMMAND ----------

update_page_cc_cartridges_table_query1 = f"""
{update_page_cc_cartridges_table_ctes}

UPDATE scen.{technology_label}_07_page_cc_cartridges
SET imp_corrected_cartridges = ac.base_quantity
FROM scen.{technology_label}_07_page_cc_cartridges ccs
INNER JOIN act_m10 ac
    ON ac.cal_date = ccs.cal_date
        AND ccs.geography = ac.market10
        AND ac.base_product_number = ccs.base_product_number 
        AND ac.platform_subset = ccs.platform_subset
        AND ccs.customer_engagement = ac.customer_engagement
"""

# COMMAND ----------

update_page_cc_cartridges_table_query2 = f"""
{update_page_cc_cartridges_table_ctes}

UPDATE scen.{technology_label}_07_page_cc_cartridges
SET imp_corrected_cartridges = 0, cartridges = 0
FROM (
    SELECT
        ccs.*
    FROM scen.{technology_label}_07_page_cc_cartridges ccs
    LEFT JOIN act_m10 ac
        ON ac.cal_date = ccs.cal_date
            AND ccs.geography = ac.market10
            AND ac.base_product_number = ccs.base_product_number 
            AND ac.platform_subset = ccs.platform_subset
            AND ccs.customer_engagement = ac.customer_engagement 
    WHERE ccs.cal_date < (
        SELECT 
            MAX(cal_date) 
        FROM prod.actuals_supplies 
        WHERE official = 1
    ) 
        AND ac.base_quantity IS NULL
) cc
WHERE scen.{technology_label}_07_page_cc_cartridges.cal_date = cc.cal_date
    AND scen.{technology_label}_07_page_cc_cartridges.geography = cc.geography
    AND scen.{technology_label}_07_page_cc_cartridges.platform_subset = cc.platform_subset 
    AND scen.{technology_label}_07_page_cc_cartridges.base_product_number = cc.base_product_number
    AND scen.{technology_label}_07_page_cc_cartridges.customer_engagement = cc.customer_engagement
"""

# COMMAND ----------

for update_query in [update_page_cc_cartridges_table_query1, update_page_cc_cartridges_table_query2]:
    submit_remote_query(configs, update_query)
