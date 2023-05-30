# Databricks notebook source
# MAGIC %md
# MAGIC # Ink Working Forecast

# COMMAND ----------

# create empty widgets for interactive sessions
dbutils.widgets.text('installed_base_version', '') # installed base version
dbutils.widgets.text('usage_share_version', '') # usage-share version
dbutils.widgets.text('run_system_cartridges', '') # run notebook boolean

# COMMAND ----------

# exit notebook if task boolean is False, else continue
notebook_run_parameter_label = 'run_system_cartridges' 
if dbutils.widgets.get(notebook_run_parameter_label).lower().strip() != 'true':
	dbutils.notebook.exit(f"EXIT: {notebook_run_parameter_label} parameter is not set to 'true'")

# COMMAND ----------

# Global Variables
# retrieve widget values and assign to variables
installed_base_version = dbutils.widgets.get('installed_base_version')
usage_share_version = dbutils.widgets.get('usage_share_version')

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
        AND UPPER(us_scen.user_name) IN ('ANAA','ANA.ANAYA@HP.COM', 'SAIMANK', 'SAIMAN.KUSIN@HP.COM', 'SONSEEAHRAY', 'SONSEEAHRAY.RUCKER@HP.COM', 'ZACP', 'ZACHARY.PEAKE@HP.COM'))

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
          , CAST(DATEADD(MONTH, us.month_num, us.min_sys_date) AS DATE) AS cal_date
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
          , CAST(DATEADD(MONTH, us.month_num, us.min_sys_date) AS DATE) AS cal_date
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

combined_usage_share = """

SELECT us.cal_date
     , us.geography_grain
     , us.geography
     , us.platform_subset
     , us.customer_engagement
     , us.measure
     , us.units
FROM prod.usage_share us
WHERE version = '{usage_share_version}'
AND NOT EXISTS (SELECT 1 FROM scen.ink_01_us_uploads usup WHERE usup.cal_date = us.cal_date AND usup.geography = us.geography AND usup.platform_subset = us.platform_subset 
                                                        AND usup.customer_engagement = us.customer_engagement  AND usup.measure = us.measure)

UNION

SELECT us.cal_date
     , us.geography_grain
     , us.geography
     , us.platform_subset
     , us.customer_engagement
     , us.measure
     , us.value
FROM scen.ink_01_us_uploads us

"""

query_list.append(["scen.ink_full_usage_share", combined_usage_share, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 2 :: tranform u/s uploads to demand measures

# COMMAND ----------

ink_02_us_dmd = f"""
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
       AND ib.version = '{installed_base_version}'
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
     FROM scen.ink_full_usage_share AS us
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
       AND UPPER(smr.user_name) IN ('ANAA','ANA.ANAYA@HP.COM', 'SAIMANK', 'SAIMAN.KUSIN@HP.COM', 'SONSEEAHRAY', 'SONSEEAHRAY.RUCKER@HP.COM', 'ZACP', 'ZACHARY.PEAKE@HP.COM'))

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
# MAGIC ## sub-routine 6:  transformed cc mix

# COMMAND ----------

ink_06_mix_rate_final = """
          SELECT cast(DATEADD(MONTH, smr.month_num, smr.min_sys_date) AS DATE)   AS cal_date
               , 'MARKET10'                                         AS geography_grain
               , smr.geography
               , smr.platform_subset
               , smr.base_product_number
               , smr.customer_engagement
               , smr.value                                          AS mix_rate
          FROM scen.ink_05_mix_uploads AS smr
          JOIN mdm.supplies_xref AS s
              ON s.base_product_number = smr.base_product_number
          WHERE 1 = 1
"""

query_list.append(["scen.ink_06_mix_rate_final", ink_06_mix_rate_final, "overwrite"])

# COMMAND ----------

combined_cc_mix = """
SELECT pcm.cal_date
     , pcm.geography_grain
     , pcm.geography
     , pcm.platform_subset
     , pcm.base_product_number
     , pcm.customer_engagement
     , pcm.mix_rate
FROM stage.page_cc_mix pcm
WHERE NOT EXISTS (SELECT 1 FROM scen.ink_06_mix_rate_final mix WHERE mix.cal_date = pcm.cal_date AND mix.geography = pcm.geography AND mix.platform_subset = pcm.platform_subset 
                                                        AND mix.customer_engagement = pcm.customer_engagement  AND mix.base_product_number = pcm.base_product_number)

UNION

SELECT pcm.cal_date
     , pcm.geography_grain
     , pcm.geography
     , pcm.platform_subset
     , pcm.base_product_number
     , pcm.customer_engagement
     , pcm.mix_rate
FROM scen.ink_06_mix_rate_final pcm

UNION

   SELECT  ac.cal_date
          , ac.geography_grain
          , ac.geography
          , ac.platform_subset
          , ac.base_product_number
          , ac.customer_engagement
          , SUM(cartridge_volume)
                   OVER (PARTITION BY ac.cal_date, ac.geography, ac.platform_subset, ac.base_product_number, ac.crg_chrome, ac.customer_engagement)/
            SUM(cartridge_volume)
                   OVER (PARTITION BY ac.cal_date, ac.geography, ac.platform_subset, ac.crg_chrome, ac.customer_engagement) AS mix_rate
     FROM stage.cartridge_units AS ac
     LEFT JOIN mdm.hardware_xref hx ON hx.platform_subset = ac.platform_subset
     WHERE 1 = 1 
     AND UPPER(hx.technology) IN ('INK','PWA')
     AND UPPER(ac.k_color) = 'BLACK' -- AND UPPER(p.crg_chrome) = 'BLK'  Do we need to include DRUM
     AND NOT EXISTS (SELECT 1 FROM scen.ink_06_mix_rate_final mix WHERE mix.cal_date = ac.cal_date AND mix.geography = ac.geography AND mix.platform_subset = ac.platform_subset 
                                                        AND mix.customer_engagement = ac.customer_engagement  AND mix.base_product_number = ac.base_product_number)

UNION

    SELECT  ac.cal_date
    	  , ac.geography_grain
          , ac.geography
          , ac.platform_subset
          , ac.base_product_number
          , ac.customer_engagement
          , SUM(cartridge_volume)
                   OVER (PARTITION BY  ac.geography, ac.platform_subset, ac.base_product_number, ac.customer_engagement)/
            SUM(cartridge_volume)
                   OVER (PARTITION BY  ac.geography, ac.platform_subset, ac.crg_chrome, ac.customer_engagement) AS mix_rate
     FROM stage.cartridge_units AS ac
     LEFT JOIN mdm.hardware_xref hx ON hx.platform_subset = ac.platform_subset
     WHERE 1 = 1 
     AND UPPER(hx.technology) IN ('INK','PWA')
     AND UPPER(ac.k_color) = 'COLOR' 
     AND UPPER(ac.crg_chrome) IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM scen.ink_06_mix_rate_final mix WHERE mix.cal_date = ac.cal_date AND mix.geography = ac.geography AND mix.platform_subset = ac.platform_subset 
                                                        AND mix.customer_engagement = ac.customer_engagement  AND mix.base_product_number = ac.base_product_number)
"""

query_list.append(["scen.ink_full_cc_mix", combined_cc_mix, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sub-routine 7: run page_cc_cartridges process with new input dataset

# COMMAND ----------

ink_07_page_cc_cartridges = """
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
     FROM scen.ink_02_us_dmd AS d
     LEFT JOIN mdm.hardware_xref AS hw
        ON UPPER(hw.platform_subset) = UPPER(d.platform_subset)
     WHERE 1 = 1
        AND UPPER(hw.technology) IN ('INK','PWA') 
     GROUP BY d.cal_date
            , d.geography
            , d.platform_subset
            , d.customer_engagement)


 ,pcrg_04_k_crgs AS
    (SELECT pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.black_demand
          , pf.yield
          , pcm.mix_rate * dmd.black_demand         AS page_demand
          , pcm.mix_rate * dmd.black_demand /
            NULLIF(pf.yield, 0)                                           AS cartridges
          , pcm.mix_rate * dmd.black_demand /
            NULLIF(pf.yield, 0)        AS  adjusted_cartridges
     FROM scen.ink_full_cc_mix AS pcm
              LEFT JOIN mdm.supplies_xref AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              LEFT JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
              LEFT JOIN pcm_02_hp_demand AS dmd
                   ON dmd.cal_date = pcm.cal_date
                       AND UPPER(dmd.geography) = UPPER(pcm.geography)
                       AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
                       AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
              LEFT JOIN pen_fills AS pf
                   ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
                       AND pf.cal_date = pcm.cal_date
                       AND UPPER(pf.market_10) = UPPER(pcm.geography)
     WHERE 1 = 1
       AND UPPER(s.k_color) = 'BLACK'
       AND UPPER(hw.technology) IN ('INK','PWA'))

, pcrg_05_color_crgs AS
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
            NULLIF(pf.yield, 0)                                          AS cartridges
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)                                          AS  adjusted_cartridges
     FROM scen.ink_06_mix_rate_final AS pcm
              LEFT JOIN mdm.supplies_xref AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              LEFT JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(pcm.platform_subset)
              LEFT JOIN pcm_02_hp_demand AS dmd
                   ON dmd.cal_date = pcm.cal_date
                       AND UPPER(dmd.geography) = UPPER(pcm.geography)
                       AND UPPER(dmd.platform_subset) = UPPER(pcm.platform_subset)
                       AND UPPER(dmd.customer_engagement) = UPPER(pcm.customer_engagement)
              LEFT JOIN pen_fills AS pf
                   ON UPPER(pf.base_product_number) = UPPER(pcm.base_product_number)
                       AND pf.cal_date = pcm.cal_date
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
FROM pcrg_04_k_crgs

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
FROM pcrg_05_color_crgs
"""


query_list.append(["scen.ink_07_page_cc_cartridges", ink_07_page_cc_cartridges, "overwrite"])

# COMMAND ----------

final_page_cc_cartridges = """
SELECT ipc.cal_date
    , ipc.geography_grain
    , ipc.geography
    , ipc.platform_subset
    , ipc.base_product_number
    , ipc.customer_engagement
    , ipc.page_cc_mix
    , ipc.demand
    , ipc.yield
    , ipc.page_demand
    , ipc.cartridges
    , pc.adjusted_cartridges
FROM scen.ink_07_page_cc_cartridges ipc 
LEFT JOIN stage.page_cc_cartridges pc ON pc.cal_date = ipc.cal_date AND pc.geography = ipc.geography AND pc.platform_subset = ipc.platform_subset 
                                        AND pc.base_product_number = ipc.base_product_number AND pc.customer_engagement = ipc.customer_engagement
WHERE ipc.cal_date <= (SELECT MAX(cal_date) FROM prod.actuals_supplies)

UNION 

SELECT ipc.cal_date
    , ipc.geography_grain
    , ipc.geography
    , ipc.platform_subset
    , ipc.base_product_number
    , ipc.customer_engagement
    , ipc.page_cc_mix
    , ipc.demand
    , ipc.yield
    , ipc.page_demand
    , ipc.cartridges
    , ipc.adjusted_cartridges
FROM scen.ink_07_page_cc_cartridges ipc 
WHERE ipc.cal_date > (SELECT MAX(cal_date) FROM prod.actuals_supplies)

UNION

SELECT pc.cal_date
    , pc.geography_grain
    , pc.geography
    , pc.platform_subset
    , pc.base_product_number
    , pc.customer_engagement
    , pc.page_cc_mix
    , pc.demand
    , pc.yield
    , pc.page_demand
    , pc.cartridges
    , pc.adjusted_cartridges
FROM stage.page_cc_cartridges pc
WHERE NOT EXISTS (SELECT 1 FROM scen.ink_07_page_cc_cartridges ipc WHERE pc.cal_date = ipc.cal_date AND pc.geography = ipc.geography AND pc.platform_subset = ipc.platform_subset 
                AND pc.base_product_number = ipc.base_product_number AND pc.customer_engagement = ipc.customer_engagement)

"""


query_list.append(["scen.ink_07_page_cc_cartridges_final", final_page_cc_cartridges, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Redshift

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
