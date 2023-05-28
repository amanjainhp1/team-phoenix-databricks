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
# MAGIC + demand
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
                   , shm.customer_engagement 
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
                   , shm.customer_engagement 
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
                   , shm.customer_engagement 
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
with actuals as (
    SELECT 'ACTUALS'                AS source
     , acts.cal_date
     , acts.base_product_number
     , acts.platform_subset
     , cref.country_scenario    AS geography_grain
     , cref.country_level_2     AS geography
     , CASE
            when  acts.customer_engagement = 'EST_INDIRECT_FULFILLMENT' THEN 'TRAD' 
            WHEN hw.technology = 'LASER' AND acts.platform_subset LIKE '%STND%' THEN 'TRAD' 
            WHEN hw.technology = 'LASER' AND acts.platform_subset LIKE '%YET2%' THEN 'HP+'
            ELSE acts.customer_engagement END AS customer_engagement
     , xref.k_color
     , xref.crg_chrome
     , CASE
           WHEN xref.crg_chrome IN ('DRUM') THEN 'DRUM'
           ELSE 'CARTRIDGE' END AS consumable_type
     , SUM(acts.base_quantity)  AS cartridge_volume
FROM prod.actuals_supplies AS acts
        LEFT JOIN mdm.supplies_xref AS xref
              ON xref.base_product_number = acts.base_product_number
        LEFT JOIN mdm.iso_cc_rollup_xref AS cref
              ON cref.country_alpha2 = acts.country_alpha2
                  AND cref.country_scenario = 'MARKET10'
        LEFT JOIN mdm.hardware_xref hw on hw.platform_subset = acts.platform_subset 
                  
WHERE 1 = 1
  AND xref.official = 1 
  AND acts.customer_engagement IN ('EST_INDIRECT_FULFILLMENT', 'I-INK', 'TRAD')
  AND NOT xref.crg_chrome IN ('HEAD', 'UNK') 
GROUP BY acts.cal_date
       , acts.base_product_number
       , acts.platform_subset
       , acts.customer_engagement
       , hw.technology = 'LASER'
       , cref.country_scenario 
       , cref.country_level_2
       , xref.k_color
       , xref.crg_chrome
       , CASE
             WHEN xref.crg_chrome IN ('DRUM') THEN 'DRUM'
             ELSE 'CARTRIDGE' END
    )

    SELECT * FROM actuals WHERE cartridge_volume > 0
"""

query_list.append(["stage.cartridge_units", cartridge_units, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## demand

# COMMAND ----------

demand = """
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
       AND ib.version = ('2023.05.11.1')
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
     FROM prod.usage_share AS us
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = us.platform_subset
     WHERE 1 = 1
       AND us.version = ('2023.05.18.1')
       AND UPPER(us.measure) IN
           ('USAGE', 'COLOR_USAGE', 'K_USAGE', 'HP_SHARE')
       AND UPPER(us.geography_grain) = 'MARKET10'
       AND NOT UPPER(hw.product_lifecycle_status) = 'E'
       AND UPPER(hw.technology) IN ('LASER','INK', 'PWA')
       AND us.cal_date > CAST('2015-10-01' AS DATE)
    )
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
                                units FOR measure IN (IB, HP_K_PAGES, NON_HP_K_PAGES, HP_COLOR_PAGES, NON_HP_COLOR_PAGES, TOTAL_COLOR_PAGES, TOTAL_K_PAGES)
         ) 
    AS final_unpivot)
         
SELECT dmd.cal_date
      , dmd.geography
      , dmd.platform_subset
      , dmd.customer_engagement
      , dmd.measure
      , dmd.units
 FROM dmd_09_unpivot_measure AS dmd
"""

query_list.append(["stage.demand", demand, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_mix_engine - toner

# COMMAND ----------

page_mix_engine = """
WITH supplies_forecast_months as 
(
	SELECT DATEADD(MONTH, 1, MAX(sup.cal_date)) AS supplies_forecast_start
	FROM stage.cartridge_units AS sup
	WHERE 1 = 1
),

     avergae_actuals as 
(   
	SELECT    'MARKET10' geography_grain
               , cu.geography
               , cu.platform_subset
               , cu.base_product_number
               , cu.customer_engagement
                  , CASE
                         WHEN UPPER(s.single_multi) = 'TRI-PACK' THEN 'MULTI'
                         ELSE 'SINGLE' END         AS single_multi
                   , s.k_color
                   , s.crg_chrome
                   , sum(cu.cartridge_volume) cartridge_volume
     FROM stage.cartridge_units AS cu
     LEFT JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = cu.platform_subset
     LEFT JOIN mdm.supplies_xref s on s.base_product_number = cu.base_product_number
     CROSS JOIN supplies_forecast_months AS fm
     WHERE 1 = 1 
       AND hw.technology = 'LASER' AND cu.cal_date BETWEEN DATEADD(MONTH, -9, fm.supplies_forecast_start) AND DATEADD(MONTH, -1, fm.supplies_forecast_start)
     group by   cu.geography
               , cu.platform_subset
               , cu.base_product_number
               , cu.customer_engagement
               , s.single_multi
               , s.k_color
               , s.crg_chrome
),
    
    page_mix as 
(
    SELECT  ac.geography_grain
          , ac.geography
          , ac.platform_subset
          , ac.base_product_number
          , ac.customer_engagement
          , ac.single_multi
          , ac.k_color
          , ac.crg_chrome
          , ac.cartridge_volume
          , SUM(cartridge_volume)
                   OVER (PARTITION BY  ac.geography, ac.platform_subset, ac.base_product_number, ac.crg_chrome, ac.customer_engagement)/
            SUM(cartridge_volume)
                   OVER (PARTITION BY  ac.geography, ac.platform_subset, ac.crg_chrome, ac.customer_engagement) AS page_mix
     FROM avergae_actuals AS ac
     WHERE 1 = 1 
)
     
    SELECT c."date" cal_date
    	  ,m.base_product_number
    	  ,m.platform_subset
    	  ,m.customer_engagement
    	  ,m.geography_grain
    	  ,m.geography
    	  ,m.k_color
    	  ,m.crg_chrome
    	  ,m.single_multi
    	  ,m.page_mix
     FROM page_mix m 
     cross join mdm.calendar c 
     CROSS JOIN supplies_forecast_months AS fm
     WHERE c."date" BETWEEN fm.supplies_forecast_start AND DATEADD(MONTH, 120, fm.supplies_forecast_start) AND c.day_of_month  = 1
"""

query_list.append(["stage.page_mix_engine", page_mix_engine, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## cc_mix_engine - ink/pwa

# COMMAND ----------

cc_mix_engine = """
WITH supplies_forecast_months as 
(
	SELECT DATEADD(MONTH, 1, MAX(sup.cal_date)) AS supplies_forecast_start
	FROM stage.cartridge_units AS sup
	WHERE 1 = 1
),

     avergae_actuals as 
(   
	SELECT    'MARKET10' geography_grain
               , cu.geography
               , cu.platform_subset
               , cu.base_product_number
               , cu.customer_engagement
                  , CASE
                         WHEN UPPER(s.single_multi) = 'TRI-PACK' THEN 'MULTI'
                         ELSE 'SINGLE' END         AS single_multi
                   , s.k_color
                   , s.crg_chrome
                   , sum(cu.cartridge_volume) cartridge_volume
     FROM stage.cartridge_units AS cu
     LEFT JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = cu.platform_subset
     LEFT JOIN mdm.supplies_xref s on s.base_product_number = cu.base_product_number
     CROSS JOIN supplies_forecast_months AS fm
     WHERE 1 = 1 
       AND hw.technology IN ('INK','PWA') AND cu.cal_date BETWEEN DATEADD(MONTH, -9, fm.supplies_forecast_start) AND DATEADD(MONTH, -1, fm.supplies_forecast_start)
     group by   cu.geography
               , cu.platform_subset
               , cu.base_product_number
               , cu.customer_engagement
               , s.single_multi
               , s.k_color
               , s.crg_chrome
),
    
    page_mix as 
(
    SELECT  ac.geography_grain
          , ac.geography
          , ac.platform_subset
          , ac.base_product_number
          , ac.customer_engagement
          , ac.single_multi
          , ac.k_color
          , ac.crg_chrome
          , ac.cartridge_volume
          , SUM(cartridge_volume)
                   OVER (PARTITION BY  ac.geography, ac.platform_subset, ac.base_product_number, ac.crg_chrome, ac.customer_engagement)/
            SUM(cartridge_volume)
                   OVER (PARTITION BY  ac.geography, ac.platform_subset, ac.crg_chrome, ac.customer_engagement) AS page_mix
     FROM avergae_actuals AS ac
     WHERE 1 = 1 
)
     
    SELECT c."date" cal_date
    	  ,m.base_product_number
    	  ,m.platform_subset
    	  ,m.customer_engagement
    	  ,m.geography_grain
    	  ,m.geography
    	  ,m.k_color
    	  ,m.crg_chrome
    	  ,m.single_multi
    	  ,m.page_mix
     FROM page_mix m 
     cross join mdm.calendar c 
     CROSS JOIN supplies_forecast_months AS fm
     WHERE c."date" BETWEEN fm.supplies_forecast_start AND DATEADD(MONTH, 120, fm.supplies_forecast_start) AND c.day_of_month  = 1
"""

query_list.append(["stage.cc_mix_engine", cc_mix_engine, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_cc_mix_override

# COMMAND ----------

page_cc_mix_override = """
WITH geography_mapping AS
    (SELECT DISTINCT market10 AS market_10
                   , region_5
     FROM mdm.iso_country_code_xref
     WHERE 1 = 1
       AND NOT market10 IS NULL
       AND NOT region_5 IS NULL
       AND NOT region_5 = 'JP'
       AND NOT region_5 LIKE 'X%')

   , prod_crg_mix_region5 AS
    (SELECT cmo.cal_date
          , map.market_10               AS market10
          , cmo.platform_subset
          , cmo.Crg_Base_Prod_Number
          , cmo.customer_engagement
          , cmo.mix_pct
          , CASE
                WHEN hw.technology = 'LASER'  THEN 'PAGE_MIX'
                WHEN hw.technology <> 'LASER' THEN 'CC_MIX'
                END      AS upload_type  -- legacy
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

   , prod_crg_mix_market10 AS
    (SELECT cmo.cal_date
          , cmo.geography               AS market10
          , cmo.platform_subset
          , cmo.Crg_Base_Prod_Number
          , cmo.customer_engagement
          , cmo.mix_pct
          , CASE
                WHEN hw.technology = 'LASER'  THEN 'PAGE_MIX'
                WHEN hw.technology <> 'LASER' THEN 'CC_MIX'
                END      AS upload_type
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
    )
       

    SELECT cmo.cal_date
          , cmo.market10
          , cmo.platform_subset
          , cmo.Crg_Base_Prod_Number base_product_number
          , cmo.customer_engagement
          , cmo.mix_pct
          , cmo.upload_type
          , cmo.k_color
          , cmo.Crg_Chrome
          , cmo.consumable_type
          , cmo.load_date
     FROM prod_crg_mix_region5 

     UNION ALL

     SELECT cmo.cal_date
          , cmo.market10
          , cmo.platform_subset
          , cmo.Crg_Base_Prod_Number base_product_number
          , cmo.customer_engagement
          , cmo.mix_pct
          , cmo.upload_type
          , cmo.k_color
          , cmo.Crg_Chrome
          , cmo.consumable_type
          , cmo.load_date
     FROM prod_crg_mix_market10 
"""

query_list.append(["stage.page_cc_mix_override", page_cc_mix_override, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## page_mix_complete

# COMMAND ----------

page_mix_complete = """
    SELECT 'PCM_ENGINE_PROJECTION' type
          ,cal_date
          ,geography_grain
          ,geography
          ,platform_subset
          ,base_product_number
          ,customer_engagement
          ,page_mix
     FROM stage.page_mix_engine AS pcm
     WHERE NOT EXISTS (
         SELECT 1 FROM stage.page_cc_mix_override pmo 
         WHERE pmo.cal_date = pcm.cal_date AND pcm.geography = pmo.market10 AND pcm.platform_subset = pmo.platform_subset AND pcm.base_product_number = pmo.base_product_number AND pcm.customer_engagement = pmo.customer_engagement
         )

     UNION ALL

     SELECT 'PCM_FORECASTER_OVERRIDE' AS type
          , pmo.cal_date
          , 'MARKET10'                AS geography_grain
          , pmo.market10              AS geography
          , pmo.platform_subset
          , pmo.base_product_number
          , pmo.customer_engagement
          , pmo.mix_pct               AS page_mix
     FROM stage.page_cc_mix_override AS pmo
     JOIN mdm.hardware_xref AS hw
         ON hw.platform_subset = m26.platform_subset
     WHERE 1 = 1
       AND hw.technology = 'LASER'
"""

query_list.append(["stage.page_mix_complete", page_mix_complete, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## cc_mix_complete

# COMMAND ----------

cc_mix_complete = """
    SELECT 'PCM_ENGINE_PROJECTION' type
          ,cal_date
          ,geography_grain
          ,geography
          ,platform_subset
          ,base_product_number
          ,customer_engagement
          ,page_mix
     FROM stage.cc_mix_engine AS pcm
     WHERE NOT EXISTS (
         SELECT 1 FROM stage.page_cc_mix_override pmo 
         WHERE pmo.cal_date = pcm.cal_date AND pcm.geography = pmo.market10 AND pcm.platform_subset = pmo.platform_subset AND pcm.base_product_number = pmo.base_product_number AND pcm.customer_engagement = pmo.customer_engagement
         )

     UNION ALL

     SELECT 'PCM_FORECASTER_OVERRIDE' AS type
          , pmo.cal_date
          , 'MARKET10'                AS geography_grain
          , pmo.market10              AS geography
          , pmo.platform_subset
          , pmo.base_product_number
          , pmo.customer_engagement
          , pmo.mix_pct               AS page_mix
     FROM stage.page_cc_mix_override AS pmo
     JOIN mdm.hardware_xref AS hw
         ON hw.platform_subset = m26.platform_subset
     WHERE 1 = 1
       AND hw.technology IN ('INK','PWA)
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
FROM stage.page_mix_complete
"""

query_list.append(["stage.page_cc_mix", page_cc_mix, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Redshift

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply EOL Update

# COMMAND ----------

submit_remote_query(configs, """with reg5_m10 as
(
select distinct region_5,market10
from mdm.iso_country_code_xref
where region_5 is not null and region_5 not in ('XU','XW')
),

reg_8_m10 as 
(
select distinct cc.country_alpha2,cc.country_level_1,c.market10
from mdm.iso_cc_rollup_xref cc
left join mdm.iso_country_code_xref c on c.country_alpha2 = cc.country_alpha2
where cc.country_scenario = 'HOST_REGION_8'
),

swm_m10 as 
(
select swm.geography
      ,swm.base_product_number
      ,swm.customer_engagement
      ,swm.platform_subset
      ,swm.eol
      ,swm.eol_date
  from mdm.supplies_hw_mapping swm
  where geography_grain = 'MARKET10' and swm.official = 1 and swm.eol_date is not null and swm.eol_date != '2100-01-01'

  union 

  select distinct r5.market10
      ,swm.base_product_number
      ,swm.customer_engagement
      ,swm.platform_subset
      ,swm.eol
      ,swm.eol_date
  from mdm.supplies_hw_mapping swm
  left join reg5_m10 r5 on r5.region_5 = swm.geography
  where geography_grain = 'REGION_5' and geography != 'JP' and swm.official = 1 and swm.eol_date is not null and swm.eol_date != '2100-01-01'
  group by r5.market10
      ,swm.base_product_number
      ,swm.customer_engagement
      ,swm.platform_subset
      ,swm.eol
      ,swm.eol_date

	union

  select distinct r5.market10
      ,swm.base_product_number
      ,swm.customer_engagement
      ,swm.platform_subset
      ,swm.eol
      ,swm.eol_date
  from mdm.supplies_hw_mapping swm
  left join reg5_m10 r5 on r5.region_5 = swm.geography
  where geography_grain = 'REGION_5' and geography = 'JP'
  and not exists (select 1 from mdm.supplies_hw_mapping s where s.official = 1 and swm.eol_date is not null and swm.eol_date != '2100-01-01'
  and s.base_product_number = swm.base_product_number and s.customer_engagement = swm.customer_engagement and s.platform_subset = swm.platform_subset
  and s.geography IN ('AP - EM','AP - DM', 'AP'))
 group by r5.market10
      ,swm.base_product_number
      ,swm.customer_engagement
      ,swm.platform_subset
      ,swm.eol
      ,swm.eol_date

	union 
	
  select distinct r8.market10
      ,swm.base_product_number
      ,swm.customer_engagement
      ,swm.platform_subset
      ,swm.eol
      ,swm.eol_date
  from mdm.supplies_hw_mapping swm
  left join reg_8_m10 r8 on r8.country_level_1 = swm.geography
  where geography_grain = 'REGION_8' and swm.eol_date is not null and swm.eol_date != '2100-01-01'
 group by r8.market10
      ,swm.base_product_number
      ,swm.customer_engagement
      ,swm.platform_subset
      ,swm.eol
      ,swm.eol_date
)

update stage.page_cc_mix 
set mix_rate = null 
from stage.page_cc_mix pg
inner join swm_m10 swm on swm.geography = pg.geography and swm.base_product_number = pg.base_product_number and pg.platform_subset = swm.platform_subset 
						  and pg.customer_engagement = swm.customer_engagement 
where pg.cal_date >= swm.eol_date 
""")
