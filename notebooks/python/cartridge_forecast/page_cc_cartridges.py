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

page_cc_cartridges = """
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
        AND UPPER(hw.technology) IN ('INK', 'LASER', 'PWA' ,'LF')
     GROUP BY d.cal_date
            , d.geography
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
                   ON UPPER(shm.base_product_number) = UPPER(v.base_product_number)
                       AND UPPER(shm.geography) = UPPER(v.geography)
     WHERE 1 = 1
       AND v.cartridge_volume > 0
       
       UNION ALL
       
       SELECT DISTINCT v.cal_date
                   , v.geography_grain
                   , v.geography
                   , shm.platform_subset
                   , v.base_product_number
                   , v.k_color
                   , v.crg_chrome
                   , v.consumable_type
                   , v.cartridge_volume
     FROM stage.cartridge_units AS v
              JOIN stage.shm_base_helper_lf AS shm
                   ON UPPER(shm.base_product_number) = UPPER(v.base_product_number)
                       AND UPPER(shm.geography) = UPPER(v.geography)
     WHERE 1 = 1
       AND v.cartridge_volume > 0)

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
              JOIN stage.shm_base_helper AS shm
                   ON UPPER(shm.base_product_number) = UPPER(v.base_product_number)
                       AND UPPER(shm.platform_subset) = UPPER(v.platform_subset)
                       AND UPPER(shm.customer_engagement) = UPPER(dmd.customer_engagement)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('INK', 'LASER', 'PWA')
       
       UNION ALL
       
       SELECT DISTINCT v.cal_date
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
              JOIN stage.supplies_xref_lf AS s
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
              JOIN stage.shm_base_helper_lf AS shm
                   ON UPPER(shm.base_product_number) = UPPER(v.base_product_number)
                       AND UPPER(shm.platform_subset) = UPPER(v.platform_subset)
                       AND UPPER(shm.customer_engagement) = UPPER(dmd.customer_engagement)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('LF'))

   , pcrg_01_k_acts AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.black_demand
          , pf.yield
          , pcm.mix_rate * dmd.black_demand                       AS page_demand
          , pcm.mix_rate * dmd.black_demand /
            NULLIF(pf.yield, 0)                                      AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.black_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.black_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.black_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.black_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS imp
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.black_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.black_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                                              AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
              JOIN mdm.supplies_xref AS S
                   ON UPPER(S.base_product_number) = UPPER(pcm.base_product_number)
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
       AND UPPER(s.k_color) = 'BLACK'
       
       UNION ALL
       
       SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.black_demand
          , pf.yield
          , pcm.mix_rate * dmd.black_demand                       AS page_demand
          , pcm.mix_rate * dmd.black_demand /
            NULLIF(pf.yield, 0)                                      AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.black_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.black_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.black_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.black_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS imp
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.black_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.black_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                                              AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
              JOIN stage.supplies_xref_lf AS S
                   ON UPPER(S.base_product_number) = UPPER(pcm.base_product_number)
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

, pcrg_02_color_acts AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.color_demand
          , pf.yield
          , pcm.mix_rate * dmd.color_demand                       AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)                                      AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.color_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.color_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.color_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.color_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS imp
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.color_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.color_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                                              AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
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
           ELSE 'SINGLE' END = 'SINGLE'
       AND UPPER(s.k_color) = 'COLOR'
       
       
       UNION ALL

       SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.color_demand
          , pf.yield
          , pcm.mix_rate * dmd.color_demand                       AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)                                      AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.color_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.color_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.color_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.color_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS imp
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.color_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.color_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                                              AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
              JOIN stage.supplies_xref_lf AS s
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
           ELSE 'SINGLE' END = 'SINGLE'
       AND UPPER(s.k_color) = 'COLOR'
       )

, pcrg_03_color_multi_acts AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.cmy_demand
          , pf.yield
          , pcm.mix_rate * dmd.cmy_demand                       AS page_demand
          , pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0) AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.cmy_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.cmy_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                              AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.cmy_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.cmy_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                              AS imp
          , pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.cmy_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.cmy_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                                            AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
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
           ELSE 'SINGLE' END <> 'SINGLE'
       AND UPPER(s.k_color) = 'COLOR')

, pcrg_04_k_fcst AS
    (SELECT pcm.type
          , pcm.cal_date
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
          , 0.0                                                            AS cartridge_volume
          , 1.0                                                            AS demand_scalar
          , 1.0                                                            AS imp
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0) *
            1.0                                                            AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
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
       AND UPPER(pcm.type) <> 'PCM_ENGINE_ACTS'
       
       UNION ALL
       
       SELECT pcm.type
          , pcm.cal_date
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
          , 0.0                                                            AS cartridge_volume
          , 1.0                                                            AS demand_scalar
          , 1.0                                                            AS imp
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0) *
            1.0                                                            AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
              JOIN stage.supplies_xref_lf AS s
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

, pcrg_05_color_fcst AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.color_demand                                               AS demand
          , pf.yield
          , pcm.mix_rate * dmd.color_demand                             AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)                                            AS cartridges
          , 0.0                                                            AS cartridge_volume
          , 1.0                                                            AS demand_scalar
          , 1.0                                                            AS imp
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0) *
            1.0                                                            AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
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
           ELSE 'SINGLE' END = 'SINGLE'
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
          , pcm.mix_rate AS page_cc_mix
          , dmd.color_demand                                               AS demand
          , pf.yield
          , pcm.mix_rate * dmd.color_demand                             AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)                                            AS cartridges
          , 0.0                                                            AS cartridge_volume
          , 1.0                                                            AS demand_scalar
          , 1.0                                                            AS imp
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0) *
            1.0                                                            AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
              JOIN stage.supplies_xref_lf AS s
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
           ELSE 'SINGLE' END = 'SINGLE'
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
          , pcm.mix_rate AS page_cc_mix
          , dmd.cmy_demand                                               AS demand
          , pf.yield
          , pcm.mix_rate * dmd.cmy_demand                             AS page_demand
          , pcm.mix_rate * dmd.cmy_demand /
            NULLIF(pf.yield, 0)                                          AS cartridges
          , 0.0                                                          AS cartridge_volume
          , 1.0                                                          AS demand_scalar
          , 1.0                                                          AS imp
          , pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0) *
            1.0                                                          AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
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
           ELSE 'SINGLE' END <> 'SINGLE'
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
    , black_demand AS demand
    , yield
    , page_demand
    , cartridges
    , cartridge_volume
    , demand_scalar
    , imp
    , imp_corrected_cartridges
	, CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset + ' ' +
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
    , color_demand AS demand
    , yield
    , page_demand
    , cartridges
    , cartridge_volume
    , demand_scalar
    , imp
    , imp_corrected_cartridges
	, CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset + ' ' +
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
    , cmy_demand AS demand
    , yield
    , page_demand
    , cartridges
    , cartridge_volume
    , demand_scalar
    , imp
    , imp_corrected_cartridges
	, CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset + ' ' +
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
    , black_demand AS demand
    , yield
    , page_demand
    , cartridges
    , cartridge_volume
    , demand_scalar
    , imp
    , imp_corrected_cartridges
	, CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset + ' ' +
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
	, CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset + ' ' +
        base_product_number + ' ' + customer_engagement AS composite_key
FROM pcrg_05_color_fcst
"""

query_list.append(["stage.page_cc_cartridges", page_cc_cartridges, "overwrite"])

# COMMAND ----------

page_cc_cartridges = """
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
     FROM stage.yield_lf AS y
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
        AND UPPER(hw.technology) IN ('INK', 'LASER', 'PWA' ,'LF')
     GROUP BY d.cal_date
            , d.geography
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
                   ON UPPER(shm.base_product_number) = UPPER(v.base_product_number)
                       AND UPPER(shm.geography) = UPPER(v.geography)
     WHERE 1 = 1
       AND v.cartridge_volume > 0
       
       union
       
       SELECT DISTINCT v.cal_date
                   , v.geography_grain
                   , v.geography
                   , shm.platform_subset
                   , v.base_product_number
                   , v.k_color
                   , v.crg_chrome
                   , v.consumable_type
                   , v.cartridge_volume
     FROM stage.cartridge_units AS v
              JOIN stage.shm_base_helper_lf AS shm
                   ON UPPER(shm.base_product_number) = UPPER(v.base_product_number)
                       AND UPPER(shm.geography) = UPPER(v.geography)
     WHERE 1 = 1
       AND v.cartridge_volume > 0)

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
              JOIN stage.shm_base_helper AS shm
                   ON UPPER(shm.base_product_number) = UPPER(v.base_product_number)
                       AND UPPER(shm.platform_subset) = UPPER(v.platform_subset)
                       AND UPPER(shm.customer_engagement) = UPPER(dmd.customer_engagement)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('INK', 'LASER', 'PWA')
       
       UNION ALL
       
       SELECT DISTINCT v.cal_date
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
              JOIN stage.supplies_xref_lf AS s
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
              JOIN stage.shm_base_helper_lf AS shm
                   ON UPPER(shm.base_product_number) = UPPER(v.base_product_number)
                       AND UPPER(shm.platform_subset) = UPPER(v.platform_subset)
                       AND UPPER(shm.customer_engagement) = UPPER(dmd.customer_engagement)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('LF'))

   , pcrg_01_k_acts AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.black_demand
          , pf.yield
          , pcm.mix_rate * dmd.black_demand                       AS page_demand
          , pcm.mix_rate * dmd.black_demand /
            NULLIF(pf.yield, 0)                                      AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.black_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.black_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.black_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.black_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS imp
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.black_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.black_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                                              AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
              JOIN mdm.supplies_xref AS S
                   ON UPPER(S.base_product_number) = UPPER(pcm.base_product_number)
              join mdm.hardware_xref hx 
              	   on UPPER(hx.platform_subset) = UPPER(pcm.platform_subset)
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
       AND UPPER(s.k_color) = 'BLACK' 
       and UPPER(hx.technology) in ('INK','PWA','LASER')
       
       union all 
       
       SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.black_demand
          , pf.yield
          , pcm.mix_rate * dmd.black_demand                       AS page_demand
          , pcm.mix_rate * dmd.black_demand /
            NULLIF(pf.yield, 0)                                      AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.black_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.black_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.black_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.black_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS imp
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.black_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.black_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                                              AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
              JOIN stage.supplies_xref_lf AS S
                   ON UPPER(S.base_product_number) = UPPER(pcm.base_product_number)
              join mdm.hardware_xref hx 
              	   on UPPER(hx.platform_subset) = UPPER(pcm.platform_subset)
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
       AND UPPER(s.k_color) = 'BLACK' 
       and UPPER(hx.technology) in ('LF')
     )

, pcrg_02_color_acts AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.color_demand
          , pf.yield
          , pcm.mix_rate * dmd.color_demand                       AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)                                      AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.color_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.color_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.color_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.color_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS imp
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.color_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.color_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                                              AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
              JOIN mdm.supplies_xref AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              join mdm.hardware_xref hx 
              	   on UPPER(hx.platform_subset) = UPPER(pcm.platform_subset)
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
           ELSE 'SINGLE' END = 'SINGLE'
       AND UPPER(s.k_color) = 'COLOR'
       AND hx.technology in ('INK','PWA','LASER')
       
       union all 
       
       SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.color_demand
          , pf.yield
          , pcm.mix_rate * dmd.color_demand                       AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)                                      AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.color_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.color_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.color_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.color_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                                AS imp
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.color_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.color_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                                              AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
              JOIN stage.supplies_xref_lf AS s
                   ON UPPER(s.base_product_number) = UPPER(pcm.base_product_number)
              join mdm.hardware_xref hx 
              	   on UPPER(hx.platform_subset) = UPPER(pcm.platform_subset)
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
           ELSE 'SINGLE' END = 'SINGLE'
       AND UPPER(s.k_color) = 'COLOR'
       and UPPER(hx.technology) = 'LF'
       )

, pcrg_03_color_multi_acts AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.cmy_demand
          , pf.yield
          , pcm.mix_rate * dmd.cmy_demand                       AS page_demand
          , pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0) AS cartridges
          , pc.cartridge_volume
          , SUM(dmd.cmy_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.cmy_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                              AS demand_scalar
          , (pc.cartridge_volume /
             NULLIF((pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0)),
                    0)) *
            SUM(dmd.cmy_demand)
            OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
            NULLIF(SUM(dmd.cmy_demand)
                   OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                   0)                                              AS imp
          , pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0) *
            ((pc.cartridge_volume /
              NULLIF((pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0)),
                     0)) *
             SUM(dmd.cmy_demand)
             OVER (PARTITION BY dmd.cal_date, dmd.geography, dmd.platform_subset, pcm.base_product_number, pcm.customer_engagement) /
             NULLIF(SUM(dmd.cmy_demand)
                    OVER (PARTITION BY dmd.cal_date, dmd.geography, pcm.base_product_number),
                    0))                                            AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
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
           ELSE 'SINGLE' END <> 'SINGLE'
       AND UPPER(s.k_color) = 'COLOR')

, pcrg_04_k_fcst AS
    (SELECT pcm.type
          , pcm.cal_date
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
          , 0.0                                                            AS cartridge_volume
          , 1.0                                                            AS demand_scalar
          , 1.0                                                            AS imp
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0) *
            1.0                                                            AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
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
       AND UPPER(pcm.type) <> 'PCM_ENGINE_ACTS'
       and hw.technology in ('INK','PWA','LASER')
       
       
       union all 
       
       SELECT pcm.type
          , pcm.cal_date
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
          , 0.0                                                            AS cartridge_volume
          , 1.0                                                            AS demand_scalar
          , 1.0                                                            AS imp
          , pcm.mix_rate * dmd.black_demand / NULLIF(pf.yield, 0) *
            1.0                                                            AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
              JOIN stage.supplies_xref_lf AS s
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
       AND UPPER(pcm.type) <> 'PCM_ENGINE_ACTS'
       and hw.technology in ('LF')
       )

, pcrg_05_color_fcst AS
    (SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.color_demand                                               AS demand
          , pf.yield
          , pcm.mix_rate * dmd.color_demand                             AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)                                            AS cartridges
          , 0.0                                                            AS cartridge_volume
          , 1.0                                                            AS demand_scalar
          , 1.0                                                            AS imp
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0) *
            1.0                                                            AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
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
           ELSE 'SINGLE' END = 'SINGLE'
       AND UPPER(pcm.type) <> 'PCM_ENGINE_ACTS'
       AND UPPER(s.k_color) = 'COLOR'
       and hw.technology in ('INK','PWA','LASER')
       
       union all 
       
       SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.color_demand                                               AS demand
          , pf.yield
          , pcm.mix_rate * dmd.color_demand                             AS page_demand
          , pcm.mix_rate * dmd.color_demand /
            NULLIF(pf.yield, 0)                                            AS cartridges
          , 0.0                                                            AS cartridge_volume
          , 1.0                                                            AS demand_scalar
          , 1.0                                                            AS imp
          , pcm.mix_rate * dmd.color_demand / NULLIF(pf.yield, 0) *
            1.0                                                            AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
              JOIN stage.supplies_xref_lf AS s
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
           ELSE 'SINGLE' END = 'SINGLE'
       AND UPPER(pcm.type) <> 'PCM_ENGINE_ACTS'
       AND UPPER(s.k_color) = 'COLOR'
       and UPPER(hw.technology) = 'LF'

     UNION ALL

     SELECT pcm.type
          , pcm.cal_date
          , pcm.geography_grain
          , pcm.geography
          , pcm.platform_subset
          , pcm.base_product_number
          , pcm.customer_engagement
          , pcm.mix_rate AS page_cc_mix
          , dmd.cmy_demand                                               AS demand
          , pf.yield
          , pcm.mix_rate * dmd.cmy_demand                             AS page_demand
          , pcm.mix_rate * dmd.cmy_demand /
            NULLIF(pf.yield, 0)                                          AS cartridges
          , 0.0                                                          AS cartridge_volume
          , 1.0                                                          AS demand_scalar
          , 1.0                                                          AS imp
          , pcm.mix_rate * dmd.cmy_demand / NULLIF(pf.yield, 0) *
            1.0                                                          AS imp_corrected_cartridges
     FROM stage.page_cc_mix_lf AS pcm
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
           ELSE 'SINGLE' END <> 'SINGLE'
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
    , black_demand AS demand
    , yield
    , page_demand
    , cartridges
    , cartridge_volume
    , demand_scalar
    , imp
    , imp_corrected_cartridges
	, CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset + ' ' +
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
    , color_demand AS demand
    , yield
    , page_demand
    , cartridges
    , cartridge_volume
    , demand_scalar
    , imp
    , imp_corrected_cartridges
	, CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset + ' ' +
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
    , cmy_demand AS demand
    , yield
    , page_demand
    , cartridges
    , cartridge_volume
    , demand_scalar
    , imp
    , imp_corrected_cartridges
	, CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset + ' ' +
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
    , black_demand AS demand
    , yield
    , page_demand
    , cartridges
    , cartridge_volume
    , demand_scalar
    , imp
    , imp_corrected_cartridges
	, CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset + ' ' +
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
	, CAST(cal_date AS VARCHAR) + ' ' + geography + ' ' + platform_subset + ' ' +
        base_product_number + ' ' + customer_engagement AS composite_key
FROM pcrg_05_color_fcst
"""

query_list.append(["stage.page_cc_cartridges", page_cc_cartridges, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Redshift

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list

# COMMAND ----------

submit_remote_query(configs, 
"""
with ac_ce as (
select ac.cal_date,ac.market10,ac.country_alpha2,ac.platform_subset,ac.base_product_number,CASE WHEN hw.technology = 'LASER' AND ac.platform_subset LIKE '%STND%' THEN 'STD'
       WHEN hw.technology = 'LASER' AND ac.platform_subset LIKE '%YET2%' THEN 'HP+'
                ELSE ac.customer_engagement END AS customer_engagement,ac.base_quantity
from stage.actuals_supplies_lf ac 
left join mdm.hardware_xref hw on hw.platform_subset = ac.platform_subset
--where ac.official = 1
),

act_m10 as (
select cal_date,market10,platform_subset,base_product_number,customer_engagement,SUM(base_quantity) base_quantity
from ac_ce
group by cal_date,market10,platform_subset,base_product_number,customer_engagement
)

UPDATE stage.page_cc_cartridges
set imp_corrected_cartridges = ac.base_quantity
FROM stage.page_cc_cartridges ccs
INNER JOIN act_m10 ac on ac.cal_date = ccs.cal_date and ccs.geography = ac.market10 and  ac.base_product_number = ccs.base_product_number 
						and ac.platform_subset = ccs.platform_subset and ccs.customer_engagement = ac.customer_engagement                 
"""
)


# COMMAND ----------

submit_remote_query(configs, 
"""
with ac_ce as (
select ac.cal_date,ac.market10,ac.country_alpha2,ac.platform_subset,ac.base_product_number,CASE WHEN hw.technology = 'LASER' AND ac.platform_subset LIKE '%STND%' THEN 'STD'
       WHEN hw.technology = 'LASER' AND ac.platform_subset LIKE '%YET2%' THEN 'HP+'
                ELSE ac.customer_engagement END AS customer_engagement,ac.base_quantity
from prod.actuals_supplies ac 
left join mdm.hardware_xref hw on hw.platform_subset = ac.platform_subset
where ac.official = 1
),

act_m10 as (
select cal_date,market10,platform_subset,base_product_number,customer_engagement,SUM(base_quantity) base_quantity
from ac_ce
group by cal_date,market10,platform_subset,base_product_number,customer_engagement
)

UPDATE stage.page_cc_cartridges 
set imp_corrected_cartridges = 0, cartridges = 0
FROM (select ccs.* from stage.page_cc_cartridges ccs
LEFT JOIN act_m10 ac on ac.cal_date = ccs.cal_date and ccs.geography = ac.market10 and  ac.base_product_number = ccs.base_product_number 
						and ac.platform_subset = ccs.platform_subset and ccs.customer_engagement = ac.customer_engagement 
WHERE ccs.cal_date <= (SELECT MAX(cal_date) FROM prod.actuals_supplies WHERE official = 1) 
and ac.base_quantity is null) cc
where stage.page_cc_cartridges.cal_date = cc.cal_date and stage.page_cc_cartridges.geography  = cc.geography  and stage.page_cc_cartridges.platform_subset = cc.platform_subset 
and stage.page_cc_cartridges.base_product_number = cc.base_product_number and stage.page_cc_cartridges.customer_engagement = cc.customer_engagement 
"""
)

