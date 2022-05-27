# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # page_cc_cartridges_adjusts

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Documentation

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## analytic

# COMMAND ----------

analytic = """
WITH date_helper AS
    (SELECT date_key
          , date AS cal_date
     FROM mdm.calendar
     WHERE 1 = 1
       AND day_of_month = 1)

   , ana_02_c2c_setup AS
    (SELECT c2c.cal_date
          , c2c.geography_grain
          , c2c.geography
          , c2c.platform_subset
          , c2c.base_product_number
          , c2c.customer_engagement
          , hw.pl
          , hw.hw_product_family
          , sup.Crg_Chrome
          , c2c.cartridges
          , c2c.imp_corrected_cartridges
          , MIN(c2c.cal_date)
            OVER (PARTITION BY c2c.geography, c2c.platform_subset, c2c.base_product_number, c2c.customer_engagement) AS min_cal_date
          , MAX(c2c.cal_date)
            OVER (PARTITION BY c2c.geography, c2c.platform_subset, c2c.base_product_number, c2c.customer_engagement) AS max_cal_date
     FROM stage.page_cc_cartridges AS c2c
              JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(c2c.platform_subset)
              JOIN mdm.supplies_xref AS sup
                   ON UPPER(sup.base_product_number) =
                      UPPER(c2c.base_product_number)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('INK', 'LASER', 'PWA'))

   , ana_03_c2c_fill_gap_1 AS
    (SELECT DISTINCT d.cal_date AS cal_date
                   , c2c.geography_grain
                   , c2c.geography
                   , c2c.platform_subset
                   , c2c.base_product_number
                   , c2c.customer_engagement
                   , c2c.pl
                   , c2c.hw_product_family
                   , c2c.Crg_Chrome
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
          , COALESCE(h.Crg_Chrome, c2c.Crg_Chrome)           AS Crg_Chrome
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
          , Crg_Chrome
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
     , Crg_Chrome
     , cartridges
     , imp_corrected_cartridges
     , CASE
           WHEN min_cal_date < CAST(DATEADD(MONTH, 1, DATE_TRUNC('MONTH', CAST(GETDATE() AS DATE))) AS DATE)
               THEN 1
           ELSE 0 END                                                          AS actuals_flag
     , COUNT(cal_date)
       OVER (PARTITION BY geography, platform_subset, base_product_number, customer_engagement
           ORDER BY cal_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
FROM ana_05_c2c_fill_gap_3
"""

query_list.append(["stage.analytic", analytic, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## channel_fill

# COMMAND ----------

channel_fill = """
WITH cfadj_01_c2c AS
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
     FROM stage.analytic AS c2c
              JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(c2c.platform_subset)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('INK', 'LASER', 'PWA'))

   , cfadj_04_c2c_avg_ship AS
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

   , cfadj_02_norm_shipments AS
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

   , cfadj_03_hw_intro AS
    (SELECT DISTINCT c2c.geography
                   , c2c.platform_subset
                   , COALESCE(ns.min_acts_date,
                              c2c.hw_intro_date) AS sys_hw_intro_date
     FROM cfadj_01_c2c AS c2c
              LEFT JOIN cfadj_02_norm_shipments AS ns
                        ON UPPER(ns.geography) = UPPER(c2c.geography)
                            AND UPPER(ns.platform_subset) =
                                UPPER(c2c.platform_subset))

   , cfadj_05_valid_crgs AS
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

   , cfadj_06_valid_months AS
    (SELECT c2c.geography_grain
          , c2c.geography
          , c2c.platform_subset
          , c2c.base_product_number
          , c2c.customer_engagement
          , c2c.sys_crg_intro_date
          , c2c.sys_hw_intro_date
          , c2c.month_offset
          , c2c.avg_shipments
          , cal.Date AS cal_date
     FROM cfadj_05_valid_crgs AS c2c
              JOIN mdm.calendar AS cal
                   ON (
                              cal.Date BETWEEN c2c.sys_hw_intro_date AND DATEADD(MONTH, 6, c2c.sys_hw_intro_date) OR
                              cal.Date = DATEADD(MONTH, -1 * c2c.month_offset,
                                                 c2c.sys_hw_intro_date)
                          )
                       AND cal.day_of_month = 1)

   , cfadj_07_power_args AS
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
                ELSE 1 + DATEDIFF(MONTH, cal_date, MAX(cal_date)
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
  AND c2c.channel_fill > 0 -- filter out negative records
"""

query_list.append(["stage.channel_fill", channel_fill, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## supplies_spares

# COMMAND ----------

supplies_spares = """
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
     FROM stage.analytic AS c2c
     WHERE 1 = 1
       AND NOT (c2c.base_product_number IN
                ('W9014MC', 'W9040MC', 'W9041MC', 'W9042MC', 'W9043MC') AND
                c2c.cal_date >
                CAST(GETDATE() - DATE_PART(DAY, GETDATE()) + 1 AS DATE)) -- EOL products
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

                            CASE WHEN c2c.cal_date < '2019-04-01' THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date BETWEEN '2019-04-01' AND '2020-06-01' THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                                    * hw.hw_ratio
                                     ELSE 0 END

                                 WHEN c2c.cal_date >= '2020-07-01' THEN 0

                            ELSE 0 END

                        WHEN UPPER(c2c.hw_product_family) IN ('TONER CLJ JADE') THEN

                            CASE WHEN c2c.cal_date < '2019-04-01' THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date BETWEEN '2019-04-01' AND '2022-03-01' THEN  -- first day of previous month

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

                            CASE WHEN c2c.cal_date < '2019-05-01' THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date BETWEEN '2019-05-01' AND '2020-06-01' THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                                    * hw.hw_ratio
                                     ELSE 0 END

                                 WHEN c2c.cal_date >= '2020-07-01' THEN 0

                            ELSE 0 END

                        WHEN UPPER(c2c.hw_product_family) IN ('TONER CLJ AGATE') THEN

                            CASE WHEN c2c.cal_date < '2019-05-01' THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date BETWEEN '2019-05-01' AND '2022-03-01' THEN  -- first day of previous month

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

                                 WHEN c2c.cal_date >= '2020-09-01' THEN 0

                            ELSE 0 END

                        WHEN UPPER(c2c.hw_product_family) IN ('TONER CLJ AGATE EPA', 'TONER CLJ JADE EPA') THEN

                            CASE WHEN c2c.cal_date < CAST('2022-04-01' AS DATE) THEN

                                     CASE WHEN UPPER(c2c.Crg_Chrome) IN ('K', 'C', 'M', 'Y', 'BLK', 'CYN', 'MAG', 'YEL')
                                          THEN (c2c.cartridges * c2c.yield) /
                                               NULLIF(SUM(c2c.cartridges * c2c.yield) OVER (PARTITION BY c2c.cal_date, c2c.geography, c2c.platform_subset, c2c.customer_engagement, c2c.Crg_Chrome), 0)
                                     ELSE 0 END

                                 WHEN c2c.cal_date >= '2021-01-01' THEN 0

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

query_list.append(["stage.supplies_spares", supplies_spares, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Redshift

# COMMAND ----------

# MAGIC %run "../../common/output_to_redshift" $query_list=query_list
