# Databricks notebook source
# create empty widgets for interactive sessions
dbutils.widgets.text('installed_base_version', '') # installed base version
dbutils.widgets.text('usage_share_version', '') # usage-share version
dbutils.widgets.text('run_vtc_and_adjusted_cartridges', '') # run notebook boolean

# COMMAND ----------

# exit notebook if task boolean is False, else continue
notebook_run_parameter_label = 'run_vtc_and_adjusted_cartridges' 
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
# MAGIC ## Run base forecast cartridge adjust processes

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1.1: analytic

# COMMAND ----------

toner_08_analytic = """
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
     FROM scen.toner_07_page_cc_cartridges AS c2c
     JOIN mdm.hardware_xref AS hw
         ON UPPER(hw.platform_subset) = UPPER(c2c.platform_subset)
     JOIN mdm.supplies_xref AS sup
         ON UPPER(sup.base_product_number) =
            UPPER(c2c.base_product_number)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('LASER'))

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

query_list.append(["scen.toner_08_analytic", toner_08_analytic, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1.2: channel_fill

# COMMAND ----------

toner_09_channel_fill = f"""
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
     FROM scen.toner_08_analytic AS c2c
     JOIN mdm.hardware_xref AS hw
         ON UPPER(hw.platform_subset) = UPPER(c2c.platform_subset)
     WHERE 1 = 1
       AND UPPER(hw.technology) IN ('LASER'))

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
       AND ns.version = '{installed_base_version}'
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
            ELSE 1 + DATEDIFF(MONTH,cal_date,MAX(cal_date)
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
     , 0 channel_fill
FROM cfadj_08_channel_fill_setup AS c2c
WHERE 1 = 1
  AND c2c.channel_fill > 0 -- filter out negative records
  AND c2c.cal_date < (SELECT MAX(cal_date) FROM prod.actuals_supplies WHERE official = 1)
  
  UNION
  
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
  AND c2c.cal_date > (SELECT MAX(cal_date) FROM prod.actuals_supplies WHERE official = 1)
"""

query_list.append(["scen.toner_09_channel_fill", toner_09_channel_fill, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1.3: supplies_spares

# COMMAND ----------

toner_10_supplies_spares = f"""
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
     FROM scen.toner_08_analytic AS c2c
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
        AND ns.version = '{installed_base_version}'
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
    , 0 AS supplies_spares
FROM case_statement
WHERE cal_date < (SELECT MAX(cal_date) FROM prod.actuals_supplies WHERE official = 1)

UNION

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
WHERE cal_date > (SELECT MAX(cal_date) FROM prod.actuals_supplies WHERE official = 1)
"""

query_list.append(["scen.toner_10_supplies_spares", toner_10_supplies_spares, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1.4: host

# COMMAND ----------

toner_11_host = f"""
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
       AND ns.version = '{installed_base_version}'
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
       AND ns.version = '{installed_base_version}'
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
       AND ns.version = '{installed_base_version}'
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

query_list.append(["scen.toner_11_host", toner_11_host, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1.5: welcome_kits

# COMMAND ----------

toner_12_welcome_kits = f"""
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
       AND ib.version = '{installed_base_version}'
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
FROM scen.toner_08_analytic AS c2c
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
FROM scen.toner_08_analytic AS c2c
JOIN wel_04_ltf_ib_step_3 AS ltf
    ON ltf.cal_date = c2c.cal_date
    AND UPPER(ltf.geography) = UPPER(c2c.geography)
    AND UPPER(ltf.platform_subset) = UPPER(c2c.platform_subset)
    AND
       UPPER(ltf.customer_engagement) = UPPER(c2c.customer_engagement)
"""

query_list.append(["scen.toner_12_welcome_kits", toner_12_welcome_kits, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1.6: vtc

# COMMAND ----------

toner_13_toner_crgs_w_vtc = f"""
WITH vtc_01_analytic_cartridges AS
    (SELECT cal_date
          , geography
          , platform_subset
          , base_product_number
          , customer_engagement
          , SUM(cartridges)               AS cartridges
          , SUM(imp_corrected_cartridges) AS imp_corrected_cartridges
     FROM scen.toner_08_analytic
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
       AND ns.version = '{installed_base_version}'
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
     LEFT JOIN scen.toner_10_supplies_spares AS ss
         ON cr.cal_date = ss.cal_date
         AND UPPER(cr.geography) = UPPER(ss.geography)
         AND UPPER(cr.base_product_number) = UPPER(ss.base_product_number)
         AND UPPER(cr.platform_subset) = UPPER(ss.platform_subset)
         AND UPPER(cr.customer_engagement) = UPPER(ss.customer_engagement)
     LEFT JOIN scen.toner_09_channel_fill AS cf
         ON cr.cal_date = cf.cal_date
         AND UPPER(cr.geography) = UPPER(cf.geography)
         AND UPPER(cr.base_product_number) = UPPER(cf.base_product_number)
         AND UPPER(cr.platform_subset) = UPPER(cf.platform_subset)
         AND UPPER(cr.customer_engagement) = UPPER(cf.customer_engagement)
     LEFT JOIN vtc_03_norm_ships AS ns
         ON UPPER(ns.geography) = UPPER(cr.geography)
         AND UPPER(ns.platform_subset) = UPPER(cr.platform_subset)
         AND ns.cal_date = cr.cal_date
     LEFT JOIN scen.toner_11_host AS h
         ON cr.cal_date = h.cal_date
         AND UPPER(cr.geography) = UPPER(h.geography)
         AND UPPER(cr.base_product_number) = UPPER(h.base_product_number)
         AND UPPER(cr.platform_subset) = UPPER(h.platform_subset)
         AND UPPER(cr.customer_engagement) = UPPER(h.customer_engagement)
     LEFT JOIN scen.toner_12_welcome_kits AS w
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
                     OVER (PARTITION BY ac.cal_date, ac.geography, ac.base_product_number,ac.platform_subset,ac.customer_engagement),
                     0) /
            NULLIF(SUM(ac.expected_crgs)
                   OVER (PARTITION BY ac.cal_date, ac.geography, ac.base_product_number,ac.platform_subset,ac.customer_engagement),
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
       AND hw.version = '{installed_base_version}')

   , c2c_vtc_06_vol_count       AS
    (SELECT DISTINCT geography
                   , platform_subset
                   , base_product_number
                   , customer_engagement
                   , COUNT(cal_date)
                     OVER (PARTITION BY geography, base_product_number,platform_subset,customer_engagement) AS vol_count -- count of months with volume
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
            OVER (PARTITION BY vtcc.geography, vtcc.base_product_number,vtcc.platform_subset,vtcc.customer_engagement) AS max_cal_date
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
FROM stage.host_cartridges AS h
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

query_list.append(["scen.toner_13_toner_crgs_w_vtc", toner_13_toner_crgs_w_vtc, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Toner_working_forecast

# COMMAND ----------

toner_working_fcst = f"""
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
          , date AS cal_date
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
         AND UPPER(cref.country_scenario) = 'MARKET10'
     WHERE 1 = 1
       AND ns.version = '{installed_base_version}'
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
       AND xref.technology IN ('LASER'))

   , toner_supplies_xref AS
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
       AND s.technology = 'LASER')

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
       AND NOT market10 = 'WORLD WIDE'
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
          , '{installed_base_version}' AS version
     FROM prod.ib

     UNION ALL

     SELECT 'USAGE_SHARE' AS record
          , 'SYSTEM'      AS user_name
          , NULL          AS load_date
          , '{usage_share_version}'  AS version
     FROM prod.usage_share

     UNION ALL

     SELECT DISTINCT 'SCENARIO_USAGE_SHARE'                 AS record
                   , us_scen.user_name                      AS user_name
                   , us_scen.load_date                      AS load_date
                   , CAST(us_scen.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_usage_share AS us_scen
     WHERE 1 = 1
       AND us_scen.upload_type = 'WORKING-FORECAST'
       AND UPPER(us_scen.user_name) IN ('JON.LINDBERG@HP.COM', 'JOHNF', 'JOHN.FLOCK@HP.COM', 'VANB', 'WILLIAM.VAN.BAIN@HP.COM', 'YONGHOONL', 'YONGHOON.LEE@HP.COM')

     UNION ALL

     SELECT DISTINCT 'SCENARIO_MIX_RATE'                AS record
                   , smr.user_name                      AS user_name
                   , smr.load_date                      AS load_date
                   , CAST(smr.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_mix_rate AS smr
     WHERE 1 = 1
       AND smr.upload_type = 'WORKING-FORECAST'
       AND UPPER(smr.user_name) IN ('JON.LINDBERG@HP.COM', 'JOHNF', 'JOHN.FLOCK@HP.COM', 'VANB', 'WILLIAM.VAN.BAIN@HP.COM', 'YONGHOONL', 'YONGHOON.LEE@HP.COM')

     UNION ALL

     SELECT DISTINCT 'SCENARIO_YIELD'                      AS record
                   , scen_y.user_name                      AS user_name
                   , scen_y.load_date                      AS load_date
                   , CAST(scen_y.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_yield AS scen_y
     WHERE 1 = 1
       AND scen_y.upload_type = 'WORKING-FORECAST'
       AND UPPER(scen_y.user_name) IN ('GRETCHENB', 'GRETCHEN.BRUNNER@HP.COM', 'JOHNF', 'JOHN.FLOCK@HP.COM', 'VANB', 'WILLIAM.VAN.BAIN@HP.COM', 'YONGHOONL', 'YONGHOON.LEE@HP.COM')

     UNION ALL

     SELECT DISTINCT 'SCENARIO_CHANNEL_FILL'           AS record
                   , cf.user_name                      AS user_name
                   , cf.load_date                      AS load_date
                   , CAST(cf.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_channel_fill AS cf
     WHERE 1 = 1
       AND cf.upload_type = 'WORKING-FORECAST'
       AND UPPER(cf.user_name) IN ('JON.LINDBERG@HP.COM', 'JOHNF', 'JOHN.FLOCK@HP.COM', 'VANB', 'WILLIAM.VAN.BAIN@HP.COM', 'YONGHOONL', 'YONGHOON.LEE@HP.COM')

     UNION ALL

     SELECT DISTINCT 'SCENARIO_SUPPLIES_SPARES'         AS record
                   , ssp.user_name                      AS user_name
                   , ssp.load_date                      AS load_date
                   , CAST(ssp.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_supplies_spares AS ssp
     WHERE 1 = 1
       AND ssp.upload_type = 'WORKING-FORECAST'
       AND UPPER(ssp.user_name) IN ('GRETCHENB', 'GRETCHEN.BRUNNER@HP.COM', 'JOHNF', 'JOHN.FLOCK@HP.COM', 'VANB', 'WILLIAM.VAN.BAIN@HP.COM', 'YONGHOONL', 'YONGHOON.LEE@HP.COM')

     UNION ALL

     SELECT DISTINCT 'SCENARIO_VTC_OVERRIDE'          AS record
                   , v.user_name                      AS user_name
                   , v.load_date                      AS load_date
                   , CAST(v.load_date AS VARCHAR(50)) AS version
     FROM scen.working_forecast_vtc_override AS v
     WHERE 1 = 1
       AND v.upload_type = 'WORKING-FORECAST'
       AND UPPER(v.user_name) IN
           ('JON.LINDBERG@HP.COM','GRETCHENB', 'GRETCHEN.BRUNNER@HP.COM', 'VANB', 'WILLIAM.VAN.BAIN@HP.COM', 'YONGHOONL', 'YONGHOON.LEE@HP.COM'))

   , toner_cf_prep       AS
    (SELECT fv.user_name
          , MAX(fv.load_date) AS max_load_date
     FROM override_filters AS fv
     WHERE 1 = 1
       AND fv.record = 'SCENARIO_CHANNEL_FILL'
     GROUP BY fv.user_name)

   , toner_cf_subset     AS
    (SELECT DISTINCT DATEADD(MONTH, cf.month_num, cf.min_sys_date) AS cal_date
                   , cf.geography
                   , cf.platform_subset
                   , cf.base_product_number
                   , cf.customer_engagement
                   , cf.value                                      AS channel_fill
     FROM scen.working_forecast_channel_fill AS cf
     JOIN toner_cf_prep AS cfp
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
     JOIN toner_cf_prep AS cfp
         ON cfp.user_name = cf.user_name
         AND cfp.max_load_date = cf.load_date
     JOIN geography_mapping AS geo
         ON geo.region_5 = cf.geography
     WHERE 1 = 1
       AND cf.upload_type = 'WORKING-FORECAST'
       AND cf.geography_grain = 'REGION_5')

   , toner_cf            AS
    (SELECT DISTINCT cfs.cal_date
                   , cfs.geography
                   , cfs.platform_subset
                   , cfs.base_product_number
                   , cfs.customer_engagement
                   , ROUND(cfs.channel_fill, 0) AS channel_fill
     FROM toner_cf_subset AS cfs
     WHERE 1 = 1)

   , toner_ss_prep       AS
    (SELECT fv.user_name
          , MAX(fv.load_date) AS max_load_date
     FROM override_filters AS fv
     WHERE 1 = 1
       AND fv.record = 'SCENARIO_SUPPLIES_SPARES'
     GROUP BY fv.user_name)

   , toner_ss_subset     AS
    (SELECT ssp.geography
          , ssp.platform_subset
          , ssp.base_product_number
          , ssp.customer_engagement
          , ssp.min_sys_date
          , ssp.month_num
          , ssp.value
     FROM scen.working_forecast_supplies_spares AS ssp
     JOIN toner_ss_prep AS sspr
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
     JOIN toner_ss_prep AS sspr
         ON sspr.user_name = ssp.user_name
         AND sspr.max_load_date = ssp.load_date
     JOIN geography_mapping AS geo
         ON geo.region_5 = ssp.geography
     WHERE 1 = 1
       AND ssp.upload_type = 'WORKING-FORECAST'
       AND ssp.geography_grain = 'REGION_5')

   , toner_ss            AS
    (SELECT DISTINCT DATEADD(MONTH, ssp.month_num, ssp.min_sys_date) AS cal_date
                   , ssp.geography
                   , ssp.platform_subset
                   , ssp.base_product_number
                   , ssp.customer_engagement
                   , ROUND(ssp.value, 0)                             AS supplies_spares -- treat value as units
     FROM toner_ss_subset AS ssp
     LEFT JOIN vtc_03_norm_ships AS c2c
         ON ssp.geography = c2c.geography
         AND DATEADD(MONTH, ssp.month_num, ssp.min_sys_date) = c2c.cal_date
         AND ssp.platform_subset = c2c.platform_subset
     WHERE 1 = 1)

   , toner_vtc_prep      AS
    (SELECT fv.user_name
          , MAX(fv.load_date) AS max_load_date
     FROM override_filters AS fv
     WHERE 1 = 1
       AND fv.record = 'SCENARIO_VTC_OVERRIDE'
     GROUP BY fv.user_name)

   , toner_vtc_subset    AS
    (SELECT v.geography
          , v.base_product_number
          , v.min_sys_date
          , v.month_num
          , v.value
     FROM scen.working_forecast_vtc_override AS v
     JOIN toner_vtc_prep AS vp
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
     JOIN toner_vtc_prep AS vp
         ON vp.user_name = v.user_name
         AND vp.max_load_date = v.load_date
     JOIN geography_mapping AS geo
         ON geo.region_5 = v.geography
     WHERE 1 = 1
       AND v.upload_type = 'WORKING-FORECAST'
       AND v.geography_grain = 'REGION_5')

   , toner_vtc           AS
    (SELECT DISTINCT DATEADD(MONTH, v.month_num, v.min_sys_date) AS cal_date
                   , v.geography
                   , v.base_product_number
                   , v.value                                     AS mvtc
     FROM toner_vtc_subset AS v
     WHERE 1 = 1)

   , toner_working_fcst  AS
    (SELECT 'IE2-WORKING-FORECAST'                                 AS record
          , GETDATE()                                              AS build_time
          , vtc.cal_date
          , vtc.geography_grain
          , vtc.geography
          , vtc.platform_subset
          , vtc.base_product_number
          , vtc.customer_engagement
          , vtc.cartridges
          , vtc.channel_fill   AS channel_fill
          , vtc.supplies_spares_crgs  AS supplies_spares_cartridges
          , vtc.expected_crgs AS expected_cartridges
          , vtc.mvtc              AS vtc
          , vtc.mvtc_adjusted_crgs                     AS adjusted_cartridges
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
     FROM scen.toner_13_toner_crgs_w_vtc AS vtc
     LEFT JOIN supplies_enrichment AS enr
         ON enr.platform_subset = vtc.platform_subset
     JOIN mdm.calendar AS cal
         ON vtc.cal_date = cal.date
     JOIN mdm.hardware_xref AS hw
         ON vtc.platform_subset = hw.platform_subset
     JOIN country_code_xref AS cc
         ON vtc.geography = cc.market10
     LEFT JOIN toner_supplies_xref AS supp
         ON supp.base_product_number = vtc.base_product_number
     LEFT JOIN pen_fills AS pf
         ON pf.market_10 = cc.market10
         AND pf.cal_date = vtc.cal_date
         AND pf.base_product_number = vtc.base_product_number
     WHERE 1 = 1 AND vtc.cal_date <= (SELECT MAX(cal_date) FROM prod.actuals_supplies WHERE official = 1)
       AND hw.technology IN ('LASER') 
       
       UNION 
    
    SELECT 'IE2-WORKING-FORECAST'                                 AS record
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
     FROM scen.toner_13_toner_crgs_w_vtc AS vtc
     LEFT JOIN toner_cf AS cf
         ON cf.geography = vtc.geography
         AND cf.cal_date = vtc.cal_date
         AND cf.platform_subset = vtc.platform_subset
         AND cf.base_product_number = vtc.base_product_number
         AND cf.customer_engagement = vtc.customer_engagement
     LEFT JOIN toner_ss AS ss
         ON ss.geography = vtc.geography
         AND ss.cal_date = vtc.cal_date
         AND ss.platform_subset = vtc.platform_subset
         AND ss.base_product_number = vtc.base_product_number
         AND ss.customer_engagement = vtc.customer_engagement
     LEFT JOIN toner_vtc AS p
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
     LEFT JOIN toner_supplies_xref AS supp
         ON supp.base_product_number = vtc.base_product_number
     LEFT JOIN pen_fills AS pf
         ON pf.market_10 = cc.market10
         AND pf.cal_date = vtc.cal_date
         AND pf.base_product_number = vtc.base_product_number
     WHERE 1 = 1 AND vtc.cal_date > (SELECT MAX(cal_date) FROM prod.actuals_supplies WHERE official = 1)
       AND hw.technology IN ('LASER')

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
     FROM toner_cf AS cf -- allows forecasters to upload records not in system for cf
     LEFT OUTER JOIN scen.toner_13_toner_crgs_w_vtc AS vtc
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
     LEFT JOIN toner_supplies_xref AS supp
         ON supp.base_product_number = cf.base_product_number
     LEFT JOIN pen_fills AS pf
         ON pf.market_10 = cc.market10
         AND pf.cal_date = cf.cal_date
         AND pf.base_product_number = cf.base_product_number
     WHERE 1 = 1
       AND hw.technology IN ('LASER')
       AND cf.channel_fill > 0
       AND vtc.geography IS NULL
       AND vtc.cal_date IS NULL
       AND vtc.platform_subset IS NULL
       AND vtc.base_product_number IS NULL
       AND vtc.customer_engagement IS NULL)

SELECT *
FROM toner_working_fcst
"""

query_list.append(["scen.toner_working_fcst", toner_working_fcst, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Redshift

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
