# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share 05
# MAGIC - Demand to bring in pages to measures

# COMMAND ----------

# for interactive sessions, define a version widget
dbutils.widgets.text("version", "")

# COMMAND ----------

# retrieve version from widget
version = dbutils.widgets.get("version")

# COMMAND ----------

query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Demand

# COMMAND ----------

us_demand = """
WITH dbd_01_ib_load AS
    (SELECT ib.cal_date
          , ib.platform_subset
          , ib.customer_engagement
          , ccx.market10 AS geography
          , ib.measure
          , ib.units
          , (SELECT MAX(version) FROM prod.ib) as version
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
          , ib.version
     FROM dbd_01_ib_load AS ib
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
     FROM stage.usage_share_staging AS us
              JOIN mdm.hardware_xref AS hw
                   ON hw.platform_subset = us.platform_subset
     WHERE 1 = 1
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
     group by ib.version
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

   , dmd_09_unpivot_measure AS
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

    SELECT 'DEMAND'                   AS record
          , dmd.cal_date
          , 'MARKET10'                 AS geography_grain
          , dmd.geography
          , dmd.platform_subset
          , dmd.customer_engagement
          , dmd.measure
          , dmd.units
          , dmd.version
          , 'PAGES' as source
          , CAST(NULL AS DATE)         AS load_date
     FROM dmd_09_unpivot_measure AS dmd
"""

query_list.append(["stage.usage_share_demand", us_demand, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write out U/S Demand

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
