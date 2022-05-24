# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share 04
# MAGIC - PRE Adjust
# MAGIC - Covid Adjusts
# MAGIC - EPA adjusts

# COMMAND ----------

query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Staging PRE Adjust

# COMMAND ----------

pre_adjust = """
SELECT us.record
    , us.cal_date
    , us.geography_grain
    , us.geography
    , us.platform_subset
    , us.customer_engagement
    , us.measure
    , us.units
    , us.ib_version
    , us.source
FROM "stage"."uss_02_npi_matures" us
"""

query_list.append(["stage.usage_share_staging_preadjust", pre_adjust, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share Covid Adjusts

# COMMAND ----------

uss_covid = """
with adjust_05_update_version as (


SELECT us.record
    , us.cal_date
    , us.geography_grain
    , us.geography
    , us.platform_subset
    , us.customer_engagement
    , us.measure
    , us.units
    , us.ib_version
    , us.source
    , (SELECT MAX(version) FROM prod.version WHERE record = 'USAGE_SHARE') as version
    , (SELECT MAX(load_date) FROM prod.version WHERE record = 'USAGE_SHARE') as load_date
FROM "stage"."uss_02_npi_matures" us
),  adjust_01_select_old_adjust as (


SELECT record
    , cal_date
    , geography_grain
    , geography
    , measure
    , platform_subset
    , customer_engagement
    , assumption
    , official
    , Units
    , version
    , load_date
FROM "prod"."usage_share_adjust"
),  adjust_02_update_not_official as (


SELECT record
    , cal_date
    , geography_grain
    , geography
    , measure
    , platform_subset
    , customer_engagement
    , assumption
    , 0 as official
    , Units
    , version
    , load_date
FROM adjust_01_select_old_adjust
WHERE  version <> 'NEW VERSION'
    AND official = 1
),  adjust_03_update_version as (


SELECT adj.record
    , adj.cal_date
    , adj.geography_grain
    , adj.geography
    , adj.measure
    , adj.platform_subset
    , adj.customer_engagement
    , adj.assumption
    , adj.official
    , adj.Units
    , (SELECT MAX(version) FROM prod.version WHERE record = 'USAGE_SHARE_ADJUST') as version
    , adj.load_date
FROM adjust_02_update_not_official adj
WHERE  version <> 'NEW VERSION'
),  adjust_04_update_official as (


SELECT adj.record
    , adj.cal_date
    , adj.geography_grain
    , adj.geography
    , adj.measure
    , adj.platform_subset
    , adj.customer_engagement
    , adj.assumption
    , 1 as official
    , adj.Units
    , adj.version
    , adj.load_date
FROM adjust_03_update_version adj
WHERE version = (SELECT MAX(version) FROM adjust_03_update_version)
),  adjust_06_adjust_adj1 as (


SELECT usa.record
    , usa.cal_date
    , usa.geography_grain
    , usa.geography
    , usa.measure
    , usa.platform_subset
    , usa.customer_engagement
    , usa.assumption
    , usa.official
    , usa.Units
    , usa.version
    , usa.load_date
FROM adjust_04_update_official usa
WHERE official=1
),  adjust_07_adjust_cc1 as (


SELECT distinct iso.region_5
    , iso.market10
FROM "mdm"."iso_country_code_xref" iso
),  adjust_08_adjust_adj2 as (


SELECT adj1.record
    , adj1.cal_date
    , 'MARKET10' AS geography_grain
    , cc1.market10 AS geography
    , adj1.measure
    , adj1.platform_subset
    , adj1.customer_engagement
    , adj1.assumption
    , adj1.official
    , adj1.Units
    , adj1.version
    , adj1.load_date
FROM adjust_06_adjust_adj1 adj1
LEFT JOIN adjust_07_adjust_cc1 cc1
    ON adj1.geography=cc1.region_5
),  adjust_10_uss_no_adjust as (


SELECT uss.record
   , uss.cal_date
   , uss.geography_grain
   , uss.geography
   , uss.measure
   , uss.platform_subset
   , uss.customer_engagement
   , usa.assumption
   , usa.official
   , uss.units
   , uss.ib_version
   , uss.source
   , usa.version
   , uss.load_date
FROM adjust_05_update_version uss
LEFT JOIN adjust_08_adjust_adj2 usa  -- true left outer join
	ON uss.platform_subset=usa.platform_subset
        AND uss.customer_engagement=usa.customer_engagement
        AND uss.cal_date=usa.cal_date
        AND uss.measure=usa.measure
        AND uss.geography=usa.geography
        AND usa.Units IS NOT NULL
WHERE 1=1
    AND uss.source <> 'TELEMETRY'
    AND usa.platform_subset IS NULL
    AND usa.customer_engagement IS NULL
    AND usa.cal_date IS NULL
    AND usa.measure IS NULL
    AND usa.geography IS NULL
UNION ALL
SELECT uss.record
   , uss.cal_date
   , uss.geography_grain
   , uss.geography
   , uss.measure
   , uss.platform_subset
   , uss.customer_engagement
   , NULL as assumption
   , NULL as official
   , uss.units
   , uss.ib_version
   , uss.source
   , uss.version
   , uss.load_date
FROM adjust_05_update_version uss
WHERE uss.source = 'TELEMETRY'
),  adjust_09_adjust_out as (


SELECT uss.record
   , uss.cal_date
   , uss.geography_grain
   , uss.geography
   , uss.measure
   , uss.platform_subset
   , uss.customer_engagement
   , usa.assumption
   , usa.official
   , (uss.units*usa.Units) as units
   , uss.ib_version
   , uss.source
   , usa.version
   , uss.load_date
FROM adjust_05_update_version uss
INNER JOIN adjust_08_adjust_adj2 usa
	ON uss.platform_subset=usa.platform_subset
        AND uss.customer_engagement=usa.customer_engagement
        AND uss.cal_date=usa.cal_date
        AND uss.measure=usa.measure
        AND uss.geography=usa.geography
        AND usa.Units IS NOT NULL
WHERE 1=1
    AND uss.source <> 'TELEMETRY'
),  adjust_11_uss_post_adjust as (


SELECT uss.record
   , uss.cal_date
   , uss.geography_grain
   , uss.geography
   , uss.measure
   , uss.platform_subset
   , uss.customer_engagement
   , uss.assumption
   , uss.official
   , uss.units
   , uss.ib_version
   , uss.source
   , uss.version
   , uss.load_date
FROM adjust_10_uss_no_adjust uss
UNION ALL
SELECT adj.record
   , adj.cal_date
   , adj.geography_grain
   , adj.geography
   , adj.measure
   , adj.platform_subset
   , adj.customer_engagement
   , adj.assumption
   , adj.official
   , adj.units
   , adj.ib_version
   , adj.source
   , adj.version
   , adj.load_date
FROM adjust_09_adjust_out adj
)SELECT ao.record
    , ao.cal_date
    , ao.geography_grain
    , ao.geography
    , ao.measure
    , ao.platform_subset
    , ao.customer_engagement
    , ao.assumption
    , ao.official
    , ao.units
    , (SELECT TOP 1 ib_version FROM adjust_11_uss_post_adjust WHERE ib_version <> 'IB_VERSION' ORDER BY 1 DESC) as ib_version
    , ao.source
    , ao.version
    , ao.load_date
FROM adjust_11_uss_post_adjust ao
"""

query_list.append(["stage.uss_covid_final", uss_covid, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share Pre-EPA Adjusts

# COMMAND ----------

pre_epa = """
SELECT us.record
    , us.cal_date
    , us.geography_grain
    , us.geography
    , us.measure
    , us.platform_subset
    , us.customer_engagement
    , us.assumption
    , us.official
    , us.units
    , us.ib_version
    , us.source
    , us.version
    , us.load_date
FROM "stage"."uss_covid_final" us
"""

query_list.append(["stage.usage_share_staging_pre_epaadjust", pre_epa, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share EPA Adjusts
# MAGIC 
# MAGIC EPA adjusts have not been used previously. Here just in case.

# COMMAND ----------

#epa_adjust = """
#with epa_01_epa_data as (
#
#
#SELECT DISTINCT epa.geography
#    , epa.platform_subset
#    , epa.customer_engagement
#    , epa.measure
#    , DATEADD(month, month_num, min_sys_date) AS cal_date
#    , epa.value
#FROM "prod"."scenario_usage_share" epa
#)SELECT us.record
#    , us.cal_date
#    , us.geography_grain
#    , us.geography
#    , us.measure
#    , us.platform_subset
#    , us.customer_engagement
#    , us.assumption
#    , us.official
#    , ed.value as units
#    , us.ib_version
#    , 'EPA_OVERRIDE' as source
#    , us.version
#    , us.load_date
#FROM "stage"."usage_share_staging_pre_epaadjust" us
#INNER JOIN epa_01_epa_data ed
#	ON us.geography = ed.geography
#        AND us.platform_subset = ed.platform_subset
#        AND us.customer_engagement = ed.customer_engagement
#        AND us.measure = ed.measure
#        AND us.cal_date = ed.cal_date
#WHERE 1=1
#	AND us.measure = 'HP_SHARE'
#	AND us.source <> 'TELEMETRY'
#"""
#
#query_list.append(["stage.epa_02_epa_adjust_out", epa_adjust, "overwrite"])

# COMMAND ----------

#epa_adjust_to_prod = """
#SELECT us.record
#      , us.cal_date
#      , us.geography_grain
#      , us.geography
#      , us.platform_subset
#      , us.customer_engagement
#      , us.measure
#      , us.units
#      , us.ib_version
#      , us.source
#      , us.version
#      , us.load_date
#FROM "stage"."epa_02_epa_adjust_out" us
#"""
#
#query_list.append(["stage.usage_share_staging_epa", epa_adjust_to_prod, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share EPA Adjust Prod

# COMMAND ----------

epa_to_prod = """
SELECT 'USAGE_SHARE' AS record
    , us.cal_date
    , us.geography_grain
    , us.geography
    , us.measure
    , us.platform_subset
    , us.customer_engagement
    , us.assumption
    , us.official
    , us.units
    , us.ib_version
    , us.source
    , (SELECT MAX(version) FROM "prod"."version" WHERE record = 'USAGE_SHARE') as version
    , us.load_date
FROM "stage"."usage_share_staging_pre_epaadjust" us
"""

query_list.append(["stage.usage_share_staging_pre_epa", epa_to_prod, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Out Covid and EPA Adjusts

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
