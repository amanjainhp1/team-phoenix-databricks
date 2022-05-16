# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share Staging
# MAGIC (final output)

# COMMAND ----------

query_list = []

# COMMAND ----------

usage_share_staging = """
with prod_00_iink_hp_share as (


SELECT DISTINCT ib.cal_date
    , ib.platform_subset
    , ib.customer_engagement
    , ib.version
    , iso.market10 AS geography
FROM "prod"."ib" AS ib
JOIN "mdm"."iso_country_code_xref" AS iso
    ON iso.country_alpha2 = ib.country
WHERE 1=1
    AND ib.version = '2022.05.12.1'
    AND ib.customer_engagement = 'I-INK'
)SELECT 'USAGE_SHARE' AS record
      , us.cal_date
      , us.geography_grain
      , us.geography
      , us.platform_subset
      , us.customer_engagement
      , us.measure
      , us.units
      , us.ib_version
      , us.source
      , us.version
      , us.load_date
FROM "stage"."usage_share_staging_pre_epa" us
WHERE 1=1
    AND NOT (us.customer_engagement = 'I-INK' AND us.measure = 'HP_SHARE')

UNION ALL

SELECT 'USAGE_SHARE' AS record
    , ib.cal_date
    , 'MARKET10' AS geography_grain
    , ib.geography
    , ib.platform_subset
    , ib.customer_engagement
    , 'HP_SHARE' AS measure
    , 1 AS units
    , ib.version AS ib_version
    , 'OVERRIDE' AS source
    , us.version
    , us.load_date
FROM prod_00_iink_hp_share AS ib
CROSS JOIN (SELECT DISTINCT version, load_date FROM "stage"."usage_share_staging_pre_epa" ) AS us
WHERE 1=1
"""

query_list.append(["stage.usage_share_staging", usage_share_staging, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Out Usage Share Staging

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
