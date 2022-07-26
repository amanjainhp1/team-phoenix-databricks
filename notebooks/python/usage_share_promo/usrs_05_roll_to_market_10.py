# Databricks notebook source
# MAGIC %md
# MAGIC # Usage Share Refactor Step 05
# MAGIC - Roll Up to Market 10

# COMMAND ----------

# for interactive sessions, define a version widget
dbutils.widgets.text("version", "")

# COMMAND ----------

# for interactive sessions, define a version widget
dbutils.widgets.text("version", "")

# COMMAND ----------

import pandas as pd
import numpy as mp
import psycopg2 as ps

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# Get country/market 10 relationships
country_info = read_redshift_to_df(configs) \
  .option("query","""
    SELECT country_alpha2, market10
    FROM "mdm"."iso_country_code_xref"
    """) \
  .load()
country_info.createOrReplaceTempView("country_info")

# COMMAND ----------

# Get ib
ib = read_redshift_to_df(configs) \
  .option("query",f"""
    SELECT country_alpha2, platform_subset, customer_engagement, cal_date, units
    FROM "prod"."ib"
    WHERE version='{version}'
    """) \
  .load()
ib.createOrReplaceTempView("ib")

# COMMAND ----------

# Read in U/S data
us_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/usage_share_country")
us_table.createOrReplaceTempView("us_table")

# COMMAND ----------

convert = f"""
with step1 as (
    SELECT us.cal_date
	, us.geography
	, us.platform_subset
	, us.customer_engagement
	, AVG(CASE WHEN us.measure='USAGE' THEN
		CASE WHEN us.data_source='TELEMETRY' THEN 1
		WHEN us.data_source LIKE 'MODELED' THEN 0 END
		ELSE NULL END) AS data_source_u
	, AVG(CASE WHEN us.measure='USAGE' THEN
		CASE WHEN us.data_source='TELEMETRY' THEN 1
		WHEN us.data_source LIKE 'MODELED' THEN 0 END
		ELSE NULL END) AS data_source_c
	, AVG(CASE WHEN us.measure='USAGE' THEN
		CASE WHEN us.data_source='TELEMETRY' THEN 1
		WHEN us.data_source LIKE 'MODELED' THEN 0 END
		ELSE NULL END) AS data_source_k
	, AVG(CASE WHEN us.measure='HP_SHARE' THEN
		CASE WHEN us.data_source='TELEMETRY' THEN 1
		WHEN us.data_source LIKE 'MODELED' THEN 0
		ELSE NULL END) AS data_source_s
	, SUM(CASE WHEN us.measure='USAGE' THEN us.units ELSE 0 END) AS usage
    , SUM(CASE WHEN us.measure='HP_SHARE' THEN us.units ELSE 0 END) AS page_share
    , SUM(CASE WHEN us.measure='COLOR_USAGE' THEN us.units ELSE 0 END) AS usage_c
	, SUM(CASE WHEN us.measure='K_USAGE' THEN us.units ELSE 0 END) AS usage_k
FROM us_table us 
GROUP BY us.cal_date
	, us.geography
	, us.platform_subset
	, us.customer_engagement
	) , step2 as (
    SELECT 
       u.cal_date
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
      ,SUM(u.data_source_u) as data_source_u
      ,SUM(u.data_source_c) as data_source_c
      ,SUM(u.data_source_k) as data_source_k
      ,SUM(u.data_source_s) as data_source_s
      ,SUM(u.usage) AS usage
      ,SUM(u.page_share) AS page_share
      ,SUM(u.usage_c) AS usage_c
	  ,SUM(u.usage_k) AS usage_k
      ,SUM(i.units) as ib
      ,c.market10
FROM step1 u
LEFT JOIN ib i
    ON 1=1
    AND u.platform_subset=i.platform_subset
    AND u.customer_engagement=i.customer_engagement
    AND u.geography=i.country_alpha2
    AND u.cal_date=i.cal_date
 LEFT JOIN country_info c
    ON u.geography=c.country_alpha2
GROUP BY 
       u.cal_date
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
      ,c.market10
) , step3 as (
    SELECT u.cal_date
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
      ,SUM(u.data_source_u) as data_source_u
      ,SUM(u.data_source_c) as data_source_c
      ,SUM(u.data_source_k) as data_source_k
      ,SUM(u.data_source_s) as data_source_s
      ,SUM(u.usage*COALESCE(ib,0)) AS pages
      ,SUM(u.page_share*usage*COALESCE(ib,0)) AS hp_pages
      ,SUM(u.usage_c*COALESCE(ib,0)) AS color_pages
	  ,SUM(u.usage_k*COALESCE(ib,0)) AS black_pages
      ,SUM(u.COALESCE(ib,0)) as ib
      ,u.market10
    FROM step2 u
    GROUP BY 
       u.cal_date
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
      ,u.market10
) ,step4 as (
    SELECT u.cal_date
      ,u.platform_subset
      ,u.customer_engagement
      ,AVG(u.data_source_u) as data_source_u
      ,AVG(u.data_source_c) as data_source_c
      ,AVG(u.data_source_k) as data_source_k
      ,AVG(u.data_source_s) as data_source_s
      ,SUM(u.pages) AS pages
      ,SUM(u.hp_pages) AS hp_pages
      ,SUM(u.color_pages) AS color_pages
	  ,SUM(u.black_pages) AS black_pages
      ,SUM(u.ib) as ib
      ,u.market10
    FROM step2 u
    GROUP BY 
       u.cal_date
      ,u.platform_subset
      ,u.customer_engagement
      ,u.market10
), step5 as (
    SELECT
      h4.cal_date
	, h4.market10
	, h4.data_source_u
	, h4.data_source_s
	, h4.data_source_c
	, h4.data_source_k
	, h4.platform_subset
	, h4.customer_engagement
	, h4.pages/nullif(ib,0) AS usage
	, h4.hp_pges/nullif(pages,0) AS page_share
	, h4.color_pages/nullif(ib,0) AS usage_c
	, h4.black_pages/nullif(ib,0) AS usage_k
	, h4.ib_version
FROM step4 h4

), step6 as (
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'USAGE' as measure
	, usage as units
	, CONCAT(data_source_u,"% TELEMETRY, ",1-data_source_u,"% MODELED") as source
FROM step_5
WHERE usage IS NOT NULL
    AND usage > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'HP_SHARE' as measure
	, page_share as units
	, CONCAT(data_source_s,"% TELEMETRY, ",1-data_source_s,"% MODELED") as source
FROM step_5
WHERE page_share IS NOT NULL
    AND page_share > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'COLOR_USAGE' as measure
	, usage_c as units
	, CONCAT(data_source_c,"% TELEMETRY, ",1-data_source_c,"% MODELED") as source
FROM step_5
WHERE usage_c IS NOT NULL
    AND usage_c > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'K_USAGE' as measure
	, usage_k as units
	, CONCAT(data_source_k,"% TELEMETRY, ",1-data_source_k,"% MODELED") as source
FROM step_5
WHERE usage_k IS NOT NULL
    AND usage_k > 0

)
SELECT "USAGE_SHARE" as record
      ,cal_date
      ,"MARKET 10" as geography_grain
      ,market10 as geography
      ,platform_subset
      ,customer_engagement
      ,measure
      ,units
      ,'{version}' as ib_version
      ,source
      ,CONCAT(CAST(current_date() AS DATE),".1") as version
      ,load_date
      FROM step_6
               
"""
convert=spark.sql(convert)
convert.createOrReplaceTempView("convert")

# COMMAND ----------

display(convert)
