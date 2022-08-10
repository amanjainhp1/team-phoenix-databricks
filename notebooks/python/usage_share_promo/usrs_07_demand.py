# Databricks notebook source
# MAGIC %md
# MAGIC # Usage Share Refactor Step 07
# MAGIC - demand

# COMMAND ----------

# for interactive sessions, define a version widget
dbutils.widgets.text("version", "")

# COMMAND ----------

# for interactive sessions, define a version widget
version = dbutils.widgets.get("version")

# COMMAND ----------

import pandas as pd
import numpy as mp
import psycopg2 as ps

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# Read in Current data
#current_table=need to get step 01 results
us_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/us_adjusted")
us_table.createOrReplaceTempView("us_table")

# COMMAND ----------

# Get IB info
ib = read_redshift_to_df(configs) \
  .option("query",f"""
    SELECT country_alpha2, platform_subset, customer_engagement, cal_date, units
    FROM "prod"."ib"
    WHERE 1=1
       AND version = '{version}'
    """) \
  .load()
ib.createOrReplaceTempView("ib")

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

ib_m10 = """
with step1 as (SELECT ib.country_alpha2
       , ib.platform_subset
       , ib.customer_engagement
       , ib.cal_date
       , ib.units
       , cc.market10
FROM ib
LEFT JOIN country_info cc
    ON ib.country_alpha2=cc.country_alpha2
), step2 as 
(SELECT market10
       , platform_subset
       , customer_engagement
       , cal_date
       , SUM(units) as units
       FROM step1
       GROUP BY 
       market10
       , platform_subset
       , customer_engagement
       , cal_date
       
)
SELECT * FROM step2
"""

ib_m10=spark.sql(ib_m10)
ib_m10.createOrReplaceTempView("ib_m10")

# COMMAND ----------

convert = f"""
with step1 as (
    SELECT us.cal_date
	, us.geography
	, us.platform_subset
	, us.customer_engagement
	, CASE WHEN us.measure = 'HP_SHARE' THEN us.source
        ELSE NULL
        END AS source_s
    , CASE WHEN us.measure like '%USAGE%' THEN us.source
        ELSE NULL
        END AS source_u
	, SUM(CASE WHEN us.measure='USAGE' THEN us.units ELSE 0 END) AS usage
    , SUM(CASE WHEN us.measure='HP_SHARE' THEN us.units ELSE 0 END) AS page_share
    , SUM(CASE WHEN us.measure='COLOR_USAGE' THEN us.units ELSE 0 END) AS usage_c
	, SUM(CASE WHEN us.measure='K_USAGE' THEN us.units ELSE 0 END) AS usage_k
FROM us_table us 
GROUP BY us.cal_date
	, us.geography
	, us.platform_subset
	, us.customer_engagement
    , us.measure
    , us.source
	) , step2 as (
    SELECT 
       u.cal_date
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
      ,MAX(u.source_s) as source_s
      ,MAX(u.source_u) as source_u
      ,SUM(u.usage) AS usage
      ,SUM(u.page_share) AS page_share
      ,SUM(u.usage_c) AS usage_c
	  ,SUM(u.usage_k) AS usage_k
      ,SUM(i.units) as ib
FROM step1 u
LEFT JOIN ib_m10 i
    ON 1=1
    AND u.platform_subset=i.platform_subset
    AND u.customer_engagement=i.customer_engagement
    AND u.geography=i.market10
    AND u.cal_date=i.cal_date
GROUP BY 
       u.cal_date
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
) , step3 as (
    SELECT u.cal_date
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
      ,MAX(u.source_s) as source_s
      ,MAX(u.source_u) as source_u
      ,SUM(u.usage*coalesce(ib,0)) AS pages
      ,SUM(u.page_share*usage*coalesce(ib,0)) AS hp_pages
      ,SUM(u.usage_c*coalesce(ib,0)) AS color_pages
	  ,SUM(u.usage_k*coalesce(ib,0)) AS black_pages
      ,SUM(u.page_share*usage_k*coalesce(ib,0)) AS hp_k_pages
      ,SUM(u.page_share*usage_c*coalesce(ib,0)) AS hp_c_pages
      ,SUM(coalesce(ib,0)) as ib
    FROM step2 u
    GROUP BY 
       u.cal_date
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
) ,step4 as (
    SELECT u.cal_date
      ,u.platform_subset
      ,u.customer_engagement
      ,MAX(u.source_u) as source_u
      ,MAX(u.source_s) as source_s
      ,SUM(u.pages) AS pages
      ,SUM(u.hp_pages) AS hp_pages
      ,SUM(u.color_pages) AS color_pages
	  ,SUM(u.black_pages) AS black_pages
      ,SUM(u.hp_k_pages) AS hp_k_pages
	  ,SUM(u.hp_c_pages) AS hp_c_pages
      ,SUM(u.ib) as ib
      ,u.geography
    FROM step3 u
    GROUP BY 
       u.cal_date
      ,u.platform_subset
      ,u.customer_engagement
      ,u.geography
), step5 as (
    SELECT
      h4.cal_date
	, h4.geography
	, h4.source_u
	, h4.source_s
	, h4.platform_subset
	, h4.customer_engagement
	, h4.pages/nullif(ib,0) AS usage
	, h4.hp_pages/nullif(pages,0) AS page_share
	, h4.color_pages/nullif(ib,0) AS usage_c
	, h4.black_pages/nullif(ib,0) AS usage_k
    , h4.pages as total_pages
	, h4.hp_pages
	, h4.color_pages as total_color_pages
	, h4.black_pages as total_k_pages
    , h4.hp_c_pages as hp_color_pages
    , h4.hp_k_pages
    , h4.ib
    
FROM step4 h4

), step6 as (
SELECT cal_date
	, geography
	, platform_subset
	, customer_engagement
	, 'USAGE' as measure
	, usage as units
	, source_u as source
FROM step5
WHERE usage IS NOT NULL
    AND usage > 0
UNION ALL
SELECT cal_date
	, geography
	, platform_subset
	, customer_engagement
	, 'HP_SHARE' as measure
	, page_share as units
	, source_s as source
FROM step5
WHERE page_share IS NOT NULL
    AND page_share > 0
UNION ALL
SELECT cal_date
	, geography
	, platform_subset
	, customer_engagement
	, 'COLOR_USAGE' as measure
	, usage_c as units
	, source_u as source
FROM step5
WHERE usage_c IS NOT NULL
    AND usage_c > 0
UNION ALL
SELECT cal_date
	, geography
	, platform_subset
	, customer_engagement
	, 'K_USAGE' as measure
	, usage_k as units
	, source_u as source
FROM step5
WHERE usage_k IS NOT NULL
    AND usage_k > 0
UNION ALL
SELECT cal_date
	, geography
	, platform_subset
	, customer_engagement
	, 'TOTAL_PAGES' as measure
	, total_pages as units
	, source_u as source
FROM step5
WHERE total_pages IS NOT NULL
    AND total_pages > 0
UNION ALL
SELECT cal_date
	, geography
	, platform_subset
	, customer_engagement
	, 'TOTAL_COLOR_PAGES' as measure
	, total_color_pages as units
	, source_u as source
FROM step5
WHERE total_color_pages IS NOT NULL
    AND total_color_pages > 0
UNION ALL
SELECT cal_date
	, geography
	, platform_subset
	, customer_engagement
	, 'TOTAL_K_PAGES' as measure
	, total_k_pages as units
	, source_u as source
FROM step5
WHERE total_k_pages IS NOT NULL
    AND total_k_pages > 0
UNION ALL
SELECT cal_date
	, geography
	, platform_subset
	, customer_engagement
	, 'HP_PAGES' as measure
	, hp_pages as units
	, source_u as source
FROM step5
WHERE hp_pages IS NOT NULL
    AND hp_pages > 0
UNION ALL
SELECT cal_date
	, geography
	, platform_subset
	, customer_engagement
	, 'HP_K_PAGES' as measure
	, hp_k_pages as units
	, source_u as source
FROM step5
WHERE hp_pages IS NOT NULL
    AND hp_pages > 0
UNION ALL
SELECT cal_date
	, geography
	, platform_subset
	, customer_engagement
	, 'HP_COLOR_PAGES' as measure
	, hp_color_pages as units
	, source_u as source
FROM step5
WHERE hp_pages IS NOT NULL
    AND hp_pages > 0
UNION ALL
SELECT cal_date
	, geography
	, platform_subset
	, customer_engagement
	, 'IB' as measure
	, ib as units
	, 'IB' as source
FROM step5
WHERE hp_pages IS NOT NULL
    AND hp_pages > 0
)
SELECT "USAGE_SHARE" as record
      ,cal_date
      ,"MARKET 10" as geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,measure
      ,units
      ,'{version}' as ib_version
      ,source
      ,CONCAT(CAST(current_date() AS DATE),".1") as version
      ,CAST(current_date() AS DATE) AS load_date
      FROM step6
               
"""
convert=spark.sql(convert)
convert.createOrReplaceTempView("convert")

# COMMAND ----------

display(convert)

# COMMAND ----------

#Look at curves
#NOVELLI PLUS YET1
test1 = """
select *
from convert
where 1=1
    and platform_subset='ALTAMIRA 3000'
    and customer_engagement='TRAD'
    and geography='GREATER ASIA'
    and measure in ('TOTAL_PAGES','HP_PAGES', 'TOTAL_COLOR_PAGES')
    --and measure in ('HP_SHARE')
"""
test1=spark.sql(test1)
test1.createOrReplaceTempView("test1")

test2 = test1.select("*").toPandas()
test2.set_index('cal_date', inplace=True)
test2.groupby(['customer_engagement','measure'])['units'].plot(xlabel='Calendar Date', ylabel='Units',legend=True)


# COMMAND ----------

test2 = """
select customer_engagement, measure, source, count(*) as numsource
from convert
group by customer_engagement, measure,source
"""

test2=spark.sql(test2)
test2.createOrReplaceTempView("test2")

# COMMAND ----------

display(test2)

# COMMAND ----------

#write_df_to_redshift(configs: config(), df: convert, destination: "stage"."usage_share_staging_post_adjust", mode: str = "overwrite")
write_df_to_s3(df=convert, destination=f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/demand", format="parquet", mode="overwrite", upper_strings=True)
