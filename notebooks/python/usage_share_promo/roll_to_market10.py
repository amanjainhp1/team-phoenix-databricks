# Databricks notebook source
# MAGIC %md
# MAGIC # Usage Share Refactor Step 05
# MAGIC - Roll Up to Market 10

# COMMAND ----------

# for interactive sessions, define widgets
dbutils.widgets.text("datestamp", "")
dbutils.widgets.text("ib_version", "")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

datestamp = dbutils.jobs.taskValues.get(taskKey = "npi", key = "datestamp") if dbutils.widgets.get("datestamp") == "" else dbutils.widgets.get("datestamp")
ib_version = dbutils.jobs.taskValues.get(taskKey = "npi", key = "ib_version") if dbutils.widgets.get("ib_version") == "" else dbutils.widgets.get("ib_version")

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

# Read in U/S data
# retrieve demand record names
demand_version = read_redshift_to_df(configs) \
    .option('query', f"SELECT max(version) as version FROM prod.version WHERE record = 'DEMAND'") \
    .load() \
    .rdd.flatMap(lambda x: x).collect()[0]


us_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}spectrum/demand/{demand_version}/*")
us_table.createOrReplaceTempView("us_table")

# COMMAND ----------

#roll up to market10 -- changing source to % of source, need to find way to roll up other notes (forecast process note, proxy used)
convert = f"""
with step1 as (
    SELECT us.cal_date
	, us.geography
	, us.platform_subset
	, us.customer_engagement
    --usage
	, SUM(CASE WHEN us.measure='USAGE' AND us.source='TELEMETRY' THEN 1
		ELSE 0 END) AS data_source_u
    , SUM(CASE WHEN us.measure='USAGE' AND us.source='NPI' THEN 1
		ELSE 0 END) AS data_source_u_n
    , SUM(CASE WHEN us.measure='USAGE' AND us.source='MATURE' THEN 1
		ELSE 0 END) AS data_source_u_o
    , SUM(CASE WHEN us.measure='USAGE' AND us.source='MODELED' THEN 1
		ELSE 0 END) AS data_source_u_m
    , SUM(CASE WHEN us.measure='USAGE' AND us.source='WORKING-FORECAST OVERRIDE' THEN 1
		ELSE 0 END) AS data_source_u_w
    , SUM(CASE WHEN us.measure='USAGE' AND us.source='EPA-DRIVERS OVERRIDE' THEN 1
		ELSE 0 END) AS data_source_u_e	
    --color usage
	, SUM(CASE WHEN us.measure='COLOR_USAGE'AND us.source='TELEMETRY' THEN 1
		ELSE 0 END) AS data_source_c
    , SUM(CASE WHEN us.measure='COLOR_USAGE' AND us.source='NPI' THEN 1
		ELSE 0 END) AS data_source_c_n
    , SUM(CASE WHEN us.measure='COLOR_USAGE' AND us.source='MATURE' THEN 1
		ELSE 0 END) AS data_source_c_o
    , SUM(CASE WHEN us.measure='COLOR_USAGE' AND us.source='MODELED' THEN 1
		ELSE 0 END) AS data_source_c_m
    , SUM(CASE WHEN us.measure='COLOR_USAGE' AND us.source='WORKING-FORECAST OVERRIDE' THEN 1
		ELSE 0 END) AS data_source_c_w
    , SUM(CASE WHEN us.measure='COLOR_USAGE' AND us.source='EPA-DRIVERS OVERRIDE' THEN 1
		ELSE 0 END) AS data_source_c_e	
    --black usage
	, SUM(CASE WHEN us.measure='K_USAGE' AND us.source='TELEMETRY' THEN 1
		ELSE 0 END) AS data_source_k
    , SUM(CASE WHEN us.measure='K_USAGE' AND us.source='NPI' THEN 1
		ELSE 0 END) AS data_source_k_n
    , SUM(CASE WHEN us.measure='K_USAGE' AND us.source='MATURE' THEN 1
		ELSE 0 END) AS data_source_k_o
    , SUM(CASE WHEN us.measure='K_USAGE' AND us.source='MODELED' THEN 1
		ELSE 0 END) AS data_source_k_m
    , SUM(CASE WHEN us.measure='K_USAGE' AND us.source='WORKING-FORECAST OVERRIDE' THEN 1
		ELSE 0 END) AS data_source_k_w
    , SUM(CASE WHEN us.measure='K_USAGE' AND us.source='EPA-DRIVERS OVERRIDE' THEN 1
		ELSE 0 END) AS data_source_k_e		
    --share     
	, SUM(CASE WHEN us.measure='USAGE' AND us.source='TELEMETRY' AND us.customer_engagement='I-INK' THEN 1
        WHEN us.measure='HP_SHARE' AND us.source='TELEMETRY' AND us.customer_engagement !='I-INK' THEN 1
		ELSE 0 END) AS data_source_s
    , SUM(CASE WHEN us.measure='USAGE' AND us.source='NPI' AND us.customer_engagement='I-INK' THEN 1
        WHEN us.measure='HP_SHARE' AND us.source='NPI' AND us.customer_engagement !='I-INK' THEN 1
		ELSE 0 END) AS data_source_s_n
    , SUM(CASE WHEN us.measure='USAGE' AND us.source='MATURE' AND us.customer_engagement='I-INK' THEN 1
        WHEN us.measure='HP_SHARE' AND us.source='MATURE' AND us.customer_engagement !='I-INK' THEN 1
		ELSE 0 END) AS data_source_s_o
    , SUM(CASE WHEN us.measure='USAGE' AND us.source='MODELED' AND us.customer_engagement='I-INK' THEN 1
        WHEN us.measure='HP_SHARE' AND us.source='MODELED' AND us.customer_engagement !='I-INK' THEN 1
		ELSE 0 END) AS data_source_s_m
    , SUM(CASE WHEN us.measure='USAGE' AND us.source='WORKING-FORECAST OVERRIDE' AND us.customer_engagement='I-INK' THEN 1
        WHEN us.measure='HP_SHARE' AND us.source='WORKING-FORECAST OVERRIDE' AND us.customer_engagement !='I-INK' THEN 1
		ELSE 0 END) AS data_source_s_w
    , SUM(CASE WHEN us.measure='USAGE' AND us.source='EPA-DRIVERS OVERRIDE' AND us.customer_engagement='I-INK' THEN 1
        WHEN us.measure='HP_SHARE' AND us.source='EPA-DRIVERS OVERRIDE' AND us.customer_engagement !='I-INK' THEN 1
		ELSE 0 END) AS data_source_s_e
		
    , SUM(CASE WHEN us.measure='USAGE' THEN us.units ELSE 0 END) AS usage
    , SUM(CASE WHEN us.measure='USAGE' AND us.customer_engagement='I-INK' THEN 1 
               WHEN us.measure='HP_SHARE' AND us.customer_engagement !='I-INK' THEN us.units ELSE 0 END) AS page_share
    , SUM(CASE WHEN us.measure='COLOR_USAGE' THEN us.units ELSE 0 END) AS usage_c
    , SUM(CASE WHEN us.measure='K_USAGE' THEN us.units ELSE 0 END) AS usage_k
    , SUM(CASE WHEN us.measure='TOTAL_PAGES' THEN us.units ELSE 0 END) as total_pages
    , SUM(CASE WHEN us.measure='TOTAL_COLOR_PAGES' THEN us.units ELSE 0 END) as total_color_pages
    , SUM(CASE WHEN us.measure='TOTAL_K_PAGES' THEN us.units ELSE 0 END) as total_k_pages
    , SUM(CASE WHEN us.measure='HP_PAGES' THEN us.units ELSE 0 END) as hp_pages
    , SUM(CASE WHEN us.measure='NON_HP_PAGES' THEN us.units ELSE 0 END) as non_hp_pages
    , SUM(CASE WHEN us.measure='NON_HP_K_PAGES' THEN us.units ELSE 0 END) as non_hp_k_pages
    , SUM(CASE WHEN us.measure='NON_HP_COLOR_PAGES' THEN us.units ELSE 0 END) as non_hp_color_pages
    , SUM(CASE WHEN us.measure='HP_K_PAGES' THEN us.units ELSE 0 END) as hp_k_pages
    , SUM(CASE WHEN us.measure='HP_COLOR_PAGES' THEN us.units ELSE 0 END) as hp_color_pages
    , SUM(CASE WHEN us.measure='IB' THEN us.units ELSE 0 END) as ib
    
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
      ,SUM(u.data_source_u_n) as data_source_u_n
      ,SUM(u.data_source_c_n) as data_source_c_n
      ,SUM(u.data_source_k_n) as data_source_k_n
      ,SUM(u.data_source_s_n) as data_source_s_n
      ,SUM(u.data_source_u_o) as data_source_u_o
      ,SUM(u.data_source_c_o) as data_source_c_o
      ,SUM(u.data_source_k_o) as data_source_k_o
      ,SUM(u.data_source_s_o) as data_source_s_o
      ,SUM(u.data_source_u_m) as data_source_u_m
      ,SUM(u.data_source_c_m) as data_source_c_m
      ,SUM(u.data_source_k_m) as data_source_k_m
      ,SUM(u.data_source_s_m) as data_source_s_m
      ,SUM(u.data_source_u_w) as data_source_u_w
      ,SUM(u.data_source_c_w) as data_source_c_w
      ,SUM(u.data_source_k_w) as data_source_k_w
      ,SUM(u.data_source_s_w) as data_source_s_w
      ,SUM(u.data_source_u_e) as data_source_u_e
      ,SUM(u.data_source_c_e) as data_source_c_e
      ,SUM(u.data_source_k_e) as data_source_k_e
      ,SUM(u.data_source_s_e) as data_source_s_e
      ,SUM(u.usage) AS usage
      ,SUM(u.page_share) AS page_share
      ,SUM(u.usage_c) AS usage_c
      ,SUM(u.usage_k) AS usage_k
      ,SUM(u.total_pages) AS total_pages
      ,SUM(u.total_color_pages) AS total_color_pages
      ,SUM(u.total_k_pages) AS total_k_pages
      ,SUM(u.hp_pages) AS hp_pages
      ,SUM(u.non_hp_pages) AS non_hp_pages
      ,SUM(u.non_hp_color_pages) AS non_hp_color_pages
      ,SUM(u.non_hp_k_pages) AS non_hp_k_pages
      ,SUM(u.hp_k_pages) AS hp_k_pages
      ,SUM(u.hp_color_pages) AS hp_color_pages
      ,SUM(u.ib) as ib
      ,c.market10
FROM step1 u
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
      ,SUM(u.data_source_u*coalesce(ib,0)) as data_source_u
      ,SUM(u.data_source_c*coalesce(ib,0)) as data_source_c
      ,SUM(u.data_source_k*coalesce(ib,0)) as data_source_k
      ,SUM(u.data_source_s*coalesce(ib,0)) as data_source_s
      ,SUM(u.data_source_u_n*coalesce(ib,0)) as data_source_u_n
      ,SUM(u.data_source_c_n*coalesce(ib,0)) as data_source_c_n
      ,SUM(u.data_source_k_n*coalesce(ib,0)) as data_source_k_n
      ,SUM(u.data_source_s_n*coalesce(ib,0)) as data_source_s_n
      ,SUM(u.data_source_u_o*coalesce(ib,0)) as data_source_u_o
      ,SUM(u.data_source_c_o*coalesce(ib,0)) as data_source_c_o
      ,SUM(u.data_source_k_o*coalesce(ib,0)) as data_source_k_o
      ,SUM(u.data_source_s_o*coalesce(ib,0)) as data_source_s_o
      ,SUM(u.data_source_u_m*coalesce(ib,0)) as data_source_u_m
      ,SUM(u.data_source_c_m*coalesce(ib,0)) as data_source_c_m
      ,SUM(u.data_source_k_m*coalesce(ib,0)) as data_source_k_m
      ,SUM(u.data_source_s_m*coalesce(ib,0)) as data_source_s_m
      ,SUM(u.data_source_u_w*coalesce(ib,0)) as data_source_u_w
      ,SUM(u.data_source_c_w*coalesce(ib,0)) as data_source_c_w
      ,SUM(u.data_source_k_w*coalesce(ib,0)) as data_source_k_w
      ,SUM(u.data_source_s_w*coalesce(ib,0)) as data_source_s_w
      ,SUM(u.data_source_u_e*coalesce(ib,0)) as data_source_u_e
      ,SUM(u.data_source_c_e*coalesce(ib,0)) as data_source_c_e
      ,SUM(u.data_source_k_e*coalesce(ib,0)) as data_source_k_e
      ,SUM(u.data_source_s_e*coalesce(ib,0)) as data_source_s_e
      ,SUM(u.total_pages) AS pages
      ,SUM(u.hp_pages) AS hp_pages
      ,SUM(u.total_color_pages) AS color_pages
      ,SUM(u.total_k_pages) AS black_pages
      ,SUM(u.non_hp_pages) AS non_hp_pages
      ,SUM(u.non_hp_color_pages) AS non_hp_color_pages
      ,SUM(u.non_hp_k_pages) AS non_hp_k_pages
      ,SUM(u.hp_k_pages) AS hp_k_pages
      ,SUM(u.hp_color_pages) AS hp_color_pages
      ,SUM(coalesce(ib,0)) as ib
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
      ,SUM(u.data_source_u) as data_source_u
      ,SUM(u.data_source_c) as data_source_c
      ,SUM(u.data_source_k) as data_source_k
      ,SUM(u.data_source_s) as data_source_s
      ,SUM(u.data_source_u_n) as data_source_u_n
      ,SUM(u.data_source_c_n) as data_source_c_n
      ,SUM(u.data_source_k_n) as data_source_k_n
      ,SUM(u.data_source_s_n) as data_source_s_n
      ,SUM(u.data_source_u_o) as data_source_u_o
      ,SUM(u.data_source_c_o) as data_source_c_o
      ,SUM(u.data_source_k_o) as data_source_k_o
      ,SUM(u.data_source_s_o) as data_source_s_o
      ,SUM(u.data_source_u_m) as data_source_u_m
      ,SUM(u.data_source_c_m) as data_source_c_m
      ,SUM(u.data_source_k_m) as data_source_k_m
      ,SUM(u.data_source_s_m) as data_source_s_m
      ,SUM(u.data_source_u_w) as data_source_u_w
      ,SUM(u.data_source_c_w) as data_source_c_w
      ,SUM(u.data_source_k_w) as data_source_k_w
      ,SUM(u.data_source_s_w) as data_source_s_w
      ,SUM(u.data_source_u_e) as data_source_u_e
      ,SUM(u.data_source_c_e) as data_source_c_e
      ,SUM(u.data_source_k_e) as data_source_k_e
      ,SUM(u.data_source_s_e) as data_source_s_e
      ,SUM(u.pages) AS pages
      ,SUM(u.hp_pages) AS hp_pages
      ,SUM(u.color_pages) AS color_pages
      ,SUM(u.black_pages) AS black_pages
      ,SUM(u.non_hp_pages) AS non_hp_pages
      ,SUM(u.non_hp_color_pages) AS non_hp_color_pages
      ,SUM(u.non_hp_k_pages) AS non_hp_k_pages
      ,SUM(u.hp_k_pages) AS hp_k_pages
      ,SUM(u.hp_color_pages) AS hp_color_pages
      ,SUM(u.ib) as ib
      ,u.market10
    FROM step3 u
    GROUP BY 
       u.cal_date
      ,u.platform_subset
      ,u.customer_engagement
      ,u.market10
), step5 as (
    SELECT
      h4.cal_date
	, h4.market10
	, h4.data_source_u/nullif(ib,0) as data_source_u
	, h4.data_source_s/nullif(ib,0) as data_source_s
	, h4.data_source_c/nullif(ib,0) as data_source_c
	, h4.data_source_k/nullif(ib,0) as data_source_k
    , h4.data_source_u_n/nullif(ib,0) as data_source_u_n
	, h4.data_source_s_n/nullif(ib,0) as data_source_s_n
	, h4.data_source_c_n/nullif(ib,0) as data_source_c_n
	, h4.data_source_k_n/nullif(ib,0) as data_source_k_n
    , h4.data_source_u_o/nullif(ib,0) as data_source_u_o
	, h4.data_source_s_o/nullif(ib,0) as data_source_s_o
	, h4.data_source_c_o/nullif(ib,0) as data_source_c_o
	, h4.data_source_k_o/nullif(ib,0) as data_source_k_o
    , h4.data_source_u_m/nullif(ib,0) as data_source_u_m
	, h4.data_source_s_m/nullif(ib,0) as data_source_s_m
	, h4.data_source_c_m/nullif(ib,0) as data_source_c_m
	, h4.data_source_k_m/nullif(ib,0) as data_source_k_m
    , h4.data_source_u_w/nullif(ib,0) as data_source_u_w
	, h4.data_source_s_w/nullif(ib,0) as data_source_s_w
	, h4.data_source_c_w/nullif(ib,0) as data_source_c_w
	, h4.data_source_k_w/nullif(ib,0) as data_source_k_w
    , h4.data_source_u_e/nullif(ib,0) as data_source_u_e
	, h4.data_source_s_e/nullif(ib,0) as data_source_s_e
	, h4.data_source_c_e/nullif(ib,0) as data_source_c_e
	, h4.data_source_k_e/nullif(ib,0) as data_source_k_e
	, h4.platform_subset
	, h4.customer_engagement
	, h4.pages/nullif(ib,0) AS usage
	, h4.hp_pages/nullif(pages,0) AS page_share
	, h4.color_pages/nullif(ib,0) AS usage_c
	, h4.black_pages/nullif(ib,0) AS usage_k
	, h4.pages total_pages
      	, h4.hp_pages AS hp_pages
      	, h4.color_pages AS total_color_pages
        , h4.black_pages AS total_k_pages
      	, h4.non_hp_pages AS non_hp_pages
      	, h4.non_hp_color_pages AS non_hp_color_pages
      	, h4.non_hp_k_pages AS non_hp_k_pages
      	, h4.hp_k_pages AS hp_k_pages
      	, h4.hp_color_pages AS hp_color_pages
        , h4.ib
FROM step4 h4

), step6 as (
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'USAGE' as measure
	, usage as units
	, CONCAT(round(data_source_u*100,0),"% T, ",round(data_source_u_n*100,0),"% N, ",round(data_source_u_o*100,0),"% MA, ",round(data_source_u_m*100,0),"% MD",round(data_source_u_w*100,0),"% WF",round(data_source_u_e*100,0),"% EP") as source
FROM step5
WHERE usage IS NOT NULL
    AND usage > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'HP_SHARE' as measure
	, page_share as units
	, CONCAT(round(data_source_s*100,0),"% T, ",round(data_source_s_n*100,0),"% N, ",round(data_source_s_o*100,0),"% MA, ",round(data_source_s_m*100,0),"% MD",round(data_source_s_w*100,0),"% WF",round(data_source_s_e*100,0),"% EP") as source
FROM step5
WHERE page_share IS NOT NULL
    AND page_share > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'COLOR_USAGE' as measure
	, usage_c as units
	, CONCAT(round(data_source_c*100,0),"% T, ",round(data_source_c_n*100,0),"% N, ",round(data_source_c_o*100,0),"% MA, ",round(data_source_c_m*100,0),"% MD",round(data_source_c_w*100,0),"% WF",round(data_source_c_e*100,0),"% EP") as source
FROM step5
WHERE usage_c IS NOT NULL
    AND usage_c > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'K_USAGE' as measure
	, usage_k as units
	, CONCAT(round(data_source_k*100,0),"% T, ",round(data_source_k_n*100,0),"% N, ",round(data_source_k_o*100,0),"% MA, ",round(data_source_k_m*100,0),"% MD",round(data_source_k_w*100,0),"% WF",round(data_source_k_e*100,0),"% EP") as source
FROM step5
WHERE usage_k IS NOT NULL
    AND usage_k > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'TOTAL_PAGES' as measure
	, total_pages as units
	, CONCAT(round(data_source_u*100,0),"% T, ",round(data_source_u_n*100,0),"% N, ",round(data_source_u_o*100,0),"% MA, ",round(data_source_u_m*100,0),"% MD",round(data_source_k_w*100,0),"% WF",round(data_source_k_e*100,0),"% EP") as source
FROM step5
WHERE total_pages IS NOT NULL
    AND total_pages > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'TOTAL_COLOR_PAGES' as measure
	, total_color_pages as units
	, CONCAT(round(data_source_u*100,0),"% T, ",round(data_source_u_n*100,0),"% N, ",round(data_source_u_o*100,0),"% MA, ",round(data_source_u_m*100,0),"% MD",round(data_source_k_w*100,0),"% WF",round(data_source_k_e*100,0),"% EP") as source
FROM step5
WHERE total_color_pages IS NOT NULL
    AND total_color_pages > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'TOTAL_K_PAGES' as measure
	, total_k_pages as units
	, CONCAT(round(data_source_u*100,0),"% T, ",round(data_source_u_n*100,0),"% N, ",round(data_source_u_o*100,0),"% MA, ",round(data_source_u_m*100,0),"% MD",round(data_source_k_w*100,0),"% WF",round(data_source_k_e*100,0),"% EP") as source
FROM step5
WHERE total_k_pages IS NOT NULL
    AND total_k_pages > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'HP_PAGES' as measure
	, hp_pages as units
	, CONCAT(round(data_source_u*100,0),"% T, ",round(data_source_u_n*100,0),"% N, ",round(data_source_u_o*100,0),"% MA, ",round(data_source_u_m*100,0),"% MD",round(data_source_k_w*100,0),"% WF",round(data_source_k_e*100,0),"% EP") as source
FROM step5
WHERE hp_pages IS NOT NULL
    AND hp_pages > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'NON_HP_PAGES' as measure
	, non_hp_pages as units
	, CONCAT(round(data_source_u*100,0),"% T, ",round(data_source_u_n*100,0),"% N, ",round(data_source_u_o*100,0),"% MA, ",round(data_source_u_m*100,0),"% MD",round(data_source_k_w*100,0),"% WF",round(data_source_k_e*100,0),"% EP") as source
FROM step5
WHERE non_hp_pages IS NOT NULL
    AND non_hp_pages > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'NON_HP_K_PAGES' as measure
	, non_hp_k_pages as units
	, CONCAT(round(data_source_u*100,0),"% T, ",round(data_source_u_n*100,0),"% N, ",round(data_source_u_o*100,0),"% MA, ",round(data_source_u_m*100,0),"% MD",round(data_source_k_w*100,0),"% WF",round(data_source_k_e*100,0),"% EP") as source
FROM step5
WHERE non_hp_k_pages IS NOT NULL
    AND non_hp_k_pages > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'NON_HP_COLOR_PAGES' as measure
	, non_hp_color_pages as units
	, CONCAT(round(data_source_u*100,0),"% T, ",round(data_source_u_n*100,0),"% N, ",round(data_source_u_o*100,0),"% MA, ",round(data_source_u_m*100,0),"% MD",round(data_source_k_w*100,0),"% WF",round(data_source_k_e*100,0),"% EP") as source
FROM step5
WHERE non_hp_color_pages IS NOT NULL
    AND non_hp_color_pages > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'HP_K_PAGES' as measure
	, hp_k_pages as units
	, CONCAT(round(data_source_u*100,0),"% T, ",round(data_source_u_n*100,0),"% N, ",round(data_source_u_o*100,0),"% MA, ",round(data_source_u_m*100,0),"% MD",round(data_source_k_w*100,0),"% WF",round(data_source_k_e*100,0),"% EP") as source
FROM step5
WHERE hp_k_pages IS NOT NULL
    AND hp_k_pages > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'HP_COLOR_PAGES' as measure
	, hp_color_pages as units
	, CONCAT(round(data_source_u*100,0),"% T, ",round(data_source_u_n*100,0),"% N, ",round(data_source_u_o*100,0),"% MA, ",round(data_source_u_m*100,0),"% MD",round(data_source_k_w*100,0),"% WF",round(data_source_k_e*100,0),"% EP") as source
FROM step5
WHERE hp_color_pages IS NOT NULL
    AND hp_color_pages > 0
UNION ALL
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'IB' as measure
	, ib as units
	, 'IB' as source
FROM step5
WHERE ib IS NOT NULL
)
, final_step as (SELECT "USAGE_SHARE" as record
      ,cal_date
      ,"MARKET10" as geography_grain
      ,market10 as geography
      ,platform_subset
      ,customer_engagement
      ,measure
      ,units
      ,'{ib_version}' as ib_version
      ,source
      ,CONCAT(CAST(current_date() AS DATE),".1") as version
      ,CAST(current_date() AS DATE) AS load_date
      FROM step6)
 SELECT * FROM final_step
               
"""
convert=spark.sql(convert)
convert.createOrReplaceTempView("convert")

# COMMAND ----------

from datetime import date

# retrieve current date
cur_date = date.today().strftime("%Y.%m.%d")

#execute stored procedure to create new version and load date
record = 'USAGE_SHARE'
source_name = f'{record} - {cur_date}'
max_version_info = call_redshift_addversion_sproc(configs=configs, record=record, source_name=source_name)

max_version = max_version_info[0]
max_load_date = max_version_info[1]

# retrieve ink and toner record names
demand_source_name = read_redshift_to_df(configs) \
    .option('query', f"SELECT source_name FROM prod.version WHERE record = 'DEMAND' AND version = '{demand_version}'") \
    .load() \
    .rdd.flatMap(lambda x: x).collect()[0]

# insert records into scenario table to link usage_share back to underlying CUPSM datasets
insert_query = f"""
INSERT INTO prod.scenario VALUES
('{source_name}', '{demand_source_name}', '{demand_version}' ,'{max_load_date}');
"""
submit_remote_query(configs, insert_query)

update_version = f"""
       SELECT record
      ,cal_date
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,measure
      ,units
      ,ib_version
      ,source
      ,'{max_version}' as version
      ,'{max_load_date}' AS load_date
      FROM convert 
      """

update_version=spark.sql(update_version)
update_version=update_version.withColumn("load_date", update_version.load_date.cast('timestamp'))
update_version.createOrReplaceTempView("update_version")

s3_destination = f"{constants['S3_BASE_BUCKET'][stack]}spectrum/usage_share/{max_version}"
print("output file name: " + s3_destination)

write_df_to_s3(df=update_version, destination=s3_destination, format="parquet", mode="overwrite", upper_strings=True)


if dbutils.widgets.get("writeout").upper() == "TRUE":
    write_df_to_redshift(configs=configs, df=update_version, destination="prod.usage_share", mode="overwrite")
