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

# Get ib
ib = read_redshift_to_df(configs) \
  .option("query",f"""
    SELECT country_alpha2, platform_subset, customer_engagement, cal_date, units
    FROM "prod"."ib"
    WHERE 1=1
       AND version = '{ib_version}'
    """) \
  .load()
ib.createOrReplaceTempView("ib")

# COMMAND ----------

# Read in U/S data
us_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/{datestamp}/usage_share_country*")
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
	, SUM(CASE WHEN us.measure='USAGE' AND us.data_source='TELEMETRY' THEN 1
		ELSE 0 END) AS data_source_u
    , SUM(CASE WHEN us.measure='USAGE' AND us.data_source='NPI' THEN 1
		ELSE 0 END) AS data_source_u_n
    , SUM(CASE WHEN us.measure='USAGE' AND us.data_source='MATURE' THEN 1
		ELSE 0 END) AS data_source_u_o
    , SUM(CASE WHEN us.measure='USAGE' AND us.data_source='MODELED' THEN 1
		ELSE 0 END) AS data_source_u_m
    --color usage
	, SUM(CASE WHEN us.measure='COLOR_USAGE'AND us.data_source='TELEMETRY' THEN 1
		ELSE 0 END) AS data_source_c
    , SUM(CASE WHEN us.measure='COLOR_USAGE' AND us.data_source='NPI' THEN 1
		ELSE 0 END) AS data_source_c_n
	, SUM(CASE WHEN us.measure='COLOR_USAGE' AND us.data_source='MATURE' THEN 1
		ELSE 0 END) AS data_source_c_o
	, SUM(CASE WHEN us.measure='COLOR_USAGE' AND us.data_source='MODELED' THEN 1
		ELSE 0 END) AS data_source_c_m
    --black usage
	, SUM(CASE WHEN us.measure='K_USAGE' AND us.data_source='TELEMETRY' THEN 1
		ELSE 0 END) AS data_source_k
    , SUM(CASE WHEN us.measure='K_USAGE' AND us.data_source='NPI' THEN 1
		ELSE 0 END) AS data_source_k_n
    , SUM(CASE WHEN us.measure='K_USAGE' AND us.data_source='MATURE' THEN 1
		ELSE 0 END) AS data_source_k_o
    , SUM(CASE WHEN us.measure='K_USAGE' AND us.data_source='MODELED' THEN 1
		ELSE 0 END) AS data_source_k_m
    --share     
	, SUM(CASE WHEN us.measure='USAGE' AND us.data_source='TELEMETRY' AND us.customer_engagement='I-INK' THEN 1
        WHEN us.measure='HP_SHARE' AND us.data_source='TELEMETRY' AND us.customer_engagement !='I-INK' THEN 1
		ELSE 0 END) AS data_source_s
    , SUM(CASE WHEN us.measure='USAGE' AND us.data_source='NPI' AND us.customer_engagement='I-INK' THEN 1
        WHEN us.measure='HP_SHARE' AND us.data_source='NPI' AND us.customer_engagement !='I-INK' THEN 1
		ELSE 0 END) AS data_source_s_n
    , SUM(CASE WHEN us.measure='USAGE' AND us.data_source='MATURE' AND us.customer_engagement='I-INK' THEN 1
        WHEN us.measure='HP_SHARE' AND us.data_source='MATURE' AND us.customer_engagement !='I-INK' THEN 1
		ELSE 0 END) AS data_source_s_o
    , SUM(CASE WHEN us.measure='USAGE' AND us.data_source='MODELED' AND us.customer_engagement='I-INK' THEN 1
        WHEN us.measure='HP_SHARE' AND us.data_source='MODELED' AND us.customer_engagement !='I-INK' THEN 1
		ELSE 0 END) AS data_source_s_m
	, SUM(CASE WHEN us.measure='USAGE' THEN us.units ELSE 0 END) AS usage
    , SUM(CASE WHEN us.measure='USAGE' AND us.customer_engagement='I-INK' THEN 1 
               WHEN us.measure='HP_SHARE' AND us.customer_engagement !='I-INK' THEN us.units ELSE 0 END) AS page_share
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
      ,SUM(u.usage*coalesce(ib,0)) AS pages
      ,SUM(u.page_share*usage*coalesce(ib,0)) AS hp_pages
      ,SUM(u.usage_c*coalesce(ib,0)) AS color_pages
	  ,SUM(u.usage_k*coalesce(ib,0)) AS black_pages
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
      ,SUM(u.pages) AS pages
      ,SUM(u.hp_pages) AS hp_pages
      ,SUM(u.color_pages) AS color_pages
	  ,SUM(u.black_pages) AS black_pages
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
	, h4.platform_subset
	, h4.customer_engagement
	, h4.pages/nullif(ib,0) AS usage
	, h4.hp_pages/nullif(pages,0) AS page_share
	, h4.color_pages/nullif(ib,0) AS usage_c
	, h4.black_pages/nullif(ib,0) AS usage_k
FROM step4 h4

), step6 as (
SELECT cal_date
	, market10
	, platform_subset
	, customer_engagement
	, 'USAGE' as measure
	, usage as units
	, CONCAT(round(data_source_u*100,0),"% T, ",round(data_source_u_n*100,0),"% N, ",round(data_source_u_o*100,0),"% MA, ",round(data_source_u_m*100,0),"% MD") as source
    , round(data_source_u+data_source_u_n+data_source_u_o+data_source_u_m,1) as test_src
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
	, CONCAT(round(data_source_s*100,0),"% T, ",round(data_source_s_n*100,0),"% N, ",round(data_source_s_o*100,0),"% MA, ",round(data_source_s_m*100,0),"% MD") as source
    , round(data_source_s+data_source_s_n+data_source_s_o+data_source_s_m,1) as test_src
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
	, CONCAT(round(data_source_c*100,0),"% T, ",round(data_source_c_n*100,0),"% N, ",round(data_source_c_o*100,0),"% MA, ",round(data_source_c_m*100,0),"% MD") as source
    , round(data_source_c+data_source_c_n+data_source_c_o+data_source_c_m,1) as test_src
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
	, CONCAT(round(data_source_k*100,0),"% T, ",round(data_source_k_n*100,0),"% N, ",round(data_source_k_o*100,0),"% MA, ",round(data_source_k_m*100,0),"% MD") as source
    , round(data_source_k+data_source_k_n+data_source_k_o+data_source_k_m,1) as test_src
FROM step5
WHERE usage_k IS NOT NULL
    AND usage_k > 0

)
, final_step as (SELECT "USAGE_SHARE" as record
      ,cal_date
      ,"MARKET 10" as geography_grain
      ,market10 as geography
      ,platform_subset
      ,customer_engagement
      ,measure
      ,units
      ,'{ib_version}' as ib_version
      ,source
      ,test_src
      ,CONCAT(CAST(current_date() AS DATE),".1") as version
      ,CAST(current_date() AS DATE) AS load_date
      FROM step6)
 SELECT * FROM final_step
               
"""
convert=spark.sql(convert)
convert.createOrReplaceTempView("convert")

# COMMAND ----------

s3_destination = f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/{datestamp}/us_market10"
print("output file name: " + s3_destination)

write_df_to_s3(df=convert, destination=s3_destination, format="parquet", mode="overwrite", upper_strings=True)
