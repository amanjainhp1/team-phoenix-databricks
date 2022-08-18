# Databricks notebook source
# MAGIC %md
# MAGIC # Usage Share Refactor Step 06
# MAGIC - Add Overrides data

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
us_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/us_market10")
us_table.createOrReplaceTempView("us_table")

# COMMAND ----------

# MAGIC %md
# MAGIC # Working Forecast

# COMMAND ----------

#read in override data
#override_in = read_redshift_to_df(configs) \
  #.option("query","""
  #  SELECT user_name, geography_grain, geography, platform_subset, customer_engagement, measure, min_sys_date,month_num, value, load_date
  #  FROM "prod"."working_forecast_usage_share"
  #  WHERE 1=1
  #  AND user_name != 'BRENTT'
  #  """) \
 # .load()
#override_in.createOrReplaceTempView("override_in")

override_in = read_sql_server_to_df(configs) \
  .option("query","""
    SELECT UPPER(user_name) as user_name, upper(geography_grain) as geography_grain, upper(geography) as geography, upper(platform_subset) as platform_subset
    , upper(customer_engagement) as customer_engagement, upper(measure) as measure, min_sys_date,month_num, value, load_date
    FROM ie2_landing.dbo.scenario_usage_share_landing
    WHERE 1=1 
        AND user_name != 'BRENTT'
        AND upper(upload_type) = 'WORKING-FORECAST'
    """) \
  .load()
override_in.createOrReplaceTempView("override_in")

# COMMAND ----------

#read in override data
override_in2 = read_redshift_to_df(configs) \
  .option("query","""
    SELECT user_name, geography_grain, geography, platform_subset, customer_engagement, measure, min_sys_date,month_num, value, load_date
    FROM "prod"."epa_drivers_usage_share"
    """) \
  .load()
override_in2.createOrReplaceTempView("override_in2")

# COMMAND ----------

override_in=spark.sql("""select
        CASE WHEN user_name = 'GRETCHENE' THEN 'GRETCHENB'
            ELSE user_name
            END AS user_name
        , geography_grain
        , geography
        , platform_subset
        , customer_engagement
        , measure
        , min_sys_date
        , month_num
        , value
        , load_date
from override_in """)
override_in.createOrReplaceTempView("override_in")

# COMMAND ----------

display(override_in)

# COMMAND ----------

override_in_gp = """
    select user_name, max(load_date) as max_load_date
    from override_in
    group by user_name
"""

override_in_gp=spark.sql(override_in_gp)
override_in_gp.createOrReplaceTempView("override_in_gp")

# COMMAND ----------

display(override_in_gp)

# COMMAND ----------

override_table = """
    with step1 as (SELECT concat(user_name,max_load_date) as grp 
    from override_in_gp
    ),
    step2 as (
    select *, concat(user_name,load_date) as grp
    from override_in
    )
    select  user_name
        , geography_grain
        , geography
        , platform_subset
        , customer_engagement
        , measure
        , min_sys_date
        , month_num
        , value
        , load_date
    from step2
    where grp in (select grp from step1)
"""

override_table=spark.sql(override_table)
override_table.createOrReplaceTempView("override_table")

# COMMAND ----------

display(override_table)

# COMMAND ----------

display(override_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Region 5 to Market10

# COMMAND ----------

override_table_r5=spark.sql("""select * from override_table where geography_grain ='REGION_5' """)
override_table_r5.createOrReplaceTempView("override_table_r5")

override_table_m10=spark.sql("""select * from override_table where geography_grain ='MARKET10' """)
override_table_m10.createOrReplaceTempView("override_table_m10")

# COMMAND ----------

country_info = read_redshift_to_df(configs) \
  .option("query","""
    SELECT distinct region_5, market10
    FROM "mdm"."iso_country_code_xref"
    """) \
  .load()
country_info.createOrReplaceTempView("country_info")

r5_to_m10 = f"""
with ap_jp_combos as (
    SELECT DISTINCT platform_subset
        , customer_engagement
        , geography
    FROM override_table_r5
    WHERE geography IN ('AP','JP')
),  ap_jp_combos2 as (
    SELECT platform_subset
        , customer_engagement
        , COUNT(*) AS column_count
    FROM ap_jp_combos
    GROUP BY platform_subset
        , customer_engagement
    HAVING COUNT(*) > 1
), ap_jp_combos3 as (
    SELECT platform_subset,customer_engagement, 'JP' as geography, 'DUP' as apjp
    FROM ap_jp_combos2
),
stp1 as (SELECT ot1.user_name
        , ot1.geography_grain
        , ot1.geography
        , cc.market10
        , ot1.platform_subset
        , ot1.customer_engagement
        , ot1.measure
        , ot1.min_sys_date
        , ot1.month_num
        , ot1.value
        , ot1.load_date
        , CASE WHEN dup.apjp='DUP' THEN 'DUP'
            ELSE 'KEEP'
            END AS apjpkeep
FROM override_table_r5 ot1
 LEFT JOIN  country_info cc
      ON upper(ot1.geography)=upper(cc.region_5)
 LEFT JOIN ap_jp_combos3 dup
     ON ot1.platform_subset=dup.platform_subset AND ot1.customer_engagement=dup.customer_engagement AND ot1.geography=dup.geography
  ),
stp2 as (SELECT user_name
        , 'MARKET10' as geography_grain
        , market10 as geography
        , platform_subset
        , customer_engagement
        , measure
        , min_sys_date
        , month_num
        , value
        , load_date
FROM stp1 
WHERE apjpkeep = 'KEEP'
  )
  select * from stp2
"""

r5_to_m10=spark.sql(r5_to_m10)
r5_to_m10.createOrReplaceTempView("r5_to_m10")

# COMMAND ----------

override_table_a = """
SELECT * FROM override_table_m10
UNION ALL
SELECT * FROM r5_to_m10
"""

override_table_a=spark.sql(override_table_a)
override_table_a.createOrReplaceTempView("override_table_a")

# COMMAND ----------

display(override_table_a)

# COMMAND ----------

###Overwrite data as pulled from override table, and then adjust total usage after overriding (ink usage=color_usage+k_usage, toner usage=k_usage)

override_table_test1 = """
SELECT platform_subset, customer_engagement, measure, geography, count(*) as numocc
FROM override_table_a
WHERE value=0
group by platform_subset, customer_engagement, measure, geography
"""

override_table_test1=spark.sql(override_table_test1)
override_table_test1.createOrReplaceTempView("override_table_test1")

# COMMAND ----------

display(override_table_test1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change to Months

# COMMAND ----------

override_table_b = """

--create dates from min_sys_date and month_num
SELECT  'overrides' as record
      ,add_months(min_sys_date, month_num) as cal_date
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,measure
      ,value as units
      ,'overwrite' as ib_version
      ,'WORKING-FORECAST OVERRIDE' as source
      ,CAST(current_date() AS DATE) AS load_date
FROM override_table_a
"""

override_table_b=spark.sql(override_table_b)
override_table_b=override_table_b.distinct()
override_table_b.createOrReplaceTempView("override_table_b")

# COMMAND ----------

# MAGIC %md
# MAGIC # EPA Overrides

# COMMAND ----------

#read in override data
#override_in2 = read_redshift_to_df(configs) \
#  .option("query","""
#    SELECT user_name, geography_grain, geography, platform_subset, customer_engagement, measure, min_sys_date,month_num, value, load_date
#    FROM "prod"."epa_drivers_usage_share"
#    """) \
#  .load()
#override_in2.createOrReplaceTempView("override_in2")

override_in2 = read_sql_server_to_df(configs) \
  .option("query","""
    SELECT UPPER(user_name) as user_name, upper(geography_grain) as geography_grain, upper(geography) as geography, upper(platform_subset) as platform_subset
    , upper(customer_engagement) as customer_engagement, upper(measure) as measure, min_sys_date,month_num, value, load_date
    FROM ie2_landing.dbo.scenario_usage_share_landing
    WHERE 1=1 
        AND upper(user_name) != 'BRENTT'
        AND upper(upload_type) = 'EPA_DRIVERS'
    """) \
  .load()
override_in2.createOrReplaceTempView("override_in2")

# COMMAND ----------

display(override_in2)

# COMMAND ----------

override_table2 = """
    select  user_name
        , geography_grain
        , geography
        , platform_subset
        , customer_engagement
        , measure
        , min_sys_date
        , month_num
        , value
        , load_date
    from override_in2
    where load_date = (select max(load_date) from override_in2)
"""

override_table2=spark.sql(override_table2)
override_table2.createOrReplaceTempView("override_table2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Region 5 to Market10

# COMMAND ----------

override_table2_r5=spark.sql("""select * from override_table2 where geography_grain ='REGION_5' """)
override_table2_r5.createOrReplaceTempView("override_table2_r5")

override_table_m102=spark.sql("""select * from override_table2 where geography_grain ='MARKET10' """)
override_table_m102.createOrReplaceTempView("override_table2_m10")

# COMMAND ----------

r5_to_m10_2 = f"""
with ap_jp_combos as (
    SELECT DISTINCT platform_subset
        , customer_engagement
        , geography
    FROM override_table2_r5
    WHERE geography IN ('AP','JP')
),  ap_jp_combos2 as (
    SELECT platform_subset
        , customer_engagement
        , COUNT(*) AS column_count
    FROM ap_jp_combos
    GROUP BY platform_subset
        , customer_engagement
    HAVING COUNT(*) > 1
), ap_jp_combos3 as (
    SELECT platform_subset,customer_engagement, 'JP' as geography, 'DUP' as apjp
    FROM ap_jp_combos2
),
stp1 as (SELECT ot1.user_name
        , ot1.geography_grain
        , ot1.geography
        , cc.market10
        , ot1.platform_subset
        , ot1.customer_engagement
        , ot1.measure
        , ot1.min_sys_date
        , ot1.month_num
        , ot1.value
        , ot1.load_date
        , CASE WHEN dup.apjp='DUP' THEN 'DUP'
            ELSE 'KEEP'
            END AS apjpkeep
FROM override_table2_r5 ot1
 LEFT JOIN  country_info cc
      ON upper(ot1.geography)=upper(cc.region_5)
 LEFT JOIN ap_jp_combos3 dup
     ON ot1.platform_subset=dup.platform_subset AND ot1.customer_engagement=dup.customer_engagement AND ot1.geography=dup.geography
  ),
stp2 as (SELECT user_name
        , 'MARKET10' as geography_grain
        , market10 as geography
        , platform_subset
        , customer_engagement
        , measure
        , min_sys_date
        , month_num
        , value
        , load_date
FROM stp1 
WHERE apjpkeep = 'KEEP'
  )
  select * from stp2
"""

r5_to_m10_2=spark.sql(r5_to_m10_2)
r5_to_m10_2.createOrReplaceTempView("r5_to_m10_2")

# COMMAND ----------

override_table2_a = """
SELECT * FROM override_table_m10
UNION ALL
SELECT * FROM r5_to_m10_2
"""

override_table2_a=spark.sql(override_table2_a)
override_table2_a.createOrReplaceTempView("override_table2_a")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change to Months

# COMMAND ----------

override_table2_b = """

--create dates from min_sys_date and month_num
SELECT  'overrides' as record
      ,add_months(min_sys_date, month_num) as cal_date
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,measure
      ,value as units
      ,'overwrite' as ib_version
      ,'EPA-DRIVERS OVERRIDE' as source
      ,CAST(current_date() AS DATE) AS load_date
FROM override_table2_a
"""

override_table2_b=spark.sql(override_table2_b)
override_table2_b=override_table2_b.distinct()
override_table2_b.createOrReplaceTempView("override_table2_b")

# COMMAND ----------

# MAGIC %md
# MAGIC # Combine Data

# COMMAND ----------


    override_table_c ="""
        SELECT * FROM override_table_b
        UNION ALL
        SELECT * FROM override_table2_b
        """


        

    override_table_c2 ="""
        SELECT * FROM override_table_b
        """
    
if override_table2_b.count() > 0: 
        override_table_c=spark.sql(override_table_c)
if override_table2_b.count() == 0:
        override_table_c=spark.sql(override_table_c2)
    
override_table_c=override_table_c.distinct()
override_table_c.createOrReplaceTempView("override_table_c")

# COMMAND ----------

display(override_table2_b)

# COMMAND ----------

display(override_table_c)

# COMMAND ----------

# MAGIC %md
# MAGIC #Update usage & Share

# COMMAND ----------

us_table.update(override_table2_b)

# COMMAND ----------

update_table = """
WITH base as (
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
           ,load_date
           ,CONCAT(platform_subset,customer_engagement,geography,cal_date,measure) AS grp
           FROM us_table
),
new as (
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
           ,load_date
           ,CONCAT(platform_subset,customer_engagement,geography,cal_date,measure) AS grp
           FROM override_table2_b
)
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
           ,load_date
  FROM base WHERE grp not in (select grp from new)
  UNION ALL
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
           ,load_date
   FROM new
    
"""

update_table=spark.sql(update_table)
update_table.createOrReplaceTempView("update_table")

# COMMAND ----------

display(update_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean up Data

# COMMAND ----------


hw_info =  read_redshift_to_df(configs) \
  .option("query",f"""
    SELECT platform_subset, technology
    FROM "mdm"."hardware_xref"
    WHERE 1=1
    """) \
  .load()
hw_info.createOrReplaceTempView("hw_info")


convert = f"""
with step1 as (
    --make wide
    SELECT us.cal_date
    , us.geography_grain
	, us.geography
	, us.platform_subset
	, us.customer_engagement
	, us.source
    , us.ib_version
	, SUM(CASE WHEN us.measure='USAGE' THEN us.units ELSE 0 END) AS usage
    , SUM(CASE WHEN us.measure='HP_SHARE' THEN us.units ELSE 0 END) AS page_share
    , SUM(CASE WHEN us.measure='COLOR_USAGE' THEN us.units ELSE 0 END) AS usage_c
	, SUM(CASE WHEN us.measure='K_USAGE' THEN us.units ELSE 0 END) AS usage_k
FROM us_table us 
GROUP BY us.cal_date
    , us.geography_grain
	, us.geography
	, us.platform_subset
	, us.customer_engagement
    , us.source
    , us.ib_version
	) , step2 as (
    SELECT 
       u.cal_date
      ,u.geography_grain
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
      ,u.source
      ,u.ib_version
      ,SUM(u.usage) AS usage
      ,SUM(u.page_share) AS page_share
      ,SUM(u.usage_c) AS usage_c
	  ,SUM(u.usage_k) AS usage_k
      ,hw.technology
FROM step1 u
    LEFT JOIN hw_info hw
        ON u.platform_subset=hw.platform_subset
GROUP BY 
       u.cal_date
      ,u.geography_grain
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
      ,u.source
      ,u.ib_version
      ,hw.technology
) , step5 as (
    SELECT u.cal_date
      ,u.geography_grain
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
      ,u.source
      ,u.ib_version
      ,CASE WHEN u.source in ('WORKING-FORECAST OVERRIDE','EPA-DRIVERS OVERRIDE') AND u.technology='INK' THEN usage_c+usage_k
            WHEN u.source in ('WORKING-FORECAST OVERRIDE','EPA-DRIVERS OVERRIDE') AND u.technology='TONER' THEN usage_k
            ELSE usage
            END AS usage
      ,u.page_share
      ,u.usage_c
      ,u.usage_k
    FROM step2 u
) 
,step6 as (
SELECT cal_date
	, geography_grain
    , geography
	, platform_subset
	, customer_engagement
	, 'USAGE' as measure
	, usage as units
	, source
    , ib_version
FROM step5
WHERE usage IS NOT NULL
    AND usage > 0
UNION ALL
SELECT cal_date
	, geography_grain
    , geography
	, platform_subset
	, customer_engagement
	, 'HP_SHARE' as measure
	, page_share as units
	, source
    , ib_version
FROM step5
WHERE page_share IS NOT NULL
    AND page_share > 0
UNION ALL
SELECT cal_date
	, geography_grain
    , geography
	, platform_subset
	, customer_engagement
	, 'COLOR_USAGE' as measure
	, usage_c as units
	, source
    , ib_version
FROM step5
WHERE usage_c IS NOT NULL
    AND usage_c > 0
UNION ALL
SELECT cal_date
	,geography_grain
    ,geography
	, platform_subset
	, customer_engagement
	, 'K_USAGE' as measure
	, usage_k as units
	, source
    , ib_version
FROM step5
WHERE usage_k IS NOT NULL
    AND usage_k > 0

)
SELECT "USAGE_SHARE" as record
      ,cal_date
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,measure
      ,units
      ,ib_version
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

#write_df_to_redshift(configs: config(), df: convert, destination: "stage"."usage_share_staging_pre_adjust", mode: str = "overwrite")
write_df_to_s3(df=convert, destination=f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/us_adjusted", format="parquet", mode="overwrite", upper_strings=True)
