# Databricks notebook source
# MAGIC %md
# MAGIC # Usage Share Refactor Step 06
# MAGIC - Add Overrides data

# COMMAND ----------

# for interactive sessions, define widgets
dbutils.widgets.text("datestamp", "")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

datestamp = dbutils.jobs.taskValues.get(taskKey = "npi", key = "datestamp") if dbutils.widgets.get("datestamp") == "" else dbutils.widgets.get("datestamp")

# COMMAND ----------

# Read in Current data
#current_table=need to get step 01 results
us_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/{datestamp}/usage_share_country")
us_table.createOrReplaceTempView("us_table")

# COMMAND ----------

# MAGIC %md
# MAGIC # Working Forecast

# COMMAND ----------

#read in override data
override_in = read_redshift_to_df(configs) \
  .option("query","""
    SELECT upper(user_name) as user_name, upper(geography_grain) as geography_grain, upper(geography) as geography, upper(platform_subset) as platform_subset
    , upper(customer_engagement) as customer_engagement, upper(measure) as measure, min_sys_date,month_num, value, load_date
    FROM scen.working_forecast_usage_share
    WHERE 1=1 
        AND upper(upload_type) = 'WORKING-FORECAST'
    """) \
  .load()
override_in.createOrReplaceTempView("override_in")

# COMMAND ----------

#read in override data
override_in2 = read_redshift_to_df(configs) \
  .option("query","""
    SELECT geography_grain, geography, platform_subset, customer_engagement, measure, min_sys_date,month_num, value, load_date
    FROM "prod"."epa_drivers_usage_share"
    """) \
  .load()
override_in2.createOrReplaceTempView("override_in2")

# COMMAND ----------

override_in=spark.sql("""select
	user_name
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

override_in_gp = """
    select user_name, max(load_date) as max_load_date
    from override_in
    WHERE load_date > '2023-01-06'
    and value > 0
    group by user_name
"""

override_in_gp=spark.sql(override_in_gp)
override_in_gp.createOrReplaceTempView("override_in_gp")

# COMMAND ----------

override_table = """
    with step1 as (SELECT concat(user_name, max_load_date) as grp 
    from override_in_gp
    ),
    step2 as (
    select *, concat(user_name, load_date) as grp
    from override_in
    )
    select  geography_grain
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
    SELECT distinct region_5, market10, country_alpha2
    FROM "mdm"."iso_country_code_xref"
    """) \
  .load()
country_info.createOrReplaceTempView("country_info")

fix_japan = f"""
with step1 as (
    SELECT load_date
    , geography_grain
    , 'JP'  as geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
    , CONCAT(geography, platform_subset, customer_engagement, measure) as grp
    FROM override_table_r5
    WHERE geography = 'AP'
), step2 as (
    SELECT *, CONCAT(geography, platform_subset, customer_engagement, measure) as grp
    FROM override_table_r5
    WHERE geography = 'JP'
), step3 as (SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
FROM step2
UNION 
SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
 FROM step1
 	WHERE grp not in (SELECT DISTINCT grp FROM step2)
  ),
step4 as (SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
FROM override_table_r5
WHERE geography != 'JP'
  )
  select * from step3
  UNION ALL
  select * from step4
"""


r5_to_c = spark.sql(fix_japan)
r5_to_c.createOrReplaceTempView("r5_to_c")

override_helper_1 = f"""
	SELECT r5.load_date
    	, 'COUNTRY' as geography_grain
    	, c.country_alpha2 as geography
    	, upper(r5.platform_subset) as platform_subset
    	, upper(r5.customer_engagement) as customer_engagement
    	, upper(r5.measure) as measure
    	, r5.min_sys_date
    	, r5.month_num
    	, r5.value
	FROM r5_to_c r5
	LEFT JOIN country_info c
	ON upper(r5.geography)=upper(c.region_5)
	"""
override_helper_1 = spark.sql(override_helper_1)
override_helper_1.createOrReplaceTempView("override_helper_1")

# COMMAND ----------

# MAGIC %md
# MAGIC # Market10 to Country

# COMMAND ----------

override_helper_2 = f"""
	SELECT m10.load_date
    	, 'COUNTRY' as geography_grain
    	, c.country_alpha2 as geography
    	, upper(m10.platform_subset) as platform_subset
    	, upper(m10.customer_engagement) as customer_engagement
    	, upper(m10.measure) as measure
    	, m10.min_sys_date
    	, m10.month_num
    	, m10.value
	FROM override_table_m10 m10
	LEFT JOIN country_info c
	ON upper(m10.geography)=upper(c.market10)
	"""
override_helper_2 = spark.sql(override_helper_2)
override_helper_2.createOrReplaceTempView("override_helper_2")

# COMMAND ----------

# MAGIC %md
# MAGIC # combine data from market10 and region5

# COMMAND ----------

override_table_a = """
--get market10 data
with step1 as (SELECT *, concat(geography, platform_subset, customer_engagement, measure) as gpid FROM override_helper_1),
--get region_5 data
step2 as  (SELECT *, concat(geography, platform_subset, customer_engagement, measure) as gpid FROM override_helper_2),
--get where have both market10 and region_5
step4 as (SELECT step1.* from step1 where gpid in (select distinct gpid from step2) 
	UNION 
	SELECT step2.* from step2 where gpid in (select distinct gpid from step1)),
step5 as (SELECT 
    geography
    , platform_subset
    , customer_engagement
    , measure
    , max(load_date) as max_date
    FROM step4
    group by 
    geography
    , platform_subset
    , customer_engagement
    , measure),
step6 as (SELECT step4.* from step4 left join step5 on step4.geography=step5.geography and step4.platform_subset=step5.platform_subset and step4.customer_engagement=step5.customer_engagement and step4.measure=step5.measure 
	where step4.load_date=step5.max_date and step4.value is not null)
--join tables 
SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
FROM step1
WHERE gpid not in (select distinct gpid from step2)
UNION ALL
SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
FROM step2
WHERE gpid not in (select distinct gpid from step1)
UNION ALL
SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
    FROM step6
"""

override_table_a=spark.sql(override_table_a)
override_table_a.createOrReplaceTempView("override_table_a")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change to Months

# COMMAND ----------

override_table_a2 = """

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

override_table_a2=spark.sql(override_table_a2)
override_table_a2=override_table_a2.distinct()
override_table_a2.createOrReplaceTempView("override_table_a2")

# COMMAND ----------

# MAGIC %md
# MAGIC # EPA Overrides

# COMMAND ----------

override_ine = read_redshift_to_df(configs) \
  .option("query","""
    SELECT upper(geography_grain) as geography_grain, upper(geography) as geography, upper(platform_subset) as platform_subset
    , upper(customer_engagement) as customer_engagement, upper(measure) as measure, min_sys_date,month_num, value, load_date
    FROM scen.working_forecast_usage_share
    WHERE 1=1 
        AND upper(upload_type) = 'EPA_DRIVERS'
    """) \
  .load()
override_ine.createOrReplaceTempView("override_ine")

# COMMAND ----------

override_table2e = """
    select  geography_grain
        , geography
        , platform_subset
        , customer_engagement
        , measure
        , min_sys_date
        , month_num
        , value
        , load_date
    from override_ine
    where load_date = (select max(load_date) from override_ine)
"""

override_table2e=spark.sql(override_table2e)
override_table2e.createOrReplaceTempView("override_table2e")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Region 5 to Market10

# COMMAND ----------

override_table_r5b=spark.sql("""select * from override_table2e where geography_grain ='REGION_5' """)
override_table_r5b.createOrReplaceTempView("override_table_r5b")

override_table_m10b=spark.sql("""select * from override_table2e where geography_grain ='MARKET10' """)
override_table_m10b.createOrReplaceTempView("override_table_m10b")

# COMMAND ----------

fix_japanb = f"""
with step1 as (
    SELECT load_date
    , geography_grain
    , 'JP'  as geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
    , CONCAT(geography, platform_subset, customer_engagement, measure) as grp
    FROM override_table_r5b
    WHERE geography = 'AP'
), step2 as (
    SELECT *, CONCAT(geography, platform_subset, customer_engagement, measure) as grp
    FROM override_table_r5b
    WHERE geography = 'JP'
), step3 as (SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
FROM step2
UNION 
SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
 FROM step1
 	WHERE grp not in (SELECT DISTINCT grp FROM step2)
  ),
step4 as (SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
FROM override_table_r5b
WHERE geography != 'JP'
  )
  select * from step3
  UNION ALL
  select * from step4
"""


r5_to_cb = spark.sql(fix_japanb)
r5_to_cb.createOrReplaceTempView("r5_to_cb")

override_helper_1b = f"""
	SELECT r5.load_date
    	, 'COUNTRY' as geography_grain
    	, c.country_alpha2 as geography
    	, upper(r5.platform_subset) as platform_subset
    	, upper(r5.customer_engagement) as customer_engagement
    	, upper(r5.measure) as measure
    	, r5.min_sys_date
    	, r5.month_num
    	, r5.value
	FROM r5_to_cb r5
	LEFT JOIN country_info c
	ON upper(r5.geography)=upper(c.region_5)
	"""
override_helper_1b = spark.sql(override_helper_1b)
override_helper_1b.createOrReplaceTempView("override_helper_1b")

# COMMAND ----------

# MAGIC %md
# MAGIC # Market10 to Country

# COMMAND ----------

override_helper_2b = f"""
	SELECT m10.load_date
    	, 'COUNTRY' as geography_grain
    	, c.country_alpha2 as geography
    	, upper(m10.platform_subset) as platform_subset
    	, upper(m10.customer_engagement) as customer_engagement
    	, upper(m10.measure) as measure
    	, m10.min_sys_date
    	, m10.month_num
    	, m10.value
	FROM override_table_m10b m10
	LEFT JOIN country_info c
	ON upper(m10.geography)=upper(c.market10)
	"""
override_helper_2b = spark.sql(override_helper_2b)
override_helper_2b.createOrReplaceTempView("override_helper_2b")

# COMMAND ----------

# MAGIC %md
# MAGIC # combine data from market10 and region5

# COMMAND ----------

override_table_b = """
with step1 as (SELECT *, concat(geography, platform_subset, customer_engagement, measure) as gpid FROM override_helper_1b),
step2 as  (SELECT *, concat(geography, platform_subset, customer_engagement, measure) as gpid FROM override_helper_2b),
--get where have both market10 and region_5
step4 as (SELECT step1.* from step1 where gpid in (select distinct gpid from step2) 
	UNION 
	SELECT step2.* from step2 where gpid in (select distinct gpid from step1)),
step5 as (SELECT 
    geography
    , platform_subset
    , customer_engagement
    , measure
    , max(load_date) as max_date
    FROM step4
    group by 
    geography
    , platform_subset
    , customer_engagement
    , measure),
step6 as (SELECT step4.* from step4 left join step5 on step4.geography=step5.geography and step4.platform_subset=step5.platform_subset and step4.customer_engagement=step5.customer_engagement and step4.measure=step5.measure
	where step4.load_date=step5.max_date and step4.value is not null)
--join tables 
SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
FROM step1
WHERE gpid not in (select distinct gpid from step2)
UNION ALL
SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
FROM step2
WHERE gpid not in (select distinct gpid from step1)
UNION ALL
SELECT load_date
    , geography_grain
    , geography
    , platform_subset
    , customer_engagement
    , measure
    , min_sys_date
    , month_num
    , value
    FROM step6
"""

override_table_b=spark.sql(override_table_b)
override_table_b.createOrReplaceTempView("override_table_b")

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
FROM override_table_b
"""

override_table2_b=spark.sql(override_table2_b)
override_table2_b=override_table2_b.distinct()
override_table2_b.createOrReplaceTempView("override_table2_b")

# COMMAND ----------

# MAGIC %md
# MAGIC # Combine Data

# COMMAND ----------

override_table_c ="""
SELECT * FROM override_table_a2
UNION ALL
SELECT * FROM override_table2_b
"""

override_table_c2 ="""
SELECT * FROM override_table_a2
"""

if override_table2_b.count() > 0:
    override_table_c=spark.sql(override_table_c)
if override_table2_b.count() == 0:
    override_table_c=spark.sql(override_table_c2)

override_table_c=override_table_c.distinct()
override_table_c.createOrReplaceTempView("override_table_c")

# COMMAND ----------

# MAGIC %md
# MAGIC #Update usage & Share

# COMMAND ----------
overlap_1 = """

---Find overlap between current and mature
SELECT c.record
      ,c.cal_date
      ,c.geography_grain
      ,c.geography
      ,c.platform_subset
      ,c.customer_engagement
      ,c.forecast_process_note
      ,CASE WHEN c.measure='HP_SHARE' AND c.data_source = 'HAVE DATA' THEN 'TELEMETRY'
            WHEN c.measure like '%USAGE%' AND c.data_source = 'DASHBOARD' THEN 'TELEMETRY'
	    WHEN c.data_source = 'TELEMETRY' THEN 'TELEMETRY'
            WHEN c.data_source = 'NPI' THEN 'NPI'
            WHEN m.source is null then c.data_source
            ELSE m.source
            END AS data_source
      ,c.version
      ,c.measure
      ,CASE WHEN c.measure='HP_SHARE' AND c.data_source = 'HAVE DATA' THEN c.units
            WHEN c.measure like '%USAGE%' AND c.data_source = 'DASHBOARD' THEN c.units
	    WHEN c.data_source = 'TELEMETRY' THEN c.units
            WHEN c.data_source = 'NPI' THEN c.units
            WHEN m.source is null then c.units
            ELSE m.units
            END AS units
      ,c.proxy_used
      ,c.ib_version
      ,c.load_date
FROM us_table c
LEFT JOIN override_table_c m
    ON 1=1
    AND c.platform_subset=m.platform_subset
    AND c.customer_engagement=m.customer_engagement
    AND c.geography=m.geography
    AND c.measure=m.measure
    AND c.cal_date=m.cal_date
"""
overlap_1=spark.sql(overlap_1)
overlap_1=overlap_1.distinct().cache()
overlap_1.createOrReplaceTempView("overlap_1")

overlap_1.count()

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
           ,data_source as source
           ,load_date
           ,CONCAT(platform_subset,customer_engagement,geography,cal_date,measure) AS grp
           FROM overlap_1
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
           FROM override_table_c
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
  FROM base 
  UNION
  SELECT new.record
           ,new.cal_date
           ,new.geography_grain
           ,new.geography
           ,new.platform_subset
           ,new.customer_engagement
           ,new.measure
           ,new.units
           ,new.ib_version
           ,new.source
           ,new.load_date
   FROM new 
   left join base on new.grp = base.grp
   where base.grp is null
   --WHERE grp not in (select distinct grp from base)
    
"""

update_table=spark.sql(update_table)
update_table.createOrReplaceTempView("update_table")

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
	, CASE WHEN us.measure = 'HP_SHARE' THEN us.source
           ELSE NULL
           END AS source_s
    , CASE WHEN us.measure like '%USAGE%' THEN us.source
            ELSE NULL
            END AS source_u
    , us.ib_version
	, SUM(CASE WHEN us.measure='USAGE' THEN us.units ELSE 0 END) AS usage
    , SUM(CASE WHEN us.measure='HP_SHARE' THEN us.units ELSE 0 END) AS page_share
    , SUM(CASE WHEN us.measure='COLOR_USAGE' THEN us.units ELSE 0 END) AS usage_c
	, SUM(CASE WHEN us.measure='K_USAGE' THEN us.units ELSE 0 END) AS usage_k
FROM update_table us 
GROUP BY us.cal_date
    	, us.geography_grain
	, us.geography
	, us.platform_subset
	, us.customer_engagement
    	, us.ib_version
    	, us.measure
    	, us.source
	) , step2 as (
    SELECT 
       u.cal_date
      ,u.geography_grain
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
      ,max(u.source_s) as source_s
      ,max(u.source_u) as source_u
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
      ,u.ib_version
      ,hw.technology
) , step5 as (
    SELECT u.cal_date
      ,u.geography_grain
      ,u.geography
      ,u.platform_subset
      ,u.customer_engagement
      ,u.source_s
      ,u.source_u
      ,u.ib_version
      ,CASE WHEN u.source_u in ('WORKING-FORECAST OVERRIDE','EPA-DRIVERS OVERRIDE') AND u.technology='INK' THEN usage_c+usage_k
            WHEN u.source_u in ('WORKING-FORECAST OVERRIDE','EPA-DRIVERS OVERRIDE') AND u.technology='TONER' THEN usage_k
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
	, source_u as source
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
	, source_s as source
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
	, source_u as source
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
	, source_u as source
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

s3_destination = f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/{datestamp}/us_adjusted"
print("output file name: " + s3_destination)

write_df_to_s3(df=convert, destination=s3_destination, format="parquet", mode="overwrite", upper_strings=True)
