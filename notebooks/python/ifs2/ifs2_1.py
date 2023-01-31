# Databricks notebook source
# MAGIC %md # Import libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as func
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md # Widgets

# COMMAND ----------

## Creating widgets for versions of drivers
dbutils.widgets.text("usage_share_version",'') # set usage_share_version to mark as official
dbutils.widgets.text("forecast_supplies_baseprod_version",'') # set forecast_supplies_baseprod_version to mark as official
dbutils.widgets.text("start_ifs2_date",'') # set starting date of ifs2
dbutils.widgets.text("end_ifs2_date",'') # set ending date of ifs2
dbutils.widgets.text("cartridge_demand_pages_ccs_mix_version",'')
dbutils.widgets.text("working_forecast_country_version",'')
dbutils.widgets.text("working_forecast_version",'')
dbutils.widgets.text("discounting_factor",'')

# COMMAND ----------

## Initializing versions in wisget variables
usage_share_version = dbutils.widgets.get("usage_share_version")
if usage_share_version == "":
    usage_share_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM prod.usage_share") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]

forecast_supplies_baseprod_version = dbutils.widgets.get("forecast_supplies_baseprod_version")
if forecast_supplies_baseprod_version == "":
    forecast_supplies_baseprod_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM fin_prod.forecast_supplies_baseprod") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]
    
start_ifs2_date = dbutils.widgets.get("start_ifs2_date")
if start_ifs2_date == "":
    start_ifs2_date = read_redshift_to_df(configs) \
        .option("query", "SELECT cast(MIN(cal_date) as date) FROM fin_prod.forecast_supplies_baseprod") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]
    
end_ifs2_date = dbutils.widgets.get("end_ifs2_date")
if end_ifs2_date == "":
    end_ifs2_date = read_redshift_to_df(configs) \
        .option("query", f"SELECT cast(DATEADD(year, 15, '{start_ifs2_date}') as date)") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]
    
cartridge_demand_pages_ccs_mix_version = dbutils.widgets.get("cartridge_demand_pages_ccs_mix_version")
if cartridge_demand_pages_ccs_mix_version == "":
    cartridge_demand_pages_ccs_mix_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM ifs2.cartridge_demand_pages_ccs_mix") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]
    
working_forecast_country_version = dbutils.widgets.get("working_forecast_country_version")
if working_forecast_country_version == "":
    working_forecast_country_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM prod.working_forecast_country") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]
    
working_forecast_version = dbutils.widgets.get("working_forecast_version")
if working_forecast_version == "":
    working_forecast_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM prod.working_forecast") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]
    
discounting_factor = dbutils.widgets.get("discounting_factor")
if discounting_factor == "":
    discounting_factor = 0.115

# COMMAND ----------

start_ifs2_date

# COMMAND ----------

start_ifs2_date = datetime.strptime(str(start_ifs2_date),"%Y-%m-%d")
end_ifs2_date = datetime.strptime(str(end_ifs2_date),"%Y-%m-%d")

# COMMAND ----------

# MAGIC %md # Delta tables

# COMMAND ----------

## Reading source tables from redshit
norm_shipments = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM prod.norm_shipments WHERE version = (select max(version) from prod.norm_shipments)") \
    .load()
usage_share1 = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM prod.usage_share WHERE version = '{usage_share_version}'") \
    .load()
hardware_xref = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM mdm.hardware_xref") \
    .load()
supplies_xref = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM mdm.supplies_xref") \
    .load()
decay_m13 = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM prod.decay_m13") \
    .load()
yield_ = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM mdm.yield") \
    .load()
forecast_supplies_baseprod = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM fin_prod.forecast_supplies_baseprod WHERE version = '{forecast_supplies_baseprod_version}'") \
    .load()
cartridge_demand_pages_ccs_mix = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM ifs2.cartridge_demand_pages_ccs_mix WHERE version = '{cartridge_demand_pages_ccs_mix_version}'") \
    .load()
iso_country_code_xref = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM mdm.iso_country_code_xref") \
    .load()
working_forecast_country = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM prod.working_forecast_country WHERE version = '{working_forecast_country_version}'") \
    .load()
working_forecast1 = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM prod.working_forecast WHERE version = '{working_forecast_version}'") \
    .load()
supplies_hw_mapping = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM mdm.supplies_hw_mapping") \
    .load()
toner_host_yield = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM ifs2.toner_host_yield") \
    .load()
calendar = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM mdm.calendar") \
    .load()

# COMMAND ----------

## Populating delta tables
tables = [
 ['prod.usage_share1' , usage_share1],
 ['mdm.hardware_xref' , hardware_xref],
 ['mdm.supplies_xref' , supplies_xref],
 ['prod.decay_m13' , decay_m13],
 ['mdm.yield' , yield_],
 ['fin_prod.forecast_supplies_baseprod' , forecast_supplies_baseprod],
 ['ifs2.cartridge_demand_pages_ccs_mix' , cartridge_demand_pages_ccs_mix],
 ['mdm.iso_country_code_xref' , iso_country_code_xref],
 ['prod.working_forecast_country' , working_forecast_country],
 ['prod.working_forecast1' , working_forecast1],
 ['mdm.supplies_hw_mapping', supplies_hw_mapping],
 ['prod.norm_shipments' , norm_shipments],
 ['ifs2.toner_host_yield' , toner_host_yield],
 ['mdm.calendar' , calendar]
]

for table in tables:
    # Define the input and output formats and paths and the table name.
    schema = table[0].split(".")[0]
    table_name = table[0].split(".")[1]
    write_format = 'delta'
    save_path = f'/tmp/delta/{schema}/{table_name}'
    
    # Load the data from its source.
    df = table[1]
    print(f'loading {table[0]}...')
    # Write the data to its target.
    df.write \
        .format(write_format) \
        .mode("overwrite") \
        .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Create the table.
    spark.sql("CREATE TABLE IF NOT EXISTS " + table[0] + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')

# COMMAND ----------

query = '''select * from mdm.yield'''

df = spark.sql(query)
df.createOrReplaceTempView("yield_main")

# COMMAND ----------

# eligible platform subsets for ink - removed PW
query = '''

select distinct ns.platform_subset 
from norm_shipments ns
inner join hardware_xref hx
on ns.platform_subset = hx.platform_subset
where ns.cal_date between '2022-11-01' and '2023-10-31'
and hx.technology in ('INK')
and hx.product_lifecycle_status in ('C','N')
and hx.official = 1


'''

ink_ps = spark.sql(query)
ink_ps.createOrReplaceTempView("ink_ps")

# COMMAND ----------

# eligible platform subsets for toner
query = '''

select distinct ns.platform_subset 
from norm_shipments ns
inner join hardware_xref hx
on ns.platform_subset = hx.platform_subset
where ns.cal_date between '2022-11-01' and '2023-10-31'
and hx.technology in ('LASER')
and hx.product_lifecycle_status in ('C','N')
and hx.official = 1


'''

toner_ps = spark.sql(query)
toner_ps.createOrReplaceTempView("toner_ps")

# COMMAND ----------

# MAGIC %md # Usage Share

# COMMAND ----------

## Creating dataframes for usage and share drivers for ink and toner
query = '''select cal_date
                , year(add_months(cal_date,2)) as year
                , (year(add_months(cal_date,2)) - year(add_months('{}',2))) + 1 as year_num
                , (month(add_months(cal_date,2)) - month(add_months('{}',2))) + 1 as month_df
                , (month(add_months(cal_date,2)))  as month_num
                , geography as market10
                , us.platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , case when measure = 'COLOR_USAGE' then sum(units)/3
                        when measure = 'HP_SHARE' then avg(units)
                        else sum(units)
                    end as units
                , ib_version
                , us.version
               from usage_share1 us
               inner join ink_ps ps
               on ps.platform_subset = us.platform_subset
               inner join hardware_xref hw on us.platform_subset = hw.platform_subset
               where 1=1
                and hw.technology in ('INK', 'PWA')
                and us.measure in ('COLOR_USAGE' , 'K_USAGE' , 'HP_SHARE')
                and us.cal_date between '{}' AND '{}'
               group by
                  cal_date
                , geography
                , us.platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , ib_version
                , us.version
                '''.format(start_ifs2_date , start_ifs2_date, start_ifs2_date, end_ifs2_date)
ink_usage_share = spark.sql(query)
ink_usage_share_pivot = ink_usage_share.groupBy("cal_date","year","year_num","month_df","month_num","market10","platform_subset","customer_engagement","hw_product_family","ib_version","us.version").pivot("measure").sum("units")
ink_usage_share_pivot.createOrReplaceTempView("ink_usage_share_pivot")

query = '''select cal_date
                , year(add_months(cal_date,2)) as year
                , (year(add_months(cal_date,2)) - year(add_months('{}',2))) + 1 as year_num
                , (month(add_months(cal_date,2)) - month(add_months('{}',2))) + 1 as month_df
                , (month(add_months(cal_date,2))) as month_num
                , geography as market10
                , us.platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , case when measure = 'HP_SHARE' then avg(units)
                        else sum(units)
                    end as units
                , ib_version
                , us.version
               from usage_share1 us
               inner join toner_ps ps
               on ps.platform_subset = us.platform_subset
               inner join hardware_xref hw on us.platform_subset = hw.platform_subset
               where 1=1
                and hw.technology in ('LASER')
                and us.measure in ('COLOR_USAGE' , 'K_USAGE' , 'HP_SHARE')
                and us.cal_date between '{}' AND '{}'
               group by
                  cal_date
                , geography
                , us.platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , ib_version
                , us.version
                '''.format(start_ifs2_date, start_ifs2_date, start_ifs2_date ,end_ifs2_date)
toner_usage_share = spark.sql(query)
toner_usage_share_pivot = toner_usage_share.groupBy("cal_date","year","year_num","month_df","month_num","market10","platform_subset","customer_engagement","hw_product_family","ib_version","us.version").pivot("measure").sum("units")
toner_usage_share_pivot.createOrReplaceTempView("toner_usage_share_pivot")

# COMMAND ----------

## Aggregating usage share drivers for ink at required level
query = '''select cal_date
                , ius.year
                , year_num
                , month_df
                , month_num
                , market10
                , platform_subset
                , customer_engagement
                , hw_product_family
                , COLOR_USAGE
                , SUM(COLOR_USAGE) over 
                	(partition by 
                		market10
                		,platform_subset
                		,customer_engagement
                		,hw_product_family
                		,ib_version
                		,version 
                	order by cal_date rows between unbounded preceding and current row)
                	as sum_of_color_usage_till_date
                , K_USAGE
                , SUM(K_USAGE) over 
                	(partition by 
                		market10
                		,platform_subset
                		,customer_engagement
                		,hw_product_family
                		,ib_version
                		,version 
                	order by cal_date rows between unbounded preceding and current row)
                	as sum_of_k_usage_till_date
                , HP_SHARE
                ,ib_version
                ,version
                from ink_usage_share_pivot ius
                '''
ink_usage_share_sum_till_date = spark.sql(query)
ink_usage_share_sum_till_date.createOrReplaceTempView("ink_usage_share_sum_till_date")

# COMMAND ----------

## Aggregating usage share drivers for toner at required level
query = '''select cal_date
                , tus.year
                , year_num
                , month_df
                , month_num
                , market10
                , platform_subset
                , customer_engagement
                , hw_product_family
                , COLOR_USAGE
                , SUM(COLOR_USAGE) over 
                	(partition by 
                		market10
                		,platform_subset
                		,customer_engagement
                		,hw_product_family
                		,ib_version
                		,version 
                	order by cal_date rows between unbounded preceding and current row)
                	as sum_of_color_usage_till_date
                , K_USAGE
                , SUM(K_USAGE) over 
                	(partition by 
                		market10
                		,platform_subset
                		,customer_engagement
                		,hw_product_family
                		,ib_version
                		,version 
                	order by cal_date rows between unbounded preceding and current row)
                	as sum_of_k_usage_till_date
                , HP_SHARE
                ,ib_version
                ,version
                from toner_usage_share_pivot tus
                '''
toner_usage_share_sum_till_date = spark.sql(query)
toner_usage_share_sum_till_date.createOrReplaceTempView("toner_usage_share_sum_till_date")

# COMMAND ----------

query = '''
        select   'INK' as record       
                , cal_date
                , year
                , year_num
                , month_df
                , month_num
                , market10
                , platform_subset
                , customer_engagement
                , hw_product_family
                , COLOR_USAGE
                , sum_of_color_usage_till_date
                , K_USAGE
                , sum_of_k_usage_till_date
                , HP_SHARE
                , ib_version
                , version
        from ink_usage_share_sum_till_date
        UNION ALL
        select   'TONER' as record       
                , cal_date
                , year
                , year_num
                , month_df
                , month_num
                , market10
                , platform_subset
                , customer_engagement
                , hw_product_family
                , COLOR_USAGE
                , sum_of_color_usage_till_date
                , K_USAGE
                , sum_of_k_usage_till_date
                , HP_SHARE
                , ib_version
                , version
        from toner_usage_share_sum_till_date
'''
usage_share_ink_union_toner = spark.sql(query)
usage_share_ink_union_toner.createOrReplaceTempView("usage_share_ink_union_toner")

# COMMAND ----------

## usage share at base product and country level
query = '''
select usiut.record       
                , cal_date
                , year
                , year_num
                , month_df
                , month_num
                , region_5
                , usiut.market10
                , iccx.country_alpha2
                , usiut.platform_subset
                , shm.base_product_number
                , crg_chrome
                , case when (usiut.record = 'TONER' and crg_chrome = 'BLK') then 'BLACK'
         when (usiut.record = 'TONER' and crg_chrome in ('CYN','MAG','YEL','MUL')) then 'COLOR'  -- work on MUL
        else 'NONE' end as crg_chrome_type
                , usiut.customer_engagement
                , hw_product_family
                ,case when crg_chrome = 'BLK' then k_usage
                else color_usage
                end as usage
                ,case when crg_chrome = 'BLK' then sum_of_k_usage_till_date
                else sum_of_color_usage_till_date
                end as sum_of_usage_till_date
            --    , COLOR_USAGE
            --    , sum_of_color_usage_till_date
            --    , K_USAGE
            --    , sum_of_k_usage_till_date
                , HP_SHARE
                , ib_version
                , usiut.version
            from usage_share_ink_union_toner as usiut
            left join iso_country_code_xref iccx
            on usiut.market10 = iccx.market10 
            left join mdm.supplies_hw_mapping shm
            on usiut.platform_subset = shm.platform_subset
            and (region_5 = shm.geography)
            and usiut.customer_engagement = shm.customer_engagement
            and shm.official = 1
            left join supplies_xref sx
            on shm.base_product_number = sx.base_product_number
            and sx.official = 1
'''
usage_share = spark.sql(query)
usage_share.createOrReplaceTempView("usage_share")

# COMMAND ----------

# MAGIC %md # Decay

# COMMAND ----------

## Decay at platform_subset, region5, customer_engagement level split on months
query = '''
with months as (


SELECT 1 AS month_num UNION ALL
SELECT 2 AS month_num UNION ALL
SELECT 3 AS month_num UNION ALL
SELECT 4 AS month_num UNION ALL
SELECT 5 AS month_num UNION ALL
SELECT 6 AS month_num UNION ALL
SELECT 7 AS month_num UNION ALL
SELECT 8 AS month_num UNION ALL
SELECT 9 AS month_num UNION ALL
SELECT 10 AS month_num UNION ALL
SELECT 11 AS month_num UNION ALL
SELECT 12 AS month_num
),


d0 as (


	select platform_subset 
		, split_name 
		, geography 
		, year 
		, cast(substring(year, 6 , 8) as integer ) as year_num
		, avg(value) as value
		, max(version) as version 
		
from prod.decay_m13
join months m 
on 1=1
where official = 1
and record = 'HW_DECAY'
group by platform_subset 
        , split_name 
        , year
        , geography
),
d1 as
(
select platform_subset 
		, split_name 
		, geography 
		, year
		, year_num 
		, m.month_num
		, value/12 as value
		, version 
		, SUM(value/12) over (partition by platform_subset, split_name, geography, year_num) as sum_year
from d0 d
join months m 
on 1 = 1
)
,d2 as (
select platform_subset 
		, split_name 
		, geography 
		, year
		, year_num 
		, month_num
		, value
		, version 
		, sum_year
		, SUM(value) over (partition by platform_subset, split_name, geography, year_num order by month_num rows between unbounded preceding and current row ) as sum_month_till_date
		, SUM(sum_year) over (partition by platform_subset, split_name, geography, month_num order by year_num rows between unbounded preceding and current row ) as sum_year_till_date

from d1

)

select d3.platform_subset 
		, split_name 
        , region_5
		, m13
		, m10
		, country_alpha2
        , shm.base_product_number
		, d3.developed_emerging
		, year
		, year_num 
		, month_num
		, value
		, d3.version 
		, sum_year
		, sum_month_till_date
		, sum_year_till_date
		, remaining_amount
from
(
select platform_subset 
		, split_name 
		, geography as m13
		, substring(geography, 0 , length(geography) - 3) as m10
		, case when substring(geography,length(geography) - 1, length(geography)) = 'DM' then 'DEVELOPED'
			else 'EMERGING'
			end as developed_emerging
		, year
		, year_num 
		, month_num
		, value
		, version 
		, sum_year
		, sum_month_till_date
		, sum_year_till_date
		, 1 - (sum_year_till_date - sum_year + (sum_month_till_date/2)) as remaining_amount
from d2
)d3
left join iso_country_code_xref iccx
on d3.m10 = iccx.market10 
and d3.developed_emerging = iccx.developed_emerging
left join mdm.supplies_hw_mapping shm
on d3.platform_subset = shm.platform_subset
and region_5 = shm.geography
and d3.split_name = shm.customer_engagement
and shm.official = 1

'''
decay = spark.sql(query)
decay.createOrReplaceTempView("decay")

# COMMAND ----------

# MAGIC %md # Yield

# COMMAND ----------

query = '''
with yield as (
SELECT distinct 
	y.record
    ,geography
      ,y.base_product_number
  ,sup.cartridge_alias
  ,sup.type
  ,sup.crg_chrome
  ,sup.k_color
  ,sup.size
  ,sup.supplies_family
  ,sup.supplies_group
  ,value
  ,effective_date
  ,version as yield_version
--  ,y.official ##need to ask
  ,ROW_NUMBER() over( partition by y.base_product_number, y.geography order by y.effective_date desc) as rn
  FROM yield_main as y
  left join mdm.supplies_xref as sup on sup.base_product_number = y.base_product_number 
  where effective_date < (add_months('{}',-1)) 
 )
 ,  yield_region as (
  select distinct y.*
  from yield y
  where y.rn = 1
  order by y.base_product_number
  	, y.effective_date
  	) 	
	select yr.geography 
		, yr.base_product_number
		, shm.platform_subset
		, shm.customer_engagement 
        , yr.crg_chrome
        , yr.type
		, value as value 
		, iccx.country_alpha2
        , yield_version
	from yield_region yr
	left join mdm.iso_country_code_xref iccx 
		on yr.geography = iccx.region_5 
	left join mdm.supplies_hw_mapping shm 
		on yr.base_product_number = shm.base_product_number 
		and yr.geography = shm.geography 
	where shm.official = 1
'''.format(start_ifs2_date)
yield_ = spark.sql(query)
yield_.createOrReplaceTempView("yield_")

# COMMAND ----------

# MAGIC %md # Host Yield

# COMMAND ----------

# host yield for ink
query = '''
with y1 as (
SELECT distinct 
	y.record
    ,geography
      ,y.base_product_number
  ,sup.cartridge_alias
  ,sup.type
  ,sup.crg_chrome
  ,sup.k_color
  ,sup.size
  ,sup.supplies_family
  ,sup.supplies_group
  ,value
  ,effective_date
  ,version as yield_version
  ,ROW_NUMBER() over( partition by y.base_product_number, y.geography order by y.effective_date desc) as rn
  FROM yield_main as y
  left join supplies_xref as sup on sup.base_product_number = y.base_product_number 
  where sup.official = 1
  and geography_grain = 'REGION_5'
  --where effective_date < (add_months(current_date ,-1)) 
 )
 ,  yield_region as (
  select distinct y.*
  from y1 y
  where y.rn = 1
  order by y.base_product_number
  	, y.effective_date
  	) 
 , yield_ps as 
 (
	select distinct yr.geography 
		, yr.base_product_number
		, crg_chrome
		, yr.type
		, shm.host_multiplier 
		, shm.platform_subset
		,k_color
		, shm.customer_engagement 
		, value as value
		, yr.effective_date
		, yr.yield_version
	from yield_region yr
	left join supplies_hw_mapping shm 
		on yr.base_product_number = shm.base_product_number 
        and yr.geography = shm.geography
	where shm.official = 1
)
, host_yield as 
(
	select geography, platform_subset, crg_chrome, customer_engagement ,value as host_yield 
	from yield_ps
	where host_multiplier > 0
	and crg_chrome not in ('HEAD','NA')
), all_ps as
(
select distinct  ns.platform_subset 
from norm_shipments ns
inner join hardware_xref hx
on ns.platform_subset = hx.platform_subset
where ns.cal_date between '2022-11-01' and '2023-10-31'
and version = (select max(version) from norm_shipments)
and hx.technology in ('INK')
and hx.product_lifecycle_status in ('C','N')
and hx.official = 1
)

select 'INK' as record, 
    hy.* from host_yield hy
inner join all_ps ps
on hy.platform_subset = ps.platform_subset
'''

host_yield_ink = spark.sql(query)
host_yield_ink.createOrReplaceTempView("host_yield_ink")

# COMMAND ----------

query = '''
select 'TONER' as record
    , NULL as geography
    , UPPER(platform_subset) as platform_subset 
    , case when (yield_name IN ('BLACK_YIELD' , 'BLACK YIELD'))  then 'BLACK'
         when (yield_name IN ('COLOR_YIELD' , 'COLOR YIELD')) then 'COLOR'
        end as crg_chrome
    , NULL as customer_engagement
    , yield 
from toner_host_yield
'''

host_yield_toner = spark.sql(query)
host_yield_toner = host_yield_toner.withColumn("yield",host_yield_toner["yield"].cast(DoubleType()))
host_yield_toner.createOrReplaceTempView("host_yield_toner")

# COMMAND ----------

query = '''
select * from host_yield_ink

UNION ALL

select * from host_yield_toner
'''

host_yield = spark.sql(query)
host_yield.createOrReplaceTempView("host_yield")

# COMMAND ----------

# MAGIC %md # Trade split

# COMMAND ----------

# trade split
query = '''
select cal_date
		, cdpcm.geography
		, platform_subset
		, base_product_number
		, customer_engagement
		, country_alpha2
		, (pgs_ccs_mix) as trade_split
		, cdpcm.version
from ifs2.cartridge_demand_pages_ccs_mix cdpcm
left join mdm.iso_country_code_xref iccx
on cdpcm.geography = iccx.market10
where cdpcm.version = '{}'
and cal_date between '{}' and '{}'

'''.format(cartridge_demand_pages_ccs_mix_version , start_ifs2_date , end_ifs2_date)
trade_split = spark.sql(query)
trade_split.createOrReplaceTempView("trade_split")

# COMMAND ----------

# MAGIC %md # Financial forecast inputs

# COMMAND ----------

##financial_forecast inputs
query = '''
select distinct fsb.record
		, shm.platform_subset
		, fsb.base_product_number
		, fsb.base_product_line_code
		, fsb.region_5
        , iccx.market10
		, fsb.country_alpha2
		, fsb.cal_date
		, fsb.insights_base_units
		, fsb.baseprod_gru
		, fsb.baseprod_contra_per_unit
		, fsb.baseprod_variable_cost_per_unit
		, fsb.baseprod_fixed_cost_per_unit
		, fsb.load_date
		, fsb.version
		, fsb.sales_gru_version
		, fsb.contra_version
		, fsb.variable_cost_version
		, fsb.fixed_cost_version
from forecast_supplies_baseprod fsb
left join mdm.supplies_hw_mapping shm
on fsb.base_product_number = shm.base_product_number
left join iso_country_code_xref iccx
on fsb.country_alpha2 = iccx.country_alpha2
where fsb.cal_date between '{}' AND '{}'
'''.format(start_ifs2_date, end_ifs2_date)
fsb = spark.sql(query)
fsb.createOrReplaceTempView("fsb")

# COMMAND ----------

# MAGIC %md # vtc

# COMMAND ----------

# # vtc
# query = '''
# select cal_date
# 	, geography_grain
# 	, geography
#     , country
# 	, platform_subset
# 	, base_product_number
# 	, customer_engagement
# 	, (imp_corrected_cartridges/cartridges) as vtc
# 	, version
# from working_forecast_country 
# where version = '{}'
# and cal_date between '{}' and '{}'
# '''.format(working_forecast_country_version , start_ifs2_date , end_ifs2_date)
# vtc = spark.sql(query)
# vtc.createOrReplaceTempView("vtc")

# COMMAND ----------

# vtc
query = '''
select cal_date
	, geography_grain
	, geography
	, platform_subset
	, base_product_number
	, customer_engagement
    , vtc
	, version
from working_forecast1
where version = '{}'
and cal_date between '{}' and '{}'
'''.format(working_forecast_version , start_ifs2_date , end_ifs2_date)
vtc = spark.sql(query)
vtc.createOrReplaceTempView("vtc")

# COMMAND ----------

# MAGIC %md # Final join

# COMMAND ----------

# final join
query = '''
select distinct
    us.record,
    us.platform_subset,
    us.base_product_number,
    us.region_5,
    us.market10,
    us.country_alpha2,
    us.cal_date,
    c.fiscal_year_qtr,
    us.year,
    us.year_num,
    us.month_num,
    ((12*(us.year_num-1))+us.month_df) as month,
    POWER(1 + double({})/12,((12*(us.year_num-1))+us.month_df)) as discounting_factor,
    us.customer_engagement,
    us.crg_chrome,
--    us.color_usage,
--    us.sum_of_color_usage_till_date,
--    us.k_usage,
--    us.sum_of_k_usage_till_date,
    us.usage,
    us.sum_of_usage_till_date,
    case when (us.sum_of_usage_till_date - hy.host_yield) < 0 then 0
        else (us.sum_of_usage_till_date - hy.host_yield)
        end as after_market_usage_cumulative,
    us.hp_share,
    d.value as decay,
    d.remaining_amount,
    y.value as yield,
    hy.host_yield as host_yield,
    case when (us.record = 'INK' and us.crg_chrome in ('CYN','MAG','YEL','MUL')) then t.trade_split*3
        else t.trade_split
        end as trade_split, 
    v.vtc,
    us.ib_version,
    us.version as usage_share_version,
    d.version as decay_version,
    y.yield_version,
    t.version as trade_split_version,
    v.version as vtc_version
from usage_share us
left join decay d
on us.platform_subset = d.platform_subset
and us.base_product_number = d.base_product_number
and us.year_num = d.year_num
and us.month_num = d.month_num
and us.market10 = d.m10
and us.country_alpha2 = d.country_alpha2
and us.customer_engagement = d.split_name
and us.region_5 = d.region_5
left join yield_ y
on us.platform_subset = y.platform_subset
and us.base_product_number = y.base_product_number
and us.region_5 = y.geography
and us.country_alpha2 = y.country_alpha2
left join host_yield hy
on (
hy.record = 'INK'
and us.platform_subset = hy.platform_subset
and us.region_5 = hy.geography
and us.customer_engagement = hy.customer_engagement
and us.crg_chrome = hy.crg_chrome)
or (
hy.record = 'TONER'
and us.platform_subset = hy.platform_subset
and us.crg_chrome_type = hy.crg_chrome
)
left join trade_split t
on us.platform_subset = t.platform_subset
and us.base_product_number = t.base_product_number
and us.market10 = t.geography
and us.country_alpha2 = t.country_alpha2
and us.cal_date = t.cal_date
and us.customer_engagement = t.customer_engagement
left join vtc v
on us.platform_subset = v.platform_subset
and us.base_product_number = v.base_product_number
and us.cal_date = v.cal_date
and us.market10 = v.geography
and us.customer_engagement = v.customer_engagement
left join calendar c
on us.cal_date = c.date

'''.format(discounting_factor)
pen_per_printer1 = spark.sql(query)
pen_per_printer1.createOrReplaceTempView("pen_per_printer1")

# COMMAND ----------

pen_per_printer1.filter((col('platform_subset') == 'AGATE 22 MANAGED') & (col('country_alpha2') == 'BG') & (col('base_product_number') == 'W9100MC') & (col('crg_chrome') == 'BLK') & (col('cal_date')== '2025-12-01')).orderBy('cal_date').display()

# COMMAND ----------

pen_per_printer1.filter((col('platform_subset') == 'MALBEC YET1') & (col('base_product_number') == '3YL58A') & (col('country_alpha2') == 'US')).orderBy('cal_date').display()

# COMMAND ----------

query = '''
    select
        record,
        platform_subset,
        base_product_number,
        region_5,
        market10,
        country_alpha2,
        cal_date,
        fiscal_year_qtr,
        year,
        year_num,
        month_num,
        month,
        customer_engagement,
        crg_chrome,
        usage,
        host_yield,
        after_market_usage_cumulative,
        after_market_usage_cumulative - lag(after_market_usage_cumulative) over (partition by platform_subset, base_product_number,region_5, country_alpha2, customer_engagement order by month) as after_market_usage,
        hp_share,
        decay,
        remaining_amount,
        yield,
        trade_split,
        vtc,
        discounting_factor,
        ib_version,
        usage_share_version,
        decay_version,
        yield_version,
        trade_split_version,
        vtc_version
        from pen_per_printer1
'''
pen_per_printer2 = spark.sql(query)
pen_per_printer2.createOrReplaceTempView("pen_per_printer2")

# COMMAND ----------

pen_per_printer2.filter((col('platform_subset') == 'MALBEC YET1') & (col('market10') == 'NORTH AMERICA') & (col('base_product_number') == '3YL58A') & (col('country_alpha2') == 'US')).orderBy('cal_date').display()

# COMMAND ----------

query = '''select
        record,
        platform_subset,
        base_product_number,
        region_5,
        market10,
        country_alpha2,
        cal_date,
        fiscal_year_qtr,
        year,
        year_num,
        month_num,
        month,
        customer_engagement,
        crg_chrome,
        usage,
        host_yield,
        after_market_usage_cumulative,
        after_market_usage,
        hp_share,
        decay,
        remaining_amount,
        yield,
        trade_split,
        vtc,
        ((after_market_usage * hp_share * remaining_amount * trade_split)/yield) * abs(vtc) as pen_per_printer,
        discounting_factor,
        ib_version,
        usage_share_version,
        decay_version,
        yield_version,
        trade_split_version,
        vtc_version
        from pen_per_printer2
'''
pen_per_printer = spark.sql(query)
pen_per_printer.createOrReplaceTempView("pen_per_printer")

# COMMAND ----------

pen_per_printer.filter((col('platform_subset') == 'MALBEC YET1') & (col('market10') == 'NORTH AMERICA') & (col('base_product_number') == '3YL58A') & (col('country_alpha2') == 'US')).orderBy('cal_date').display()

# COMMAND ----------

query = '''select
        ppp.record,
        ppp.platform_subset,
        ppp.base_product_number,
        ppp.region_5,
        ppp.market10,
        ppp.country_alpha2,
        ppp.cal_date,
        ppp.fiscal_year_qtr,
        year,
        year_num,
        month_num,
        month,
        customer_engagement,
        crg_chrome,
        usage,
        host_yield,
        after_market_usage_cumulative,
        after_market_usage,
        hp_share,
        decay,
        remaining_amount,
        yield,
        trade_split,
        vtc,
        pen_per_printer,
        discounting_factor,
        fsb.insights_base_units,
		fsb.baseprod_gru as baseprod_gru,
		fsb.baseprod_contra_per_unit as baseprod_contra_per_unit,
		fsb.baseprod_variable_cost_per_unit as baseprod_variable_cost_per_unit,
		fsb.baseprod_fixed_cost_per_unit as baseprod_fixed_cost_per_unit,
        (fsb.baseprod_gru - fsb.baseprod_contra_per_unit) * pen_per_printer as aru,
        (fsb.baseprod_gru - fsb.baseprod_contra_per_unit - (fsb.baseprod_variable_cost_per_unit + fsb.baseprod_fixed_cost_per_unit)) * pen_per_printer as gmu,
        ((fsb.baseprod_gru - fsb.baseprod_contra_per_unit) * pen_per_printer)/discounting_factor as ifs2_aru,
        ((fsb.baseprod_gru - fsb.baseprod_contra_per_unit - (fsb.baseprod_variable_cost_per_unit + fsb.baseprod_fixed_cost_per_unit)) * pen_per_printer)/discounting_factor as ifs2_gmu,
        ib_version,
        usage_share_version,
        decay_version,
        yield_version,
        trade_split_version,
        vtc_version,
        fsb.version as forecast_supplies_baseprod_version
        from pen_per_printer ppp
        inner join fsb fsb
        on ppp.platform_subset = fsb.platform_subset
        and ppp.base_product_number = fsb.base_product_number
        and ppp.region_5 = fsb.region_5
        and ppp.market10 = fsb.market10
		and ppp.country_alpha2 = fsb.country_alpha2
		and ppp.cal_date = fsb.cal_date
'''
ifs2 = spark.sql(query)
ifs2.createOrReplaceTempView("ifs2")

# COMMAND ----------

ifs2.filter((col('platform_subset') == 'BETELGEUSE') & (col('market10') == 'NORTH AMERICA') & (col('base_product_number') == 'W2110A') & (col('country_alpha2') == 'US')).orderBy('cal_date').display()

# COMMAND ----------

ifs2.filter((col('platform_subset') == 'MALBEC YET1') & (col('market10') == 'NORTH AMERICA') & (col('base_product_number') == '3YL58A') & (col('country_alpha2') == 'US')).orderBy('cal_date').display()

# COMMAND ----------

ifs2.filter((col('platform_subset') == 'RUBY LITE 60 MANAGED') & (col('market10') == 'NORTH AMERICA') & (col('base_product_number') == '3YL58A') & (col('country_alpha2') == 'US') ).orderBy('cal_date').display()

# COMMAND ----------

# MAGIC %md # Write to redshift

# COMMAND ----------

write_df_to_redshift(configs, ifs2, "ifs2.ifs2", "overwrite")

# COMMAND ----------

ifs2.count()
