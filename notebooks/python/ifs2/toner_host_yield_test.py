# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import functions as func

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

## Creating widgets for versions of drivers
dbutils.widgets.text("usage_share_version",'') # set usage_share_version to mark as official
dbutils.widgets.text("forecast_supplies_baseprod_version",'') # set forecast_supplies_baseprod_version to mark as official
dbutils.widgets.text("start_ifs2_date",'') # set starting date of ifs2
dbutils.widgets.text("end_ifs2_date",'') # set ending date of ifs2
dbutils.widgets.text("cartridge_demand_pages_ccs_mix_version",'')
dbutils.widgets.text("working_forecast_country_version",'')

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

# COMMAND ----------

start_ifs2_date = datetime.strptime(str(start_ifs2_date),"%Y-%m-%d")
end_ifs2_date = datetime.strptime(str(end_ifs2_date),"%Y-%m-%d")

# COMMAND ----------

## Reading source tables from redshit
usage_share = read_redshift_to_df(configs) \
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
supplies_hw_mapping = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM mdm.supplies_hw_mapping") \
    .load()

# COMMAND ----------

## Populating delta tables
tables = [
  ['prod.usage_share' , usage_share],
  ['mdm.hardware_xref' , hardware_xref],
  ['mdm.supplies_xref' , supplies_xref],
  ['prod.decay_m13' , decay_m13],
  ['mdm.yield' , yield_],
  ['fin_prod.forecast_supplies_baseprod' , forecast_supplies_baseprod],
  ['ifs2.cartridge_demand_pages_ccs_mix' , cartridge_demand_pages_ccs_mix],
  ['mdm.iso_country_code_xref' , iso_country_code_xref],
  ['prod.working_forecast_country' , working_forecast_country],
  ['mdm.supplies_hw_mapping', supplies_hw_mapping]
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
#     df.write \
#       .format(write_format) \
#       .mode("overwrite") \
#       .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Create the table.
    spark.sql("CREATE TABLE IF NOT EXISTS " + table[0] + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')

# COMMAND ----------

## Creating dataframes for usage and share drivers for ink and toner
query = '''select cal_date
                , year(add_months(cal_date,2)) as year
                , (year(add_months(cal_date,2)) - year(add_months('{}',2))) + 1 as year_num
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
               from usage_share us
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
                '''.format(start_ifs2_date , start_ifs2_date, end_ifs2_date)
ink_usage_share = spark.sql(query)
ink_usage_share_pivot = ink_usage_share.groupBy("cal_date","year","year_num","market10","platform_subset","customer_engagement","hw_product_family","ib_version","us.version").pivot("measure").sum("units")
ink_usage_share_pivot.createOrReplaceTempView("ink_usage_share_pivot")

query = '''select cal_date
                , year(add_months(cal_date,2)) as year
                , (year(add_months(cal_date,2)) - year(add_months('{}',2))) + 1 as year_num
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
               from usage_share us
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
                '''.format(start_ifs2_date, start_ifs2_date, end_ifs2_date)
toner_usage_share = spark.sql(query)
toner_usage_share_pivot = toner_usage_share.groupBy("cal_date","year","year_num","market10","platform_subset","customer_engagement","hw_product_family","ib_version","us.version").pivot("measure").sum("units")
toner_usage_share_pivot.createOrReplaceTempView("toner_usage_share_pivot")

# COMMAND ----------

query = '''select distinct platform_subset
from toner_usage_share_pivot
'''

toner_platform = spark.sql(query)
toner_platform.createOrReplaceTempView("toner_platform")

# COMMAND ----------

## Aggregating usage share drivers for ink at required level
query = '''select cal_date
                , ius.year
                , year_num
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

ink_usage_share_sum_till_date.display()

# COMMAND ----------

## Aggregating usage share drivers for toner at required level
query = '''select cal_date
                , tus.year
                , year_num
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
                , region_5
                , usiut.market10
                , iccx.country_alpha2
                , usiut.platform_subset
                , shm.base_product_number
                , crg_chrome
                , usiut.customer_engagement
                , hw_product_family
                , COLOR_USAGE
                , sum_of_color_usage_till_date
                , K_USAGE
                , sum_of_k_usage_till_date
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

usage_share.display()

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
  FROM mdm.yield as y
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

yield_.display()

# COMMAND ----------

query = '''
select geography, platform_subset, base_product_number, customer_engagement, crg_chrome, value as host_yield, country_alpha2
from yield_
where type in ('HOST','TRADE/HOST')

'''
host_yield = spark.sql(query)
host_yield.createOrReplaceTempView("host_yield")


# COMMAND ----------

host_yield.count()

# COMMAND ----------

## platform subsets present in usage share but not in host yield
query = '''
select distinct platform_subset
from ink_usage_share_sum_till_date iusstd
where platform_subset not in (select distinct platform_subset from host_yield)

'''
spark.sql(query).display()

# COMMAND ----------

## platform subsets present in usage share with hosts
query = '''

select distinct platform_subset
from ink_usage_share_sum_till_date iusstd
where platform_subset in (select distinct platform_subset from host_yield)

'''
spark.sql(query).display()

# COMMAND ----------

## platform subsets with multiple hosts
query = '''

select distinct platform_subset from
(
    select geography, platform_subset, crg_chrome, count(base_product_number), country_alpha2, customer_engagement
    from host_yield
    group by geography, platform_subset, crg_chrome, country_alpha2, customer_engagement
    having count(base_product_number) > 1
)

'''
spark.sql(query).display()

# COMMAND ----------

test2.display()

# COMMAND ----------

query = '''
select distinct platform_subset
from usage_share
where record = 'INK'

'''
test3 = spark.sql(query)
test3.count()

# COMMAND ----------

usage_share_hostYield.groupby("country_alpha2","platform_subset","crg_chrome","customer_engagement").sum("host_yield").display()

# COMMAND ----------

query = '''
select cal_date
		, cdpcm.geography
		, platform_subset
		, base_product_number
		, customer_engagement
		, country_alpha2
		, (pgs_ccs_mix * 3) as trade_split
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

trade_split.display()

# COMMAND ----------

##financail_forecast inputs
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

query = '''
select cal_date
	, geography_grain
	, geography
    , country
	, platform_subset
	, base_product_number
	, customer_engagement
	, ((imp_corrected_cartridges - cartridges)/imp_corrected_cartridges) as vtc
	, version
from working_forecast_country 
where version = '{}'
and cal_date between '{}' and '{}'
'''.format(working_forecast_country_version , start_ifs2_date , end_ifs2_date)
vtc = spark.sql(query)
vtc.createOrReplaceTempView("vtc")

# COMMAND ----------



# COMMAND ----------

vtc.display()

# COMMAND ----------

host_yield = read_redshift_to_df(configs) \
    .option("query", f"""SELECT * FROM stage.laser_host_assumptions""") \
    .load()

# COMMAND ----------

host_yield.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------


bucket = f"dataos-core-{stack}-team-phoenix-fin" 
bucket_prefix = "landing/odw/"

latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

latest_file = latest_file.split("/")[len(latest_file.split("/"))-1]

print(latest_file)

# COMMAND ----------

## toner_host_yield

toner_host_yield = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("inferSchema", "True") \
    .option("header","True") \
    .option("treatEmptyValuesAsNulls", "False")\
    .load(f"s3a://{bucket}/{bucket_prefix}/{latest_file}")

# COMMAND ----------



# COMMAND ----------

toner_host_yield.display()

# COMMAND ----------

from pyspark.sql.functions import countDistinct
 
# applying the function countDistinct()
# on df using select()
df2 = toner_host_yield.select(countDistinct("Platform Subset"))

# COMMAND ----------

df_temp = toner_host_yield.withColumn("new_black",f.when(f.col("Black").isNotNull(),lit("Black")).otherwise(f.col("Black")))
df_temp = df_temp.withColumn("new_color",f.when(f.col("Color").isNotNull(),lit("Color")).otherwise(f.col("Color")))
df_temp = df_temp.withColumnRenamed("Platform Subset" , "platform_subset")
df_temp = df_temp.withColumnRenamed("Product Name" , "product_name")
df_temp = df_temp.withColumnRenamed("Black Yield" , "black_yield")
df_temp = df_temp.withColumnRenamed("Black Large" , "black_large")
df_temp = df_temp.withColumnRenamed("Black Large Yield" , "black_large_yield")
df_temp = df_temp.withColumnRenamed("Color Large" , "color_large")
df_temp = df_temp.withColumnRenamed("Color Large Yield" , "color_large_yield")
df_temp = df_temp.withColumnRenamed("Color Yield" , "color_yield")
df_temp.createOrReplaceTempView("df")

# COMMAND ----------

submit_remote_query(configs , '''CREATE TABLE IF NOT EXISTS ifs2.toner_host_yield
(
	status_type VARCHAR(30)  ENCODE lzo
	,platform_subset VARCHAR(255)  ENCODE lzo
	,in_box_cartridges VARCHAR(255)   ENCODE lzo
	,yield_name VARCHAR(255)   ENCODE lzo
    ,yield  VARCHAR(255)   ENCODE lzo
    );
    
    GRANT ALL ON TABLE ifs2.toner_host_yield to GROUP phoenix_dev;
''')

# COMMAND ----------

query = '''

INSERT INTO ifs2.toner_host_yield
select "status type" as status_type
    , "platform subset" as platform_subset
    , "In-Box Cartridge" as in_box_cartridges
    , "yield_name" 
    , "yield"
 from  (select distinct "status type" ,"platform subset", "In-Box Cartridge" ,"black yield" , "color yield" , "black large yield"  , "color large yield"  from stage.laser_host_assumptions ) UNPIVOT (
    yield for yield_name in ("black yield" , "color yield" , "black large yield" , "color large yield")
    );
'''

submit_remote_query(configs, query)

# COMMAND ----------

query = '''

select  
    PL
    , platform_subset
    , Level
    , black_yield
    , color_yield
    , black_large_yield
    , color_large_yield
from df
'''
df_new = spark.sql(query)
df_new.createOrReplaceTempView("df")

# COMMAND ----------

df_new.display()

# COMMAND ----------

query = '''
select * from df
'''
spark.sql(query).count()

# COMMAND ----------

df_new.filter(col('platform_subset') == 'Pyramid 3:1').display()

# COMMAND ----------

df_new.filter(col('platform_subset') == 'HOPPER 55CTO').display()

# COMMAND ----------

df_new_black = df_new.groupBy("platform_subset").pivot("new_black").sum("black_yield")
df_new_black.createOrReplaceTempView("df_new_black")

# COMMAND ----------

df_new_black.display()

# COMMAND ----------

df_new_color = df_new.groupBy("platform_subset").pivot("new_color").sum("color_yield")
df_new_color.createOrReplaceTempView("df_new_color")

# COMMAND ----------

df_new_color.filter(col('platform_subset') == 'Azalea').display()

# COMMAND ----------

query = '''
select * from df where platform_subset = 'Pyramid 3:1'
'''
spark.sql(query).display()

# COMMAND ----------

query = '''

select t.platform_subset
    , t.pl
    , b.Black as black
    , c.Color as color
from df t
inner join df_new_black b
    on t.platform_subset = b.platform_subset
inner join df_new_color c
    on t.platform_subset = c.platform_subset
inner join toner_platform tc
    on upper(t.platform_subset) = tc.platform_subset
'''

final = spark.sql(query)

# COMMAND ----------

inner join toner_platform tc
    on t.platform_subset = tc.platform_subset

# COMMAND ----------

final.display()

# COMMAND ----------

final.filter(col('platform_subset') == 'Pyramid 3:1').display()

# COMMAND ----------


