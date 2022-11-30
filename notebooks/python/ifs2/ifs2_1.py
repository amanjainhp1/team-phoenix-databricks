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
  ['mdm.iso_country_code_xref' , iso_country_code_xref]
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

## Creating dataframes for usage and share drivers for ink and toner
query = '''select cal_date
                , year(add_months(cal_date,2)) as year
                , (year(add_months(cal_date,2)) - year(add_months('{}',2))) + 1 as year_num
                , geography_grain
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
                , geography_grain
                , us.platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , ib_version
                , us.version
                '''.format(start_ifs2_date , start_ifs2_date, end_ifs2_date)
ink_usage_share = spark.sql(query)
ink_usage_share_pivot = ink_usage_share.groupBy("cal_date","year","year_num","geography_grain","platform_subset","customer_engagement","hw_product_family","ib_version","us.version").pivot("measure").sum("units")
ink_usage_share_pivot.createOrReplaceTempView("ink_usage_share_pivot")

query = '''select cal_date
                , year(add_months(cal_date,2)) as year
                , (year(add_months(cal_date,2)) - year(add_months('{}',2))) + 1 as year_num
                , geography_grain
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
                , geography_grain
                , us.platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , ib_version
                , us.version
                '''.format(start_ifs2_date, start_ifs2_date, end_ifs2_date)
toner_usage_share = spark.sql(query)
toner_usage_share_pivot = toner_usage_share.groupBy("cal_date","year","year_num","geography_grain","platform_subset","customer_engagement","hw_product_family","ib_version","us.version").pivot("measure").sum("units")
toner_usage_share_pivot.createOrReplaceTempView("toner_usage_share_pivot")

# COMMAND ----------

## Aggregating usage share drivers for ink at required level
query = '''select cal_date
                , ius.year
                , year_num
                , geography_grain
                , platform_subset
                , customer_engagement
                , hw_product_family
                , COLOR_USAGE
                , SUM(COLOR_USAGE) over 
                	(partition by 
                		geography_grain
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
                		geography_grain
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
                , geography_grain
                , platform_subset
                , customer_engagement
                , hw_product_family
                , COLOR_USAGE
                , SUM(COLOR_USAGE) over 
                	(partition by 
                		geography_grain
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
                		geography_grain
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
where official = '1'
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

select platform_subset 
		, split_name 
		, m13
		, m10
		, country_alpha2
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

'''
decay = spark.sql(query)
decay.createOrReplaceTempView("decay")

# COMMAND ----------

decay.display()

# COMMAND ----------

## yield at geography, base_prod_num level
query = '''
with rn as (
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
      --,y.[active]
      --,[active_at]
      --,[inactive_at]
      --y.[load_date]
      ,version
      --,[official]
      --,[geography_grain]
  ,ROW_NUMBER() over( partition by y.base_product_number, y.geography order by y.effective_date desc) as rn

  FROM mdm.yield as y
  left join mdm.supplies_xref as sup on sup.base_product_number = y.base_product_number

  where effective_date < (add_months('{}',-1))
  --and sup.cartridge_alias = 'CEDELLA-CYN-910-A-N'
 )
  select distinct Rn.*
  from rn
  where RN.rn = 1
  order by rn.base_product_number, rn.effective_date

'''.format(start_ifs2_date)
yield_ = spark.sql(query)
yield_.createOrReplaceTempView("yield_")

# COMMAND ----------

yield_.display()

# COMMAND ----------

query = '''
select geography
        , platform_subset
        , customer_engagement
        , base_product_number
        , cal_date
        , ((avg(pgs_ccs_mix) * 3) ) as trade_split
        , version
from cartridge_demand_pages_ccs_mix
group by version 
        , platform_subset 
        , geography 
        , base_product_number 
        , cal_date 
        , customer_engagement
'''
trade_split = spark.sql(query)
trade_split.createOrReplaceTempView("trade_split")

# COMMAND ----------

trade_split.display()

# COMMAND ----------

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

fsb.display()

# COMMAND ----------

fsb.count()

# COMMAND ----------


