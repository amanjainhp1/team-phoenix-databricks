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
decay = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM prod.decay") \
    .load()
yield_ = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM mdm.yield") \
    .load()
forecast_supplies_baseprod = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM fin_prod.forecast_supplies_baseprod WHERE version = '{forecast_supplies_baseprod_version}'") \
    .load()

# COMMAND ----------

## Populating delta tables
tables = [
  ['prod.usage_share' , usage_share],
  ['mdm.hardware_xref' , hardware_xref],
  ['mdm.supplies_xref' , supplies_xref],
  ['prod.decay' , decay],
  ['mdm.yield' , yield_],
  ['fin_prod.forecast_supplies_baseprod' , forecast_supplies_baseprod]
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
## to add starting cal_date before doing sum_till_date
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
## to add starting cal_date before doing sum_till_date
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

## Getting decay at year, platform_subset, geography, split_name level
query = '''select platform_subset 
            , split_name 
            , geography 
            , d.year 
            , cast(substring(d.year, 6 , 8) as integer) as year_num
            , avg(value) as value
            , max(version) as version 
           from prod.decay as d
           where official = '1'
           and record = 'HW_DECAY'
           group by platform_subset , split_name , d.year , geography
                '''
decay = spark.sql(query)
decay.createOrReplaceTempView("decay")

# COMMAND ----------

decay.display()

# COMMAND ----------

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


decay as (


	select platform_subset 
		, split_name 
		, geography 
		, year 
		, cast(substring(year, 6 , 8) as integer ) as year_num
		, avg(value) as value
		, max(version) as version 
from prod.decay
join months m 
on 1=1
where official = '1'
and record = 'HW_DECAY'
group by platform_subset 
        , split_name 
        , year
        , geography
)
select platform_subset 
		, split_name 
		, geography 
		, year
		, year_num 
		, m.month_num
		, value/12 as value
		, version 
from decay d
join months m 
on 1 = 1

'''
decay = spark.sql(query)
decay.createOrReplaceTempView("decay")

# COMMAND ----------

decay.display()

# COMMAND ----------


