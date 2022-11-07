# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

dbutils.widgets.text("forecast_fin_version", "")
dbutils.widgets.text("currency_hedge_version", "")

# COMMAND ----------

forecast_fin_version = dbutils.widgets.get("forecast_fin_version")
if forecast_fin_version == "":
    forecast_fin_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM fin_prod.forecast_supplies_baseprod") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]

currency_hedge_version = dbutils.widgets.get("currency_hedge_version")
if currency_hedge_version == "":
    currency_hedge_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM prod.currency_hedge") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]

# COMMAND ----------

def get_data_by_table(table):
    df = read_redshift_to_df(configs) \
        .option("dbtable", table) \
        .load()
    
    for column in df.dtypes:
        if column[1] == 'string':
            df = df.withColumn(column[0], upper(col(column[0]))) 

    return df

# COMMAND ----------

forecast_base_pl = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.forecast_base_pl") \
    .load()
iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load()
country_currency_map = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.country_currency_map") \
    .load()
rdma = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.rdma") \
    .load()
supplies_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.supplies_xref") \
    .load()
product_line_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.product_line_xref") \
    .load()
product_line_scenarios_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.product_line_scenarios_xref") \
    .load()
calendar = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.calendar") \
    .load()
supplies_stf_landing = read_redshift_to_df(configs) \
    .option("dbtable", "stage.supplies_stf_landing") \
    .load()
forecast_fixed_cost_input = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.forecast_fixed_cost_input") \
    .load()
actuals_supplies_baseprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_supplies_baseprod") \
    .load()
currency_hedge = read_redshift_to_df(configs) \
    .option("dbtable", "prod.currency_hedge") \
    .load()
working_forecast_country = read_redshift_to_df(configs) \
    .option("query", "SELECT * FROM prod.working_forecast_country WHERE version = (SELECT max(version) from prod.working_forecast_country)") \
    .load()
version = read_redshift_to_df(configs) \
    .option("dbtable", "prod.version") \
    .load()

# COMMAND ----------

tables = [
    ['fin_stage.forecast_base_pl' ,forecast_base_pl],
    ['mdm.iso_country_code_xref' ,iso_country_code_xref],
    ['mdm.country_currency_map' ,country_currency_map],
    ['mdm.rdma' ,rdma],
    ['mdm.supplies_xref' ,supplies_xref],
    ['mdm.product_line_xref' ,product_line_xref],
    ['mdm.product_line_scenarios_xref' ,product_line_scenarios_xref],
    ['mdm.calendar' ,calendar],
    ['stage.supplies_stf_landing' ,supplies_stf_landing],
    ['fin_prod.forecast_fixed_cost_input'  ,forecast_fixed_cost_input],
    ['fin_prod.actuals_supplies_baseprod' ,actuals_supplies_baseprod],
    ['prod.currency_hedge' ,currency_hedge],
    ['prod.working_forecast_country' ,working_forecast_country],
    ['prod.version',version]
]


##'prod.working_forecast_country' ,

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

base_product_filter_vars = """


SELECT 'FIXED_COST' AS record
    , max(version) as version
FROM forecast_fixed_cost_input

UNION ALL

SELECT 'CURRENCY_HEDGE' AS record
    , MAX(version) AS version
FROM currency_hedge

UNION ALL

select 'PRODUCT_LINE_SCENARIOS' as record
    , max(version) as version
from product_line_scenarios_xref where pl_scenario = 'FINANCE-HEDGE'

"""

base_product_filter_vars = spark.sql(base_product_filter_vars)
base_product_filter_vars.createOrReplaceTempView("base_product_filter_vars")

# COMMAND ----------

forecast_supplies_baseprod_region_schema = read_redshift_to_df(configs) \
        .option("query", "SELECT * FROM fin_prod.forecast_supplies_baseprod_region where 1 = 0") \
        .load()

forecast_supplies_baseprod_region_stf_schema = read_redshift_to_df(configs) \
        .option("query", "SELECT * FROM fin_prod.forecast_supplies_baseprod_region_stf where 1 = 0") \
        .load()

stf_gru_schema = read_redshift_to_df(configs) \
        .option("query", "SELECT * FROM fin_prod.stf_gru where 1 = 0") \
        .load()

current_stf_dollarization_schema = read_redshift_to_df(configs) \
        .option("query", "SELECT * FROM fin_prod.current_stf_dollarization where 1 = 0") \
        .load()

stf_dollarization_coc_schema = read_redshift_to_df(configs) \
        .option("query", "SELECT * FROM fin_prod.stf_dollarization_coc where 1 = 0") \
        .load()

actuals_plus_stf_schema = read_redshift_to_df(configs) \
        .option("query", "SELECT * FROM fin_prod.actuals_plus_stf where 1 = 0") \
        .load()

# COMMAND ----------

forecast_supplies_baseprod_region = """


with  __dbt__CTE__bpo_03_forecast_country as (



select
			base_product_number
			, base_product_line_code
			, region_5
			, country_alpha2
			, cal_date
			, case when ((baseprod_gru is null) or (baseprod_gru = 0)) then 0
				else insights_base_units 
			end as insights_base_units
			, COALESCE(baseprod_gru, 0) as baseprod_gru
			, COALESCE(baseprod_contra_per_unit, 0) as baseprod_contra_per_unit
			, COALESCE(baseprod_revenue_currency_hedge_unit, 0) as baseprod_revenue_currency_hedge_unit
			, COALESCE(baseprod_variable_cost_per_unit, 0) as baseprod_variable_cost_per_unit
			, COALESCE(baseprod_fixed_cost_per_unit, 0) as baseprod_fixed_cost_per_unit
			, (SELECT version from version WHERE record = 'FORECAST_SUPPLIES_BASEPROD' 
				AND version = (SELECT MAX(version) FROM version WHERE record = 'FORECAST_SUPPLIES_BASEPROD')) AS version
		from
			forecast_base_pl
)select 
			'FORECAST_SUPPLIES_BASEPROD_REGION' as record
			, base_product_number
			, base_product_line_code
			, region_5
			, cal_date
			, sum(baseprod_gru * insights_base_units) / nullif(sum(insights_base_units), 0) AS baseprod_gru
			, sum(baseprod_contra_per_unit*insights_base_units) / nullif(sum(insights_base_units), 0) baseprod_contra_per_unit
			, sum(baseprod_revenue_currency_hedge_unit * insights_base_units) / nullif(sum(insights_base_units), 0) baseprod_revenue_currency_hedge_unit
			, sum((baseprod_gru - baseprod_contra_per_unit + baseprod_revenue_currency_hedge_unit) * insights_base_units) / nullif(sum(insights_base_units), 0) as baseprod_aru
			, sum(baseprod_variable_cost_per_unit * insights_base_units) / nullif(sum(insights_base_units), 0) baseprod_variable_cost_per_unit
			, sum((baseprod_gru-baseprod_contra_per_unit + baseprod_revenue_currency_hedge_unit -baseprod_variable_cost_per_unit) * insights_base_units) / nullif(sum(insights_base_units), 0) as baseprod_contribution_margin_unit
			, sum(baseprod_fixed_cost_per_unit * insights_base_units) / nullif(sum(insights_base_units), 0) baseprod_fixed_cost_per_unit
			, sum((baseprod_gru-baseprod_contra_per_unit + baseprod_revenue_currency_hedge_unit - baseprod_variable_cost_per_unit - baseprod_fixed_cost_per_unit) * insights_base_units)/nullif(sum(insights_base_units), 0) as baseprod_gross_margin_unit
			, version
		from
			__dbt__CTE__bpo_03_forecast_country
		group by
			base_product_number
			, base_product_line_code
			, region_5
			, cal_date
			, version
 """

forecast_supplies_baseprod_region = spark.sql(forecast_supplies_baseprod_region)
forecast_supplies_baseprod_region = forecast_supplies_baseprod_region_schema.union(forecast_supplies_baseprod_region)
write_df_to_redshift(configs, forecast_supplies_baseprod_region, "fin_prod.forecast_supplies_baseprod_region", "append")
forecast_supplies_baseprod_region.createOrReplaceTempView("forecast_supplies_baseprod_region")

# COMMAND ----------

submit_remote_query(configs , '''truncate table fin_prod.forecast_supplies_baseprod_region_stf''')

# COMMAND ----------

forecast_supplies_baseprod_region_stf = """


with  __dbt__CTE__bpo_06_currency_sum_stf as (


select
	currency_hedge.product_category AS pl_level_1
	,region_5
	,currency_hedge.month as cal_date
	,SUM(revenue_currency_hedge) AS revenue_currency_hedge
from currency_hedge currency_hedge
LEFT JOIN
(
select distinct iso.region_5
	,currency_iso_code
from iso_country_code_xref  iso
LEFT JOIN country_currency_map map ON map.country_alpha2 = iso.country_alpha2
where currency_iso_code is not null
) as currency_map ON currency = currency_iso_code
where currency_hedge.version = '{}'
group by
	currency_hedge.product_category
	,currency_hedge.month
	,region_5
),  __dbt__CTE__bpo_05_revenue_sum_stf as (


select 
			product_line_scenarios_xref.pl_level_1
			, region_fin_w.region_5
			, region_fin_w.cal_date
			, SUM(COALESCE(region_fin_w.baseprod_gru, 0) * COALESCE(stf.units, 0)) revenue_sum
		from
			(select region_5, cal_date, base_product_number, baseprod_gru, base_product_line_code from forecast_supplies_baseprod_region where version = '{}') region_fin_w
			INNER JOIN
			supplies_stf_landing stf
				on stf.geography = region_fin_w.region_5
				and stf.cal_date = region_fin_w.cal_date
				and stf.base_product_number = region_fin_w.base_product_number
			INNER JOIN
			product_line_scenarios_xref product_line_scenarios_xref
				on product_line_scenarios_xref.pl = region_fin_w.base_product_line_code
		where
			product_line_scenarios_xref.pl_scenario = 'FINANCE-HEDGE'
			and product_line_scenarios_xref.version = (select version from base_product_filter_vars where record = 'PRODUCT_LINE_SCENARIOS')
		group by
			product_line_scenarios_xref.pl_level_1
			, region_fin_w.region_5
			, region_fin_w.cal_date
),  __dbt__CTE__bpo_07_currency_hedge_per as (


select 
			currency.pl_level_1
		    , currency.region_5
			, currency.cal_date
			, product_line_scenarios_xref.pl as base_product_line_code
			, COALESCE(currency.revenue_currency_hedge/NULLIF(revenue.revenue_sum, 0), 0) as currency_per
		from
		__dbt__CTE__bpo_06_currency_sum_stf as currency
		INNER JOIN
		__dbt__CTE__bpo_05_revenue_sum_stf as revenue
			on currency.pl_level_1 = revenue.pl_level_1
			and currency.region_5 = revenue.region_5
			and currency.cal_date = revenue.cal_date
		INNER JOIN
		product_line_scenarios_xref product_line_scenarios_xref
				on product_line_scenarios_xref.pl_level_1 = revenue.pl_level_1
		where
			product_line_scenarios_xref.pl_scenario = 'FINANCE-HEDGE'
			and product_line_scenarios_xref.version = (select version from base_product_filter_vars where record = 'PRODUCT_LINE_SCENARIOS')
),  __dbt__CTE__bpo_08_Fixed_Revenue_Sum as (


select 
			region_fin_w.base_product_line_code
			, country_xref.region_3
			, calendar.fiscal_year_qtr
			, sum(COALESCE(region_fin_w.baseprod_gru, 0) * COALESCE(stf.units, 0)) AS revenue_sum
		from
			(select base_product_line_code, baseprod_gru, region_5, cal_date, base_product_number from forecast_supplies_baseprod_region where version = '{}') region_fin_w
			INNER JOIN
			supplies_stf_landing stf
				ON stf.geography = region_fin_w.region_5
				AND stf.cal_date = region_fin_w.cal_date
				AND stf.base_product_number = region_fin_w.base_product_number
			INNER JOIN
			iso_country_code_xref country_xref
				ON country_xref.region_5 = region_fin_w.region_5
			INNER JOIN
			calendar calendar
				ON calendar.date = region_fin_w.cal_date
		where region_3 is not null
		group by
			region_fin_w.base_product_line_code
			, country_xref.region_3
			, calendar.fiscal_year_qtr
),  __dbt__CTE__bpp_21_FixedCost_Region as (


select distinct
		pl as base_product_line_code
		, case
			when region_3 = 'EU' then 'EMEA'
			else region_3
		end as region_3
		, fiscal_yr_qtr
		, MAX(fiscal_yr_qtr) OVER (PARTITION BY pl, region_3) AS max_qtr
		, SUM(fixed_cost_k_qtr) * 1000 as fixed_cost_qtr
		, version as fin_version
	from
		forecast_fixed_cost_input
	where official = 1
	group by
		pl
		, region_3
		, fiscal_yr_qtr
		, version
),  __dbt__CTE__bpo_09_Fixed_per as (


select 
			rev_sum.base_product_line_code
			, rev_sum.region_3
			, rev_sum.fiscal_year_qtr as fiscal_yr_qtr
			, COALESCE(fixed_cost.fixed_cost_qtr, 0) / nullif(COALESCE(rev_sum.revenue_sum,0),0) AS fixed_cost_per
		from
			__dbt__CTE__bpo_08_Fixed_Revenue_Sum rev_sum
			INNER JOIN
			__dbt__CTE__bpp_21_FixedCost_Region fixed_cost
				on rev_sum.base_product_line_code = fixed_cost.base_product_line_code
				and rev_sum.region_3 = fixed_cost.region_3
				and rev_sum.fiscal_year_qtr = fixed_cost.fiscal_yr_qtr
)
select distinct
		'FORECAST_SUPPLIES_BASEPROD_STF' as record
		, base_product_number
		, fin.base_product_line_code
		, fin.region_5
		, fin.cal_date
		, avg(baseprod_gru) as baseprod_gru
		, avg(baseprod_contra_per_unit) as baseprod_contra_per_unit
		, avg((baseprod_gru * COALESCE(currency_per, 0))) as baseprod_revenue_currency_hedge_unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * COALESCE(currency_per, 0)))) as baseprod_aru
		, avg(baseprod_variable_cost_per_unit) as baseprod_variable_cost_per_unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * COALESCE(currency_per, 0)) - baseprod_variable_cost_per_unit)) as baseprod_contribution_margin_unit
		, avg(COALESCE(baseprod_gru * fixed_cost_per, 0)) as baseprod_fixed_cost_per_unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * COALESCE(currency_per, 0)) - baseprod_variable_cost_per_unit - COALESCE(baseprod_gru * fixed_cost_per, 0))) as baseprod_gross_margin_unit
		, fin.version
	from
		(select * from forecast_supplies_baseprod_region where version = '{}') fin
		INNER JOIN
		calendar calendar
			on calendar.date = fin.cal_date
		INNER JOIN
		iso_country_code_xref country_xref
			on country_xref.region_5 = fin.region_5
		LEFT JOIN
		__dbt__CTE__bpo_07_currency_hedge_per currency_hedge
			on currency_hedge.base_product_line_code = fin.base_product_line_code
			and currency_hedge.region_5 = fin.region_5
			and currency_hedge.cal_date = fin.cal_date
		LEFT JOIN
		__dbt__CTE__bpo_09_Fixed_per fixed
			on fixed.base_product_line_code = fin.base_product_line_code
			and fixed.region_3 = country_xref.region_3
			and calendar.fiscal_year_qtr = fixed.fiscal_yr_qtr
	where baseprod_gru is not null 
	GROUP BY
		base_product_number
		, fin.base_product_line_code
		, fin.region_5
		, fin.cal_date
		, fin.version
        
""".format(currency_hedge_version,forecast_fin_version,forecast_fin_version,forecast_fin_version)


forecast_supplies_baseprod_region_stf = spark.sql(forecast_supplies_baseprod_region_stf)
forecast_supplies_baseprod_region_stf = forecast_supplies_baseprod_region_stf_schema.union(forecast_supplies_baseprod_region_stf)
write_df_to_redshift(configs, forecast_supplies_baseprod_region_stf, "fin_prod.forecast_supplies_baseprod_region_stf", "append")
forecast_supplies_baseprod_region_stf.createOrReplaceTempView("forecast_supplies_baseprod_region_stf")

# COMMAND ----------

submit_remote_query(configs , '''truncate table fin_prod.stf_gru''')

# COMMAND ----------

stf_gru = """


SELECT 
	fin.base_product_number
	, fin.base_product_line_code
	, fin.region_5
	, fin.cal_date
	, fin.BaseProd_GRU
FROM forecast_supplies_baseprod_region_stf fin
where fin.version = '{}'
""".format(forecast_fin_version)

stf_gru = spark.sql(stf_gru)
stf_gru = stf_gru_schema.union(stf_gru)
write_df_to_redshift(configs, stf_gru, "fin_prod.stf_gru", "append")
stf_gru.createOrReplaceTempView("stf_gru")

# COMMAND ----------

submit_remote_query(configs , '''truncate table fin_prod.current_stf_dollarization''')

# COMMAND ----------

current_stf_dollarization =  """


with __dbt__CTE__bpo_30_Insights_Units as (


select
		base_product_number
		, region_5
		, cal_date
		, SUM(cartridges) cartridges
	from
		working_forecast_country c2c
		inner join
		iso_country_code_xref ctry
			on ctry.country_alpha2 = c2c.country
	where c2c.version = (select max(version) from working_forecast_country)
	group by
		base_product_number
		, region_5
		, cal_date
)select distinct
	stf.geography
	, stf.base_product_number
	, rdma.pl
	, supplies_xref.technology
	, stf.cal_date
	, stf.units
	, coalesce((stf.units*supplies_xref.equivalents_multiplier), stf.units) as equivalent_units
	, (fin.baseprod_gru * stf.units) as gross_revenue
	, (fin.baseprod_contra_per_unit * stf.units) as contra
	, (fin.baseprod_revenue_currency_hedge_unit * stf.units) as revenue_currency_hedge
	, (fin.baseprod_aru * stf.units) as net_revenue
	, (fin.baseprod_variable_cost_per_unit * stf.units) as variable_cost
	, (fin.baseprod_contribution_margin_unit * stf.units) as contribution_margin
	, (fin.baseprod_fixed_cost_per_unit * stf.units) as fixed_Cost
	, ((fin.baseprod_variable_cost_per_unit * stf.units)+(fin.baseprod_fixed_cost_per_unit * stf.units)) as total_cos
	, (fin.baseprod_gross_margin_unit * stf.units) as gross_margin
	, c2c.cartridges as insights_units
	, stf.username

from 
	supplies_stf_landing stf
	left join
	forecast_supplies_baseprod_region_stf fin
		on fin.base_product_number = stf.base_product_number
		and fin.region_5 = stf.geography
		and fin.cal_date = stf.cal_date
		and fin.version = '{}'
	left join
	rdma rdma
		on rdma.base_prod_number = stf.base_product_number
	inner join
	iso_country_code_xref ctry
		on ctry.region_5 = stf.geography
	left join
	__dbt__CTE__bpo_30_Insights_Units c2c
		on c2c.base_product_number = stf.base_product_number
		and c2c.region_5 = stf.geography
		and c2c.cal_date = stf.cal_date
	left join
	supplies_xref supplies_xref
		on supplies_xref.base_product_number = stf.base_product_number
""".format(forecast_fin_version)


current_stf_dollarization = spark.sql(current_stf_dollarization)
current_stf_dollarization = current_stf_dollarization.withColumn("contractual_discounts" , (lit(None).cast(StringType())).cast(DoubleType())) \
                .withColumn("discretionary_discounts" , (lit(None).cast(StringType())).cast(DoubleType()))
current_stf_dollarization = current_stf_dollarization.select('geography','base_product_number','pl','technology','cal_date','units','equivalent_units','gross_revenue','contractual_discounts','discretionary_discounts','contra','revenue_currency_hedge','net_revenue','variable_cost','contribution_margin','fixed_cost','total_cos','gross_margin','insights_units','username')
current_stf_dollarization = current_stf_dollarization_schema.union(current_stf_dollarization)
write_df_to_redshift(configs, current_stf_dollarization, "fin_prod.current_stf_dollarization", "append")
current_stf_dollarization.createOrReplaceTempView("current_stf_dollarization")

# COMMAND ----------

##To be checked

#%run ./supplies_stf_promote

# COMMAND ----------

stf_dollarization = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.stf_dollarization") \
    .load()

stf_dollarization.createOrReplaceTempView("stf_dollarization")

# COMMAND ----------

submit_remote_query(configs , '''truncate table fin_prod.stf_dollarization_coc''')

# COMMAND ----------

stf_dollarization_coc = """


SELECT
	 record
	, geography
	, base_product_number
	, pl
	, UPPER(technology) AS technology
	, cal_date
	, units
	, equivalent_units
	, gross_revenue
	, contra
	, revenue_currency_hedge
	, net_revenue
	, variable_cost
	, contribution_margin
	, fixed_cost
	, gross_Margin
	, insights_units
	, version
	, load_date
	, username
FROM stf_dollarization

UNION ALL

SELECT 
    'SUPPLIES_STF' as record
    , geography
	, base_product_number
	, pl
	, UPPER(technology) AS technology
	, cal_date
	, units
	, equivalent_units
	, gross_revenue
	, contra
	, revenue_currency_hedge
	, net_revenue
	, variable_cost
	, contribution_margin
	, fixed_cost
	, gross_Margin
	, insights_units
    , 'WORKING VERSION' as version
    , current_date() as load_date
    , username
FROM current_stf_dollarization
"""

stf_dollarization_coc = spark.sql(stf_dollarization_coc)
stf_dollarization_coc = stf_dollarization_coc_schema.union(stf_dollarization_coc)
write_df_to_redshift(configs, stf_dollarization_coc, "fin_prod.stf_dollarization_coc", "append")
stf_dollarization_coc.createOrReplaceTempView("stf_dollarization_coc")

# COMMAND ----------

submit_remote_query(configs , '''truncate table fin_prod.actuals_plus_stf''')

# COMMAND ----------

#check columns
actuals_plus_stf = """


with __dbt__CTE__bpo_32_actuals_Fiscal_Yr as (



SELECT 
			cast(fiscal_yr as int) - 2 start_fiscal_yr
		FROM
	calendar
		WHERE date = current_date()
),  __dbt__CTE__bpo_33_actuals as (



SELECT region_5 as geography
    ,base_product_number
    ,bp.pl
    ,technology
    ,cal_date
    , sum(revenue_units) as units
    , sum(equivalent_units) as equivalent_units
    , SUM(gross_revenue) AS gross_revenue
    , SUM(contractual_discounts) AS contractual_discounts
    , SUM(discretionary_discounts) AS discretionary_discounts
    , SUM(contractual_discounts) + sum(discretionary_discounts) as contra
    , SUM(net_currency) AS revenue_currency_hedge
    , SUM(net_revenue) AS net_revenue
    , NULL as variable_cost
    , NULL as contribution_margin
    , NULL as fixed_cost
    , SUM(total_cos) AS total_cos
    , SUM(gross_profit) AS gross_margin
    , Sum(revenue_units) as insights_units
    , NULL as username
FROM actuals_supplies_baseprod bp
LEFT JOIN iso_country_code_xref iso ON bp.country_alpha2 = iso.country_alpha2
LEFT JOIN product_line_xref plx ON bp.pl = plx.pl
LEFT JOIN calendar cal ON cal.Date = cal_Date
WHERE 1=1
    and day_of_month = 1
    and fiscal_yr > (select start_fiscal_yr from __dbt__CTE__bpo_32_actuals_Fiscal_Yr)
    and bp.pl NOT IN ('1N', 'GD', 'GM', 'LU', 'K6', 'UD', 'HF', '65')
    and customer_engagement <> 'EST_DIRECT_FULFILLMENT'
group by region_5
    , base_product_number
    , bp.pl
    , cal_date
    , technology
)select
		'ACTUALS' as record
		, geography
		, base_product_number
		, pl
		, technology
		, cal_date
		, units
		, equivalent_units
		, gross_revenue
		, contractual_discounts
		, discretionary_discounts
		, contra
		, revenue_currency_hedge
		, net_revenue
		, variable_cost
		, contribution_margin
		, fixed_cost
		, total_cos
		, gross_margin
		, insights_units
		, cast(username as string) as username
	from 
		__dbt__CTE__bpo_33_actuals

	UNION ALL

	select
		'STF' as record
		, geography
		, base_product_number
		, pl
		, technology
		, cal_date
		, units
		, equivalent_units
		, gross_revenue
		, contractual_discounts
		, discretionary_discounts
		, contra
		, revenue_currency_hedge
		, net_revenue
		, variable_cost
		, contribution_margin
		, fixed_cost
		, total_cos
		, gross_margin
		, insights_units
		, username
	from 
		current_stf_dollarization
        
"""

actuals_plus_stf = spark.sql(actuals_plus_stf)
actuals_plus_stf = actuals_plus_stf_schema.union(actuals_plus_stf)
write_df_to_redshift(configs, actuals_plus_stf, "fin_prod.actuals_plus_stf", "append")
actuals_plus_stf.createOrReplaceTempView("actuals_plus_stf")

# COMMAND ----------


