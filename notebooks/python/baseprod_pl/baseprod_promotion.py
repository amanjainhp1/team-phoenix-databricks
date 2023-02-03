# Databricks notebook source
dbutils.widgets.text("currency_hedge_version", "")

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

max_info = call_redshift_addversion_sproc(configs, 'FORECAST_SUPPLIES_BASEPROD', 'FORECAST_SUPPLIES_BASEPROD')

forecast_fin_version = max_info[0]
forecast_fin_load_date = (max_info[1])

# COMMAND ----------

max_info = call_redshift_addversion_sproc(configs, 'ACTUALS_PLUS_FORECAST_FINANCIALS', 'ACTUALS_PLUS_FORECAST_FINANCIALS')

actuals_plus_forecast_financials_version = max_info[0]
actuals_plus_forecast_financials_load_date = (max_info[1])

# COMMAND ----------

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

iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load()
country_currency_map = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.country_currency_map") \
    .load()
yield_ = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.yield") \
    .load()
supplies_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.supplies_xref") \
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
adjusted_revenue_epa = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.adjusted_revenue_epa") \
    .load()
forecast_base_pl = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.forecast_base_pl") \
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
trade_forecast = read_redshift_to_df(configs) \
    .option("dbtable", "prod.trade_forecast") \
    .load()
product_line_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.product_line_xref") \
    .load()

# COMMAND ----------

tables = [
  ['mdm.iso_country_code_xref' ,iso_country_code_xref],
  ['mdm.country_currency_map' ,country_currency_map],
  ['mdm.yield' ,yield_],
  ['mdm.supplies_xref',supplies_xref],
  ['mdm.product_line_scenarios_xref' ,product_line_scenarios_xref],
  ['mdm.calendar' ,calendar],
  ['mdm.product_line_xref' ,product_line_xref],
  ['stage.supplies_stf_landing' ,supplies_stf_landing],
  ['fin_prod.forecast_fixed_cost_input' ,forecast_fixed_cost_input],
  ['fin_prod.actuals_supplies_baseprod' ,actuals_supplies_baseprod],
  ['fin_prod.adjusted_revenue_epa' ,adjusted_revenue_epa],
  ['fin_stage.forecast_base_pl' ,forecast_base_pl],
  ['prod.currency_hedge' ,currency_hedge],
  ['prod.working_forecast_country' ,working_forecast_country],
  ['prod.version' ,version],
  ['prod.trade_forecast' ,trade_forecast]  
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

forecast_supplies_baseprod_schema = read_redshift_to_df(configs) \
        .option("query", "SELECT * FROM fin_prod.forecast_supplies_baseprod where 1 = 0") \
        .load()


forecast_supplies_baseprod_mkt10_schema = read_redshift_to_df(configs) \
        .option("query", "SELECT * FROM fin_prod.forecast_supplies_baseprod_mkt10 where 1 = 0") \
        .load()

actuals_plus_forecast_financials_schema = read_redshift_to_df(configs) \
        .option("query", "SELECT * FROM fin_prod.actuals_plus_forecast_financials where 1 = 0") \
        .load()

# COMMAND ----------

forecast_supplies_baseprod = """


SELECT 'FORECAST_SUPPLIES_BASEPROD' AS record
      ,base_product_number
      ,base_product_line_code
      ,region_5
      ,country_alpha2
      ,cal_date
      ,insights_base_units
      ,baseprod_gru
      ,baseprod_contra_per_unit
      ,baseprod_revenue_currency_hedge_unit
      ,baseprod_variable_cost_per_unit
      ,baseprod_fixed_cost_per_unit
      , cast('{}' as timestamp) as load_date
      , '{}'  as version
	  , sales_gru_version
	  , contra_version
	  , variable_cost_version
	  , fixed_cost_version
	  , currency_hedge_version		
FROM forecast_base_pl
""".format(forecast_fin_load_date,forecast_fin_version)

forecast_supplies_baseprod = spark.sql(forecast_supplies_baseprod)
forecast_supplies_baseprod = forecast_supplies_baseprod_schema.union(forecast_supplies_baseprod)
write_df_to_redshift(configs, forecast_supplies_baseprod, "fin_prod.forecast_supplies_baseprod", "append")
forecast_supplies_baseprod.createOrReplaceTempView("forecast_supplies_baseprod")

# COMMAND ----------

forecast_supplies_baseprod_mkt10 = """


with __dbt__CTE__bpo_01_filter_vars as (



SELECT max(version) as version
, 'FORECAST_SUPPLIES_BASEPROD' as record
from forecast_supplies_baseprod
),  __dbt__CTE__bpo_03_forecast_country as (



select
			base_product_number
			, base_product_line_code
			, region_5
			, country_alpha2
			, cal_date
			, case when ((baseprod_gru is null) or (baseprod_gru = 0)) then 0
				else insights_base_units 
			end as insights_base_units
			, coalesce(baseprod_gru, 0) as baseprod_gru
			, coalesce(baseprod_contra_per_unit, 0) as baseprod_contra_per_unit
			, coalesce(baseprod_revenue_currency_hedge_unit, 0) as baseprod_revenue_currency_hedge_unit
			, coalesce(baseprod_variable_cost_per_unit, 0) as baseprod_variable_cost_per_unit
			, coalesce(baseprod_fixed_cost_per_unit, 0) as baseprod_fixed_cost_per_unit
			, (SELECT version from version WHERE record = 'FORECAST_SUPPLIES_BASEPROD' 
				AND version = (SELECT MAX(version) FROM version WHERE record = 'FORECAST_SUPPLIES_BASEPROD')) AS version
		from
			forecast_base_pl
),  __dbt__CTE__bpo_11_mkt10_financials_working as (



select 
			base_product_number
			, base_product_line_code
			, country_xref.market10
			, cal_date
			, sum(baseprod_gru * insights_base_units)/nullif(sum(insights_base_units), 0) BaseProd_GRU
			, sum(baseprod_contra_per_unit*insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_contra_per_unit
			, sum(baseprod_revenue_currency_hedge_unit * insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_revenue_currency_hedge_unit
			, sum(baseprod_variable_cost_per_unit * insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_variable_cost_per_unit
			, sum(baseprod_fixed_cost_per_unit * insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_fixed_cost_per_unit
			, fin.version
		from
			__dbt__CTE__bpo_03_forecast_country fin
			inner join
			iso_country_code_xref country_xref
				on fin.country_alpha2 = country_xref.country_alpha2
		group by
			base_product_number
			, base_product_line_code
			, country_xref.market10
			, cal_date
			, fin.version
),  __dbt__CTE__bpo_12_revenue_sum_trade as (



select 
			mkt_fin_w.base_product_line_code
			, mkt_fin_w.market10
			, mkt_fin_w.cal_date
			, sum(coalesce(mkt_fin_w.baseprod_gru, 0) * coalesce(trade.cartridges, 0)) revenue_sum
		from
			__dbt__CTE__bpo_11_mkt10_financials_working mkt_fin_w
			inner join
			trade_forecast trade
				on trade.market10 = mkt_fin_w.market10
				and trade.cal_date = mkt_fin_w.cal_date
				and trade.base_product_number = mkt_fin_w.base_product_number
		where 1=1
		group by
			mkt_fin_w.base_product_line_code
			, mkt_fin_w.market10
			, mkt_fin_w.cal_date
),  __dbt__CTE__bpo_13_currency_sum_trade as (


select
	plx.pl as base_product_line_code
	,market10
	,currency_hedge.month as cal_date
	,sum(revenue_currency_hedge) as revenue_currency_hedge
from currency_hedge currency_hedge
left join product_line_xref plx 
	on currency_hedge.profit_center = plx.profit_center_code
left join
(
select distinct country_ref.market10
	,currency_iso_code
from iso_country_code_xref  country_ref
left join country_currency_map map ON map.country_alpha2 = country_ref.country_alpha2
where currency_iso_code is not null
) as currency_map ON currency = currency_iso_code
where currency_hedge.version = '{}'
group by
	plx.pl
	,currency_hedge.month
	,market10
),  __dbt__CTE__bpo_14_currency_hedge_per_trade as (



select 
			currency.base_product_line_code
		    , currency.market10
			, currency.cal_date
			, currency.revenue_currency_hedge/revenue.revenue_sum as currency_per
		from
		__dbt__CTE__bpo_12_revenue_sum_trade as revenue
		inner join
		__dbt__CTE__bpo_13_currency_sum_trade as currency
			on currency.base_product_line_code = revenue.base_product_line_code
			and currency.market10 = revenue.market10
			and currency.cal_date = revenue.cal_date
        where 1=1
),  __dbt__CTE__bpo_15_Fixed_Revenue_Sum_trade as (



select 
			mkt_fin_w.base_product_line_code
			, country_xref.region_3
			, calendar.fiscal_year_qtr as fiscal_yr_qtr
			, sum(coalesce(mkt_fin_w.baseprod_gru, 0) * coalesce(trade.cartridges, 0)) Revenue_sum
		from
			__dbt__CTE__bpo_11_mkt10_financials_working mkt_fin_w
			inner join
			trade_forecast trade
				on trade.market10 = mkt_fin_w.market10
				and trade.cal_date = mkt_fin_w.cal_date
				and trade.base_product_number = mkt_fin_w.base_product_number
			inner join
			iso_country_code_xref country_xref
				on country_xref.market10 = mkt_fin_w.market10
			inner join
			calendar calendar
				on calendar.date = mkt_fin_w.cal_date
		where region_3 is not null
		group by
			mkt_fin_w.base_product_line_code
			, country_xref.region_3
			, calendar.Fiscal_Year_Qtr
),  __dbt__CTE__bpp_21_FixedCost_Region as (


select distinct
		pl as base_product_line_code
		, case
			when region_3 = 'EU' then 'EMEA'
			else region_3
		end as region_3
		, fiscal_yr_qtr
		, max(fiscal_yr_qtr) over (partition by pl, region_3) as max_qtr
		, sum(fixed_cost_k_qtr) * 1000 as fixed_cost_qtr
		, version as Fin_version
	from
		forecast_fixed_cost_input
	where official = 1
	group by
		pl
		, region_3
		, fiscal_yr_qtr
		, version
),  __dbt__CTE__bpo_16_Fixed_per_trade as (



select 
			rev_sum.base_product_line_code
			, rev_sum.region_3
			, rev_sum.fiscal_yr_qtr
			, coalesce(rev_sum.Revenue_Sum,0) /nullif(coalesce(fixed_cost.fixed_cost_qtr, 0),0) fixed_cost_per
		from
			__dbt__CTE__bpo_15_Fixed_Revenue_Sum_trade Rev_Sum
			inner join
			__dbt__CTE__bpp_21_FixedCost_Region fixed_cost
				on Rev_Sum.base_product_line_code = fixed_cost.base_product_line_code
				and Rev_Sum.region_3 = fixed_cost.region_3
				and Rev_Sum.fiscal_yr_qtr = fixed_cost.fiscal_yr_qtr
)select
		'FORECAST_SUPPLIES_BASEPROD_MARKET10' as record
		, fin.base_product_number
		, fin.base_product_line_code
		, fin.market10
		, fin.cal_date
		, avg(baseprod_gru) as baseprod_gru
		, avg(baseprod_contra_per_unit) as baseprod_contra_per_unit
		, avg((baseprod_gru * coalesce(currency_per, 0))) as baseprod_revenue_currency_hedge_Unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * coalesce(currency_per, 0)))) as baseprod_aru
		, avg(baseprod_variable_cost_per_unit) as baseprod_variable_cost_per_unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * coalesce(currency_per, 0)) - baseprod_variable_cost_per_unit)) as baseprod_contribution_margin_Unit
		, avg(coalesce(baseprod_gru * fixed_cost_per, 0)) as baseprod_fixed_cost_per_unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * coalesce(currency_per, 0)) - baseprod_variable_cost_per_unit - coalesce(baseprod_gru * fixed_cost_per, 0))) as baseprod_gross_margin_unit
		, fin.version
	from
		__dbt__CTE__bpo_11_mkt10_financials_working fin
		inner join
		calendar calendar
			on calendar.date = fin.cal_date
		left join
		iso_country_code_xref country_xref
			on country_xref.market10 = fin.market10
		left join
		__dbt__CTE__bpo_14_currency_hedge_per_trade currency_hedge
			on currency_hedge.base_product_line_code = fin.base_product_line_code
			and currency_hedge.market10 = fin.market10
			and currency_hedge.cal_date = fin.cal_date
		left join
		__dbt__CTE__bpo_16_Fixed_per_trade fixed
			on fixed.base_product_line_code = fin.base_product_line_code
			and fixed.region_3 = country_xref.region_3
			and calendar.fiscal_year_qtr = fixed.Fiscal_yr_qtr

	where baseprod_gru is not null
	group by
		base_product_number
		, fin.base_product_line_code
		, fin.market10
		, fin.cal_date
		, fin.version""".format(currency_hedge_version)

forecast_supplies_baseprod_mkt10 = spark.sql(forecast_supplies_baseprod_mkt10)
forecast_supplies_baseprod_mkt10 = forecast_supplies_baseprod_mkt10_schema.union(forecast_supplies_baseprod_mkt10)
write_df_to_redshift(configs, forecast_supplies_baseprod_mkt10, "fin_prod.forecast_supplies_baseprod_mkt10", "append")
forecast_supplies_baseprod_mkt10.createOrReplaceTempView("forecast_supplies_baseprod_mkt10")

# COMMAND ----------

submit_remote_query(configs , '''truncate table fin_prod.actuals_plus_forecast_financials''')

# COMMAND ----------

actuals_plus_forecast_financials = """

with __dbt__CTE__bpo_19_actuals as (

SELECT distinct
			actuals_supplies_baseprod.base_product_number
			, actuals_supplies_baseprod.platform_subset
			, case 
				when actuals_supplies_baseprod.customer_engagement = 'EST_INDIRECT_FULFILLMENT' then 'TRAD'
				else actuals_supplies_baseprod.customer_engagement
			end as customer_engagement
			, actuals_supplies_baseprod.pl base_product_line_code
			, actuals_supplies_baseprod.country_alpha2
			, actuals_supplies_baseprod.cal_date
			, COALESCE(actuals_supplies_baseprod.revenue_units, 0) as revenue_units
			, COALESCE(actuals_supplies_baseprod.equivalent_units, 0) as equivalent_units
			, COALESCE(actuals_supplies_baseprod.yield_x_units, 0) as yield_x_units
			, COALESCE(actuals_supplies_baseprod.yield_x_units_black_only, 0) as yield_x_units_black_only
			, COALESCE(actuals_supplies_baseprod.gross_revenue, 0) as gross_revenue
			, COALESCE(actuals_supplies_baseprod.net_revenue, 0) as net_revenue
			, COALESCE(actuals_supplies_baseprod.contractual_discounts + actuals_supplies_baseprod.discretionary_discounts, 0) as contra
			, COALESCE(actuals_supplies_baseprod.total_cos, 0) as total_cos
			, COALESCE(actuals_supplies_baseprod.gross_profit, 0) as gross_profit
			, actuals_supplies_baseprod.version as financials_version
		FROM actuals_supplies_baseprod actuals_supplies_baseprod
		where 1=1
		and actuals_supplies_baseprod.customer_engagement <> 'EST_INDIRECT_FULFILLMENT'
), __dbt__CTE__bpo_20_actuals_sum as (
    
    
    SELECT 
			actuals.base_product_number
			, actuals.platform_subset
			, actuals.customer_engagement 
			, actuals.base_product_line_code
			, actuals.country_alpha2
			, actuals.cal_date
			, financials_version
			, sum(actuals.revenue_units) as revenue_units
			, sum(actuals.equivalent_units) as equivalent_units
			, sum(actuals.yield_x_units) as yield_x_units
			, sum(actuals.yield_x_units_black_only) as yield_x_units_black_only
			, sum(actuals.gross_revenue) as gross_revenue
			, sum(actuals.net_revenue) as net_revenue
			, sum(actuals.contra) as contra
			, sum(actuals.total_cos) as total_cos
			, sum(gross_profit) as gross_profit
		FROM __dbt__CTE__bpo_19_actuals actuals
		group by
			actuals.base_product_number
			, actuals.platform_subset
			, actuals.customer_engagement 
			, actuals.base_product_line_code
			, actuals.country_alpha2
			, actuals.cal_date
			, actuals.financials_version

), __dbt__CTE__bpo_21_adjusted_rev as (



SELECT distinct
			adjusted_revenue.base_product_number
			, adjusted_revenue.platform_subset
			, case 
				when adjusted_revenue.customer_engagement = 'EST_INDIRECT_FULFILLMENT' then 'TRAD'
				else adjusted_revenue.customer_engagement
			end as customer_engagement
			, adjusted_revenue.pl base_product_line_code
			, adjusted_revenue.country_alpha2
			, adjusted_revenue.cal_date
			, COALESCE(adjusted_revenue.cc_net_revenue,0) as cc_net_revenue
			, COALESCE(adjusted_revenue.cc_inventory_impact, 0) as cc_inventory_impact
			, COALESCE(adjusted_revenue.adjusted_revenue, 0) as adjusted_revenue
		FROM adjusted_revenue_epa adjusted_revenue
        where adjusted_revenue.customer_engagement <> 'EST_INDIRECT_FULFILLMENT'
		and adjusted_revenue.version = (select max(version) from adjusted_revenue_epa)
),  __dbt__CTE__bpo_22_adjusted_rev_sum as (



SELECT distinct
			adjusted_rev.base_product_number
			, adjusted_rev.platform_subset
			, adjusted_rev.customer_engagement 
			, adjusted_rev.base_product_line_code
			, adjusted_rev.country_alpha2
			, adjusted_rev.cal_date
			, sum(adjusted_rev.cc_net_revenue) as cc_net_revenue
			, sum(adjusted_rev.cc_inventory_impact) as cc_inventory_impact
			, sum(adjusted_rev.adjusted_revenue) as adjusted_revenue
		FROM __dbt__CTE__bpo_21_adjusted_rev adjusted_rev
		group by
			adjusted_rev.base_product_number
			, adjusted_rev.platform_subset
			, adjusted_rev.customer_engagement 
			, adjusted_rev.base_product_line_code
			, adjusted_rev.country_alpha2
			, adjusted_rev.cal_date
),  __dbt__CTE__bpo_18_Fiscal_Yr_Filter as (



SELECT cast(fiscal_yr as int) - 5 start_fiscal_yr
        , cast(fiscal_yr as int) + 5 end_fiscal_yr
		FROM
		calendar
		WHERE Date = current_date()
),  __dbt__CTE__bpo_23_supplies_baseprod_actuals as (



SELECT 
			'ACTUALS' AS record_type
			, actuals_supplies_baseprod.base_product_number
			, actuals_supplies_baseprod.platform_subset
			, actuals_supplies_baseprod.customer_engagement
			, actuals_supplies_baseprod.base_product_line_code
			, supplies_xref.technology
			, iso_country_code_xref.region_5
			, iso_country_code_xref.country_alpha2
			, iso_country_code_xref.market10
			, actuals_supplies_baseprod.cal_date
			, calendar.fiscal_year_qtr
			, calendar.fiscal_year_half
			, calendar.fiscal_yr
			, financials_version
			, SUM(COALESCE(actuals_supplies_baseprod.revenue_units, 0)) units
			, SUM(COALESCE(actuals_supplies_baseprod.equivalent_units, 0)) equivalent_units
			, SUM(COALESCE(actuals_supplies_baseprod.yield_x_units, 0)) yield_x_units
			, SUM(COALESCE(actuals_supplies_baseprod.yield_x_units_black_only, 0)) yield_x_units_black_only
			, SUM(COALESCE(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0)) shipment_units
			, SUM(COALESCE(cartridge_demand_c2c_country_splits.cartridges,0)) demand_units
			, SUM(COALESCE(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0) - COALESCE(cartridge_demand_c2c_country_splits.cartridges, 0)) vtc
			, SUM(COALESCE(actuals_supplies_baseprod.gross_revenue, 0))/nullif(sum(COALESCE(actuals_supplies_baseprod.revenue_units, 0)),0) baseprod_gru
			, SUM(COALESCE(actuals_supplies_baseprod.net_revenue, 0))/nullif(sum(COALESCE(revenue_units, 0)),0) baseprod_aru
			, SUM(COALESCE(actuals_supplies_baseprod.contra, 0))/nullif(sum(COALESCE(revenue_units, 0)), 0) contra_per_unit
			, SUM(COALESCE(actuals_supplies_baseprod.total_cos, 0))/nullif(sum(COALESCE(revenue_units, 0)), 0) cos_per_unit
			, SUM(COALESCE(actuals_supplies_baseprod.gross_profit, 0))/nullif(sum(COALESCE(revenue_units, 0)), 0) gpu
			, SUM(COALESCE(adjusted_revenue.cc_net_revenue, 0)) cc_net_revenue
			, SUM(COALESCE(adjusted_revenue.cc_inventory_impact, 0)) cc_inventory_impact
			, SUM(COALESCE(adjusted_revenue.adjusted_revenue, 0)) adjusted_revenue
		FROM __dbt__CTE__bpo_20_actuals_sum actuals_supplies_baseprod
		LEFT JOIN
		__dbt__CTE__bpo_22_adjusted_rev_sum adjusted_revenue
			ON adjusted_revenue.base_product_number = actuals_supplies_baseprod.base_product_number
			and adjusted_revenue.cal_date = actuals_supplies_baseprod.cal_date
			and adjusted_revenue.country_alpha2 = actuals_supplies_baseprod.country_alpha2
			and adjusted_revenue.platform_subset = actuals_supplies_baseprod.platform_subset
			and UPPER(adjusted_revenue.customer_engagement) = UPPER(actuals_supplies_baseprod.customer_engagement)
		LEFT JOIN
		(select base_product_number, country, cal_date, platform_subset, customer_engagement, imp_corrected_cartridges, cartridges from working_forecast_country  where version = (select max(version) from working_forecast_country)) as cartridge_demand_c2c_country_splits
			ON cartridge_demand_c2c_country_splits.base_product_number = actuals_supplies_baseprod.base_product_number
			and cartridge_demand_c2c_country_splits.country = actuals_supplies_baseprod.country_alpha2
			and cartridge_demand_c2c_country_splits.cal_date = actuals_supplies_baseprod.cal_date
			and cartridge_demand_c2c_country_splits.platform_subset = actuals_supplies_baseprod.platform_subset
			and cartridge_demand_c2c_country_splits.customer_engagement = upper(actuals_supplies_baseprod.customer_engagement)
		LEFT JOIN
		supplies_xref supplies_xref
			ON supplies_xref.base_product_number = actuals_supplies_baseprod.base_product_number
		LEFT JOIN
		iso_country_code_xref iso_country_code_xref
			ON iso_country_code_xref.country_alpha2 = actuals_supplies_baseprod.country_alpha2
		CROSS JOIN
		__dbt__CTE__bpo_18_Fiscal_Yr_Filter fiscal
		INNER JOIN
		calendar calendar
			ON calendar.date = actuals_supplies_baseprod.cal_date
			AND calendar.fiscal_Yr >= fiscal.start_fiscal_yr
		GROUP BY 
			actuals_supplies_baseprod.base_product_number
			, actuals_supplies_baseprod.platform_subset
			, actuals_supplies_baseprod.customer_engagement
			, actuals_supplies_baseprod.base_product_line_code 
			, supplies_xref.technology
			, iso_country_code_xref.region_5
			, iso_country_code_xref.country_alpha2
			, iso_country_code_xref.market10
			, actuals_supplies_baseprod.cal_date
			, calendar.fiscal_year_qtr
			, calendar.fiscal_year_half
			, calendar.fiscal_yr
			, financials_version
), __dbt__CTE__bpo_25_yields as (



SELECT 
			base_product_number,
			geography AS region_5,
			-- NOTE: assumes effective_date is in YYYYMM format. Multiplying by 100 and adding 1 to get to YYYYMMDD
			effective_date,
			COALESCE(LEAD(effective_date) OVER (PARTITION BY base_product_number, geography ORDER BY effective_date),
			CAST('2099-08-30' AS DATE)) AS next_effective_date,
			yield.value AS yield
		FROM yield
		WHERE official = 1	
		AND geography_grain = 'REGION_5'
),  __dbt__CTE__bpo_24_sub_months as (



SELECT date AS cal_date					
			FROM mdm.calendar
			WHERE day_of_month = 1
			and Date >= (select min(cal_date) from forecast_supplies_baseprod where
					version = '{}')
			and Date <= (select max(cal_date) from forecast_supplies_baseprod where
					version = '{}')
),  __dbt__CTE__bpo_26_sub_yields as (



SELECT 
			base_product_number,
			sub_months.cal_date,
			region_5,
			yield
		FROM __dbt__CTE__bpo_25_yields yields
		JOIN __dbt__CTE__bpo_24_sub_months sub_months
			ON yields.effective_date <= sub_months.cal_date
			AND yields.next_effective_date > sub_months.cal_date
), __dbt__CTE__bpo_27_supplies_baseprod_forecast as (
    
    
SELECT 
			'FORECAST' AS record_type
			, forecast_supplies_baseprod.base_product_number 
			, cartridge_demand_c2c_country_splits.platform_subset
			, cartridge_demand_c2c_country_splits.customer_engagement
			, forecast_supplies_baseprod.base_product_line_code 
			, supplies_xref.technology
			, iso_country_code_xref.region_5
			, iso_country_code_xref.country_alpha2
			, iso_country_code_xref.market10
			, forecast_supplies_baseprod.cal_date
			, calendar.fiscal_year_qtr
			, calendar.fiscal_year_half
			, calendar.fiscal_yr
			, '{}' as financials_version
			, COALESCE(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0) units
			, COALESCE(cartridge_demand_c2c_country_splits.imp_corrected_cartridges * supplies_xref.equivalents_multiplier, 0) equivalent_units
			, COALESCE(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0) shipment_units
			, COALESCE(cartridge_demand_c2c_country_splits.cartridges, 0) demand_units
			, yield * cartridge_demand_c2c_country_splits.imp_corrected_cartridges AS yield_x_units,
						CASE	
							WHEN k_color != 'BLACK' THEN NULL
							ELSE cartridge_demand_c2c_country_splits.imp_corrected_cartridges * yield
						END AS yield_x_units_black_only
			, (COALESCE(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0) - COALESCE(cartridge_demand_c2c_country_splits.cartridges, 0)) vtc
			, COALESCE(forecast_supplies_baseprod.baseprod_gru,0) as baseprod_gru
			, (COALESCE(forecast_supplies_baseprod.baseprod_gru, 0) - COALESCE(forecast_supplies_baseprod.baseprod_contra_per_unit, 0) + COALESCE(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0)) baseprod_aru
			, ((COALESCE(forecast_supplies_baseprod.baseprod_gru, 0) - COALESCE(forecast_supplies_baseprod.baseprod_contra_per_unit, 0) + COALESCE(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0)) * COALESCE(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0)) cc_net_revenue
			, COALESCE(forecast_supplies_baseprod.baseprod_contra_per_unit, 0) as contra_per_unit
			, (COALESCE(forecast_supplies_baseprod.baseprod_variable_cost_per_unit, 0) + COALESCE(forecast_supplies_baseprod.baseprod_fixed_cost_per_unit, 0)) cos_per_unit
			, (COALESCE(forecast_supplies_baseprod.baseprod_gru, 0) - COALESCE(forecast_supplies_baseprod.baseprod_contra_per_unit, 0) + COALESCE(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0) - COALESCE(forecast_supplies_baseprod.baseprod_variable_cost_per_unit, 0) - COALESCE(forecast_supplies_baseprod.baseprod_fixed_cost_per_unit, 0)) gpu
			, 0 as cc_inventory_impact
			, ((COALESCE(forecast_supplies_baseprod.baseprod_gru, 0) - COALESCE(forecast_supplies_baseprod.baseprod_contra_per_unit, 0) + COALESCE(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0)) * COALESCE(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0)) adjusted_revenue
		FROM
		forecast_base_pl forecast_supplies_baseprod
		LEFT JOIN
		(select * from working_forecast_country cartridge_demand_c2c_country_splits where cartridge_demand_c2c_country_splits.imp_corrected_cartridges >=1 and cartridge_demand_c2c_country_splits.version = (select max(version) from working_forecast_country)) cartridge_demand_c2c_country_splits
			on forecast_supplies_baseprod.base_product_number = cartridge_demand_c2c_country_splits.base_product_number
			and forecast_supplies_baseprod.country_alpha2 = cartridge_demand_c2c_country_splits.country
			and forecast_supplies_baseprod.cal_date = cartridge_demand_c2c_country_splits.cal_date
		LEFT JOIN
		iso_country_code_xref iso_country_code_xref
		on forecast_supplies_baseprod.country_alpha2 = iso_country_code_xref.country_alpha2
		LEFT JOIN
		calendar calendar
		on calendar.Date = forecast_supplies_baseprod.cal_date
		LEFT JOIN
		supplies_xref supplies_xref
		on supplies_xref.base_product_number = forecast_supplies_baseprod.base_product_number
		LEFT JOIN
		__dbt__CTE__bpo_26_sub_yields sub_yields
			on sub_yields.base_product_number = forecast_supplies_baseprod.base_product_number
			and sub_yields.cal_date = forecast_supplies_baseprod.cal_date
			and sub_yields.region_5 = forecast_supplies_baseprod.region_5
		CROSS JOIN
		__dbt__CTE__bpo_18_Fiscal_Yr_Filter current_fiscal_yr
		WHERE fiscal_yr <= end_fiscal_yr
)
    select
			record_type
			, base_product_number
			, platform_subset
			, customer_engagement
			, base_product_line_code
			, technology
			, region_5
			, market10
			, country_alpha2
			, cal_date
			, fiscal_year_qtr
			, fiscal_year_half
			, fiscal_yr
			, round(units ,6) Units
			, round(shipment_units ,6) shipment_units
			, round(equivalent_units ,6) equivalent_units
			, round(demand_units ,6) demand_units
			, round(yield_x_units ,6) yield_x_units
			, round(yield_x_units_black_only ,6) yield_x_units_black_only
			, round(vtc ,6) vtc
			, round((baseprod_gru*units) ,6) gross_revenue
			, round((baseprod_gru*shipment_units) ,6) shipment_gross_revenue
			, round((baseprod_gru*demand_units) ,6) demand_gross_revenue
			, round((baseprod_aru*units) ,6) net_revenue
			, round((baseprod_aru*shipment_units) ,6) shipment_net_revenue
			, round((baseprod_aru*demand_units) ,6) demand_net_revenue
			, round((contra_per_unit * units) ,6) contra
			, round((cos_per_unit * units) ,6) total_cos
			, round((gpu * units) ,6) gross_profit
			, round(cc_net_revenue ,6) cc_net_revenue
			, round(cc_inventory_impact ,6) cc_inventory_impact
			, round(adjusted_revenue ,6) adjusted_revenue
			, financials_version
			, '{}' as version
			, '{}' as load_date
		FROM __dbt__CTE__bpo_23_supplies_baseprod_actuals
		UNION ALL
		SELECT record_type
			, base_product_number
			, platform_subset
			, customer_engagement
			, base_product_line_code
			, technology
			, region_5
			, market10
			, country_alpha2
			, cal_date
			, fiscal_year_qtr
			, fiscal_year_half
			, fiscal_yr
			, round(units ,6) Units
			, round(shipment_units ,6) shipment_units
			, round(equivalent_units ,6) equivalent_units
			, round(demand_units ,6) demand_units
			, round(yield_x_units ,6) yield_x_units
			, round(yield_x_units_black_only ,6) yield_x_units_black_only
			, round(vtc ,6) vtc
			, round((baseprod_gru*units) ,6) gross_revenue
			, round((baseprod_gru*shipment_units) ,6) shipment_gross_revenue
			, round((baseprod_gru*demand_units) ,6) demand_gross_revenue
			, round((baseprod_aru*units) ,6) net_revenue
			, round((baseprod_aru*shipment_units) ,6) shipment_net_revenue
			, round((baseprod_aru*demand_units) ,6) demand_net_revenue
			, round((contra_per_unit * units) ,6) contra
			, round((cos_per_unit * units) ,6) total_cos
			, round((gpu * units) ,6) gross_profit
			, round(cc_net_revenue ,6) cc_net_revenue
			, round(cc_inventory_impact ,6) cc_inventory_impact
			, round(adjusted_revenue ,6) adjusted_revenue
			, financials_version
			, '{}' as version
			, '{}' as load_date
		FROM __dbt__CTE__bpo_27_supplies_baseprod_forecast
""".format(forecast_fin_version , forecast_fin_version , forecast_fin_version , actuals_plus_forecast_financials_version , actuals_plus_forecast_financials_load_date , actuals_plus_forecast_financials_version , actuals_plus_forecast_financials_load_date)

actuals_plus_forecast_financials = spark.sql(actuals_plus_forecast_financials)
actuals_plus_forecast_financials = actuals_plus_forecast_financials_schema.union(actuals_plus_forecast_financials)
write_df_to_redshift(configs, actuals_plus_forecast_financials, "fin_prod.actuals_plus_forecast_financials", "append")
actuals_plus_forecast_financials.createOrReplaceTempView("actuals_plus_forecast_financials")
