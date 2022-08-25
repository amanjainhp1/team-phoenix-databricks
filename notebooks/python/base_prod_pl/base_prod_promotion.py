# Databricks notebook source
dbutils.widgets.text("forecast_fin_version", "2022.08.01.1")
dbutils.widgets.text("currency_hedge_version", "2022.08.03.1")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

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
      ,baseprod_variablecost_per_unit
      ,baseprod_fixedcost_per_unit
      ,(SELECT load_date from prod.version WHERE record = 'FORECAST_SUPPLIES_BASEPROD' 
				AND load_date = (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'FORECAST_SUPPLIES_BASEPROD')) AS load_date,
	   (SELECT version from prod.version WHERE record = 'FORECAST_SUPPLIES_BASEPROD' 
				AND version = (SELECT MAX(version) FROM "prod"."version" WHERE record = 'FORECAST_SUPPLIES_BASEPROD')) AS version
	  , sales_gru_version
	  , contra_version
	  , variable_cost_version
	  , fixed_cost_version
	  , currency_hedge_version		
FROM "fin_stage"."forecast_base_pl"
"""

query_list.append(["fin_prod.forecast_supplies_baseprod", forecast_supplies_baseprod, "append"])

# COMMAND ----------

forecast_supplies_baseprod_region = """


with __dbt__CTE__bpo_01_filter_vars as (



SELECT max(version) as version
, 'FORECAST_SUPPLIES_BASEPROD' as record
from "fin_prod"."forecast_supplies_baseprod"
),  __dbt__CTE__bpo_03_forecast_country as (



select
			base_product_number
			, base_product_line_code
			, region_5
			, country_alpha2
			, cal_date
			, case when ((BaseProd_GRU is null) or (BaseProd_GRU = 0)) then 0
				else Insights_Base_Units 
			end as Insights_Base_Units
			, coalesce(BaseProd_GRU, 0) as BaseProd_GRU
			, coalesce(BaseProd_Contra_perUnit, 0) as BaseProd_Contra_perUnit
			, coalesce(BaseProd_RevenueCurrencyHedge_Unit, 0) as BaseProd_RevenueCurrencyHedge_Unit
			, coalesce(BaseProd_VariableCost_perUnit, 0) as BaseProd_VariableCost_perUnit
			, coalesce(BaseProd_FixedCost_perUnit, 0) as BaseProd_FixedCost_perUnit
			, (SELECT version from IE2_Prod.dbo.version WHERE record = 'forecast_supplies_baseprod' 
				AND version = (SELECT MAX(version) FROM "prod"."version" WHERE record = 'forecast_supplies_baseprod')) AS version
		from
			"fin_stage"."forecast_base_pl"
)select 
			'FORECAST_SUPPLIES_BASEPROD_REGION' as record
			, base_product_number
			, base_product_line_code
			, region_5
			, cal_date
			, sum(baseprod_gru*insights_base_units)/nullif(sum(insights_base_units), 0) AS baseprod_gru
			, sum(baseprod_contra_per_unit*insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_contra_per_unit
			, sum(baseprod_revenue_currency_hedge_unit * insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_revenue_currency_hedge_unit
			, sum((baseprod_gru-baseprod_contra_per_unit+baseprod_revenue_currency_hedge_unit)*insights_base_units)/nullif(sum(insights_base_units), 0) as baseprod_aru
			, sum(baseprod_variable_cost_per_unit * insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_variable_cost_per_unit
			, sum((baseprod_gru-baseprod_contra_per_unit+baseprod_revenue_currency_hedge_unit-baseprod_variable_cost_per_unit)*insights_base_units)/nullif(sum(insights_base_units), 0) as baseprod_contribution_margin_unit
			, sum(baseprod_fixed_cost_per_unit * insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_fixed_cost_per_unit
			, sum((baseprod_gru-baseprod_contra_per_unit+baseprod_revenue_currency_hedge_unit-baseprod_variable_cost_per_unit-baseprod_fixed_cost_per_unit)*insights_base_units)/nullif(sum(insights_base_units), 0) as baseprod_gross_margin_unit
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

query_list.append(["fin_prod.forecast_supplies_baseprod_region", forecast_supplies_baseprod_region, "append"])

# COMMAND ----------

forecast_supplies_baseprod_region_stf = f"""


with __dbt__CTE__bpp_01_filter_vars as (


SELECT 'FIXEDCOST' AS record
    , MAX(version) AS version
FROM "fin_prod"."forecast_fixed_cost_input"

UNION ALL

SELECT 'CURRENCYHEDGE' AS record
    , MAX(version) AS version
FROM "prod"."currency_hedge"

UNION ALL

SELECT 'PRODUCT_LINE_SCENARIOS' AS record
    , MAX(version) AS version
FROM "mdm"."product_line_scenarios_xref" WHERE pl_scenario = 'FINANCE-HEDGE'

UNION ALL

SELECT 'VARIABLE_COST_INK' as record
    , MAX(version) as version
FROM "fin_prod"."forecast_variable_cost_ink"


UNION ALL

SELECT 'VARIABLE_COST_TONER' as record
   , MAX(version) as version
FROM "fin_prod"."forecast_variable_cost_toner"
),  __dbt__CTE__bpo_06_currency_sum_stf as (


select
	currency_hedge.product_category AS pl_level_1
	,region_5
	,currency_hedge.month as cal_date
	,SUM(revenue_currency_hedge) AS revenue_currency_hedge
from "prod"."currency_hedge" currency_hedge
LEFT JOIN
(
select distinct iso.region_5
	,currency_iso_code
from "mdm"."iso_country_code_xref"  iso
LEFT JOIN "mdm"."country_currency_map_landing" map ON map.country_alpha2 = iso.country_alpha2
where currency_iso_code is not null
) as currency_map ON currency = currency_iso_code
where currency_hedge.version = '{dbutils.widgets.get("currency_hedge_version")}'
group by
	currency_hedge.product_category
	,currency_hedge.month
	,region_5
),  __dbt__CTE__bpo_05_revenue_sum_stf as (


select 
			product_line_scenarios_xref.pl_level_1
			, region_fin_w.region_5
			, region_fin_w.cal_date
			, SUM(coalesce(region_fin_w.baseprod_gru, 0) * coalesce(stf.units, 0)) revenue_sum
		from
			(select region_5, cal_date, base_product_number, baseprod_gru, base_product_line_code from "fin_prod"."forecast_supplies_baseprod_region" where version = '{dbutils.widgets.get("forecast_fin_version")}') region_fin_w
			INNER JOIN
			"stage"."supplies_stf_landing" stf
				on stf.geography = region_fin_w.region_5
				and stf.cal_date = region_fin_w.cal_date
				and stf.base_product_number = region_fin_w.base_product_number
			INNER JOIN
			"mdm"."product_line_scenarios_xref" product_line_scenarios_xref
				on product_line_scenarios_xref.pl = region_fin_w.base_product_line_code
		where
			product_line_scenarios_xref.pl_scenario = 'FINANCE-HEDGE'
			and product_line_scenarios_xref.version = (select version from __dbt__CTE__bpp_01_filter_vars where record = 'PRODUCT_LINE_SCENARIOS')
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
			, ISNULL(currency.revenue_currency_hedge/NULLIF(revenue.Revenue_sum, 0), 0) as currency_per
		from
		__dbt__CTE__bpo_06_currency_sum_stf as currency
		INNER JOIN
		__dbt__CTE__bpo_05_revenue_sum_stf as revenue
			on currency.pl_level_1 = revenue.pl_level_1
			and currency.region_5 = revenue.region_5
			and currency.cal_date = revenue.cal_date
		INNER JOIN
		"mdm"."product_line_scenarios_xref" product_line_scenarios_xref
				on product_line_scenarios_xref.pl_level_1 = revenue.pl_level_1
		where
			product_line_scenarios_xref.pl_scenario = 'FINANCE-HEDGE'
			and product_line_scenarios_xref.version = (select version from __dbt__CTE__bpp_01_filter_vars where record = 'PRODUCT_LINE_SCENARIOS')
),  __dbt__CTE__bpo_08_Fixed_Revenue_Sum as (


select 
			region_fin_w.base_product_line_code
			, country_xref.region_3
			, calendar.fiscal_year_qtr
			, sum(coalesce(region_fin_w.baseprod_gru, 0) * coalesce(stf.units, 0)) AS revenue_sum
		from
			(select base_product_line_code, baseprod_gru, region_5, cal_date, base_product_number from "fin_prod"."forecast_supplies_baseprod_region" where version = '{dbutils.widgets.get("forecast_fin_version")}') region_fin_w
			INNER JOIN
			"stage"."supplies_stf_landing" stf
				ON stf.geography = region_fin_w.region_5
				AND stf.cal_date = region_fin_w.cal_date
				AND stf.base_product_number = region_fin_w.base_product_number
			INNER JOIN
			"mdm"."iso_country_code_xref" country_xref
				ON country_xref.region_5 = region_fin_w.region_5
			INNER JOIN
			"mdm"."calendar" calendar
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
		"fin_prod"."forecast_fixed_cost_input"
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
			, coalesce(fixed_cost.fixed_cost_qtr, 0) /nullif(coalesce(rev_sum.revenue_sum,0),0) AS fixed_cost_per
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
		, avg((baseprod_gru * coalesce(currency_per, 0))) as baseprod_revenue_currency_hedge_unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * coalesce(currency_per, 0)))) as baseprod_aru
		, avg(baseprod_variable_cost_per_unit) as baseprod_variable_cost_per_unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * coalesce(currency_per, 0)) - baseprod_variable_cost_per_unit)) as baseprod_contribution_margin_unit
		, avg(coalesce(baseprod_gru * fixed_cost_per, 0)) as baseprod_fixed_cost_per_unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * coalesce(currency_per, 0)) - baseprod_variable_cost_per_unit - coalesce(baseprod_gru * fixed_cost_per, 0))) as baseprod_gross_margin_unit
		, fin.version
	from
		(select * from "fin_prod"."forecast_supplies_baseprod_region" where version = '{dbutils.widgets.get("forecast_fin_version")}') fin
		INNER JOIN
		"mdm"."calendar" calendar
			on calendar.date = fin.cal_date
		INNER JOIN
		"mdm"."iso_country_code_xref" country_xref
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
        
"""

query_list.append(["fin_prod.forecast_supplies_baseprod_region_stf", forecast_supplies_baseprod_region_stf, "append"])

# COMMAND ----------

forecast_supplies_baseprod_mkt10 = f"""


with __dbt__CTE__bpo_01_filter_vars as (


SELECT max(version) as version
, 'FORECAST_SUPPLIES_BASEPROD' as record
from "fin_prod"."forecast_supplies_baseprod"
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
			, coalesce(baseprod_revenue_currency_hedge_unit, 0) as baseprod_revenue_currency_hedge_Unit
			, coalesce(baseprod_variable_cost_per_unit, 0) as baseprod_variable_cost_per_unit
			, coalesce(baseprod_fixed_cost_per_unit, 0) as baseprod_fixed_cost_per_unit
			, (SELECT version from "prod"."version" WHERE record = 'FORECAST_SUPPLIES_BASEPROD' 
				AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'FORECAST_SUPPLIES_BASEPROD')) AS version
		from
			"fin_stage"."forecast_base_pl"
),  __dbt__CTE__bpo_11_mkt10_financials_working as (



select 
			base_product_number
			, base_product_line_code
			, country_xref.market10
			, cal_date
			, SUM(baseprod_gru*insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_gru
			, SUM(baseprod_contra_per_unit*insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_contra_per_unit
			, SUM(baseprod_revenue_currency_hedge_Unit * insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_revenue_currency_hedge_Unit
			, SUM(baseprod_variable_cost_per_unit * insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_variable_cost_per_unit
			, SUM(baseprod_fixed_cost_per_unit * insights_base_units)/nullif(sum(insights_base_units), 0) baseprod_fixed_cost_per_unit
			, fin.version
		from
			__dbt__CTE__bpo_03_forecast_country fin
			INNER JOIN
			"mdm"."iso_country_code_xref" country_xref
				on fin.country_alpha2 = country_xref.country_alpha2
		group by
			base_product_number
			, base_product_line_code
			, country_xref.market10
			, cal_date
			, fin.version
),  __dbt__CTE__bpp_01_filter_vars as (



SELECT 'FIXED_COST' AS record
    , max(version) as version
FROM "fin_prod"."forecast_fixed_cost_input"

UNION ALL

SELECT 'CURRENCY_HEDGE' AS record
    , MAX(version) AS version
FROM "prod"."currency_hedge"

UNION ALL

select 'PRODUCT_LINE_SCENARIOS' as record
    , MAX(version) as version
from "mdm"."product_line_scenarios_xref" where pl_scenario = 'FINANCE-HEDGE'

UNION ALL

select 'VARIABLE_COST_INK' as record
    , MAX(version) as version
from "fin_prod"."forecast_variable_cost_ink"


UNION ALL

select 'VARIABLE_COST_TONER' as record
   , MAX(version) as version
from "fin_prod"."forecast_variable_cost_toner"
),  __dbt__CTE__bpo_12_revenue_sum_trade as (



select 
			product_line_scenarios_xref.pl_level_1
			, mkt_fin_w.market10
			, mkt_fin_w.cal_date
			, sum(coalesce(mkt_fin_w.baseprod_gru, 0) * coalesce(trade.cartridges, 0)) revenue_sum
		from
			__dbt__CTE__bpo_11_mkt10_financials_working mkt_fin_w
			INNER JOIN
			"prod"."trade_forecast" trade
				on trade.market10 = mkt_fin_w.market10
				and trade.cal_date = mkt_fin_w.cal_date
				and trade.base_product_number = mkt_fin_w.base_product_number
			INNER JOIN
			"mdm"."product_line_scenarios_xref" product_line_scenarios_xref
				on product_line_scenarios_xref.pl = mkt_fin_w.base_product_line_code
		where
			product_line_scenarios_xref.pl_scenario = 'FINANCE-HEDGE'
			and product_line_scenarios_xref.version = (select version from __dbt__CTE__bpp_01_filter_vars where record = 'PRODUCT_LINE_SCENARIOS')
		group by
			product_line_scenarios_xref.pl_level_1
			, mkt_fin_w.market10
			, mkt_fin_w.cal_date
),  __dbt__CTE__bpo_13_currency_sum_trade as (


select
	currency_hedge.product_category as pl_level_1
	,market10
	,currency_hedge.month as cal_date
	,SUM(revenue_currency_hedge) as revenue_currency_hedge
from "prod"."currency_hedge" currency_hedge
LEFT JOIN
(
select distinct country_ref.market10
	,currency_iso_code
from "mdm"."iso_country_code_xref"  country_ref
LEFT JOIN "mdm"."country_currency_map" map ON map.country_alpha2 = country_ref.country_alpha2
where currency_iso_code is not null
) as currency_map ON currency = currency_iso_code
where currency_hedge.version = '{dbutils.widgets.get("currency_hedge_version")}'
GROUP BY
	currency_hedge.product_category
	,currency_hedge.month
	,market10
),  __dbt__CTE__bpo_14_currency_hedge_per_trade as (



select 
			currency.pl_level_1
		    , currency.market10
			, currency.cal_date
			, product_line_scenarios_xref.pl as base_product_line_code
			, currency.revenue_currency_hedge/revenue.revenue_sum as currency_per
		from
		__dbt__CTE__bpo_12_revenue_sum_trade as revenue
		INNER JOIN
		__dbt__CTE__bpo_13_currency_sum_trade as currency
			on currency.pl_level_1 = revenue.pl_level_1
			and currency.market10 = revenue.market10
			and currency.cal_date = revenue.cal_date
		INNER JOIN
		"mdm"."product_line_scenarios_xref" product_line_scenarios_xref
				on product_line_scenarios_xref.pl_level_1 = revenue.pl_level_1
		where
			product_line_scenarios_xref.pl_scenario = 'FINANCE-HEDGE'
			and product_line_scenarios_xref.version = (select version from __dbt__CTE__bpp_01_filter_vars where record = 'PRODUCT_LINE_SCENARIOS')
),  __dbt__CTE__bpo_15_Fixed_Revenue_Sum_trade as (



select 
			mkt_fin_w.base_product_line_code
			, country_xref.region_3
			, calendar.fiscal_year_qtr as fiscal_yr_qtr
			, SUM(coalesce(mkt_fin_w.baseprod_gru, 0) * coalesce(trade.cartridges, 0)) AS revenue_sum
		from
			__dbt__CTE__bpo_11_mkt10_financials_working mkt_fin_w
			INNER JOIN
			"prod"."trade_forecast" trade
				on trade.market10 = mkt_fin_w.market10
				and trade.cal_date = mkt_fin_w.cal_date
				and trade.base_product_number = mkt_fin_w.base_product_number
			INNER JOIN
			"mdm"."iso_country_code_xref" country_xref
				on country_xref.market10 = mkt_fin_w.market10
			INNER JOIN
			"mdm"."calendar" calendar
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
		, version as fin_version
	from
		"fin_prod"."forecast_fixed_cost_input"
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
			, coalesce(rev_sum.revenue_sum,0) /nullif(coalesce(fixed_cost.fixed_cost_qtr, 0),0) AS fixed_cost_per
		from
			__dbt__CTE__bpo_15_Fixed_Revenue_Sum_trade rev_sum
			INNER JOIN
			__dbt__CTE__bpp_21_FixedCost_Region fixed_cost
				on rev_sum.base_product_line_code = fixed_cost.base_product_line_code
				and rev_sum.region_3 = fixed_cost.region_3
				and rev_sum.fiscal_yr_qtr = fixed_cost.fiscal_yr_qtr
)select
		'FORECAST_SUPPLIES_BASEPROD_MARKET10' as record
		, fin.base_product_number
		, fin.base_product_line_code
		, fin.market10
		, fin.cal_date
		, avg(baseprod_gru) as baseprod_gru
		, avg(baseprod_contra_per_unit) as baseprod_contra_per_unit
		, avg((baseprod_gru * coalesce(currency_per, 0))) as baseprod_revenue_currency_hedge_unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * coalesce(currency_per, 0)))) as baseprod_aru
		, avg(baseprod_variable_cost_per_unit) as baseprod_variable_cost_per_unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * coalesce(currency_per, 0)) - baseprod_variable_cost_per_unit)) as baseprod_contribution_margin_unit
		, avg(coalesce(baseprod_gru * fixed_cost_per, 0)) as baseprod_fixed_cost_per_unit
		, avg((baseprod_gru - baseprod_contra_per_unit + (baseprod_gru * coalesce(currency_per, 0)) - baseprod_variable_cost_per_unit - coalesce(baseprod_gru * fixed_cost_per, 0))) as baseprod_gross_margin_unit
		, fin.version
	from
		__dbt__CTE__bpo_11_mkt10_financials_working fin
		INNER JOIN
		"mdm"."calendar" calendar
			on calendar.date = fin.cal_date
		LEFT JOIN
		"mdm"."iso_country_code_xref" country_xref
			on country_xref.market10 = fin.market10
		LEFT JOIN
		__dbt__CTE__bpo_14_currency_hedge_per_trade currency_hedge
			on currency_hedge.base_product_line_code = fin.base_product_line_code
			and currency_hedge.market10 = fin.market10
			and currency_hedge.cal_date = fin.cal_date
		LEFT JOIN
		__dbt__CTE__bpo_16_Fixed_per_trade fixed
			on fixed.base_product_line_code = fin.base_product_line_code
			and fixed.region_3 = country_xref.region_3
			and calendar.fiscal_year_qtr = fixed.fiscal_yr_qtr

	where baseprod_gru is not null
	group by
		base_product_number
		, fin.base_product_line_code
		, fin.market10
		, fin.cal_date
		, fin.version
"""

query_list.append(["fin_prod.forecast_supplies_baseprod_mkt10", forecast_supplies_baseprod_mkt10, "append"])

# COMMAND ----------


actuals_plus_forecast_financials = f"""

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
			, coalesce(actuals_supplies_baseprod.revenue_units, 0) as revenue_units
			, coalesce(actuals_supplies_baseprod.equivalent_units, 0) as equivalent_units
			, coalesce(actuals_supplies_baseprod.yield_x_units, 0) as yield_x_units
			, coalesce(actuals_supplies_baseprod.yield_x_units_black_only, 0) as yield_x_units_black_only
			, coalesce(actuals_supplies_baseprod.gross_revenue, 0) as gross_revenue
			, coalesce(actuals_supplies_baseprod.net_revenue, 0) as net_revenue
			, coalesce(actuals_supplies_baseprod.contractual_discounts + actuals_supplies_baseprod.discretionary_discounts, 0) as contra
			, coalesce(actuals_supplies_baseprod.total_cos, 0) as total_cos
			, coalesce(actuals_supplies_baseprod.gross_profit, 0) as gross_profit
			, actuals_supplies_baseprod.version as financials_version
		FROM "fin_prod"."actuals_supplies_baseprod" actuals_supplies_baseprod
		where 1=1
		and actuals_supplies_baseprod.customer_engagement <> 'EST_INDIRECT_FULFILLMENT'
        -- FIX :: filter below is no longer correct
		-- and version = (select max(version) from IE2_Financials.dbo.actuals_supplies_baseprod)
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
			, coalesce(adjusted_revenue.cc_net_revenue,0) as cc_net_revenue
			, coalesce(adjusted_revenue.cc_inventory_impact, 0) as cc_inventory_impact
			, coalesce(adjusted_revenue.adjusted_revenue, 0) as adjusted_revenue
		FROM "fin_prod"."adjusted_revenue_epa" adjusted_revenue
        where adjusted_revenue.customer_engagement <> 'EST_INDIRECT_FULFILLMENT'
		and adjusted_revenue.version = (select max(version) from "fin_prod"."adjusted_revenue_epa")
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



SELECT 
			fiscal_yr-5 start_fiscal_yr
			, fiscal_yr+5 end_fiscal_yr
		FROM
		"mdm"."calendar"
		WHERE Date = CONVERT(DATE, GETDATE())
),  __dbt__CTE__bpo_23_supplies_baseprod_actuals as (



SELECT 
			'ACTUALS' AS record_type
			, actuals_supplies_baseprod.base_product_number
			, actuals_supplies_baseprod.platform_subset
			, actuals_supplies_baseprod.customer_engagement
			, actuals_supplies_baseprod.base_product_line_code
			, supplies_xref.technology
			/*, CASE 
				WHEN (actuals_supplies_baseprod.pl in ('5T', 'GJ', 'GK', 'GP', 'IU', 'LS')) THEN 1
				ELSE 0
			  END AS IsCanon*/
			, iso_country_code_xref.region_5
			, iso_country_code_xref.country_alpha2
			, iso_country_code_xref.market10
			, actuals_supplies_baseprod.cal_date
			, calendar.fiscal_year_qtr
			, calendar.fiscal_year_half
			, calendar.fiscal_yr
			, financials_version
			, SUM(coalesce(actuals_supplies_baseprod.revenue_units, 0)) units
			, SUM(coalesce(actuals_supplies_baseprod.equivalent_units, 0)) equivalent_units
			, SUM(coalesce(actuals_supplies_baseprod.yield_x_units, 0)) yield_x_units
			, SUM(coalesce(actuals_supplies_baseprod.yield_x_units_black_only, 0)) yield_x_units_black_only
			, SUM(coalesce(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0)) shipment_units
			, SUM(coalesce(cartridge_demand_c2c_country_splits.cartridges,0)) demand_units
			, SUM(coalesce(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0) - coalesce(cartridge_demand_c2c_country_splits.cartridges, 0)) vtc
			--, SUM(gross_revenue) AS GrossRevenue
			, SUM(coalesce(actuals_supplies_baseprod.gross_revenue, 0))/nullif(sum(coalesce(actuals_supplies_baseprod.revenue_units, 0)),0) base_prod_gru
			--, SUM(actuals_supplies_baseprod.net_revenue) AS net_revenue
			, SUM(coalesce(actuals_supplies_baseprod.net_revenue, 0))/nullif(sum(coalesce(revenue_units, 0)),0) baseprod_aru
			, SUM(coalesce(actuals_supplies_baseprod.contra, 0))/nullif(sum(coalesce(revenue_units, 0)), 0) contra_per_unit
			, SUM(coalesce(actuals_supplies_baseprod.total_cos, 0))/nullif(sum(coalesce(revenue_units, 0)), 0) cos_per_unit
			, SUM(coalesce(actuals_supplies_baseprod.gross_profit, 0))/nullif(sum(coalesce(revenue_units, 0)), 0) gpu
			, SUM(coalesce(adjusted_revenue.cc_net_revenue, 0)) cc_net_revenue
			, SUM(coalesce(adjusted_revenue.cc_inventory_impact, 0)) cc_inventory_impact
			, SUM(coalesce(adjusted_revenue.adjusted_revenue, 0)) adjusted_revenue
			--, actuals_supplies_baseprod.financials_version
			/*, SUM(contractual_discounts) + SUM(discretionary_discounts) AS contra
			, SUM(net_currency) AS revenue_currency
			, SUM(net_revenue) AS net_revenue
			, SUM(total_cos) AS total_cos
			, SUM(gross_profit) AS gross_profit*/
		FROM __dbt__CTE__bpo_20_actuals_sum actuals_supplies_baseprod
		LEFT JOIN
		__dbt__CTE__bpo_22_adjusted_rev_sum adjusted_revenue
			ON adjusted_revenue.base_product_number = actuals_supplies_baseprod.base_product_number
			and adjusted_revenue.cal_date = actuals_supplies_baseprod.cal_date
			and adjusted_revenue.country_alpha2 = actuals_supplies_baseprod.country_alpha2
			and adjusted_revenue.platform_subset = actuals_supplies_baseprod.platform_subset
			and UPPER(adjusted_revenue.customer_engagement) = UPPER(actuals_supplies_baseprod.customer_engagement)
		LEFT JOIN
		(select base_product_number, country, cal_date, platform_subset, customer_engagement, imp_corrected_cartridges, cartridges from "prod"."working_forecast_country"  where version = (select max(version) from "prod"."working_forecast_country")) as cartridge_demand_c2c_country_splits
			ON cartridge_demand_c2c_country_splits.base_product_number = actuals_supplies_baseprod.base_product_number
			and cartridge_demand_c2c_country_splits.country = actuals_supplies_baseprod.country_alpha2
			and cartridge_demand_c2c_country_splits.cal_date = actuals_supplies_baseprod.cal_date
			and cartridge_demand_c2c_country_splits.platform_subset = actuals_supplies_baseprod.platform_subset
			and cartridge_demand_c2c_country_splits.customer_engagement = upper(actuals_supplies_baseprod.customer_engagement)
		LEFT JOIN
		"mdm"."supplies_xref" supplies_xref
			ON supplies_xref.base_product_number = actuals_supplies_baseprod.base_product_number
		LEFT JOIN
		"mdm"."iso_country_code_xref" iso_country_code_xref
			ON iso_country_code_xref.country_alpha2 = actuals_supplies_baseprod.country_alpha2
		CROSS JOIN
		__dbt__CTE__bpo_18_Fiscal_Yr_Filter fiscal
		INNER JOIN
		"mdm"."calendar" calendar
			ON calendar.date = actuals_supplies_baseprod.cal_date
			AND calendar.fiscal_Yr >= fiscal.start_fiscal_yr
		
		--WHERE Fiscal_Yr >= fiscal.start_fiscal_yr
		--and cartridge_demand_c2c_country_splits.version = (select max(version) from ie2_prod.dbo.cartridge_demand_c2c_country_splits)
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
			[value] AS yield
		FROM "prod"."yield"
		WHERE official = 1	
		AND geography_grain = 'region_5'
),  __dbt__CTE__bpo_24_sub_months as (



SELECT "date" AS cal_date					
			FROM mdm.calendar
			WHERE day_of_month = 1
			and Date >= (select min(cal_date) from "fin_prod"."forecast_supplies_baseprod" where
					version = '{dbutils.widgets.get("forecast_fin_version")}')
			and Date <= (select max(cal_date) from "fin_prod"."forecast_supplies_baseprod" where
					version = '{dbutils.widgets.get("forecast_fin_version")}')
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
),  __dbt__CTE__bpo_18_Fiscal_Yr_Filter as (



SELECT 
			fiscal_yr-5 start_fiscal_yr
			, fiscal_yr+5 end_fiscal_yr
		FROM
		"mdm"."calendar"
		WHERE Date = CONVERT(DATE, GETDATE())
) , __dbt_CTE__bpo_27_supplies_baseprod_forecast as (
    
    
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
			, '{dbutils.widgets.get("forecast_fin_version")}' as financials_version
			, coalesce(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0) units
			, coalesce(cartridge_demand_c2c_country_splits.imp_corrected_cartridges * supplies_xref.equivalents_multiplier, 0) equivalent_units
			, coalesce(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0) shipment_units
			, coalesce(cartridge_demand_c2c_country_splits.cartridges, 0) demand_units
			, yield * cartridge_demand_c2c_country_splits.imp_corrected_cartridges AS yield_x_units,
						CASE	
							WHEN k_color != 'BLACK' THEN NULL
							ELSE cartridge_demand_c2c_country_splits.imp_corrected_cartridges * yield
						END AS yield_x_units_black_only
			, (coalesce(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0) - coalesce(cartridge_demand_c2c_country_splits.cartridges, 0)) vtc
			, coalesce(forecast_supplies_baseprod.baseprod_gru,0) as baseprod_gru
			, (coalesce(forecast_supplies_baseprod.baseprod_gru, 0) - coalesce(forecast_supplies_baseprod.baseprod_contra_per_unit, 0) + coalesce(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0)) baseprod_aru
			, ((coalesce(forecast_supplies_baseprod.baseprod_gru, 0) - coalesce(forecast_supplies_baseprod.baseprod_contra_per_unit, 0) + coalesce(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0))*coalesce(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0)) cc_net_revenue
			, coalesce(forecast_supplies_baseprod.baseprod_contra_per_unit, 0) as contra_per_unit
			, (coalesce(forecast_supplies_baseprod.baseprod_variable_cost_per_unit, 0) + coalesce(forecast_supplies_baseprod.baseprod_fixed_cost_per_unit, 0)) cos_per_unit
			, (coalesce(forecast_supplies_baseprod.baseprod_gru, 0) - coalesce(forecast_supplies_baseprod.baseprod_contra_per_unit, 0) + coalesce(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0) - coalesce(forecast_supplies_baseprod.baseprod_variable_cost_per_unit, 0) - coalesce(forecast_supplies_baseprod.baseprod_fixed_cost_per_unit, 0)) gpu
			, 0 as cc_inventory_impact
			, ((coalesce(forecast_supplies_baseprod.baseprod_gru, 0) - coalesce(forecast_supplies_baseprod.baseprod_contra_per_unit, 0) + coalesce(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0))*coalesce(cartridge_demand_c2c_country_splits.imp_corrected_cartridges, 0)) adjusted_revenue
		FROM
		"fin_stage"."forecast_Base_PL" forecast_supplies_baseprod
		/*(select * from "IE2_Financials"."dbo"."forecast_supplies_baseprod" forecast_supplies_baseprod where version = '2022.08.01.1' ) forecast_supplies_baseprod
		*/
		LEFT JOIN
		(select * from "prod"."working_forecast_country" cartridge_demand_c2c_country_splits where cartridge_demand_c2c_country_splits.imp_corrected_cartridges >=1 and cartridge_demand_c2c_country_splits.version = (select max(version) from "prod"."working_forecast_country")) cartridge_demand_c2c_country_splits
			on forecast_supplies_baseprod.base_product_number = cartridge_demand_c2c_country_splits.base_product_number
			and forecast_supplies_baseprod.country_alpha2 = cartridge_demand_c2c_country_splits.country
			and forecast_supplies_baseprod.cal_date = cartridge_demand_c2c_country_splits.cal_date
		LEFT JOIN
		"mdm"."iso_country_code_xref" iso_country_code_xref
		on forecast_supplies_baseprod.country_alpha2 = iso_country_code_xref.country_alpha2
		LEFT JOIN
		"mdm"."calendar" calendar
		on calendar.Date = forecast_supplies_baseprod.cal_date
		LEFT JOIN
		"mdm"."supplies_xref" supplies_xref
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
			, round((base_prod_gru*units) ,6) gross_revenue
			, round((base_prod_gru*shipment_units) ,6) shipment_gross_revenue
			, round((base_prod_gru*demand_units) ,6) demand_gross_revenue
			, round((base_prod_aru*units) ,6) net_revenue
			, round((base_prod_aru*shipment_units) ,6) shipment_net_revenue
			, round((base_prod_aru*demand_units) ,6) demand_net_revenue
			, round((contra_per_unit * units) ,6) contra
			, round((cos_per_unit * units) ,6) total_cos
			, round((gpu * units) ,6) gross_profit
			, round(cc_net_revenue ,6) cc_net_revenue
			, round(cc_inventory_impact ,6) cc_inventory_impact
			, round(adjusted_revenue ,6) adjusted_revenue
			, financials_version
			, (SELECT MAX(version) FROM "prod"."version" WHERE record = 'ACTUALS_PLUS_FORECAST_FINANCIALS') as version
			, (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'ACTUALS_PLUS_FORECAST_FINANCIALS') as load_date
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
			, round((base_prod_gru*units) ,6) gross_revenue
			, round((base_prod_gru*shipment_units) ,6) shipment_gross_revenue
			, round((base_prod_gru*demand_units) ,6) demand_gross_revenue
			, round((base_prod_aru*units) ,6) net_revenue
			, round((base_prod_aru*shipment_units) ,6) shipment_net_revenue
			, round((base_prod_aru*demand_units) ,6) demand_net_revenue
			, round((contra_per_unit * units) ,6) contra
			, round((cos_per_unit * units) ,6) total_cos
			, round((gpu * units) ,6) gross_profit
			, round(cc_net_revenue ,6) cc_net_revenue
			, round(cc_inventory_impact ,6) cc_inventory_impact
			, round(adjusted_revenue ,6) adjusted_revenue
			, financials_version
			, (SELECT MAX(version) FROM "prod"."version" WHERE record = 'ACTUALS_PLUS_FORECAST_FINANCIALS') as version
			, (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'ACTUALS_PLUS_FORECAST_FINANCIALS') as load_date
		FROM __dbt__CTE__bpo_27_supplies_baseprod_forecast
"""

query_list.append(["fin_prod.actuals_plus_forecast_financials", actuals_plus_forecast_financials, "append"])       

# COMMAND ----------

current_stf_dollarization =  f"""


with __dbt__CTE__lpf_01_filter_vars AS (


SELECT 'ACTUALS_SUPPLIES_BASEPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_baseprod"

UNION ALL

SELECT 'ACTUALS_SUPPLIES_SALESPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_salesprod"

UNION ALL

SELECT 'IBP_SUPPLIES_FORECAST' AS record
    , MAX(version) AS version
FROM "prod"."ibp_supplies_forecast"

UNION ALL

SELECT 'INSIGHTS_UNITS' AS record
    , MAX(version) AS version
FROM "prod"."working_forecast_country"

UNION ALL

SELECT 'LIST_PRICE_GPSY' AS record
    , MAX(version) AS version
FROM "prod"."list_price_gpsy"

UNION ALL

SELECT 'ACCT_RATES' AS record
    , MAX(version) AS version
FROM "prod"."acct_rates"

),  __dbt__CTE__bpo_30_Insights_Units as (


select
		base_product_number
		, region_5
		, cal_date
		, SUM(cartridges) cartridges
	from
		"prod"."working_forecast_country" c2c
		inner join
		"mdm"."iso_country_code_xref" ctry
			on ctry.country_alpha2 = c2c.country
	where c2c.version = (select version from __dbt__CTE__lpf_01_filter_vars where record = 'INSIGHTS_UNITS')
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
	, (fin.baseprod_gru*stf.units) as gross_revenue
	, NULL as contractual_discounts
	, NULL as discretionary_discounts
	, (fin.baseprod_contra_per_unit*stf.units) as contra
	, (fin.baseprod_revenue_currency_hedge_unit * stf.units) as revenue_currency_hedge
	, (fin.baseprod_aru * stf.units) as net_revenue
	, (fin.baseprod_variable_cost_per_unit * stf.units) as variable_Cost
	, (fin.baseprod_contribution_margin_unit * stf.units) as contribution_margin
	, (fin.baseprod_fixed_cost_per_unit * stf.units) as fixed_Cost
	, ((fin.baseprod_variable_cost_per_unit * stf.units)+(fin.baseprod_fixed_cost_per_unit * stf.units)) as total_cos
	, (fin.baseprod_gross_margin_unit * stf.units) as gross_margin
	, c2c.cartridges as insights_units
	, stf.username

from 
	"stage"."supplies_stf_landing" stf
	left join
	"fin_prod"."forecast_supplies_baseprod_region_stf" fin
		on fin.base_product_number = stf.base_product_number
		and fin.region_5 = stf.geography
		and fin.cal_date = stf.cal_date
		and fin.version = '{dbutils.widgets.get("forecast_fin_version")}'
	left join
	"Imdm"."rdma" rdma
		on rdma.Base_Prod_Number = stf.base_product_number
	inner join
	"mdm"."iso_country_code_xref" ctry
		on ctry.region_5 = stf.geography
	left join
	__dbt__CTE__bpo_30_Insights_Units c2c
		on c2c.base_product_number = stf.base_product_number
		and c2c.region_5 = stf.geography
		and c2c.cal_date = stf.cal_date
	left join
	"mdm"."supplies_xref" supplies_xref
		on supplies_xref.base_product_number = stf.base_product_number
"""

query_list.append(["fin_prod.current_stf_dollarization", current_stf_dollarization, "append"])

# COMMAND ----------

actuals_plus_stf = """


with __dbt__CTE__bpo_32_actuals_Fiscal_Yr as (



SELECT 
			fiscal_yr-2 start_fiscal_yr
		FROM
	"mdm"."calendar"
		WHERE date = CONVERT(DATE, GETDATE())
),  __dbt__CTE__bpo_33_actuals as (



SELECT region_5 as geography
    ,base_product_number
    ,bp.pl
    ,technology
    ,cal_date
    , sum(revenue_units) as units
    , sum(equivalent_units) as equivalent_units
    , SUM(gross_revenue) AS grossrevenue
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
FROM "fin_prod"."actuals_supplies_baseprod" bp
LEFT JOIN "mdm"."iso_country_code_xref" iso ON bp.country_alpha2 = iso.country_alpha2
LEFT JOIN "mdm"."product_line_xref" plx ON bp.pl = plx.pl
LEFT JOIN "mdm"."calendar" cal ON cal.Date = cal_Date
WHERE 1=1
    and day_of_month = 1
    and fiscal_yr > (select start_fiscal_yr from __dbt__CTE__bpo_32_actuals_Fiscal_Yr)
    and bp.pl NOT IN ('1N', 'GD', 'GM', 'LU', 'K6', 'UD', 'HF', '65')
    and customer_engagement <> 'EST_DIRECT_FULFILLMENT'
    -- FIX :: filter below is no longer correct
    -- and bp.version = (select max(version) from "fin_prod"."actuals_supplies_baseprod")
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
		, total_cosS
		, gross_margin
		, insights_units
		, cast(username as varchar) as username
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
		"fin_prod"."current_stf_dollarization"
        
"""

query_list.append(["fin_prod.actuals_plus_stf", actuals_plus_stf, "append"])

# COMMAND ----------

stf_dollarization_coc = """


SELECT
	 record
	, geography
	, base_product_number
	, pl
	,UPPER(technology) AS technology
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
FROM "fin_prod"."stf_dollarization"

UNION ALL

SELECT 
    'SUPPLIES_STF' as record
    ,geography
	, base_product_number
	, pl
	,UPPER(technology) AS technology
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
    , GETDATE() as load_date
    , username
FROM "fin_prod"."current_stf_dollarization"
"""
