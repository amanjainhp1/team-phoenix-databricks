# Databricks notebook source
dbutils.widgets.text("tasks","base_prod_pl;all")

# COMMAND ----------

dbutils.widgets.text("sales_gru_version", "2022.08.09.1")
dbutils.widgets.text("currency_hedge_version", "2022.08.03.1")

# COMMAND ----------

tasks = dbutils.widgets.get("tasks").split(";")

if 'base_prod_pl' or 'all' not in tasks:
    dbutils.notebook.exit("notebook skipped")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

query_list = []

# COMMAND ----------

bpp_01_base_gru_insights = f"""


WITH __dbt__CTE__lpf_01_filter_vars as (


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
),  __dbt__CTE__bpp_02_filter_dates as (


SELECT 'MIN_IBP_DATE' AS record
    , MIN(cal_date) AS cal_date
FROM "prod"."ibp_supplies_forecast"
WHERE version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT 'MAX_IBP_DATE' AS record
    , MAX(cal_date) AS cal_date
FROM "prod"."ibp_supplies_forecast"
WHERE version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT 'CURRENCY_HEDGE_MONTH' AS record
    ,MAX(currency_hedge.Month) AS cal_date
FROM "prod"."currency_hedge"                
),  __dbt__CTE__bpp_03_Base_GRU_Gpsy as (


SELECT
		ibp_sales_units.base_product_number
		, ibp_sales_units.base_product_line_code
		, ibp_sales_units.cal_date
		, ibp_sales_units.region_5
		, ibp_sales_units.country_alpha2
		, SUM(ibp_sales_units.base_prod_fcst_revenue_units) base_units
		, SUM((ibp_sales_units.units * sales_gru_gpsy.salesproduct_gru*ibp_sales_units.base_product_amount_percent)/100) AS base_gr
		, SUM((ibp_sales_units.units * sales_gru_gpsy.salesproduct_gru*ibp_sales_units.base_product_amount_percent)/100)/sum(ibp_sales_units.base_prod_fcst_revenue_units) AS base_gru
		, MAX(ibp_sales_units.cal_date) OVER (PARTITION BY ibp_sales_units.base_product_number, ibp_sales_units.country_alpha2) AS max_cal_date
	FROM
		"fin_stage"."lpf_01_ibp_combined" ibp_sales_units
		INNER JOIN
		"fin_prod"."forecast_sales_gru" sales_gru_gpsy
			ON ibp_sales_units.sales_product_number = sales_gru_gpsy.sales_product_number
			AND ibp_sales_units.country_alpha2 = sales_gru_gpsy.country_alpha2
	WHERE sales_gru_gpsy.version = '{dbutils.widgets.get("sales_gru_version")}'
	GROUP BY
		ibp_sales_units.base_product_number
		, ibp_sales_units.base_product_line_code
		, ibp_sales_units.cal_date
		, ibp_sales_units.region_5
		, ibp_sales_units.country_alpha2
),  __dbt__CTE__bpp_05_last_GRU as (


SELECT distinct
		base_product_number
		, country_alpha2
		, base_gru
		, max_cal_date
	FROM
		__dbt__CTE__bpp_03_Base_GRU_Gpsy
	WHERE
		cal_date = max_cal_date
),  __dbt__CTE__bpp_04_all_months as (


SELECT
		calendar.Date AS cal_date
	FROM
		"mdm"."calendar" calendar
	WHERE
		calendar.Day_of_Month = 1
		AND calendar.Date >
		    (SELECT cal_date FROM __dbt__CTE__bpp_02_filter_dates WHERE record = 'MIN_IBP_DATE')
),  __dbt__CTE__bpp_06_extend_months as (


SELECT
		last_GRU.base_product_number
		, last_GRU.country_alpha2
		, all_months.cal_date
		, last_GRU.base_gru
	FROM
		__dbt__CTE__bpp_05_last_GRU last_GRU
		JOIN
		__dbt__CTE__bpp_04_all_months all_months
		    ON 1 = 1
		WHERE exists
		(SELECT 1 FROM "prod"."working_forecast_country" insights_units
			WHERE insights_units.base_product_number = last_GRU.base_product_number
				AND insights_units.country = last_GRU.country_alpha2
				AND insights_units.cal_date = all_months.cal_date
				AND insights_units.version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'INSIGHTS_UNITS'))
		AND all_months.cal_date > last_GRU.max_cal_date
),  __dbt__CTE__bpp_07_GRU_extend as (


SELECT
		base_product_number
		, country_alpha2
		, cal_date
		, base_gru
	FROM
		__dbt__CTE__bpp_03_Base_GRU_Gpsy

	UNION

	SELECT
		base_product_number
		, country_alpha2
		, cal_date
		, base_gru
	FROM
		__dbt__CTE__bpp_06_extend_months
),  __dbt__CTE__bpp_08_NPI_GRU as (


SELECT distinct
		insights_units_ibp.base_product_number
		, insights_units_ibp.country as country_alpha2
		, insights_units_ibp.cal_date
		, ff_uat_npi_base_gru.gru
	FROM
		(SELECT base_product_number, country, cal_date FROM "prod"."working_forecast_country" WHERE version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'INSIGHTS_UNITS')) insights_units_ibp
		INNER JOIN
		"mdm"."iso_country_code_xref" iso_country_code_xref
			ON insights_units_ibp.country = iso_country_code_xref.country_alpha2
		INNER JOIN
		"fin_prod"."npi_base_gru" ff_uat_npi_base_gru         
			ON ff_uat_npi_base_gru.base_product_number = insights_units_ibp.base_product_number
			AND ff_uat_npi_base_gru.region_5 = iso_country_code_xref.region_5
	WHERE 
		not exists
		    (SELECT 1 FROM __dbt__CTE__bpp_07_GRU_extend base_gru_ibp
		      WHERE base_gru_ibp.base_product_number = insights_units_ibp.base_product_number
	            AND base_gru_ibp.country_alpha2 = insights_units_ibp.country)
		AND insights_units_ibp.cal_date >= (SELECT cal_date FROM __dbt__CTE__bpp_02_filter_dates WHERE record = 'MIN_IBP_DATE')
		AND ff_uat_npi_base_gru.official = 1
),  __dbt__CTE__bpp_09_Base_GRU AS (


SELECT distinct
		Base_GRU_ibp.base_product_number
		, Base_GRU_ibp.country_alpha2
		, Base_GRU_ibp.cal_date
		, Base_GRU_ibp.base_gru
		--, max_cal_date
	FROM
		__dbt__CTE__bpp_07_GRU_extend Base_GRU_ibp

	UNION ALL

	SELECT distinct
		NPI_GRU.base_product_number
		, NPI_GRU.country_alpha2
		, NPI_GRU.cal_date
		, NPI_GRU.GRU as base_gru
		--, max_cal_date
	FROM
		__dbt__CTE__bpp_08_NPI_GRU NPI_GRU
),  __dbt__CTE__bpp_50_Base_GRU_override as (


SELECT distinct
		base_gru_ibp.base_product_number
		, base_gru_ibp.country_alpha2
		, base_gru_ibp.cal_date
		, coalesce(gru_override.gru, base_gru_ibp.base_gru) as base_gru
	FROM
		__dbt__CTE__bpp_09_Base_GRU base_gru_ibp
		LEFT JOIN
		"mdm"."iso_country_code_xref" ctry
			ON ctry.country_alpha2 = Base_GRU_ibp.country_alpha2
		LEFT JOIN
		"fin_prod"."forecast_gru_override" gru_override                          
			ON base_gru_ibp.base_product_number = gru_override.base_product_number
			AND ctry.region_5 = gru_override.region_5
			AND gru_override.official = 1
),  __dbt__CTE__bpp_10_Base_GRU_minIBP as (


SELECT distinct
		insights_units.base_product_number
		, insights_units.country as country_alpha2
		, insights_units.cal_date
		, SUM(insights_units.cartridges) OVER (PARTITION BY insights_units.base_product_number, insights_units.country
												, insights_units.cal_date) AS cartridges
		, base_gru
	FROM
		(SELECT base_product_number, country, cal_date, imp_corrected_cartridges as cartridges FROM "prod"."working_forecast_country"
		WHERE cal_date >= (SELECT cal_date FROM __dbt__CTE__bpp_02_filter_dates WHERE record = 'MIN_IBP_DATE')
		AND cal_date <= (SELECT cal_date FROM __dbt__CTE__bpp_02_filter_dates WHERE record = 'MAX_IBP_DATE')
		AND cartridges > 0
		AND version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'INSIGHTS_UNITS')) insights_units
		LEFT JOIN
		__dbt__CTE__bpp_50_Base_GRU_override gru_extend
			ON gru_extend.base_product_number = insights_units.base_product_number
			AND gru_extend.country_alpha2 = insights_units.country
			AND gru_extend.cal_date = insights_units.cal_date
),  __dbt__CTE__bpp_47_Base_GRU_maxIBP AS (


SELECT distinct
		insights_units.base_product_number
		, insights_units.country as country_alpha2
		, insights_units.cal_date
		, SUM(insights_units.cartridges) OVER (PARTITION BY insights_units.base_product_number, insights_units.country
												, insights_units.cal_date) AS cartridges
		, base_gru
	FROM
		(SELECT base_product_number, country, cal_date, imp_corrected_cartridges as cartridges FROM "prod"."working_forecast_country"
		WHERE --cal_date >= (SELECT cal_date FROM __dbt__CTE__bpp_02_filter_dates WHERE record = 'MIN_IBP_DATE')
		cal_date > (SELECT cal_date FROM __dbt__CTE__bpp_02_filter_dates WHERE record = 'MAX_IBP_DATE')
		AND cartridges > 0
		AND version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'INSIGHTS_UNITS')) insights_units
		LEFT JOIN
		__dbt__CTE__bpp_50_Base_GRU_override gru_extend
			ON gru_extend.base_product_number = insights_units.base_product_number
			AND gru_extend.country_alpha2 = insights_units.country
			AND gru_extend.cal_date = insights_units.cal_date
),  __dbt__CTE__bpp_48_Base_GRU_C2C_Merge as (


SELECT 
		base_product_number
		, country_alpha2
		, cal_date
		, cartridges
		, base_gru
	FROM
		__dbt__CTE__bpp_10_Base_GRU_minIBP

UNION ALL

SELECT 
		base_product_number
		, country_alpha2
		, cal_date
		, cartridges
		, base_gru
	FROM
		__dbt__CTE__bpp_47_Base_GRU_maxIBP
)SELECT DISTINCT
		base_gru_c2c.base_product_number
		, rdma.pl as base_product_line_code
		, base_gru_c2c.country_alpha2
		, iso_country_code_xref.region_5
		, base_gru_c2c.cal_date
		, calendar.fiscal_year_qtr
		, base_gru_c2c.cartridges
		, coalesce(base_gru_c2c.base_gru
				, lag(base_gru_c2c.base_gru) over
				    (PARTITION BY base_gru_c2c.base_product_number, base_gru_c2c.country_alpha2
				    ORDER BY base_gru_c2c.cal_date)) AS base_gru
	FROM
		__dbt__CTE__bpp_48_Base_GRU_C2C_Merge base_gru_c2c
	    INNER JOIN
	    "mdm"."calendar" calendar
	        ON calendar.date = base_gru_c2c.cal_date
	    INNER JOIN
	    "mdm"."iso_country_code_xref" iso_country_code_xref
	        ON iso_country_code_xref.country_alpha2 = base_gru_c2c.country_alpha2
		INNER JOIN
		"mdm"."rdma" rdma
			ON rdma.base_prod_number = base_gru_c2c.base_product_number
"""

query_list.append(["fin_stage.bpp_01_base_gru_insights", bpp_01_base_gru_insights, "overwrite"])

# COMMAND ----------

bpp_02_contra_insights = """


with __dbt__CTE__bpp_12_Contra_Region as (


SELECT
		forecast_contra.pl AS base_product_line_code
		, forecast_contra.region_5
		, forecast_contra.fiscal_yr_qtr
		, MAX(forecast_contra.fiscal_yr_qtr) OVER (PARTITION BY forecast_contra.pl, forecast_contra.region_5) AS max_qtr
		, forecast_contra.contra_per_qtr
		, forecast_contra.version AS fin_version
	FROM
		 "fin_prod"."forecast_contra_input" forecast_contra    
	WHERE official = 1
),  __dbt__CTE__bpp_14_last_Contra_per AS (


SELECT
		contra.base_product_line_code
		, contra.region_5
		, contra.contra_per_qtr
		, fin_version
	FROM
		__dbt__CTE__bpp_12_Contra_Region contra
	WHERE
		max_Qtr = Contra.fiscal_yr_qtr
),  __dbt__CTE__bpp_13_all_Qtrs_Contra AS (


SELECT distinct
    fiscal_year_qtr
FROM "mdm"."calendar"
WHERE
	fiscal_year_qtr <= (SELECT max(base_gru.fiscal_year_qtr) FROM "fin_stage"."bpp_01_base_gru_insights" base_gru)
	and fiscal_year_qtr > (SELECT max(contra.fiscal_yr_qtr) FROM __dbt__CTE__bpp_12_Contra_Region contra)
),  __dbt__CTE__bpp_15_extend_Contra_per as (


SELECT
		last_contra_per.base_product_line_code
		, last_contra_per.region_5
		, all_qtrs.fiscal_year_qtr
		, last_contra_per.contra_per_qtr
		, last_contra_per.fin_version
FROM
		__dbt__CTE__bpp_14_last_Contra_per last_contra_per
		CROSS JOIN
		__dbt__CTE__bpp_13_all_Qtrs_Contra all_qtrs
),  __dbt__CTE__bpp_16_Contra_combine as (


SELECT base_product_line_code
		, region_5
		, fiscal_yr_qtr AS fiscal_year_qtr
		, contra_per_qtr
		, fin_version
	FROM __dbt__CTE__bpp_12_Contra_Region
	UNION
	SELECT base_product_line_code
		, region_5
		, fiscal_year_qtr
		, contra_per_qtr
		, fin_version
	FROM __dbt__CTE__bpp_15_extend_Contra_per
)SELECT distinct
	base_gru.base_product_number
	, base_gru.base_product_line_code
	, base_gru.region_5
	, base_gru.country_alpha2
	, base_gru.cal_date
	, base_gru.fiscal_year_qtr
	, (base_gru.base_gru * contra_extend.contra_per_qtr) AS base_product_contra_usd
	, base_gru.base_gru
	, fin_version
FROM
	"fin_stage"."bpp_01_base_gru_insights" base_gru
	INNER JOIN
	__dbt__CTE__bpp_16_Contra_combine contra_extend
		ON base_gru.base_product_line_code = contra_extend.base_product_line_code
		AND base_gru.fiscal_year_qtr = contra_extend.fiscal_year_qtr
		AND base_gru.region_5 = contra_extend.region_5
"""

query_list.append(["fin_stage.bpp_02_contra_insights", bpp_02_contra_insights, "overwrite"])

# COMMAND ----------

bpp_03_base_currency_hedge_insights =  f"""


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
),   __dbt__CTE__bpp_18_revenue_sum AS (


SELECT
		product_line_scenarios_xref.pl_level_1
		, country_currency_map.currency_iso_code
		, base_gru.cal_date
		, SUM(coalesce(base_gru.cartridges, 0) * coalesce(base_gru.base_gru, 0)) AS revenue_sum
	FROM
	"fin_stage"."bpp_01_base_gru_insights" base_gru
	INNER JOIN
	"mdm"."country_currency_map" country_currency_map
		ON country_currency_map.country_alpha2 = base_gru.country_alpha2
	INNER JOIN
	"mdm"."product_line_scenarios_xref" product_line_scenarios_xref
		ON product_line_scenarios_xref.pl = base_gru.base_product_line_code
	WHERE
		product_line_scenarios_xref.pl_scenario = 'FINANCE-HEDGE'
		AND product_line_scenarios_xref.version = (SELECT version FROM __dbt__CTE__bpp_01_filter_vars WHERE record = 'PRODUCT_LINE_SCENARIOS')
	GROUP BY
		product_line_scenarios_xref.pl_level_1
		, country_currency_map.currency_iso_code
		, base_gru.cal_date
),  __dbt__CTE__bpp_19_revenue_currency_per AS (


SELECT
		revenue_sum.pl_level_1
		, revenue_sum.currency_iso_code
		, revenue_sum.cal_date
		, coalesce(revenue_currency_hedge, 0)/nullif(revenue_sum.revenue_sum, 0) AS hedge_per
		, currency_hedge.version
	FROM
		__dbt__CTE__bpp_18_revenue_sum revenue_sum
		INNER JOIN
		"prod"."currency_hedge" currency_hedge                
			on currency_hedge.Product_category = revenue_sum.pl_level_1
			and currency_hedge.currency = revenue_sum.currency_iso_code
			and revenue_sum.cal_date = currency_hedge.month
	WHERE currency_hedge.version = '{dbutils.widgets.get("currency_hedge_version")}'
)SELECT distinct
		base_gru.base_product_number
		, base_gru.country_alpha2
		, base_gru.cal_date
		, coalesce(base_gru.base_gru, 0) * coalesce(revenue_currency_per.hedge_per, 0) as baseprod_revenue_currency_hedge_unit
		, revenue_currency_per.version
	FROM
		"fin_stage"."bpp_01_base_gru_insights" base_gru
		INNER JOIN
		"mdm"."country_currency_map" country_currency_map
			ON country_currency_map.country_alpha2 = base_gru.country_alpha2
		INNER JOIN
		"mdm"."product_line_scenarios_xref" product_line_scenarios_xref
			ON product_line_scenarios_xref.pl = base_gru.base_product_line_code
		INNER JOIN
		__dbt__CTE__bpp_19_revenue_currency_per revenue_currency_per
			ON revenue_currency_per.cal_date = base_gru.cal_date
			AND revenue_currency_per.currency_iso_code = country_currency_map.currency_iso_code
			AND revenue_currency_per.pl_level_1 = product_line_scenarios_xref.pl_level_1
	WHERE
		product_line_scenarios_xref.pl_scenario = 'FINANCE-HEDGE'
		AND product_line_scenarios_xref.version = (SELECT version FROM __dbt__CTE__bpp_01_filter_vars WHERE record = 'PRODUCT_LINE_SCENARIOS')
"""

query_list.append(["fin_stage.bpp_03_base_currency_hedge_insights", bpp_03_base_currency_hedge_insights, "overwrite"])

# COMMAND ----------

bpp_04_base_fixed_cost_insights = """


with __dbt__CTE__bpp_22_Base_GrossRev_Reg3 AS (


SELECT distinct
		base_gru.base_product_line_code
		, iso_country_code_xref.region_3
		, base_gru.fiscal_year_qtr
		, sum(base_gru.base_gru * base_gru.cartridges) AS base_gr
	FROM
	"fin_stage"."bpp_01_base_gru_insights" base_gru
	INNER JOIN
	"mdm"."iso_country_code_xref" iso_country_code_xref
		ON Base_GRU.country_alpha2 = iso_country_code_xref.country_alpha2
	GROUP BY
		base_gru.base_product_line_code
		, iso_country_code_xref.region_3
		, base_gru.fiscal_year_qtr
),  __dbt__CTE__bpp_21_FixedCost_Region as (


SELECT DISTINCT
		pl AS base_product_line_code
		, CASE
			WHEN region_3 = 'EU' THEN 'EMEA'
			ELSE region_3
		END AS region_3
		, fiscal_yr_qtr
		, MAX(fiscal_yr_qtr) OVER (PARTITION BY pl, region_3) AS max_qtr
		, SUM(fixed_cost_k_qtr) * 1000 AS fixed_cost_qtr
		, version AS fin_version
	FROM
		"fin_prod"."forecast_fixed_cost_input"          
	WHERE official = 1
	group by
		pl
		, region_3
		, fiscal_yr_qtr
		, version
),  __dbt__CTE__bpp_23_Fixed_Cost_per as (


SELECT distinct
		base_grossrev.base_product_line_code
		, base_grossrev.region_3
		, base_grossrev.fiscal_year_qtr
		, fixed_cost.max_qtr
		, coalesce(fixed_cost_qtr, 0)/nullif(coalesce(base_grossrev.base_gr, 0), 0) AS per
		, fin_version
	FROM
		__dbt__CTE__bpp_22_Base_GrossRev_Reg3 base_grossrev
		INNER JOIN
		__dbt__CTE__bpp_21_FixedCost_Region fixed_cost
			ON fixed_cost.base_product_line_code = base_grossrev.base_product_line_code
			AND fixed_cost.fiscal_yr_qtr = base_grossrev.fiscal_year_qtr
			AND fixed_cost.region_3 = base_grossrev.region_3
),  __dbt__CTE__bpp_25_last_Fixed_per as (


SELECT distinct
		fixed_cost_per.base_product_line_code
		, fixed_cost_per.region_3
		, fixed_cost_per.per
		, fixed_cost_per.fin_version
	FROM
		__dbt__CTE__bpp_23_Fixed_Cost_per fixed_cost_per
	WHERE
		fixed_cost_per.max_qtr = fixed_cost_per.fiscal_year_qtr
),  __dbt__CTE__bpp_24_all_Qtrs_FixedCost AS (


SELECT distinct
    fiscal_year_qtr
FROM "mdm"."calendar"
WHERE
	fiscal_year_qtr <= (SELECT MAX(base_gru.fiscal_year_qtr) FROM "fin_stage"."bpp_01_base_gru_insights" base_gru)
	and fiscal_year_qtr > (SELECT MAX(fixed_cost.fiscal_yr_qtr) FROM __dbt__CTE__bpp_21_FixedCost_Region fixed_cost)
),  __dbt__CTE__bpp_26_extend_FixedCost_per AS (


SELECT distinct
		last_fixed_per.base_product_line_code
		, last_fixed_per.region_3
		, all_qtrs.fiscal_year_qtr
		, last_fixed_per.per
		, last_fixed_per.fin_version
	FROM
		__dbt__CTE__bpp_25_last_Fixed_per last_fixed_per
		JOIN
		__dbt__CTE__bpp_24_all_Qtrs_FixedCost all_qtrs
		    ON 1=1
),  __dbt__CTE__bpp_27_FixedCost_combine AS (


SELECT base_product_line_code
		, region_3
		, fiscal_year_qtr
		, per
		, fin_version
	FROM __dbt__CTE__bpp_23_Fixed_Cost_per
	UNION
	SELECT base_product_line_code
		, region_3
		, fiscal_year_qtr
		, per
		, fin_version
	FROM __dbt__CTE__bpp_26_extend_FixedCost_per
)SELECT distinct
	base_gru.base_product_number
	, base_gru.country_alpha2
	, base_gru.cal_date
	, coalesce(base_gru.base_gru, 0) * coalesce(fixed_cost_Per.per, 0) AS baseprod_fixed_cost_per_unit
	, fixed_cost_per.fin_version
FROM
	"fin_stage"."bpp_01_base_gru_insights" base_gru
	INNER JOIN
	"mdm"."calendar" calendar
		ON calendar.date = base_gru.cal_date
	INNER JOIN
	"mdm"."iso_country_code_xref" iso_country_code_xref
		ON iso_country_code_xref.country_alpha2 = base_gru.country_alpha2
	INNER JOIN
	__dbt__CTE__bpp_27_FixedCost_combine fixed_cost_per
		ON fixed_cost_Per.base_product_line_code = base_gru.base_product_line_code
		AND fixed_cost_Per.fiscal_year_qtr = calendar.fiscal_year_qtr
		AND fixed_cost_Per.region_3 = iso_country_code_xref.region_3
"""

query_list.append(["fin_stage.bpp_04_base_fixed_cost_insights", bpp_04_base_fixed_cost_insights, "overwrite"])

# COMMAND ----------

bpp_05_base_variable_cost_insights = """


with __dbt__CTE__bpp_29_Base_Product_family as (


SELECT
		distinct base_gru.base_product_number
		, rdma.Product_Family
	FROM
	"fin_stage"."bpp_01_base_gru_insights" base_gru
	INNER JOIN
	"mdm"."rdma" rdma
		on rdma.Base_Prod_Number = base_gru.base_product_number
),  __dbt__CTE__bpp_30_Ink_Qtr_costs as (


/*
SELECT
		base_family.base_product_number
		, forecast_variable_cost_ink_landing.region_5
		, forecast_variable_cost_ink_landing.Fiscal_Yr_Qtr
		, forecast_variable_cost_ink_landing.Variable_Cost
		, forecast_variable_cost_ink_landing.version as Fin_Version
		, max(Fiscal_Yr_Qtr) over
		    (partition by forecast_variable_cost_ink_landing.Product_Family, forecast_variable_cost_ink_landing.region_5) as max_Qtr
	FROM
		__dbt__CTE__bpp_29_Base_Product_family base_family
		inner join
		"fin_prod"."forecast_variable_cost_ink" forecast_variable_cost_ink_landing
			on forecast_variablecost_ink_landing.Product_Family = upper(base_family.Product_Family)
	WHERE base_family.official = 1
*/

SELECT
		rdma.base_prod_number as base_product_number
		, forecast_variable_cost_ink.region_5
		, forecast_variable_cost_ink.fiscal_yr_qtr
		, forecast_variable_cost_ink.variable_cost
		, forecast_variable_cost_ink.version as fin_version
		, MAX(fiscal_yr_qtr) over
		    (partition by forecast_variable_cost_ink.product_family, forecast_variable_cost_ink.region_5) as max_qtr
	FROM
		"fin_prod"."forecast_variable_cost_ink" forecast_variable_cost_ink
		INNER JOIN
		"mdm"."rdma" rdma
			on upper(forecast_variable_cost_ink.product_family) = upper(rdma.product_family)
	WHERE forecast_variable_cost_ink.official = 1
),  __dbt__CTE__bpp_31_Toner_Qtr_costs as (


SELECT
		forecast_variable_cost_toner.base_product_number
		, forecast_variable_cost_toner.region_5
		, forecast_variable_cost_toner.fiscal_yr_qtr
		, forecast_variable_cost_toner.variable_cost
		, forecast_variable_cost_toner.version as fin_version
		, MAX(fiscal_yr_qtr) over (partition by forecast_variable_cost_toner.base_product_number, forecast_variable_cost_toner.region_5) as max_qtr
	FROM
	"fin_prod"."forecast_variable_cost_toner" forecast_variable_cost_toner
	INNER JOIN
	"mdm"."supplies_xref" supplies_xref
	  on supplies_xref.base_product_number = forecast_variable_cost_toner.base_product_number
	WHERE supplies_xref.technology = 'LASER' and forecast_variable_cost_toner.official = 1
),  __dbt__CTE__bpp_32_Ink_Toner_Qtr_costs as (


SELECT
		base_product_number
		, region_5
		, fiscal_yr_qtr
		, variable_cost
		, fin_version
		, max_qtr
	FROM
		__dbt__CTE__bpp_30_Ink_Qtr_costs
	UNION ALL
	SELECT
		base_product_number
		, region_5
		, fiscal_yr_qtr
		, variable_cost
		, fin_version
		, max_qtr
	FROM
		__dbt__CTE__bpp_31_Toner_Qtr_costs
),  __dbt__CTE__bpp_34_last_Variable_Cost as (


SELECT
		base_product_number
		, region_5
		--, FixedCost_Per.max_Qtr
		, variable_cost
		, fin_version
	FROM
		__dbt__CTE__bpp_32_Ink_Toner_Qtr_costs ink_toner_costs_qtr
	WHERE
		max_qtr = ink_toner_costs_qtr.fiscal_yr_qtr
),  __dbt__CTE__bpp_33_all_Qtrs_VariableCost as (


SELECT distinct
    fiscal_year_qtr
FROM "mdm"."calendar"
WHERE
	fiscal_year_qtr <= (SELECT MAX(base_gru.fiscal_year_qtr) FROM "fin_stage"."bpp_01_base_gru_insights" base_gru)
	and fiscal_year_qtr > (SELECT MAX(var_cost.fiscal_yr_qtr) FROM __dbt__CTE__bpp_32_Ink_Toner_Qtr_costs var_cost)
),  __dbt__CTE__bpp_35_extend_VarCost as (


SELECT
		var_cost.base_product_number
		, var_cost.region_5
		, all_qtrs.fiscal_year_qtr fiscal_yr_qtr
		, var_cost.variable_cost
		, var_cost.fin_version
	FROM
		__dbt__CTE__bpp_34_last_Variable_Cost var_cost
		JOIN
		__dbt__CTE__bpp_33_all_Qtrs_VariableCost all_qtrs
	        on 1=1
),  __dbt__CTE__bpp_36_VariableCost_combine as (


SELECT base_product_number
		, region_5
		, fiscal_yr_qtr
		, variable_cost
		, fin_version
FROM __dbt__CTE__bpp_32_Ink_Toner_Qtr_costs
union all
SELECT base_product_number
		, region_5
		, fiscal_yr_qtr
		, variable_cost
		, fin_version
FROM __dbt__CTE__bpp_35_extend_VarCost
),  __dbt__CTE__bpp_36a_VariableCost_build_out as (


SELECT var_cost.base_product_number
		, var_cost.region_5
		, var_cost.fiscal_yr_qtr
		, var_cost.variable_cost
		, var_cost.fin_version
		, calendar.date as cal_date
		, iso_country_code.country_alpha2
FROM __dbt__CTE__bpp_36_VariableCost_combine var_cost
INNER JOIN
    "mdm"."calendar" calendar
    ON var_cost.fiscal_yr_qtr = calendar.fiscal_year_qtr
INNER JOIN
    "mdm"."iso_country_code_xref" iso_country_code
    on var_cost.region_5 = iso_country_code.region_5
WHERE
    calendar.day_of_month = 1
)/*
SELECT
		base_gru.base_product_number
		, base_gru.cal_date
		, base_gru.country_alpha2
		, variable_cost_extend.variable_cost as variable_cost_usd
		, variable_cost_extend.fin_version
	FROM
	"fin_stage"."bpp_01_base_gru_insights" base_gru
	INNER JOIN
	"mdm"."calendar" calendar
		on calendar.Date = base_gru.cal_date
	INNER JOIN
	__dbt__CTE__bpp_36_VariableCost_combine variable_cost_extend
		ON base_gru.base_product_number = variable_cost_extend.base_product_number
		AND base_gru.region_5 = variable_cost_extend.region_5
		AND variable_cost_extend.fiscal_yr_qtr = calendar.fiscal_year_qtr
*/

SELECT
		base_gru.base_product_number
		, base_gru.cal_date
		, base_gru.country_alpha2
		, variable_cost_extend.Variable_Cost as variable_cost_usd
		, variable_cost_extend.fin_version
	FROM
	"fin_stage"."bpp_01_base_gru_insights" base_gru
	INNER JOIN
	__dbt__CTE__bpp_36a_VariableCost_build_out variable_cost_extend
		on base_gru.base_product_number = variable_cost_extend.base_product_number
		and base_gru.country_alpha2 = variable_cost_extend.country_alpha2
		and variable_cost_extend.cal_date = base_gru.cal_date"""

query_list.append(["fin_stage.bpp_05_base_variable_cost_insights", bpp_05_base_variable_cost_insights, "overwrite"])

# COMMAND ----------

forecast_base_pl = f"""

with __dbt__CTE__bpp_49_Base_Contra_Insights_Override as (


select distinct
	base_gru.base_product_number
	, base_gru.base_product_line_code
	, base_gru.region_5
	, base_gru.country_alpha2
	, base_gru.cal_date
	, base_gru.fiscal_year_qtr
	, coalesce(base_gru.base_gru*contra_override.contra_per_qtr, base_product_contra_usd) as base_product_contra_usd
	--, (base_gru.base_gru * contra_extend.contra_per_qtr) base_product_contra_usd
	, fin_version
from
	"fin_stage"."bpp_02_contra_insights" base_gru
	LEFT JOIN
	(select * from "fin_prod"."forecast_contra_input" where official = 1 and base_product_number is not null) contra_override
		on base_gru.base_product_number = contra_override.base_product_number
		and base_gru.fiscal_year_qtr = contra_override.fiscal_yr_qtr
		and base_gru.region_5 = contra_override.region_5
),  __dbt__CTE__bpp_38_Base_PL_Contra as (


select distinct
	gru.base_product_number
	, gru.base_product_line_code
	, gru.region_5
	, gru.country_alpha2
	, gru.cal_date
	, gru.cartridges as insights_base_units
	, gru.base_gru as baseprod_gru
	, contra.base_product_contra_usd as baseprod_contra_per_unit
	, contra.fin_version as contra_version
from
	"fin_stage"."bpp_01_base_gru_insights" gru
	LEFT JOIN
	__dbt__CTE__bpp_49_Base_Contra_Insights_Override contra
		on contra.base_product_number = gru.base_product_number
		and contra.cal_date = gru.cal_date
		and contra.country_alpha2 = gru.country_alpha2
),  __dbt__CTE__bpp_39_Base_PL_Hedge as (


select distinct
	gru.base_product_number
	, gru.base_product_line_code
	, gru.region_5
	, gru.country_alpha2
	, gru.cal_date
	, gru.insights_base_units as insights_base_units
	, gru.baseprod_gru
	, gru.baseprod_contra_per_unit
	, currency_hedge.baseprod_revenue_currency_hedge_unit
	, gru.contra_version
	, currency_hedge.version as currency_hedge_version
from
	__dbt__CTE__bpp_38_Base_PL_Contra gru
	LEFT JOIN
	"fin_stage"."bpp_03_base_currency_hedge_insights" currency_hedge
		on currency_hedge.base_product_number = gru.base_product_number
		and currency_hedge.country_alpha2 = gru.country_alpha2
		and currency_hedge.cal_date = gru.cal_date
),  __dbt__CTE__bpp_40_Base_PL_FCost as (


select distinct
	gru.base_product_number
	, gru.base_product_line_code
	, gru.region_5
	, gru.country_alpha2
	, gru.cal_date
	, gru.insights_base_units
	, gru.baseprod_gru
	, gru.baseprod_contra_per_unit
	, gru.baseprod_revenue_currency_hedge_unit
	, fcost.baseprod_fixed_cost_per_unit
	, gru.contra_version
	, fcost.fin_version as fixed_cost_version
	, gru.currency_hedge_version
from
	__dbt__CTE__bpp_39_Base_PL_Hedge gru
	LEFT JOIN
	"fin_stage"."bpp_04_base_fixed_cost_insights" fcost
		on fcost.base_product_number = gru.base_product_number
		and fcost.country_alpha2 = gru.country_alpha2
		and fcost.cal_date = gru.cal_date
)select distinct
	gru.base_product_number
	, gru.base_product_line_code
	, gru.region_5
	, gru.country_alpha2
	, gru.cal_date
	, gru.insights_base_units
	, gru.baseprod_gru
	, gru.baseprod_contra_per_unit
	, gru.baseprod_revenue_currency_hedge_unit
	, vcost.variable_cost_usd as baseprod_variable_cost_per_unit
	, gru.baseprod_fixed_cost_per_unit
	, '{dbutils.widgets.get("currency_hedge_version")}' as sales_gru_version
	, gru.contra_version
	, vcost.fin_version as variable_cost_version
	, gru.fixed_cost_version
	, gru.currency_hedge_version
	, NULL AS load_date
	, NULL AS version
from
	__dbt__CTE__bpp_40_Base_PL_FCost gru
	LEFT JOIN
	"fin_stage"."bpp_05_base_variable_cost_insights" vcost
		on vcost.base_product_number = gru.base_product_number
		and vcost.country_alpha2 = gru.country_alpha2
		and vcost.cal_date = gru.cal_date
"""

query_list.append(["fin_stage.forecast_base_pl", forecast_base_pl, "overwrite"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list

# COMMAND ----------


