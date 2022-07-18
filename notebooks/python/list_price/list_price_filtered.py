# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

query_list = []

# COMMAND ----------

lpf_01_ibp_combined = f"""

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
    , max(version) AS version
FROM "prod"."working_forecast_country"

UNION ALL

SELECT 'LIST_PRICE_GPSY' AS record
    , max(version) AS version
FROM "prod"."list_price_gpsy"

UNION ALL

SELECT 'ACCT_RATES' AS record
    , max(version) AS version
FROM "prod"."acct_rates"
),  __dbt__CTE__lpf_06_ibp_regiON AS (


SELECT DISTINCT
		ibp_supplies_forecast.country_alpha2
		, iso_country_code_xref.regiON_5
		, ibp_supplies_forecast.sales_product_number
		, rdma_base_to_sales_product_map.sales_product_line_code
		, rdma_base_to_sales_product_map.base_product_number
		, rdma_base_to_sales_product_map.bASe_product_line_code
		, rdma_base_to_sales_product_map.base_prod_per_sales_prod_qty
		, rdma_base_to_sales_product_map.base_product_amount_percent
		, (ibp_supplies_forecast.units*rdma_base_to_sales_product_map.base_prod_per_sales_prod_qty) AS Base_Prod_fcst_Revenue_Units
		, ibp_supplies_forecast.cal_date
		, ibp_supplies_forecast.units
		, ibp_supplies_forecast.version
		, 'IBP' AS source
	FROM
		"prod"."ibp_supplies_forecast" ibp_supplies_forecast      
		LEFT JOIN
		"mdm"."iso_country_code_xref" iso_country_code_xref
			ON iso_country_code_xref.country_alpha2 = ibp_supplies_forecast.country_alpha2
		INNER JOIN
		"mdm"."rdma_base_to_sales_product_map" rdma_base_to_sales_product_map
			ON rdma_base_to_sales_product_map.sales_product_number = ibp_supplies_forecast.sales_product_number
	WHERE
		ibp_supplies_forecast.version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')
),  __dbt__CTE__lpf_02_filter_dates AS (


SELECT
    'ACTUALS_SUPPLIES' AS record
    , dateadd(month, -12, min(cal_date)) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                                 
WHERE
    version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'IBP_SUPPLIES_FORECAST' AS record
    , min(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                           
WHERE version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'MAX_IBP_SUPPLIES_FORECAST' AS record
    , max(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                             
WHERE
    version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT 'EFFECTIVE_DATE_ACCT_RATES' AS record
    , max(EffectiveDate) AS cal_date
FROM
    "prod"."acct_rates"                                       

UNION ALL

SELECT 'EOQ' AS record
    , max(load_date) AS cal_date
FROM
    "prod"."list_price_eoq"                                   
),  __dbt__CTE__lpf_03_Actuals_Baseprod AS (


SELECT
	actuals_supplies_baseprod.country_alpha2
	, actuals_supplies_baseprod.base_product_number
	, actuals_supplies_baseprod.pl base_product_line_code
	, sum(actuals_supplies_baseprod.revenue_units) revenue_units
	, sum(actuals_supplies_baseprod.gross_revenue) Gross_Rev
	, actuals_supplies_baseprod.version
FROM
    "fin_prod"."actuals_supplies_baseprod" actuals_supplies_baseprod
WHERE
	actuals_supplies_baseprod.version =
	            (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'ACTUALS_SUPPLIES_BASEPROD')
	AND actuals_supplies_baseprod.cal_date >
	            (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'ACTUALS_SUPPLIES')
GROUP BY
    actuals_supplies_baseprod.country_alpha2
    , actuals_supplies_baseprod.base_product_number
    , actuals_supplies_baseprod.pl
	, actuals_supplies_baseprod.version
),  __dbt__CTE__lpf_04_Actuals_Salesprod AS (


SELECT
    country_alpha2
	, sales_product_number
	, pl sales_product_line_code
	, SUM(revenue_units) revenue_units
	, sum(gross_revenue) Gross_Rev
	, version
FROM
    "fin_prod"."actuals_supplies_salesprod"
WHERE
	version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'ACTUALS_SUPPLIES_SALESPROD') AND
	   cal_date > (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'ACTUALS_SUPPLIES')
GROUP BY country_alpha2, sales_product_number, pl, version
),  __dbt__CTE__lpf_07_ibp_actuals AS (


SELECT
		DISTINCT
		Actuals_SalesProd.sales_product_number
		, rdma_base_to_sales_product_map.sales_product_line_code
		, Actuals_BaseProd.base_product_number
		, rdma_base_to_sales_product_map.base_product_line_code
		, Actuals_BaseProd.country_alpha2
		, iso_country_code_xref.regiON_5
		, Calendar.Date AS cal_date
		, base_prod_per_sales_prod_qty
		, base_product_amount_percent
		, Actuals_SalesProd.revenue_units AS Base_Prod_fcst_Revenue_Units
		, Actuals_BaseProd.revenue_units AS sum_bASe_fcst
		, Actuals_SalesProd.revenue_units/base_prod_per_sales_prod_qty AS units
		, Actuals_SalesProd.version
		, 'ACTUALS' AS source
	FROM
		__dbt__CTE__lpf_03_Actuals_Baseprod Actuals_BaseProd
		INNER JOIN
		"mdm"."rdma_base_to_sales_product_map" rdma_base_to_sales_product_map
			ON rdma_base_to_sales_product_map.base_product_number = Actuals_BaseProd.base_product_number
		INNER JOIN
		__dbt__CTE__lpf_04_Actuals_Salesprod Actuals_SalesProd
			ON Actuals_SalesProd.sales_product_number = rdma_base_to_sales_product_map.sales_product_number
			AND Actuals_BaseProd.country_alpha2 = Actuals_SalesProd.country_alpha2
		LEFT JOIN
		"mdm"."iso_country_code_xref" iso_country_code_xref
			ON iso_country_code_xref.country_alpha2 = Actuals_BaseProd.country_alpha2
		join
		 "mdm"."calendar" Calendar
		    ON 1 = 1
		/*INNER JOIN
		"IE2_Staging"."dbt"."Insights_Units" Insights_Units                               
			ON Insights_Units.base_product_number = Actuals_BASeProd.base_product_number
			AND Insights_Units.country = Actuals_BASeProd.country_alpha2
			AND Insights_Units.cal_date = Calendar.Date*/
	WHERE
		Actuals_salesprod.revenue_units > 0
		AND Actuals_BaseProd.revenue_units > 0
		AND Calendar.Date >= (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'IBP_SUPPLIES_FORECAST')
		AND Calendar.Day_of_month = 1
		AND Calendar.Date <= (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'MAX_IBP_SUPPLIES_FORECAST')
		AND not exists (SELECT 1 FROM __dbt__CTE__lpf_06_ibp_regiON ibp_regiON
						WHERE ibp_regiON.base_product_number = Actuals_BASeProd.base_product_number
						AND ibp_regiON.country_alpha2 = Actuals_BASeProd.country_alpha2
						AND ibp_regiON.sales_product_number = Actuals_SalesProd.sales_product_number)
		AND EXISTS (SELECT 1 FROM "prod"."working_forecast_country" Insights_Units
					WHERE Insights_Units.base_product_number = Actuals_BaseProd.base_product_number
					AND Insights_Units.country = Actuals_BaseProd.country_alpha2
					AND Insights_Units.cal_date = Calendar.Date
					AND cal_date >= (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'IBP_SUPPLIES_FORECAST')
					AND version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'INSIGHTS_UNITS')
					AND cartridges > 0)
)SELECT
		ibp_regiON.country_alpha2
		, ibp_regiON.regiON_5
		, ibp_regiON.sales_product_number
		, ibp_regiON.sales_product_line_code
		, ibp_regiON.base_product_number
		, ibp_regiON.base_product_line_code
		, ibp_regiON.base_prod_per_sales_prod_qty
		, ibp_regiON.base_product_amount_percent
		, ibp_regiON.Base_Prod_fcst_Revenue_Units
		, ibp_regiON.cal_date
		, ibp_regiON.units
		, ibp_regiON.version
		, ibp_regiON.source
	FROM
        __dbt__CTE__lpf_06_ibp_regiON ibp_regiON

UNION ALL

	SELECT
		ibp_actuals.country_alpha2
		, ibp_actuals.regiON_5
		, ibp_actuals.sales_product_number
		, ibp_actuals.sales_product_line_code
		, ibp_actuals.base_product_number
		, ibp_actuals.base_product_line_code
		, ibp_actuals.base_prod_per_sales_prod_qty
		, ibp_actuals.base_product_amount_percent
		, ibp_actuals.Base_Prod_fcst_Revenue_Units
		, ibp_actuals.cal_date
		, ibp_actuals.units
		, ibp_actuals.version
		, ibp_actuals.source
	FROM
	    __dbt__CTE__lpf_07_ibp_actuals ibp_actuals
"""

query_list.append(["fin_stage.lpf_01_ibp_combined", lpf_01_ibp_combined, "overwrite"])

# COMMAND ----------

lpf_02_list_price_EU = """


with __dbt__CTE__lpf_09_ibp_grp as (


SELECT distinct
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
	--WHERE sales_product_line_code in ('GJ', '5T')
),  __dbt__CTE__lpf_11_country_price_term_map as (


SELECT
		price_term_codes.country_alpha2 country_group
		, case
			when iso_cc_rollup_xref.country_level_1 is null then price_term_codes.country_alpha2
			else iso_cc_rollup_xref.country_alpha2
		end as country_code
		, price_term_codes.price_term_code
	FROM
		"mdm"."list_price_term_codes" price_term_codes
		left join
		"mdm"."iso_cc_rollup_xref" iso_cc_rollup_xref
			on iso_cc_rollup_xref.country_level_1 = price_term_codes.country_alpha2
			WHERE country_scenario = 'LIST_PRICE'
),   __dbt__CTE__lpf_01_filter_vars AS (


SELECT 'ACTUALS_SUPPLIES_BASEPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_bASeprod"

UNION ALL

SELECT 'ACTUALS_SUPPLIES_SALESPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_salesprod"

UNION ALL

SELECT 'IBP_SUPPLIES_FORECAST' AS record
    , MAX(version) AS version
FROM "prod"."ibp_supplies_forecASt"

UNION ALL

SELECT 'INSIGHTS_UNITS' AS record
    , max(version) AS version
FROM "prod"."working_forecast_country"

UNION ALL

SELECT 'LIST_PRICE_GPSY' AS record
    , max(version) AS version
FROM "prod"."list_price_gpsy"

UNION ALL

SELECT 'ACCT_RATES' AS record
    , max(version) AS version
FROM "prod"."acct_rates"
),  __dbt__CTE__lpf_10_list_price_all as (


SELECT distinct
		list_price_gpsy."product_number" as sales_product_number
		, list_price_gpsy."product_line" as sales_product_line_code
		, list_price_gpsy."country_code" as country_alpha2
		, list_price_gpsy."currency_code" as currency_code
		, list_price_gpsy."price_term_code" as Price_term_code
		, list_price_gpsy."price_start_effective_date" as price_start_effective_date
		, list_price_gpsy."qbl_sequence_number" as QB_Sequence_Number
		, list_price_gpsy."list_price" as List_Price
		, case
			when list_price_gpsy."price_term_code" = 'DP' then 1
			when list_price_gpsy."price_term_code" = 'DF' then 2
			when list_price_gpsy."price_term_code" = 'IN' then 3
			when list_price_gpsy."price_term_code" = 'RP' then 4
			end as price_term_code_priority
		, country_price_term_map.country_group
		, country_price_term_map.Price_Term_Code ctry_grp_price_term_code
		, list_price_gpsy.version as list_price_version
		, ibp_grp.version as sales_unit_version
		, ibp_grp.source as sales_unit_source
	FROM
	    "prod"."list_price_gpsy" list_price_gpsy
	    inner join
	    __dbt__CTE__lpf_09_ibp_grp ibp_grp
		    on ibp_grp.sales_product_number = list_price_gpsy."product_number"
		left join
		__dbt__CTE__lpf_11_country_price_term_map country_price_term_map
			on country_price_term_map.country_code = list_price_gpsy."country_code"
	WHERE
	    list_price_gpsy.version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'LIST_PRICE_GPSY')
	    and list_price_gpsy.currency_code <> 'CO'
		and list_price_gpsy.price_term_code in ('DP', 'DF', 'IN', 'RP')
)SELECT distinct
	ibp_grp.sales_product_number
	, ibp_grp.country_alpha2
	, list_price_all.sales_product_line_code
	, list_price_all.currency_code
	, list_price_all.price_term_code
	, list_price_all.price_start_effective_date
	, list_price_all.QB_Sequence_Number
	, list_price_all.List_Price
	, list_price_all.price_term_code_priority
	, list_price_all.list_price_version
	, list_price_all.sales_unit_version
	, list_price_all.sales_unit_source
FROM
	__dbt__CTE__lpf_10_list_price_all list_price_all
	inner join
	"mdm"."iso_country_code_xref" iso_country_code_xref
		on iso_country_code_xref.region_5 = list_price_all.country_alpha2
	inner join
	__dbt__CTE__lpf_09_ibp_grp ibp_grp
		on ibp_grp.sales_product_number = list_price_all.sales_product_number
		and iso_country_code_xref.country_alpha2 = ibp_grp.country_alpha2
	inner join
	"mdm"."list_price_EU_CountryList" list_price_EU_CountryList
		on list_price_EU_CountryList.country_alpha2 = ibp_grp.country_alpha2
		and
		    ((list_price_EU_CountryList.currency = 'EURO' and list_price_all.currency_code = 'EC')
		    or (list_price_EU_CountryList.currency = 'DOLLAR' and list_price_all.currency_code = 'UD'))
WHERE
	(list_price_all.country_alpha2 = 'EU')
	and Price_term_code in ('DP', 'DF')
	and not exists
	(SELECT * FROM __dbt__CTE__lpf_10_list_price_all list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		and list_price_all.country_alpha2 = list_price_all2.country_alpha2
		and list_price_all.currency_code = list_price_all2.currency_code
		and list_price_all.price_term_code = list_price_all2.price_term_code
		and (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			or (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			and list_price_all.QB_sequence_number < list_price_all2.QB_sequence_number)))
"""

query_list.append(["fin_stage.lpf_02_list_price_EU", lpf_02_list_price_EU, "overwrite"])

# COMMAND ----------

lpf_03_list_price_APJ = """


with __dbt__CTE__lpf_09_ibp_grp as (


SELECT distinct
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
	--WHERE sales_product_line_code in ('GJ', '5T')
),  __dbt__CTE__lpf_11_country_price_term_map as (


SELECT
		price_term_codes.country_alpha2 country_group
		, case
			when iso_cc_rollup_xref.country_level_1 is null then price_term_codes.country_alpha2
			else iso_cc_rollup_xref.country_alpha2
		end as country_code
		, price_term_codes.price_term_code
	FROM
		"mdm"."list_price_term_codes" price_term_codes
		left join
		"mdm"."iso_cc_rollup_xref" iso_cc_rollup_xref
			on iso_cc_rollup_xref.country_level_1 = price_term_codes.country_alpha2
			WHERE country_scenario = 'LIST_PRICE'
),  __dbt__CTE__lpf_01_filter_vars AS (


SELECT 'ACTUALS_SUPPLIES_BASEPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_bASeprod"

UNION ALL

SELECT 'ACTUALS_SUPPLIES_SALESPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_salesprod"

UNION ALL

SELECT 'IBP_SUPPLIES_FORECAST' AS record
    , MAX(version) AS version
FROM "prod"."ibp_supplies_forecASt"

UNION ALL

SELECT 'INSIGHTS_UNITS' AS record
    , max(version) AS version
FROM "prod"."working_forecast_country"

UNION ALL

SELECT 'LIST_PRICE_GPSY' AS record
    , max(version) AS version
FROM "prod"."list_price_gpsy"

UNION ALL

SELECT 'ACCT_RATES' AS record
    , max(version) AS version
FROM "prod"."acct_rates"
),  __dbt__CTE__lpf_10_list_price_all as (


SELECT distinct
		list_price_gpsy."product_number" as sales_product_number
		, list_price_gpsy."product_line" as sales_product_line_code
		, list_price_gpsy."country_code" as country_alpha2
		, list_price_gpsy."currency_code" as currency_code
		, list_price_gpsy."price_term_code" as Price_term_code
		, list_price_gpsy."price_start_effective_date" as price_start_effective_date
		, list_price_gpsy."qbl_sequence_number" as QB_Sequence_Number
		, list_price_gpsy."list_price" as List_Price
		, case
			when list_price_gpsy."price_term_code" = 'DP' then 1
			when list_price_gpsy."price_term_code" = 'DF' then 2
			when list_price_gpsy."price_term_code" = 'IN' then 3
			when list_price_gpsy."price_term_code" = 'RP' then 4
			end as price_term_code_priority
		, country_price_term_map.country_group
		, country_price_term_map.Price_Term_Code ctry_grp_price_term_code
		, list_price_gpsy.version as list_price_version
		, ibp_grp.version as sales_unit_version
		, ibp_grp.source as sales_unit_source
	FROM
	    "prod"."list_price_gpsy" list_price_gpsy
	    inner join
	    __dbt__CTE__lpf_09_ibp_grp ibp_grp
		    on ibp_grp.sales_product_number = list_price_gpsy."product_number"
		left join
		__dbt__CTE__lpf_11_country_price_term_map country_price_term_map
			on country_price_term_map.country_code = list_price_gpsy."country_code"
	WHERE
	    list_price_gpsy.version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'LIST_PRICE_GPSY')
	    and list_price_gpsy.currency_code <> 'CO'
		and list_price_gpsy.price_term_code in ('DP', 'DF', 'IN', 'RP')
)SELECT distinct
	list_price_all.sales_product_number
	, list_price_all.sales_product_line_code
	, list_price_all.country_alpha2
	, list_price_all.currency_code
	, list_price_all.price_term_code
	, list_price_all.price_start_effective_date
	, list_price_all.QB_Sequence_Number
	, list_price_all.List_Price
	, list_price_all.country_group
	, list_price_all.price_term_code_priority
	, case
		when list_price_all.country_group = 'EA' then 'UD'
		else list_price_all.currency_code
		end as SELECTed_currency_code
	, coalesce(list_price_all.ctry_grp_price_term_code, list_price_all.price_term_code) as SELECTed_price_term_code
	/*, list_price_all.list_price_version
	, list_price_all.sales_unit_version
	, list_price_all.sales_unit_source*/
FROM
	 __dbt__CTE__lpf_10_list_price_all list_price_all
WHERE
	 exists (SELECT 1 FROM __dbt__CTE__lpf_09_ibp_grp ibp_grp WHERE ibp_grp.region_5 in ('AP', 'JP')
	 and ibp_grp.sales_product_number = list_price_all.sales_product_number
		and list_price_all.country_alpha2 = ibp_grp.country_alpha2)
	and list_price_all.price_term_code <> 'RP'
	and not exists
	(SELECT * FROM __dbt__CTE__lpf_10_list_price_all list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		and list_price_all.country_alpha2 = list_price_all2.country_alpha2
		and list_price_all.price_term_code = list_price_all2.price_term_code
		and (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			or (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			and list_price_all.QB_sequence_number < list_price_all2.QB_sequence_number)))
"""

query_list.append(["fin_stage.lpf_03_list_price_APJ", lpf_03_list_price_APJ, "overwrite"])

# COMMAND ----------

lpf_04_list_price_LA = """


with __dbt__CTE__lpf_09_ibp_grp as (


SELECT distinct
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
	--WHERE sales_product_line_code in ('GJ', '5T')
),   __dbt__CTE__lpf_11_country_price_term_map as (


SELECT
		price_term_codes.country_alpha2 country_group
		, case
			when iso_cc_rollup_xref.country_level_1 is null then price_term_codes.country_alpha2
			else iso_cc_rollup_xref.country_alpha2
		end as country_code
		, price_term_codes.price_term_code
	FROM
		"mdm"."list_price_term_codes" price_term_codes
		left join
		"mdm"."iso_cc_rollup_xref" iso_cc_rollup_xref
			on iso_cc_rollup_xref.country_level_1 = price_term_codes.country_alpha2
			WHERE country_scenario = 'LIST_PRICE'
),  __dbt__CTE__lpf_01_filter_vars AS (


SELECT 'ACTUALS_SUPPLIES_BASEPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_bASeprod"

UNION ALL

SELECT 'ACTUALS_SUPPLIES_SALESPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_salesprod"

UNION ALL

SELECT 'IBP_SUPPLIES_FORECAST' AS record
    , MAX(version) AS version
FROM "prod"."ibp_supplies_forecASt"

UNION ALL

SELECT 'INSIGHTS_UNITS' AS record
    , max(version) AS version
FROM "prod"."working_forecast_country"

UNION ALL

SELECT 'LIST_PRICE_GPSY' AS record
    , max(version) AS version
FROM "prod"."list_price_gpsy"

UNION ALL

SELECT 'ACCT_RATES' AS record
    , max(version) AS version
FROM "prod"."acct_rates"
),  __dbt__CTE__lpf_10_list_price_all as (


SELECT distinct
		list_price_gpsy."product_number" as sales_product_number
		, list_price_gpsy."product_line" as sales_product_line_code
		, list_price_gpsy."country_code" as country_alpha2
		, list_price_gpsy."currency_code" as currency_code
		, list_price_gpsy."price_term_code" as Price_term_code
		, list_price_gpsy."price_start_effective_date" as price_start_effective_date
		, list_price_gpsy."qbl_sequence_number" as QB_Sequence_Number
		, list_price_gpsy."list_price" as List_Price
		, case
			when list_price_gpsy."price_term_code" = 'DP' then 1
			when list_price_gpsy."price_term_code" = 'DF' then 2
			when list_price_gpsy."price_term_code" = 'IN' then 3
			when list_price_gpsy."price_term_code" = 'RP' then 4
			end as price_term_code_priority
		, country_price_term_map.country_group
		, country_price_term_map.Price_Term_Code ctry_grp_price_term_code
		, list_price_gpsy.version as list_price_version
		, ibp_grp.version as sales_unit_version
		, ibp_grp.source as sales_unit_source
	FROM
	    "prod"."list_price_gpsy" list_price_gpsy
	    inner join
	    __dbt__CTE__lpf_09_ibp_grp ibp_grp
		    on ibp_grp.sales_product_number = list_price_gpsy."product_number"
		left join
		__dbt__CTE__lpf_11_country_price_term_map country_price_term_map
			on country_price_term_map.country_code = list_price_gpsy."country_code"
	WHERE
	    list_price_gpsy.version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'LIST_PRICE_GPSY')
	    and list_price_gpsy.currency_code <> 'CO'
		and list_price_gpsy.price_term_code in ('DP', 'DF', 'IN', 'RP')
)SELECT distinct
	list_price_all.sales_product_number
	, list_price_all.sales_product_line_code
	, list_price_all.country_alpha2
	, list_price_all.currency_code
	, list_price_all.price_term_code
	, list_price_all.price_start_effective_date
	, list_price_all.QB_Sequence_Number
	, list_price_all.List_Price
	, list_price_all.country_group
	, list_price_all.price_term_code_priority
	, case
		when list_price_all.country_alpha2 = 'MX' then 'MP'
		when (list_price_all.country_alpha2 = 'BR' and product_line_xref.business_division = 'OPS') then 'UD'
		when (list_price_all.country_alpha2 = 'BR' and product_line_xref.business_division = 'HPS') then 'BC'
		else 'UD'
		end as SELECTed_currency_code
	, case
		when (list_price_all.country_alpha2 = 'BR' and product_line_xref.business_division = 'OPS') then 'IN'
		when list_price_all.ctry_grp_price_term_code is not null then list_price_all.ctry_grp_price_term_code
		else list_price_all.price_term_code
		end as SELECTed_price_term_code
	--, coalesce(country_price_term_map.price_term_code, list_price_all.price_term_code) as SELECTed_price_term_code
	/*, list_price_all.list_price_version
	, list_price_all.sales_unit_version
	, list_price_all.sales_unit_source*/
FROM
	__dbt__CTE__lpf_10_list_price_all list_price_all
	/*inner join
	__dbt__CTE__lpf_09_ibp_grp ibp_grp
		on ibp_grp.sales_product_number = list_price_all.sales_product_number
		and list_price_all.country_alpha2 = ibp_grp.country_alpha2*/
	inner join
	"mdm"."product_line_xref" product_line_xref
		on product_line_xref.pl = list_price_all.sales_product_line_code
WHERE
	 exists (SELECT 1 FROM __dbt__CTE__lpf_09_ibp_grp ibp_grp WHERE ibp_grp.region_5 = 'LA'
			and ibp_grp.sales_product_number = list_price_all.sales_product_number
		and list_price_all.country_alpha2 = ibp_grp.country_alpha2)
	and list_price_all.price_term_code <> 'RP'
	and product_line_xref.PL_category = 'SUP' 
	and product_line_xref.technology in ('LASER', 'INK', 'PWA')
	and not exists
	(SELECT * FROM __dbt__CTE__lpf_10_list_price_all list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		and list_price_all.country_alpha2 = list_price_all2.country_alpha2
		and list_price_all.price_term_code = list_price_all2.price_term_code
		and list_price_all.currency_code = list_price_all2.currency_code
		and (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			or (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			and list_price_all.QB_sequence_number < list_price_all2.QB_sequence_number)))
"""

query_list.append(["fin_stage.lpf_04_list_price_LA", lpf_04_list_price_LA, "overwrite"])

# COMMAND ----------

lpf_05_list_price_NA = """


with __dbt__CTE__lpf_09_ibp_grp as (


SELECT distinct
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
	--WHERE sales_product_line_code in ('GJ', '5T')
),   __dbt__CTE__lpf_11_country_price_term_map as (


SELECT
		price_term_codes.country_alpha2 country_group
		, case
			when iso_cc_rollup_xref.country_level_1 is null then price_term_codes.country_alpha2
			else iso_cc_rollup_xref.country_alpha2
		end as country_code
		, price_term_codes.price_term_code
	FROM
		"mdm"."list_price_term_codes" price_term_codes
		left join
		"mdm"."iso_cc_rollup_xref" iso_cc_rollup_xref
			on iso_cc_rollup_xref.country_level_1 = price_term_codes.country_alpha2
			WHERE country_scenario = 'LIST_PRICE'
),  __dbt__CTE__lpf_01_filter_vars AS (


SELECT 'ACTUALS_SUPPLIES_BASEPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_bASeprod"

UNION ALL

SELECT 'ACTUALS_SUPPLIES_SALESPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_salesprod"

UNION ALL

SELECT 'IBP_SUPPLIES_FORECAST' AS record
    , MAX(version) AS version
FROM "prod"."ibp_supplies_forecASt"

UNION ALL

SELECT 'INSIGHTS_UNITS' AS record
    , max(version) AS version
FROM "prod"."working_forecast_country"

UNION ALL

SELECT 'LIST_PRICE_GPSY' AS record
    , max(version) AS version
FROM "prod"."list_price_gpsy"

UNION ALL

SELECT 'ACCT_RATES' AS record
    , max(version) AS version
FROM "prod"."acct_rates"
),  __dbt__CTE__lpf_10_list_price_all as (


SELECT distinct
		list_price_gpsy."product_number" as sales_product_number
		, list_price_gpsy."product_line" as sales_product_line_code
		, list_price_gpsy."country_code" as country_alpha2
		, list_price_gpsy."currency_code" as currency_code
		, list_price_gpsy."price_term_code" as Price_term_code
		, list_price_gpsy."price_start_effective_date" as price_start_effective_date
		, list_price_gpsy."qbl_sequence_number" as QB_Sequence_Number
		, list_price_gpsy."list_price" as List_Price
		, case
			when list_price_gpsy."price_term_code" = 'DP' then 1
			when list_price_gpsy."price_term_code" = 'DF' then 2
			when list_price_gpsy."price_term_code" = 'IN' then 3
			when list_price_gpsy."price_term_code" = 'RP' then 4
			end as price_term_code_priority
		, country_price_term_map.country_group
		, country_price_term_map.Price_Term_Code ctry_grp_price_term_code
		, list_price_gpsy.version as list_price_version
		, ibp_grp.version as sales_unit_version
		, ibp_grp.source as sales_unit_source
	FROM
	    "prod"."list_price_gpsy" list_price_gpsy
	    inner join
	    __dbt__CTE__lpf_09_ibp_grp ibp_grp
		    on ibp_grp.sales_product_number = list_price_gpsy."product_number"
		left join
		__dbt__CTE__lpf_11_country_price_term_map country_price_term_map
			on country_price_term_map.country_code = list_price_gpsy."country_code"
	WHERE
	    list_price_gpsy.version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'LIST_PRICE_GPSY')
	    and list_price_gpsy.currency_code <> 'CO'
		and list_price_gpsy.price_term_code in ('DP', 'DF', 'IN', 'RP')
)SELECT distinct
	list_price_all.sales_product_number
	, list_price_all.sales_product_line_code
	, list_price_all.country_alpha2
	, list_price_all.currency_code
	, list_price_all.price_term_code
	, list_price_all.price_start_effective_date
	, list_price_all.QB_Sequence_Number
	, list_price_all.List_Price
	--, country_price_term_map.country_group
	, list_price_all.price_term_code_priority
	, case
		when list_price_all.country_alpha2 = 'CA' then 'CD'
		else list_price_all.currency_code
		end as SELECTed_currency_code
FROM
	__dbt__CTE__lpf_10_list_price_all list_price_all
WHERE
	 exists (SELECT 1 FROM __dbt__CTE__lpf_09_ibp_grp ibp_grp WHERE ibp_grp.region_5 = 'NA'
		and ibp_grp.sales_product_number = list_price_all.sales_product_number
		and list_price_all.country_alpha2 = ibp_grp.country_alpha2)
	and list_price_all.price_term_code <> 'RP'
	and not exists
	(SELECT * FROM __dbt__CTE__lpf_10_list_price_all list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		and list_price_all.country_alpha2 = list_price_all2.country_alpha2
		and list_price_all.price_term_code = list_price_all2.price_term_code
		and (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			or (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			and list_price_all.QB_sequence_number < list_price_all2.QB_sequence_number)))
"""

query_list.append(["fin_stage.lpf_05_list_price_NA", lpf_05_list_price_NA, "overwrite"])

# COMMAND ----------


forecast_Sales_GRU_staging = f"""


with __dbt__CTE__lpf_01_filter_vars AS (


SELECT 'ACTUALS_SUPPLIES_BASEPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_bASeprod"

UNION ALL

SELECT 'ACTUALS_SUPPLIES_SALESPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_salesprod"

UNION ALL

SELECT 'IBP_SUPPLIES_FORECAST' AS record
    , MAX(version) AS version
FROM "prod"."ibp_supplies_forecASt"

UNION ALL

SELECT 'INSIGHTS_UNITS' AS record
    , max(version) AS version
FROM "prod"."working_forecast_country"

UNION ALL

SELECT 'LIST_PRICE_GPSY' AS record
    , max(version) AS version
FROM "prod"."list_price_gpsy"

UNION ALL

SELECT 'ACCT_RATES' AS record
    , max(version) AS version
FROM "prod"."acct_rates"
),  __dbt__CTE__lpf_17_list_price_APJ_filtered as (


SELECT distinct
	list_price_APJ.sales_product_number
	, list_price_APJ.sales_product_line_code
	, list_price_APJ.country_alpha2
	, list_price_APJ.currency_code
	, list_price_APJ.price_term_code
	, list_price_APJ.price_start_effective_date
	, list_price_APJ.QB_Sequence_Number
	, list_price_APJ.List_Price
	, list_price_APJ.price_term_code_priority
	/*, list_price_APJ.list_price_version
	, list_price_APJ.sales_unit_version
	, list_price_APJ.sales_unit_source*/
FROM
	"fin_stage"."lpf_03_list_price_APJ" list_price_APJ
WHERE
	list_price_APJ.Price_term_code = list_price_APJ.SELECTed_price_term_code
	and list_price_APJ.currency_code = list_price_APJ.SELECTed_currency_code
	and not exists(
	SELECT 1 FROM "fin_stage"."lpf_03_list_price_APJ" list_price_APJ_2
		WHERE list_price_APJ.sales_product_number = list_price_APJ_2.sales_product_number
		and list_price_APJ.country_alpha2 = list_price_APJ_2.country_alpha2
		and list_price_APJ_2.Price_term_code = list_price_APJ_2.SELECTed_price_term_code
		and list_price_APJ_2.currency_code = list_price_APJ_2.SELECTed_currency_code
		and list_price_APJ.price_term_code_priority > list_price_APJ_2.price_term_code_priority)
),  __dbt__CTE__lpf_19_list_price_LA_filtered as (


SELECT distinct
	list_price_LA.sales_product_number
	, list_price_LA.sales_product_line_code
	, list_price_LA.country_alpha2
	, list_price_LA.currency_code
	, list_price_LA.price_term_code
	, list_price_LA.price_start_effective_date
	, list_price_LA.QB_Sequence_Number
	, list_price_LA.List_Price
	, list_price_LA.price_term_code_priority
	/*, list_price_LA.list_price_version
	, list_price_LA.sales_unit_version
	, list_price_LA.sales_unit_source*/
FROM
	"fin_stage"."lpf_04_list_price_LA" list_price_LA
WHERE
	list_price_LA.Price_term_code = list_price_LA.SELECTed_price_term_code
	and list_price_LA.currency_code = list_price_LA.SELECTed_currency_code
	and not exists(
	SELECT 1 FROM "fin_stage"."lpf_04_list_price_LA" list_price_LA2
		WHERE list_price_LA.sales_product_number = list_price_LA2.sales_product_number
		and list_price_LA.country_alpha2 = list_price_LA2.country_alpha2
		and list_price_LA2.Price_term_code = list_price_LA2.SELECTed_price_term_code
		and list_price_LA2.currency_code = list_price_LA2.SELECTed_currency_code
		and list_price_LA.price_term_code_priority > list_price_LA2.price_term_code_priority)
),  __dbt__CTE__lpf_21_list_price_NA_filtered as (


SELECT distinct
	list_price_NA.sales_product_number
	, list_price_NA.sales_product_line_code
	, list_price_NA.country_alpha2
	, list_price_NA.currency_code
	, list_price_NA.price_term_code
	, list_price_NA.price_start_effective_date
	, list_price_NA.QB_Sequence_Number
	, list_price_NA.List_Price
	, list_price_NA.price_term_code_priority
	/*, list_price_NA.list_price_version
	, list_price_NA.sales_unit_version
	, list_price_NA.sales_unit_source*/
FROM
	"fin_stage"."lpf_05_list_price_NA" list_price_NA
WHERE
	--list_price_NA.Price_term_code = list_price_NA.SELECTed_price_term_code
	list_price_NA.currency_code = list_price_NA.SELECTed_currency_code
	and not exists(
	SELECT 1 FROM "fin_stage"."lpf_05_list_price_NA" list_price_NA2
		WHERE list_price_NA.sales_product_number = list_price_NA2.sales_product_number
		and list_price_NA.country_alpha2 = list_price_NA2.country_alpha2
		/*and list_price_NA2.Price_term_code = list_price_NA2.SELECTed_price_term_code*/
		and list_price_NA2.currency_code = list_price_NA2.SELECTed_currency_code
		and list_price_NA.price_term_code_priority > list_price_NA2.price_term_code_priority)
),  __dbt__CTE__lpf_09_ibp_grp as (


SELECT distinct
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
	--WHERE sales_product_line_code in ('GJ', '5T')
),  __dbt__CTE__lpf_14_list_price_EU_filtered as (


SELECT distinct
		list_price_EU.sales_product_number
		, list_price_EU.sales_product_line_code
		, list_price_EU.country_alpha2
		, list_price_EU.currency_code
		, list_price_EU.price_term_code
		, list_price_EU.price_start_effective_date
		, list_price_EU.QB_Sequence_Number
		, list_price_EU.List_Price
		, list_price_EU.price_term_code_priority
		, list_price_EU.list_price_version
		, list_price_EU.sales_unit_version
		, list_price_EU.sales_unit_source

FROM
	"fin_stage"."lpf_02_list_price_eu" list_price_EU
	inner join
	__dbt__CTE__lpf_09_ibp_grp ibp_grp
    	on ibp_grp.sales_product_number = list_price_EU.sales_product_number
	    and ibp_grp.country_alpha2 = list_price_EU.country_alpha2
WHERE
	not exists
	(
		SELECT 1 FROM
		"fin_stage"."lpf_02_list_price_eu" list_price_EU2
		WHERE list_price_EU.sales_product_number = list_price_EU2.sales_product_number
		and list_price_EU.country_alpha2 = list_price_EU2.country_alpha2
		and list_price_EU.price_term_code_priority > list_price_EU2.price_term_code_priority
	)
),   __dbt__CTE__lpf_11_country_price_term_map as (


SELECT
		price_term_codes.country_alpha2 country_group
		, case
			when iso_cc_rollup_xref.country_level_1 is null then price_term_codes.country_alpha2
			else iso_cc_rollup_xref.country_alpha2
		end as country_code
		, price_term_codes.price_term_code
	FROM
		"mdm"."list_price_term_codes" price_term_codes
		left join
		"mdm"."iso_cc_rollup_xref" iso_cc_rollup_xref
			on iso_cc_rollup_xref.country_level_1 = price_term_codes.country_alpha2
			WHERE country_scenario = 'LIST_PRICE'
),  __dbt__CTE__lpf_10_list_price_all as (


SELECT distinct
		list_price_gpsy."product_number" as sales_product_number
		, list_price_gpsy."product_line" as sales_product_line_code
		, list_price_gpsy."country_code" as country_alpha2
		, list_price_gpsy."currency_code" as currency_code
		, list_price_gpsy."price_term_code" as Price_term_code
		, list_price_gpsy."price_start_effective_date" as price_start_effective_date
		, list_price_gpsy."qbl_sequence_number" as QB_Sequence_Number
		, list_price_gpsy."list_price" as List_Price
		, case
			when list_price_gpsy."price_term_code" = 'DP' then 1
			when list_price_gpsy."price_term_code" = 'DF' then 2
			when list_price_gpsy."price_term_code" = 'IN' then 3
			when list_price_gpsy."price_term_code" = 'RP' then 4
			end as price_term_code_priority
		, country_price_term_map.country_group
		, country_price_term_map.Price_Term_Code ctry_grp_price_term_code
		, list_price_gpsy.version as list_price_version
		, ibp_grp.version as sales_unit_version
		, ibp_grp.source as sales_unit_source
	FROM
	    "prod"."list_price_gpsy" list_price_gpsy
	    inner join
	    __dbt__CTE__lpf_09_ibp_grp ibp_grp
		    on ibp_grp.sales_product_number = list_price_gpsy."product_number"
		left join
		__dbt__CTE__lpf_11_country_price_term_map country_price_term_map
			on country_price_term_map.country_code = list_price_gpsy."country_code"
	WHERE
	    list_price_gpsy.version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'LIST_PRICE_GPSY')
	    and list_price_gpsy.currency_code <> 'CO'
		and list_price_gpsy.price_term_code in ('DP', 'DF', 'IN', 'RP')
),  __dbt__CTE__lpf_15_list_price_USRP as (


SELECT distinct
	list_price_all.sales_product_number
	, list_price_all.sales_product_line_code
	, list_price_all.country_alpha2
	, list_price_all.currency_code
	, list_price_all.price_term_code
	, list_price_all.price_start_effective_date
	, list_price_all.QB_Sequence_Number
	, list_price_all.List_Price
	, list_price_all.price_term_code_priority
	/*, list_price_all.list_price_version
	, list_price_all.sales_unit_version
	, list_price_all.sales_unit_source*/
FROM
	__dbt__CTE__lpf_10_list_price_all list_price_all
WHERE
	((list_price_all.Price_term_code = 'RP'))
	and not exists
	(SELECT * FROM __dbt__CTE__lpf_10_list_price_all list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		and list_price_all.country_alpha2 = list_price_all2.country_alpha2
		and list_price_all.currency_code = list_price_all2.currency_code
		and list_price_all.price_term_code = list_price_all2.price_term_code
		and (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			or (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			and list_price_all.QB_sequence_number < list_price_all2.QB_sequence_number)))
),  __dbt__CTE__lpf_22_no_list_price as (


SELECT distinct
		ibp_grp.sales_product_number
		, ibp_grp.country_alpha2
FROM
	__dbt__CTE__lpf_09_ibp_grp ibp_grp
WHERE
	not exists
	(SELECT 1 FROM __dbt__CTE__lpf_14_list_price_EU_filtered list_price_EU_filtered
		WHERE list_price_EU_filtered.sales_product_number = ibp_grp.sales_product_number
		and list_price_EU_filtered.country_alpha2 = ibp_grp.country_alpha2
	)
	and not exists
	(SELECT 1 FROM __dbt__CTE__lpf_17_list_price_APJ_filtered list_price_APJ_filtered
		WHERE list_price_APJ_filtered.sales_product_number = ibp_grp.sales_product_number
		and list_price_APJ_filtered.country_alpha2 = ibp_grp.country_alpha2
	)
	and not exists
	(SELECT 1 FROM __dbt__CTE__lpf_19_list_price_LA_filtered list_price_LA_filtered
		WHERE list_price_LA_filtered.sales_product_number = ibp_grp.sales_product_number
		and list_price_LA_filtered.country_alpha2 = ibp_grp.country_alpha2
	)
	and not exists
	(SELECT 1 FROM __dbt__CTE__lpf_21_list_price_NA_filtered list_price_NA_filtered
		WHERE list_price_NA_filtered.sales_product_number = ibp_grp.sales_product_number
		and list_price_NA_filtered.country_alpha2 = ibp_grp.country_alpha2
	)
),  __dbt__CTE__lpf_23_list_price_RP_filtered as (


SELECT distinct
	no_list_price.sales_product_number
	, no_list_price.country_alpha2
	, list_price_USRP.sales_product_line_code
	, list_price_USRP.currency_code
	, list_price_USRP.price_term_code
	, list_price_USRP.price_start_effective_date
	, list_price_USRP.QB_Sequence_Number
	, list_price_USRP.List_Price
	/*, list_price_USRP.list_price_version
	, list_price_USRP.sales_unit_version
	, list_price_USRP.sales_unit_source*/
FROM
	__dbt__CTE__lpf_15_list_price_USRP list_price_USRP
	inner join
	__dbt__CTE__lpf_22_no_list_price no_list_price
			on list_price_USRP.sales_product_number = no_list_price.sales_product_number
),  __dbt__CTE__lpf_24_list_price_DP_DF_IN_RP as (


SELECT distinct
		sales_product_number
		, sales_product_line_code
		, country_alpha2
		, currency_code
		, price_term_code
		, price_start_effective_date
		, QB_Sequence_Number
		, List_Price
		/*, list_price_version
		, sales_unit_version
		, sales_unit_source*/
	FROM
		__dbt__CTE__lpf_17_list_price_APJ_filtered

	union all

	SELECT distinct
		sales_product_number
		, sales_product_line_code
		, country_alpha2
		, currency_code
		, price_term_code
		, price_start_effective_date
		, QB_Sequence_Number
		, List_Price
		/*, list_price_version
		, sales_unit_version
		, sales_unit_source*/
	FROM
		__dbt__CTE__lpf_19_list_price_LA_filtered

	union all

	SELECT distinct
		sales_product_number
		, sales_product_line_code
		, country_alpha2
		, currency_code
		, price_term_code
		, price_start_effective_date
		, QB_Sequence_Number
		, List_Price
		/*, list_price_version
		, sales_unit_version
		, sales_unit_source*/
	FROM
		__dbt__CTE__lpf_21_list_price_NA_filtered

	union all


	SELECT distinct
		sales_product_number
		, sales_product_line_code
		, country_alpha2
		, currency_code
		, price_term_code
		, price_start_effective_date
		, QB_Sequence_Number
		, List_Price
		/*, list_price_version
		, sales_unit_version
		, sales_unit_source*/
	FROM
		__dbt__CTE__lpf_14_list_price_EU_filtered

	union all

	SELECT distinct
		sales_product_number
		, sales_product_line_code
		, country_alpha2
		, currency_code
		, price_term_code
		, price_start_effective_date
		, QB_Sequence_Number
		, List_Price
		/*, list_price_version
		, sales_unit_version
		, sales_unit_source*/
	FROM
		__dbt__CTE__lpf_23_list_price_RP_filtered
),  __dbt__CTE__lpf_25_country_currency_map as (


SELECT distinct
    country_currency_map_landing."country_alpha2"
    , acct_rates."CurrencyCode"
	, acct_rates."AccountingRate"
FROM
      "mdm"."country_currency_map" country_currency_map_landing
	  inner join
	  "prod"."acct_rates" acct_rates
		on acct_rates.[IsoCurrCd] = country_currency_map_landing."currency_iso_code"
		and acct_rates.EffectiveDate = '{dbutils.widgets.get("accounting_eff_date")}'
		and acct_rates.version = '{dbutils.widgets.get("accounting_rate_version")}'
WHERE country_currency_map_landing.country_alpha2 is not null
),  __dbt__CTE__lpf_02_filter_dates AS (


SELECT
    'ACTUALS_SUPPLIES' AS record
    , dateadd(month, -12, min(cal_date)) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                                 
WHERE
    version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'IBP_SUPPLIES_FORECAST' AS record
    , min(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                           
WHERE version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'MAX_IBP_SUPPLIES_FORECAST' AS record
    , max(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                             
WHERE
    version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT 'EFFECTIVE_DATE_ACCT_RATES' AS record
    , max(EffectiveDate) AS cal_date
FROM
    "prod"."acct_rates"                                       

UNION ALL

SELECT 'EOQ' AS record
    , max(load_date) AS cal_date
FROM
    "prod"."list_price_eoq"                                   
),  __dbt__CTE__lpf_26_eoq as (


SELECT
		region_5
		, "product_line" as product_line
		, "eoq_discount_pct" as eoq_discount
		, load_date as eoq_load_date
	FROM
	"prod"."list_price_eoq"
	WHERE
	load_date = (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'EOQ')
),  __dbt__CTE__lpf_27_list_price_eoq as (


SELECT distinct
		list_price_DP_DF_IN_RP.sales_product_number
		, rdma_base_to_sales_product_map.sales_product_line_code
		, list_price_DP_DF_IN_RP.country_alpha2
		, iso_country_code_xref.region_5
		, list_price_DP_DF_IN_RP.currency_code
		, list_price_DP_DF_IN_RP.price_term_code
		, list_price_DP_DF_IN_RP.price_start_effective_date
		, list_price_DP_DF_IN_RP.QB_Sequence_Number
		, list_price_DP_DF_IN_RP.List_Price
		, country_currency_map.country_alpha2 currency_country
		, acct_rates.AccountingRate
		, count(List_Price) over (partition by list_price_DP_DF_IN_RP.sales_product_number, list_price_DP_DF_IN_RP.country_alpha2, list_price_DP_DF_IN_RP.Price_term_code) as count_List_Price
		, list_price_eoq.eoq_discount
	FROM
		__dbt__CTE__lpf_24_list_price_DP_DF_IN_RP list_price_DP_DF_IN_RP
		left join
		(SELECT distinct sales_product_number, sales_product_line_code FROM "mdm"."rdma_base_to_sales_product_map") rdma_base_to_sales_product_map
			on list_price_DP_DF_IN_RP.sales_product_number = rdma_base_to_sales_product_map.sales_product_number
		left join
		"mdm"."iso_country_code_xref" iso_country_code_xref
			on iso_country_code_xref.country_alpha2 = list_price_DP_DF_IN_RP.country_alpha2
		left join
		__dbt__CTE__lpf_25_country_currency_map country_currency_map
			on country_currency_map.country_alpha2 = list_price_DP_DF_IN_RP.country_alpha2
			and country_currency_map.CurrencyCode = list_price_DP_DF_IN_RP.currency_code
		left join
		"prod"."acct_rates" acct_rates
			on acct_rates.CurrencyCode = list_price_DP_DF_IN_RP.currency_code
		left join
		__dbt__CTE__lpf_26_eoq list_price_eoq
			on list_price_eoq.product_line = rdma_base_to_sales_product_map.sales_product_line_code
			and iso_country_code_xref.region_5 = list_price_eoq.Region_5
	WHERE --acct_rates.version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'ACCT_RATES')
	1=1
	and acct_rates.version = '{dbutils.widgets.get("accounting_rate_version")}'
		and acct_rates.EffectiveDate = '{dbutils.widgets.get("accounting_eff_date")}'
)SELECT distinct
		'FORECAST_SALES_GRU' as record
		,'{dbutils.widgets.get("forecast_record")}' as build_type
		,list_price_eoq.sales_product_number
		, list_price_eoq.region_5
		, list_price_eoq.country_alpha2
		, list_price_eoq.currency_code
		, list_price_eoq.price_term_code
		, list_price_eoq.price_start_effective_date
		, list_price_eoq.QB_Sequence_Number
		, list_price_eoq.List_Price
		, list_price_eoq.sales_product_line_code
		, list_price_eoq.AccountingRate
		, list_price_eoq.List_Price/list_price_eoq.AccountingRate as List_Price_USD
		, 0 as ListPriceAdder_LC
		, list_price_eoq.AccountingRate as CurrencyCode_Adder
		, 0 as ListPriceAdder_USD
		, coalesce(EOQ_Discount, 0) as EOQ_Discount
		, ((List_Price/AccountingRate) * (1-coalesce(EOQ_Discount, 0))) as SalesProduct_GRU
		, null as load_date
		, null as version
	FROM
		__dbt__CTE__lpf_27_list_price_eoq list_price_eoq
	WHERE
		(((count_List_Price > 1) and (currency_country is not null)) or (count_List_Price = 1))
"""

query_list.append(["fin_stage.forecast_Sales_GRU_staging", forecast_Sales_GRU_staging, "overwrite"])

# COMMAND ----------

list_price_version_staging = """


with __dbt__CTE__lpf_01_filter_vars AS (


SELECT 'ACTUALS_SUPPLIES_BASEPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_bASeprod"

UNION ALL

SELECT 'ACTUALS_SUPPLIES_SALESPROD' AS record
    , MAX(version) AS version
FROM "fin_prod"."actuals_supplies_salesprod"

UNION ALL

SELECT 'IBP_SUPPLIES_FORECAST' AS record
    , MAX(version) AS version
FROM "prod"."ibp_supplies_forecASt"

UNION ALL

SELECT 'INSIGHTS_UNITS' AS record
    , max(version) AS version
FROM "prod"."working_forecast_country"

UNION ALL

SELECT 'LIST_PRICE_GPSY' AS record
    , max(version) AS version
FROM "prod"."list_price_gpsy"

UNION ALL

SELECT 'ACCT_RATES' AS record
    , max(version) AS version
FROM "prod"."acct_rates"
),  __dbt__CTE__lpf_02_filter_dates AS (


SELECT
    'ACTUALS_SUPPLIES' AS record
    , dateadd(month, -12, min(cal_date)) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                                 
WHERE
    version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'IBP_SUPPLIES_FORECAST' AS record
    , min(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                           
WHERE version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'MAX_IBP_SUPPLIES_FORECAST' AS record
    , max(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                             
WHERE
    version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT 'EFFECTIVE_DATE_ACCT_RATES' AS record
    , max(EffectiveDate) AS cal_date
FROM
    "prod"."acct_rates"                                       

UNION ALL

SELECT 'EOQ' AS record
    , max(load_date) AS cal_date
FROM
    "prod"."list_price_eoq"                                   
),  __dbt__CTE__lpf_09_ibp_grp as (


SELECT distinct
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
	--WHERE sales_product_line_code in ('GJ', '5T')
),  __dbt__CTE__lpf_11_country_price_term_map as (


SELECT
		price_term_codes.country_alpha2 country_group
		, case
			when iso_cc_rollup_xref.country_level_1 is null then price_term_codes.country_alpha2
			else iso_cc_rollup_xref.country_alpha2
		end as country_code
		, price_term_codes.price_term_code
	FROM
		"mdm"."list_price_term_codes" price_term_codes
		left join
		"mdm"."iso_cc_rollup_xref" iso_cc_rollup_xref
			on iso_cc_rollup_xref.country_level_1 = price_term_codes.country_alpha2
			WHERE country_scenario = 'LIST_PRICE'
),  __dbt__CTE__lpf_10_list_price_all as (


SELECT distinct
		list_price_gpsy."product_number" as sales_product_number
		, list_price_gpsy."product_line" as sales_product_line_code
		, list_price_gpsy."country_code" as country_alpha2
		, list_price_gpsy."currency_code" as currency_code
		, list_price_gpsy."price_term_code" as Price_term_code
		, list_price_gpsy."price_start_effective_date" as price_start_effective_date
		, list_price_gpsy."qbl_sequence_number" as QB_Sequence_Number
		, list_price_gpsy."list_price" as List_Price
		, case
			when list_price_gpsy."price_term_code" = 'DP' then 1
			when list_price_gpsy."price_term_code" = 'DF' then 2
			when list_price_gpsy."price_term_code" = 'IN' then 3
			when list_price_gpsy."price_term_code" = 'RP' then 4
			end as price_term_code_priority
		, country_price_term_map.country_group
		, country_price_term_map.Price_Term_Code ctry_grp_price_term_code
		, list_price_gpsy.version as list_price_version
		, ibp_grp.version as sales_unit_version
		, ibp_grp.source as sales_unit_source
	FROM
	    "prod"."list_price_gpsy" list_price_gpsy
	    inner join
	    __dbt__CTE__lpf_09_ibp_grp ibp_grp
		    on ibp_grp.sales_product_number = list_price_gpsy."product_number"
		left join
		__dbt__CTE__lpf_11_country_price_term_map country_price_term_map
			on country_price_term_map.country_code = list_price_gpsy."country_code"
	WHERE
	    list_price_gpsy.version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'LIST_PRICE_GPSY')
	    and list_price_gpsy.currency_code <> 'CO'
		and list_price_gpsy.price_term_code in ('DP', 'DF', 'IN', 'RP')
),  __dbt__CTE__lpf_26_eoq as (


SELECT
		region_5
		, "product_line" as product_line
		, "eoq_discount_pct" as eoq_discount
		, load_date as eoq_load_date
	FROM
	"prod"."list_price_eoq"
	WHERE
	load_date = (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'EOQ')
)SELECT distinct
	'' as record
	, (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'LIST_PRICE_GPSY') as lp_gpsy_version
	, (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST') as ibp_version
	, '{dbutils.widgets.get("accounting_rate_version")}' as acct_rates_version
	, (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'EOQ') as eoq_load_date
	, NULL as version
	, NULL as load_date

	FROM
		__dbt__CTE__lpf_10_list_price_all list_price_all
		inner join 
		__dbt__CTE__lpf_26_eoq eoq
			on list_price_all.sales_product_line_code = eoq.product_line
"""

query_list.append(["fin_stage.list_price_version_staging", list_price_version_staging, "overwrite"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
