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
),  __dbt__CTE__lpf_06_ibp_region AS (


SELECT DISTINCT
		ibp_supplies_forecast.country_alpha2
		, iso_country_code_xref.region_5
		, ibp_supplies_forecast.sales_product_number
		, rdma_base_to_sales_product_map.sales_product_line_code
		, rdma_base_to_sales_product_map.base_product_number
		, rdma_base_to_sales_product_map.baSe_product_line_code
		, rdma_base_to_sales_product_map.base_prod_per_sales_prod_qty
		, rdma_base_to_sales_product_map.base_product_amount_percent
		, (ibp_supplies_forecast.units*rdma_base_to_sales_product_map.base_prod_per_sales_prod_qty) AS base_prod_fcst_revenue_units
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
    , MIN(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                           
WHERE version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'MAX_IBP_SUPPLIES_FORECAST' AS record
    , MAX(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                             
WHERE
    version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT 'EFFECTIVE_DATE_ACCT_RATES' AS record
    , MAX(effective_date) AS cal_date
FROM
    "prod"."acct_rates"                                       

UNION ALL

SELECT 'EOQ' AS record
    , MAX(load_date) AS cal_date
FROM
    "prod"."list_price_eoq"                                   
),  __dbt__CTE__lpf_03_Actuals_Baseprod AS (


SELECT
	actuals_supplies_baseprod.country_alpha2
	, actuals_supplies_baseprod.base_product_number
	, actuals_supplies_baseprod.pl AS base_product_line_code
	, SUM(actuals_supplies_baseprod.revenue_units) AS revenue_units
	, SUM(actuals_supplies_baseprod.gross_revenue) AS gross_rev
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
	, pl AS sales_product_line_code
	, SUM(revenue_units) AS revenue_units
	, SUM(gross_revenue) AS gross_rev
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
		actuals_sales_prod.sales_product_number
		, rdma_base_to_sales_product_map.sales_product_line_code
		, actuals_base_prod.base_product_number
		, rdma_base_to_sales_product_map.base_product_line_code
		, actuals_base_prod.country_alpha2
		, iso_country_code_xref.region_5
		, calendar.date AS cal_date
		, base_prod_per_sales_prod_qty
		, base_product_amount_percent
		, actuals_sales_prod.revenue_units AS base_prod_fcst_revenue_units
		, actuals_base_prod.revenue_units AS sum_base_fcst
		, actuals_sales_prod.revenue_units/base_prod_per_sales_prod_qty AS units
		, actuals_sales_prod.version
		, 'ACTUALS' AS source
	FROM
		__dbt__CTE__lpf_03_Actuals_Baseprod actuals_base_prod
		INNER JOIN
		"mdm"."rdma_base_to_sales_product_map" rdma_base_to_sales_product_map
			ON rdma_base_to_sales_product_map.base_product_number = actuals_base_prod.base_product_number
		INNER JOIN
		__dbt__CTE__lpf_04_Actuals_Salesprod actuals_sales_prod
			ON actuals_sales_prod.sales_product_number = rdma_base_to_sales_product_map.sales_product_number
			AND actuals_base_prod.country_alpha2 = actuals_sales_prod.country_alpha2
		LEFT JOIN
		"mdm"."iso_country_code_xref" iso_country_code_xref
			ON iso_country_code_xref.country_alpha2 = actuals_base_prod.country_alpha2
		JOIN
		 "mdm"."calendar" calendar
		    ON 1 = 1
	WHERE
		actuals_sales_prod.revenue_units > 0
		AND actuals_base_prod.revenue_units > 0
		AND calendar.date >= (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'IBP_SUPPLIES_FORECAST')
		AND calendar.day_of_month = 1
		AND calendar.date <= (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'MAX_IBP_SUPPLIES_FORECAST')
		AND not exists (SELECT 1 FROM __dbt__CTE__lpf_06_ibp_region ibp_region
						WHERE ibp_region.base_product_number = actuals_base_prod.base_product_number
						AND ibp_region.country_alpha2 = actuals_base_prod.country_alpha2
						AND ibp_region.sales_product_number = actuals_sales_prod.sales_product_number)
		AND EXISTS (SELECT 1 FROM "prod"."working_forecast_country" insights_units
					WHERE insights_units.base_product_number = actuals_base_prod.base_product_number
					AND insights_units.country = actuals_base_prod.country_alpha2
					AND insights_units.cal_date = calendar.date
					AND cal_date >= (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'IBP_SUPPLIES_FORECAST')
					AND version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'INSIGHTS_UNITS')
					AND cartridges > 0)
)SELECT
		ibp_region.country_alpha2
		, ibp_region.region_5
		, ibp_region.sales_product_number
		, ibp_region.sales_product_line_code
		, ibp_region.base_product_number
		, ibp_region.base_product_line_code
		, ibp_region.base_prod_per_sales_prod_qty
		, ibp_region.base_product_amount_percent
		, ibp_region.base_prod_fcst_revenue_units
		, ibp_region.cal_date
		, ibp_region.units
		, ibp_region.version
		, ibp_region.source
	FROM
        __dbt__CTE__lpf_06_ibp_region ibp_region

UNION ALL

	SELECT
		ibp_actuals.country_alpha2
		, ibp_actuals.region_5
		, ibp_actuals.sales_product_number
		, ibp_actuals.sales_product_line_code
		, ibp_actuals.base_product_number
		, ibp_actuals.base_product_line_code
		, ibp_actuals.base_prod_per_sales_prod_qty
		, ibp_actuals.base_product_amount_percent
		, ibp_actuals.base_prod_fcst_revenue_units
		, ibp_actuals.cal_date
		, ibp_actuals.units
		, ibp_actuals.version
		, ibp_actuals.source
	FROM
	    __dbt__CTE__lpf_07_ibp_actuals ibp_actuals
"""

query_list.append(["fin_stage.lpf_01_ibp_combined", lpf_01_ibp_combined, "overwrite"])

# COMMAND ----------

lpf_02_list_price_all = """


with __dbt__CTE__lpf_09_ibp_grp as (


SELECT DISTINCT
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
		price_term_codes.country_alpha2 AS country_group
		, case
			WHEN iso_cc_rollup_xref.country_level_1 is null THEN price_term_codes.country_alpha2
			else iso_cc_rollup_xref.country_alpha2
		end AS country_code
		, price_term_codes.price_term_code
	FROM
		"mdm"."list_price_term_codes" price_term_codes
		LEFT JOIN
		"mdm"."iso_cc_rollup_xref" iso_cc_rollup_xref
			ON iso_cc_rollup_xref.country_level_1 = price_term_codes.country_alpha2
			WHERE country_scenario = 'LIST_PRICE'
),   __dbt__CTE__lpf_01_filter_vars AS (


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
)
SELECT DISTINCT
		list_price_gpsy."product_number" AS sales_product_number
		, list_price_gpsy."product_line" AS sales_product_line_code
		, list_price_gpsy."country_code" AS country_alpha2
		, list_price_gpsy."currency_code" AS currency_code
		, list_price_gpsy."price_term_code" AS price_term_code
		, list_price_gpsy."price_start_effective_date" AS price_start_effective_date
		, list_price_gpsy."qbl_sequence_number" AS qb_sequence_number
		, list_price_gpsy."list_price" AS list_price
		, case
			WHEN list_price_gpsy."price_term_code" = 'DP' THEN 1
			WHEN list_price_gpsy."price_term_code" = 'DF' THEN 2
			WHEN list_price_gpsy."price_term_code" = 'IN' THEN 3
			WHEN list_price_gpsy."price_term_code" = 'RP' THEN 4
			end AS price_term_code_priority
		, country_price_term_map.country_group
		, country_price_term_map.price_term_code AS ctry_grp_price_term_code
		, list_price_gpsy.version AS list_price_version
		, ibp_grp.version AS sales_unit_version
		, ibp_grp.source AS sales_unit_source
	FROM
	    "prod"."list_price_gpsy" list_price_gpsy
	    INNER JOIN
	    __dbt__CTE__lpf_09_ibp_grp ibp_grp
		    ON ibp_grp.sales_product_number = list_price_gpsy."product_number"
		LEFT JOIN
		__dbt__CTE__lpf_11_country_price_term_map country_price_term_map
			ON country_price_term_map.country_code = list_price_gpsy."country_code"
	WHERE
	    list_price_gpsy.version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'LIST_PRICE_GPSY')
	    AND list_price_gpsy.currency_code <> 'CO'
		AND list_price_gpsy.price_term_code in ('DP', 'DF', 'IN', 'RP')
"""
query_list.append(["fin_stage.lpf_02_list_price_all", lpf_02_list_price_all, "overwrite"])

# COMMAND ----------

lpf_03_list_price_eu = """


with __dbt__CTE__lpf_09_ibp_grp as (


SELECT DISTINCT
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
)
SELECT DISTINCT
	ibp_grp.sales_product_number
	, ibp_grp.country_alpha2
	, list_price_all.sales_product_line_code
	, list_price_all.currency_code
	, list_price_all.price_term_code
	, list_price_all.price_start_effective_date
	, list_price_all.qb_sequence_number
	, list_price_all.list_price
	, list_price_all.price_term_code_priority
	, list_price_all.list_price_version
	, list_price_all.sales_unit_version
	, list_price_all.sales_unit_source
FROM
	"fin_stage"."lpf_02_list_price_all" list_price_all
	INNER JOIN
	"mdm"."iso_country_code_xref" iso_country_code_xref
		on iso_country_code_xref.region_5 = list_price_all.country_alpha2
	INNER JOIN
	__dbt__CTE__lpf_09_ibp_grp ibp_grp
		on ibp_grp.sales_product_number = list_price_all.sales_product_number
		and iso_country_code_xref.country_alpha2 = ibp_grp.country_alpha2
	INNER JOIN
	"mdm"."list_price_eu_countrylist" list_price_eu_countrylist
		ON list_price_eu_countrylist.country_alpha2 = ibp_grp.country_alpha2
		AND
		    ((list_price_eu_countrylist.currency = 'EURO' AND list_price_all.currency_code = 'EC')
		    OR (list_price_eu_countrylist.currency = 'DOLLAR' AND list_price_all.currency_code = 'UD'))
WHERE
	(list_price_all.country_alpha2 = 'EU')
	AND price_term_code IN ('DP', 'DF')
	AND not exists
	(SELECT * FROM "fin_stage"."lpf_02_list_price_all" list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		AND list_price_all.country_alpha2 = list_price_all2.country_alpha2
		AND list_price_all.currency_code = list_price_all2.currency_code
		AND list_price_all.price_term_code = list_price_all2.price_term_code
		AND (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			OR (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			AND list_price_all.qb_sequence_number < list_price_all2.qb_sequence_number)))
"""

query_list.append(["fin_stage.lpf_03_list_price_eu", lpf_03_list_price_eu, "overwrite"])

# COMMAND ----------

lpf_04_list_price_apj = """


with __dbt__CTE__lpf_09_ibp_grp as (


SELECT DISTINCT
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
)
SELECT DISTINCT
	list_price_all.sales_product_number
	, list_price_all.sales_product_line_code
	, list_price_all.country_alpha2
	, list_price_all.currency_code
	, list_price_all.price_term_code
	, list_price_all.price_start_effective_date
	, list_price_all.qb_sequence_number
	, list_price_all.list_price
	, list_price_all.country_group
	, list_price_all.price_term_code_priority
	, CASE
		WHEN list_price_all.country_group = 'EA' THEN 'UD'
		ELSE list_price_all.currency_code
		END AS selected_currency_code
	, coalesce(list_price_all.ctry_grp_price_term_code, list_price_all.price_term_code) as selected_price_term_code
FROM
	 "fin_stage"."lpf_02_list_price_all" list_price_all
WHERE
	 EXISTS (SELECT 1 FROM __dbt__CTE__lpf_09_ibp_grp ibp_grp WHERE ibp_grp.region_5 in ('AP', 'JP')
	 AND ibp_grp.sales_product_number = list_price_all.sales_product_number
		AND list_price_all.country_alpha2 = ibp_grp.country_alpha2)
	AND list_price_all.price_term_code <> 'RP'
	AND not exists
	(SELECT * FROM "fin_stage"."lpf_02_list_price_all" list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		AND list_price_all.country_alpha2 = list_price_all2.country_alpha2
		AND list_price_all.price_term_code = list_price_all2.price_term_code
		AND (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			OR (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			AND list_price_all.qb_sequence_number < list_price_all2.qb_sequence_number)))
"""

query_list.append(["fin_stage.lpf_04_list_price_apj", lpf_04_list_price_apj, "overwrite"])

# COMMAND ----------

lpf_05_list_price_la = """


with __dbt__CTE__lpf_09_ibp_grp as (


SELECT DISTINCT
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
	--WHERE sales_product_line_code in ('GJ', '5T')
)
SELECT DISTINCT
	list_price_all.sales_product_number
	, list_price_all.sales_product_line_code
	, list_price_all.country_alpha2
	, list_price_all.currency_code
	, list_price_all.price_term_code
	, list_price_all.price_start_effective_date
	, list_price_all.qb_sequence_number
	, list_price_all.list_price
	, list_price_all.country_group
	, list_price_all.price_term_code_priority
	, case
		WHEN list_price_all.country_alpha2 = 'MX' THEN 'MP'
		WHEN (list_price_all.country_alpha2 = 'BR' and product_line_xref.business_division = 'OPS') THEN 'UD'
		WHEN (list_price_all.country_alpha2 = 'BR' and product_line_xref.business_division = 'HPS') THEN 'BC'
		else 'UD'
		end as selected_currency_code
	, case
		WHEN (list_price_all.country_alpha2 = 'BR' and product_line_xref.business_division = 'OPS') THEN 'IN'
		WHEN list_price_all.ctry_grp_price_term_code is not null THEN list_price_all.ctry_grp_price_term_code
		else list_price_all.price_term_code
		end as selected_price_term_code
FROM
	"fin_stage"."lpf_02_list_price_all" list_price_all
	INNER JOIN
	"mdm"."product_line_xref" product_line_xref
		ON product_line_xref.pl = list_price_all.sales_product_line_code
WHERE
	 exists (SELECT 1 FROM __dbt__CTE__lpf_09_ibp_grp ibp_grp WHERE ibp_grp.region_5 = 'LA'
			AND ibp_grp.sales_product_number = list_price_all.sales_product_number
		AND list_price_all.country_alpha2 = ibp_grp.country_alpha2)
	AND list_price_all.price_term_code <> 'RP'
	AND product_line_xref.pl_category = 'SUP' 
	AND product_line_xref.technology in ('LASER', 'INK', 'PWA')
	AND not exists
	(SELECT * FROM "fin_stage"."lpf_02_list_price_all" list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		AND list_price_all.country_alpha2 = list_price_all2.country_alpha2
		AND list_price_all.price_term_code = list_price_all2.price_term_code
		AND list_price_all.currency_code = list_price_all2.currency_code
		AND (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			OR (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			AND list_price_all.qb_sequence_number < list_price_all2.qb_sequence_number)))
"""

query_list.append(["fin_stage.lpf_05_list_price_la", lpf_05_list_price_la, "overwrite"])

# COMMAND ----------

lpf_06_list_price_na = """


with __dbt__CTE__lpf_09_ibp_grp as (


SELECT DISTINCT
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
)
SELECT DISTINCT
	list_price_all.sales_product_number
	, list_price_all.sales_product_line_code
	, list_price_all.country_alpha2
	, list_price_all.currency_code
	, list_price_all.price_term_code
	, list_price_all.price_start_effective_date
	, list_price_all.qb_sequence_number
	, list_price_all.list_price
	, list_price_all.price_term_code_priority
	, case
		WHEN list_price_all.country_alpha2 = 'CA' THEN 'CD'
		else list_price_all.currency_code
		end as selected_currency_code
FROM
	"fin_stage"."lpf_02_list_price_all" list_price_all
WHERE
	 exists (SELECT 1 FROM __dbt__CTE__lpf_09_ibp_grp ibp_grp WHERE ibp_grp.region_5 = 'NA'
		AND ibp_grp.sales_product_number = list_price_all.sales_product_number
		AND list_price_all.country_alpha2 = ibp_grp.country_alpha2)
	AND list_price_all.price_term_code <> 'RP'
	AND not exists
	(SELECT * FROM "fin_stage"."lpf_02_list_price_all" list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		AND list_price_all.country_alpha2 = list_price_all2.country_alpha2
		AND list_price_all.price_term_code = list_price_all2.price_term_code
		AND (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			OR (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			AND list_price_all.qb_sequence_number < list_price_all2.qb_sequence_number)))
"""

query_list.append(["fin_stage.lpf_06_list_price_na", lpf_06_list_price_na, "overwrite"])

# COMMAND ----------

forecast_sales_gru = f"""


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
),  __dbt__CTE__lpf_17_list_price_apj_filtered as (


SELECT DISTINCT
	list_price_apj.sales_product_number
	, list_price_apj.sales_product_line_code
	, list_price_apj.country_alpha2
	, list_price_apj.currency_code
	, list_price_apj.price_term_code
	, list_price_apj.price_start_effective_date
	, list_price_apj.qb_sequence_number
	, list_price_apj.list_price
	, list_price_apj.price_term_code_priority
FROM
	"fin_stage"."lpf_04_list_price_apj" list_price_apj
WHERE
	list_price_apj.Price_term_code = list_price_apj.selected_price_term_code
	AND list_price_apj.currency_code = list_price_apj.selected_currency_code
	AND not exists(
	SELECT 1 FROM "fin_stage"."lpf_04_list_price_apj" list_price_apj_2
		WHERE list_price_apj.sales_product_number = list_price_apj_2.sales_product_number
		AND list_price_apj.country_alpha2 = list_price_apj_2.country_alpha2
		AND list_price_apj_2.price_term_code = list_price_apj_2.selected_price_term_code
		AND list_price_apj_2.currency_code = list_price_apj_2.selected_currency_code
		AND list_price_apj.price_term_code_priority > list_price_apj_2.price_term_code_priority)
),  __dbt__CTE__lpf_19_list_price_la_filtered AS (


SELECT DISTINCT
	list_price_la.sales_product_number
	, list_price_la.sales_product_line_code
	, list_price_la.country_alpha2
	, list_price_la.currency_code
	, list_price_la.price_term_code
	, list_price_la.price_start_effective_date
	, list_price_la.qb_sequence_number
	, list_price_la.list_price
	, list_price_la.price_term_code_priority
FROM
	"fin_stage"."lpf_05_list_price_la" list_price_la
WHERE
	list_price_la.price_term_code = list_price_la.selected_price_term_code
	AND list_price_la.currency_code = list_price_la.selected_currency_code
	AND not exists(
	SELECT 1 FROM "fin_stage"."lpf_05_list_price_la" list_price_la2
		WHERE list_price_la.sales_product_number = list_price_la2.sales_product_number
		AND list_price_la.country_alpha2 = list_price_la2.country_alpha2
		AND list_price_la2.price_term_code = list_price_la2.selected_price_term_code
		AND list_price_la2.currency_code = list_price_la2.selected_currency_code
		AND list_price_la.price_term_code_priority > list_price_la2.price_term_code_priority)
),  __dbt__CTE__lpf_21_list_price_na_filtered as (


SELECT DISTINCT
	list_price_na.sales_product_number
	, list_price_na.sales_product_line_code
	, list_price_na.country_alpha2
	, list_price_na.currency_code
	, list_price_na.price_term_code
	, list_price_na.price_start_effective_date
	, list_price_na.qb_sequence_number
	, list_price_na.list_price
	, list_price_na.price_term_code_priority
FROM
	"fin_stage"."lpf_06_list_price_na" list_price_na
WHERE
	list_price_na.currency_code = list_price_na.selected_currency_code
	AND not exists(
	SELECT 1 FROM "fin_stage"."lpf_06_list_price_NA" list_price_na2
		WHERE list_price_na.sales_product_number = list_price_na2.sales_product_number
		AND list_price_na.country_alpha2 = list_price_na2.country_alpha2
		AND list_price_na2.currency_code = list_price_na2.selected_currency_code
		AND list_price_na.price_term_code_priority > list_price_na2.price_term_code_priority)
),  __dbt__CTE__lpf_09_ibp_grp as (


SELECT DISTINCT
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
),  __dbt__CTE__lpf_14_list_price_eu_filtered as (


SELECT DISTINCT
		list_price_eu.sales_product_number
		, list_price_eu.sales_product_line_code
		, list_price_eu.country_alpha2
		, list_price_eu.currency_code
		, list_price_eu.price_term_code
		, list_price_eu.price_start_effective_date
		, list_price_eu.qb_sequence_number
		, list_price_eu.list_price
		, list_price_eu.price_term_code_priority
		, list_price_eu.list_price_version
		, list_price_eu.sales_unit_version
		, list_price_eu.sales_unit_source

FROM
	"fin_stage"."lpf_03_list_price_eu" list_price_eu
	INNER JOIN
	__dbt__CTE__lpf_09_ibp_grp ibp_grp
    	ON ibp_grp.sales_product_number = list_price_eu.sales_product_number
	    AND ibp_grp.country_alpha2 = list_price_eu.country_alpha2
WHERE
	not exists
	(
		SELECT 1 FROM
		"fin_stage"."lpf_03_list_price_eu" list_price_eu2
		WHERE list_price_eu.sales_product_number = list_price_eu2.sales_product_number
		AND list_price_eu.country_alpha2 = list_price_eu2.country_alpha2
		AND list_price_eu.price_term_code_priority > list_price_eu2.price_term_code_priority
	)
),   __dbt__CTE__lpf_11_country_price_term_map as (


SELECT
		price_term_codes.country_alpha2 country_group
		, case
			WHEN iso_cc_rollup_xref.country_level_1 is null THEN price_term_codes.country_alpha2
			else iso_cc_rollup_xref.country_alpha2
		end as country_code
		, price_term_codes.price_term_code
	FROM
		"mdm"."list_price_term_codes" price_term_codes
		LEFT JOIN
		"mdm"."iso_cc_rollup_xref" iso_cc_rollup_xref
			ON iso_cc_rollup_xref.country_level_1 = price_term_codes.country_alpha2
			WHERE country_scenario = 'LIST_PRICE'
),  __dbt__CTE__lpf_15_list_price_usrp as (


SELECT DISTINCT
	list_price_all.sales_product_number
	, list_price_all.sales_product_line_code
	, list_price_all.country_alpha2
	, list_price_all.currency_code
	, list_price_all.price_term_code
	, list_price_all.price_start_effective_date
	, list_price_all.qb_sequence_number
	, list_price_all.list_price
	, list_price_all.price_term_code_priority
FROM
	"fin_stage"."lpf_02_list_price_all" list_price_all
WHERE
	((list_price_all.Price_term_code = 'RP'))
	AND not exists
	(SELECT * FROM "fin_stage"."lpf_02_list_price_all" list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		AND list_price_all.country_alpha2 = list_price_all2.country_alpha2
		AND list_price_all.currency_code = list_price_all2.currency_code
		AND list_price_all.price_term_code = list_price_all2.price_term_code
		AND (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			OR (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			AND list_price_all.qb_sequence_number < list_price_all2.qb_sequence_number)))
),  __dbt__CTE__lpf_22_no_list_price as (


SELECT DISTINCT
		ibp_grp.sales_product_number
		, ibp_grp.country_alpha2
FROM
	__dbt__CTE__lpf_09_ibp_grp ibp_grp
WHERE
	not exists
	(SELECT 1 FROM __dbt__CTE__lpf_14_list_price_eu_filtered list_price_eu_filtered
		WHERE list_price_eu_filtered.sales_product_number = ibp_grp.sales_product_number
		AND list_price_eu_filtered.country_alpha2 = ibp_grp.country_alpha2
	)
	AND not exists
	(SELECT 1 FROM __dbt__CTE__lpf_17_list_price_APJ_filtered list_price_apj_filtered
		WHERE list_price_apj_filtered.sales_product_number = ibp_grp.sales_product_number
		AND list_price_apj_filtered.country_alpha2 = ibp_grp.country_alpha2
	)
	AND not exists
	(SELECT 1 FROM __dbt__CTE__lpf_19_list_price_LA_filtered list_price_la_filtered
		WHERE list_price_la_filtered.sales_product_number = ibp_grp.sales_product_number
		AND list_price_la_filtered.country_alpha2 = ibp_grp.country_alpha2
	)
	AND not exists
	(SELECT 1 FROM __dbt__CTE__lpf_21_list_price_NA_filtered list_price_na_filtered
		WHERE list_price_na_filtered.sales_product_number = ibp_grp.sales_product_number
		AND list_price_na_filtered.country_alpha2 = ibp_grp.country_alpha2
	)
),  __dbt__CTE__lpf_23_list_price_rp_filtered as (


SELECT DISTINCT
	no_list_price.sales_product_number
	, no_list_price.country_alpha2
	, list_price_usrp.sales_product_line_code
	, list_price_usrp.currency_code
	, list_price_usrp.price_term_code
	, list_price_usrp.price_start_effective_date
	, list_price_usrp.qb_sequence_number
	, list_price_usrp.list_price
FROM
	__dbt__CTE__lpf_15_list_price_usrp list_price_usrp
	INNER JOIN
	__dbt__CTE__lpf_22_no_list_price no_list_price
			ON list_price_usrp.sales_product_number = no_list_price.sales_product_number
),  __dbt__CTE__lpf_24_list_price_dp_df_in_rp as (


SELECT DISTINCT
		sales_product_number
		, sales_product_line_code
		, country_alpha2
		, currency_code
		, price_term_code
		, price_start_effective_date
		, qb_sequence_number
		, list_price
	FROM
		__dbt__CTE__lpf_17_list_price_apj_filtered

	UNION ALL

	SELECT DISTINCT
		sales_product_number
		, sales_product_line_code
		, country_alpha2
		, currency_code
		, price_term_code
		, price_start_effective_date
		, qb_sequence_number
		, list_price
	FROM
		__dbt__CTE__lpf_19_list_price_la_filtered

	UNION ALL

	SELECT DISTINCT
		sales_product_number
		, sales_product_line_code
		, country_alpha2
		, currency_code
		, price_term_code
		, price_start_effective_date
		, qb_sequence_number
		, list_price
	FROM
		__dbt__CTE__lpf_21_list_price_na_filtered

	UNION ALL


	SELECT DISTINCT
		sales_product_number
		, sales_product_line_code
		, country_alpha2
		, currency_code
		, price_term_code
		, price_start_effective_date
		, qb_sequence_number
		, list_price
	FROM
		__dbt__CTE__lpf_14_list_price_eu_filtered

	UNION ALL

	SELECT DISTINCT
		sales_product_number
		, sales_product_line_code
		, country_alpha2
		, currency_code
		, price_term_code
		, price_start_effective_date
		, qb_sequence_number
		, list_price
	FROM
		__dbt__CTE__lpf_23_list_price_rp_filtered
),  __dbt__CTE__lpf_25_country_currency_map as (


SELECT DISTINCT
    country_currency_map_landing."country_alpha2"
    , acct_rates."curency_code"
	, acct_rates."accounting_rate"
FROM
      "mdm"."country_currency_map" country_currency_map_landing
	  INNER JOIN
	  "prod"."acct_rates" acct_rates
		ON acct_rates.iso_curr_cd = country_currency_map_landing."currency_iso_code"
		AND acct_rates.effective_date = '{dbutils.widgets.get("accounting_eff_date")}'
		AND acct_rates.version = '{dbutils.widgets.get("accounting_rate_version")}'
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
    , MIN(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                           
WHERE version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'MAX_IBP_SUPPLIES_FORECAST' AS record
    , MAX(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                             
WHERE
    version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT 'EFFECTIVE_DATE_ACCT_RATES' AS record
    , MAX(effective_date) AS cal_date
FROM
    "prod"."acct_rates"                                       

UNION ALL

SELECT 'EOQ' AS record
    , MAX(load_date) AS cal_date
FROM
    "prod"."list_price_eoq"                                   
),  __dbt__CTE__lpf_26_eoq as (


SELECT
		region_5
		, "product_line" AS product_line
		, "eoq_discount_pct" AS eoq_discount
		, load_date AS eoq_load_date
	FROM
	"prod"."list_price_eoq"
	WHERE
	load_date = (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'EOQ')
),  __dbt__CTE__lpf_27_list_price_eoq as (


SELECT DISTINCT
		list_price_dp_df_in_rp.sales_product_number
		, rdma_base_to_sales_product_map.sales_product_line_code
		, list_price_dp_df_in_rp.country_alpha2
		, iso_country_code_xref.region_5
		, list_price_dp_df_in_rp.currency_code
		, list_price_dp_df_in_rp.price_term_code
		, list_price_dp_df_in_rp.price_start_effective_date
		, list_price_dp_df_in_rp.qb_sequence_number
		, list_price_dp_df_in_rp.list_price
		, country_currency_map.country_alpha2 AS currency_country
		, acct_rates.accounting_rate
		, count(list_price) over (partition by list_price_dp_df_in_rp.sales_product_number, list_price_dp_df_in_rp.country_alpha2, list_price_dp_df_in_rp.price_term_code) as count_List_Price
		, list_price_eoq.eoq_discount
	FROM
		__dbt__CTE__lpf_24_list_price_DP_DF_IN_RP list_price_dp_df_in_rp
		LEFT JOIN
		(SELECT DISTINCT sales_product_number, sales_product_line_code FROM "mdm"."rdma_base_to_sales_product_map") rdma_base_to_sales_product_map
			on list_price_DP_DF_IN_RP.sales_product_number = rdma_base_to_sales_product_map.sales_product_number
		LEFT JOIN
		"mdm"."iso_country_code_xref" iso_country_code_xref
			ON iso_country_code_xref.country_alpha2 = list_price_dp_df_in_rp.country_alpha2
		LEFT JOIN
		__dbt__CTE__lpf_25_country_currency_map country_currency_map
			ON country_currency_map.country_alpha2 = list_price_dp_df_in_rp.country_alpha2
			AND country_currency_map.curency_code = list_price_dp_df_in_rp.currency_code
		LEFT JOIN
		"prod"."acct_rates" acct_rates
			ON acct_rates.curency_code = list_price_dp_df_in_rp.currency_code
		LEFT JOIN
		__dbt__CTE__lpf_26_eoq list_price_eoq
			ON list_price_eoq.product_line = rdma_base_to_sales_product_map.sales_product_line_code
			AND iso_country_code_xref.region_5 = list_price_eoq.region_5
	WHERE 
	1=1
	AND acct_rates.version = '{dbutils.widgets.get("accounting_rate_version")}'
		AND acct_rates.effective_date = '{dbutils.widgets.get("accounting_eff_date")}'
)SELECT DISTINCT
		'FORECAST_SALES_GRU' AS record
		,'{dbutils.widgets.get("forecast_record")}' AS build_type
		,list_price_eoq.sales_product_number
		, list_price_eoq.region_5
		, list_price_eoq.country_alpha2
		, list_price_eoq.currency_code
		, list_price_eoq.price_term_code
		, list_price_eoq.price_start_effective_date
		, list_price_eoq.qb_sequence_number
		, list_price_eoq.list_price
		, list_price_eoq.sales_product_line_code
		, list_price_eoq.accounting_rate
		, list_price_eoq.list_price/list_price_eoq.accounting_rate AS list_price_usd
		, 0 AS list_price_adder_lc
		, list_price_eoq.accounting_rate AS currency_code_adder
		, 0 AS list_price_adder_usd
		, coalesce(eoq_discount, 0) AS eoq_discount
		, ((list_price/accounting_rate) * (1-coalesce(eoq_discount, 0))) AS sales_product_gru
		, null AS load_date
		, null AS version
	FROM
		__dbt__CTE__lpf_27_list_price_eoq list_price_eoq
	WHERE
		(((count_list_price > 1) and (currency_country is not null)) or (count_list_price = 1))
"""

query_list.append(["fin_stage.forecast_sales_gru", forecast_sales_gru, "overwrite"])

# COMMAND ----------

list_price_version = f"""


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
),  __dbt__CTE__lpf_02_filter_dates AS (


SELECT
    'ACTUALS_SUPPLIES' AS record
    , dateadd(month, -12, MIN(cal_date)) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                                 
WHERE
    version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'IBP_SUPPLIES_FORECAST' AS record
    , MIN(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                           
WHERE version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'MAX_IBP_SUPPLIES_FORECAST' AS record
    , MAX(cal_date) AS cal_date
FROM
    "prod"."ibp_supplies_forecast"                             
WHERE
    version = (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT 'EFFECTIVE_DATE_ACCT_RATES' AS record
    , MAX(effective_date) AS cal_date
FROM
    "prod"."acct_rates"                                       

UNION ALL

SELECT 'EOQ' AS record
    , MAX(load_date) AS cal_date
FROM
    "prod"."list_price_eoq"                                   
),  __dbt__CTE__lpf_09_ibp_grp as (


SELECT DISTINCT
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    "fin_stage"."lpf_01_ibp_combined" ibp_combined
),  __dbt__CTE__lpf_26_eoq as (


SELECT
		region_5
		, "product_line" AS product_line
		, "eoq_discount_pct" AS eoq_discount
		, load_date AS eoq_load_date
	FROM
	"prod"."list_price_eoq"
	WHERE
	load_date = (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'EOQ')
)SELECT DISTINCT
	'{dbutils.widgets.get("forecast_record")}' AS record
	, (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'LIST_PRICE_GPSY') AS lp_gpsy_version
	, (SELECT version FROM __dbt__CTE__lpf_01_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST') AS ibp_version
	, '{dbutils.widgets.get("accounting_rate_version")}' AS acct_rates_version
	, (SELECT cal_date FROM __dbt__CTE__lpf_02_filter_dates WHERE record = 'EOQ') AS eoq_load_date
	, NULL AS version
	, NULL AS load_date

	FROM
		"fin_stage"."lpf_02_list_price_all" list_price_all
		INNER JOIN 
		__dbt__CTE__lpf_26_eoq eoq
			ON list_price_all.sales_product_line_code = eoq.product_line
"""

query_list.append(["fin_stage.list_price_version", list_price_version, "overwrite"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
