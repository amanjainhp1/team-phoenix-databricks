# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

query_list = []

# COMMAND ----------

forecast_Sales_GRU = """


SELECT DISTINCT
		Sales_GRU.record
		, Sales_GRU.build_type
		, Sales_GRU.sales_product_number
		, Sales_GRU.region_5
		, Sales_GRU.country_alpha2
		, Sales_GRU.currency_code
		, Sales_GRU.price_term_code
		, Sales_GRU.price_start_effective_date
		, Sales_GRU.qb_sequence_number
		, Sales_GRU.list_price
		, Sales_GRU.sales_product_line_code
		, Sales_GRU.accountingrate
		, Sales_GRU.list_price_usd
		, Sales_GRU.listpriceadder_lc
		, Sales_GRU.currencycode_adder
		, Sales_GRU.listpriceadder_usd
		, Sales_GRU.eoq_discount
		, Sales_GRU.salesproduct_gru
		,(SELECT load_date FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED' 
				AND load_date = (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS load_date,
			(SELECT version FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED' 
				AND version = (SELECT MAX(version) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS version
	FROM
		"fin_stage"."forecast_sales_gru" Sales_GRU
"""

query_list.append(["fin_prod.forecast_sales_gru", forecast_Sales_GRU, "append"])

# COMMAND ----------

list_price_version = """


SELECT DISTINCT
	lpv.record
	, lpv.lp_gpsy_version
	, lpv.ibp_version
	, lpv.acct_rates_version
	, lpv.eoq_load_date
	,(SELECT load_date FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED' 
				AND load_date = (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS load_date,
			(SELECT version FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED' 
				AND version = (SELECT MAX(version) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS version
	FROM
		"fin_stage"."list_price_version" lpv
"""

query_list.append(["fin_prod.list_price_version", list_price_version, "append"])

# COMMAND ----------

list_price_filtered = """


SELECT 
       sales_product_number
       , country_alpha2
       , currency_code
       , price_term_code
       , price_start_effective_date
       , qb_sequence_number
       , list_price
       , sales_product_line_code AS product_line
       , accountingrate
       , list_price_usd AS listpriceusd
       , (SELECT load_date FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED'
				AND load_date = (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS load_date
FROM "fin_stage"."forecast_sales_gru"
"""

query_list.append(["prod.list_price_filtered", list_price_filtered, "append"])

# COMMAND ----------

forecast_GRU_Sales_to_Base = """

SELECT
		ibp_sales_units.country_alpha2
		, ibp_sales_units.sales_product_number
		, ibp_sales_units.sales_product_line_code
		, ibp_sales_units.units AS ibp_sales_product_forecast_units
		, sales_gru_gpsy.salesproduct_gru
		, (ibp_sales_units.units * sales_gru_gpsy.salesproduct_gru) AS salesprod_grossrevenue
		, ibp_sales_units.base_product_number
		, ibp_sales_units.base_product_line_code
		, ibp_sales_units.base_prod_per_sales_prod_qty 
		, ibp_sales_units.base_product_amount_percent
		, ibp_sales_units.base_prod_fcst_revenue_units
		, (ibp_sales_units.units * sales_gru_gpsy.salesproduct_gru*ibp_sales_units.base_product_amount_percent)/100 AS baseprod_grossrevenue
		, ibp_sales_units.region_5
		, sum(ibp_sales_units.base_prod_fcst_revenue_units) over (partition by ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2) AS base_units
		, sum((ibp_sales_units.units * sales_gru_gpsy.salesproduct_gru*ibp_sales_units.base_product_amount_percent)/100) over (partition by ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2) AS Base_GR
		, sum((ibp_sales_units.units * sales_gru_gpsy.salesproduct_gru*ibp_sales_units.base_product_amount_percent)/100) over (partition by ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2)/sum(ibp_sales_units.base_prod_fcst_revenue_units) over (partition by ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2) AS  base_gru
	    , (SELECT version FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED'
				AND version = (SELECT MAX(version) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS version
	FROM
		"fin_stage"."lpf_01_ibp_combined" ibp_sales_units
		INNER JOIN
		"fin_stage"."forecast_sales_gru" Sales_GRU_Gpsy
			ON ibp_sales_units.sales_product_number = Sales_GRU_Gpsy.sales_product_number
			AND ibp_sales_units.country_alpha2 = Sales_GRU_Gpsy.country_alpha2
		WHERE 
		ibp_sales_units.cal_date = (SELECT min(cal_date) FROM "fin_stage"."lpf_01_ibp_combined")
"""

query_list.append(["fin_prod.forecast_gru_sales_to_base", forecast_GRU_Sales_to_Base, "append"])

# COMMAND ----------

list_price_dashboard = """


with  __dbt__CTE__lpp_06_list_price_APJ as (



SELECT DISTINCT
			'LIST_PRICE_GPSY' AS record
			, sales_product_number
			, sales_product_line_code AS "product_line"
			, list_price_APJ.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code AS currencycode_gpsy
			, price_term_code AS "price_term_code"
			, price_start_effective_date AS "price_start_effective_date"
			, qb_sequence_number AS "qb_sequence_number"
			, list_price AS "list_price"
			, 0 AS accountingrate
			, 0 AS "listpriceusd"
			, 0 AS "salesproduct_gru"
		FROM 
			"fin_stage"."lpf_04_list_price_apj" list_price_APJ
			INNER JOIN
			"mdm"."iso_country_code_xref" country_xref
				on list_price_APJ.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_07_list_price_EU as (



SELECT DISTINCT
			'LIST_PRICE_GPSY' AS record
			, sales_product_number
			, sales_product_line_code AS "product_line"
			, list_price_EU.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code AS currencycode_gpsy
			, price_term_code AS "price_term_code"
			, price_start_effective_date AS "price_start_effective_date"
			, qb_sequence_number AS "qb_sequence_number"
			, list_price AS "list_price"
			, 0 AS accountingrate
			, 0 AS "listpriceusd"
			, 0 AS "salesproduct_gru"
		FROM 
			"fin_stage"."lpf_03_list_price_eu" list_price_EU
			INNER JOIN
			"mdm"."iso_country_code_xref" country_xref
				on list_price_EU.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_08_list_price_LA AS (


SELECT DISTINCT
			'LIST_PRICE_GPSY' AS record
			, sales_product_number
			, sales_product_line_code AS "product_line"
			, list_price_LA.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code AS currencycode_gpsy
			, price_term_code AS "price_term_code"
			, price_start_effective_date AS "price_start_effective_date"
			, qb_sequence_number AS "qb_sequence_number"
			, list_price AS "list_price"
			, 0 AS accountingrate
			, 0 AS "listpriceusd"
			, 0 AS "salesproduct_gru"
		FROM 
			"fin_stage"."lpf_05_list_price_la" list_price_LA
			INNER JOIN
			"mdm"."iso_country_code_xref" country_xref
				on list_price_LA.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_09_list_price_NA AS (



SELECT DISTINCT
			'LIST_PRICE_GPSY' AS record
			, sales_product_number
			, sales_product_line_code AS "product_line"
			, list_price_NA.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code AS currencycode_gpsy
			, price_term_code AS "price_term_code"
			, price_start_effective_date AS "price_start_effective_date"
			, qb_sequence_number AS "qb_sequence_number"
			, list_price AS "list_price"
			, 0 AS accountingrate
			, 0 AS "listpriceusd"
			, 0 AS "salesproduct_gru"
		FROM 
			"fin_stage"."lpf_06_list_price_na" list_price_NA
			INNER JOIN
			"mdm"."iso_country_code_xref" country_xref
				on list_price_NA.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_10_list_price_filtered as (



SELECT DISTINCT
			'LIST_PRICE_FILTERED' AS record
			, sales_product_number
			, sales_product_line_code AS "product_line"
			, forecast_Sales_GRU.country_alpha2
			, iso_country_code_xref.country
			, iso_country_code_xref.region_5
			, iso_country_code_xref.region_3
			, iso_country_code_xref.market10
			, currency_code AS currencycode_gpsy
			, price_term_code AS "price_term_code"
			, price_start_effective_date AS "price_start_effective_date"
			, qb_sequence_number AS "qb_sequence_number"
			, list_price AS "list_price"
			, accountingrate
			, list_price_usd AS "listpriceusd"
			, salesproduct_gru
		FROM
			"fin_stage"."forecast_sales_gru" forecast_Sales_GRU
			INNER JOIN
			"mdm"."iso_country_code_xref" iso_country_code_xref
				ON forecast_Sales_GRU.country_alpha2 = iso_country_code_xref.country_alpha2
),  __dbt__CTE__lpp_12_list_price_all as (


SELECT 
			record
			, sales_product_number
			, "product_line"
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
		FROM
			__dbt__CTE__lpp_06_list_price_APJ

		UNION

		SELECT 
			record
			, sales_product_number
			, "product_line"
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
		FROM
			__dbt__CTE__lpp_07_list_price_EU

		UNION

		SELECT 
			record
			, sales_product_number
			, "product_line"
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
		FROM
			__dbt__CTE__lpp_08_list_price_LA

		UNION

		SELECT 
			record
			, sales_product_number
			, "product_line"
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
		FROM
			__dbt__CTE__lpp_09_list_price_NA

		UNION

		SELECT 
			record
			, sales_product_number
			, "product_line"
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
		FROM
			__dbt__CTE__lpp_10_list_price_filtered
)SELECT 
			record
			, lp.sales_product_number
			, rdma.base_product_number
			, lp."product_line"
			, rdma.base_product_line_code
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
			, (SELECT acct_rates_version FROM "fin_stage"."list_price_version") AS accounting_rate_version
			, (SELECT lp_gpsy_version FROM "fin_stage"."list_price_version" ) AS gpsy_version
			, (SELECT version FROM "prod".version WHERE record = 'LIST_PRICE_FILTERED' 
				AND version = (SELECT MAX(version) FROM "prod".version WHERE record = 'LIST_PRICE_FILTERED')) AS sales_gru_version
		FROM
			__dbt__CTE__lpp_12_list_price_all lp
			LEFT JOIN
			"mdm"."rdma_base_to_sales_product_map" rdma
				ON lp.sales_product_number = rdma.sales_product_number
"""

query_list.append(["fin_prod.list_price_dashboard", list_price_dashboard, "append"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
