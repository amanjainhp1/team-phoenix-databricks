# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

max_info = call_redshift_addversion_sproc(configs, 'LIST_PRICE_FILTERED', 'LIST_PRICE_FILTERED')

max_version = max_info[0]
max_load_date = (max_info[1])

# COMMAND ----------

query_list = []

# COMMAND ----------

forecast_sales_gru = """


SELECT DISTINCT
		sales_gru.record
		, sales_gru.build_type
		, sales_gru.sales_product_number
		, sales_gru.region_5
		, sales_gru.country_alpha2
		, sales_gru.currency_code
		, sales_gru.price_term_code
		, sales_gru.price_start_effective_date
		, sales_gru.qb_sequence_number
		, sales_gru.list_price
		, sales_gru.sales_product_line_code
		, sales_gru.accounting_rate
		, sales_gru.list_price_usd
		, sales_gru.list_price_adder_lc
		, sales_gru.currency_code_adder
		, sales_gru.list_price_adder_usd
		, sales_gru.eoq_discount
		, sales_gru.sales_product_gru
		, cast('{}' as timestamp) as load_date
        , '{}'  as version
	FROM
		"fin_stage"."forecast_sales_gru" sales_gru
""".format(max_load_date,max_version)

query_list.append(["fin_prod.forecast_sales_gru", forecast_sales_gru, "append"])

# COMMAND ----------

list_price_version = """


SELECT DISTINCT
	lpv.record
	, lpv.lp_gpsy_version
	, lpv.ibp_version
	, lpv.acct_rates_version
	, cast(lpv.eoq_load_date as date) as eoq_load_date
	, cast('{}' as timestamp) as load_date
    , '{}'  as version
	FROM
		"fin_stage"."list_price_version" lpv
""".format(max_load_date,max_version)

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
       , accounting_rate
       , list_price_usd
       , cast('{}' as timestamp) as load_date
FROM "fin_stage"."forecast_sales_gru"
""".format(max_load_date)

query_list.append(["fin_prod.list_price_filtered", list_price_filtered, "append"])

# COMMAND ----------

submit_remote_query(configs , '''truncate table fin_prod.forecast_gru_sales_to_base''')

# COMMAND ----------

forecast_gru_sales_to_base = """

SELECT
		ibp_sales_units.country_alpha2
		, ibp_sales_units.sales_product_number
		, ibp_sales_units.sales_product_line_code
		, ibp_sales_units.units AS ibp_sales_product_forecast_units
		, sales_gru_gpsy.sales_product_gru
		, (ibp_sales_units.units * sales_gru_gpsy.sales_product_gru) AS salesprod_grossrevenue
		, ibp_sales_units.base_product_number
		, ibp_sales_units.base_product_line_code
		, ibp_sales_units.base_prod_per_sales_prod_qty 
		, ibp_sales_units.base_product_amount_percent
		, ibp_sales_units.base_prod_fcst_revenue_units
		, (ibp_sales_units.units * sales_gru_gpsy.sales_product_gru*ibp_sales_units.base_product_amount_percent)/100 AS baseprod_grossrevenue
		, ibp_sales_units.region_5
		, SUM(ibp_sales_units.base_prod_fcst_revenue_units) over (partition by ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2) AS base_units
		, SUM((ibp_sales_units.units * sales_gru_gpsy.sales_product_gru*ibp_sales_units.base_product_amount_percent)/100) OVER (PARTITION BY ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2) AS Base_GR
		, SUM((ibp_sales_units.units * sales_gru_gpsy.sales_product_gru*ibp_sales_units.base_product_amount_percent)/100) OVER (PARTITION BY ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2)/sum(ibp_sales_units.base_prod_fcst_revenue_units) OVER (PARTITION BY ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2) AS  base_gru
	    , '{}'  as version
	FROM
		"fin_stage"."lpf_01_ibp_combined" ibp_sales_units
		INNER JOIN
		"fin_stage"."forecast_sales_gru" sales_gru_gpsy
			ON ibp_sales_units.sales_product_number = sales_gru_gpsy.sales_product_number
			AND ibp_sales_units.country_alpha2 = sales_gru_gpsy.country_alpha2
		WHERE 
		ibp_sales_units.cal_date = (SELECT min(cal_date) FROM "fin_stage"."lpf_01_ibp_combined")
""".format(max_version)

query_list.append(["fin_prod.forecast_gru_sales_to_base", forecast_gru_sales_to_base, "append"])

# COMMAND ----------

submit_remote_query(configs , '''truncate table fin_prod.list_price_dashboard''')

# COMMAND ----------

list_price_dashboard = """


with  __dbt__CTE__lpp_06_list_price_apj as (



SELECT DISTINCT
			'LIST_PRICE_GPSY' AS record
			, sales_product_number
			, sales_product_line_code AS "product_line"
			, list_price_APJ.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code AS currency_code_gpsy
			, price_term_code AS "price_term_code"
			, price_start_effective_date AS "price_start_effective_date"
			, qb_sequence_number AS "qb_sequence_number"
			, list_price AS "list_price"
			, 0 AS accounting_rate
			, 0 AS "list_price_usd"
			, 0 AS "sales_product_gru"
		FROM 
			"fin_stage"."lpf_04_list_price_apj" list_price_apj
			INNER JOIN
			"mdm"."iso_country_code_xref" country_xref
				on list_price_apj.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_07_list_price_eu as (



SELECT DISTINCT
			'LIST_PRICE_GPSY' AS record
			, sales_product_number
			, sales_product_line_code AS "product_line"
			, list_price_EU.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code AS currency_code_gpsy
			, price_term_code AS "price_term_code"
			, price_start_effective_date AS "price_start_effective_date"
			, qb_sequence_number AS "qb_sequence_number"
			, list_price AS "list_price"
			, 0 AS accounting_rate
			, 0 AS "list_price_usd"
			, 0 AS "sales_product_gru"
		FROM 
			"fin_stage"."lpf_03_list_price_eu" list_price_eu
			INNER JOIN
			"mdm"."iso_country_code_xref" country_xref
				on list_price_eu.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_08_list_price_la AS (


SELECT DISTINCT
			'LIST_PRICE_GPSY' AS record
			, sales_product_number
			, sales_product_line_code AS "product_line"
			, list_price_LA.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code AS currency_code_gpsy
			, price_term_code AS "price_term_code"
			, price_start_effective_date AS "price_start_effective_date"
			, qb_sequence_number AS "qb_sequence_number"
			, list_price AS "list_price"
			, 0 AS accounting_rate
			, 0 AS "list_price_usd"
			, 0 AS "sales_product_gru"
		FROM 
			"fin_stage"."lpf_05_list_price_la" list_price_la
			INNER JOIN
			"mdm"."iso_country_code_xref" country_xref
				on list_price_la.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_09_list_price_na AS (



SELECT DISTINCT
			'LIST_PRICE_GPSY' AS record
			, sales_product_number
			, sales_product_line_code AS "product_line"
			, list_price_NA.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code AS currency_code_gpsy
			, price_term_code AS "price_term_code"
			, price_start_effective_date AS "price_start_effective_date"
			, qb_sequence_number AS "qb_sequence_number"
			, list_price AS "list_price"
			, 0 AS accounting_rate
			, 0 AS "list_price_usd"
			, 0 AS "sales_product_gru"
		FROM 
			"fin_stage"."lpf_06_list_price_na" list_price_na
			INNER JOIN
			"mdm"."iso_country_code_xref" country_xref
				on list_price_na.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_10_list_price_filtered as (



SELECT DISTINCT
			'LIST_PRICE_FILTERED' AS record
			, sales_product_number
			, sales_product_line_code AS "product_line"
			, forecast_sales_gru.country_alpha2
			, iso_country_code_xref.country
			, iso_country_code_xref.region_5
			, iso_country_code_xref.region_3
			, iso_country_code_xref.market10
			, currency_code AS currency_code_gpsy
			, price_term_code AS "price_term_code"
			, price_start_effective_date AS "price_start_effective_date"
			, qb_sequence_number AS "qb_sequence_number"
			, list_price AS "list_price"
			, accounting_rate
			, list_price_usd
			, sales_product_gru
		FROM
			"fin_stage"."forecast_sales_gru" forecast_sales_gru
			INNER JOIN
			"mdm"."iso_country_code_xref" iso_country_code_xref
				ON forecast_sales_gru.country_alpha2 = iso_country_code_xref.country_alpha2
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
			, currency_code_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accounting_rate
			, "list_price_usd"
			, "sales_product_gru"
		FROM
			__dbt__CTE__lpp_06_list_price_apj

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
			, currency_code_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accounting_rate
			, "list_price_usd"
			, "sales_product_gru"
		FROM
			__dbt__CTE__lpp_07_list_price_eu

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
			, currency_code_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accounting_rate
			, "list_price_usd"
			, "sales_product_gru"
		FROM
			__dbt__CTE__lpp_08_list_price_la

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
			, currency_code_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accounting_rate
			, "list_price_usd"
			, "sales_product_gru"
		FROM
			__dbt__CTE__lpp_09_list_price_na

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
			, currency_code_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accounting_rate
			, "list_price_usd"
			, "sales_product_gru"
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
			, currency_code_gpsy
			, "price_term_code"
			, "price_start_effective_date"
			, "qb_sequence_number"
			, "list_price"
			, accounting_rate
			, "list_price_usd"
			, "sales_product_gru"
			, (SELECT acct_rates_version FROM "fin_stage"."list_price_version") AS accounting_rate_version
			, (SELECT lp_gpsy_version FROM "fin_stage"."list_price_version" ) AS gpsy_version
			, '{}' AS sales_gru_version
		FROM
			__dbt__CTE__lpp_12_list_price_all lp
			LEFT JOIN
			"mdm"."rdma_base_to_sales_product_map" rdma
				ON lp.sales_product_number = rdma.sales_product_number
""".format(max_version)

query_list.append(["fin_prod.list_price_dashboard", list_price_dashboard, "append"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list

# COMMAND ----------

list_price_filtered = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM fin_prod.list_price_filtered where load_date = (select max(load_date) from fin_prod.list_price_filtered)") \
    .load()

# COMMAND ----------

load_date = list_price_filtered.agg({"load_date": "max"}).collect()[0][0]

dateTimeStr = str(load_date)

# write to parquet file in s3

s3_list_price_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/list_price_filtered_historical/" + dateTimeStr[:10]

write_df_to_s3(list_price_filtered, s3_list_price_output_bucket, "parquet", "overwrite")
