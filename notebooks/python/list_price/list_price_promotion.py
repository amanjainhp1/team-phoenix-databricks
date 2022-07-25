# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

query_list = []

# COMMAND ----------

forecast_Sales_GRU = """


SELECT distinct
		Sales_GRU.record
		, Sales_GRU.build_type
		, Sales_GRU.sales_product_number
		, Sales_GRU.region_5
		, Sales_GRU.country_alpha2
		, Sales_GRU.currency_code
		, Sales_GRU.price_term_code
		, Sales_GRU.price_start_effective_date
		, Sales_GRU.qb_sequence_number
		, Sales_GRU.List_Price
		, Sales_GRU.sales_product_line_code
		, Sales_GRU.AccountingRate
		, Sales_GRU.list_price_usd
		, Sales_GRU.listpriceadder_lc
		, Sales_GRU.currencycode_adder
		, Sales_GRU.listpriceadder_usd
		, Sales_GRU.eoq_discount
		, Sales_GRU.salesproduct_gru
		,(SELECT load_date from "prod"."version" WHERE record = 'LIST_PRICE_FILTERED' 
				AND load_date = (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS load_date,
			(SELECT version from "prod"."version" WHERE record = 'LIST_PRICE_FILTERED' 
				AND version = (SELECT MAX(version) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS version
	from
		"fin_stage"."forecast_Sales_gru_staging" Sales_GRU
"""

query_list.append(["fin_prod.forecast_sales_gru", forecast_Sales_GRU, "append"])

# COMMAND ----------

list_price_version = """


select distinct
	lpv.record
	, lpv.lp_gpsy_version
	, lpv.ibp_version
	, lpv.acct_rates_version
	, lpv.eoq_load_date
	,(SELECT load_date from "prod"."version" WHERE record = 'LIST_PRICE_FILTERED' 
				AND load_date = (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS load_date,
			(SELECT version from "prod"."version" WHERE record = 'LIST_PRICE_FILTERED' 
				AND version = (SELECT MAX(version) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS version
	from
		"fin_stage"."list_price_version_staging" lpv
"""

query_list.append(["fin_prod.list_price_version", list_price_version, "append"])

# COMMAND ----------

list_price_filtered = """


select 
       sales_product_number
       , country_alpha2
       , currency_code
       , price_term_code
       , price_start_effective_date
       , QB_Sequence_Number
       , List_Price
       , sales_product_line_code as product_line
       , AccountingRate
       , List_Price_USD as listpriceusd
       , (SELECT load_date from "prod"."version" WHERE record = 'LIST_PRICE_FILTERED'
				AND load_date = (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS load_date
from "fin_stage"."forecast_Sales_GRU_staging"
"""

query_list.append(["prod.list_price_filtered", list_price_filtered, "append"])

# COMMAND ----------

forecast_GRU_Sales_to_Base = """

select
		ibp_sales_units.country_alpha2
		, ibp_sales_units.sales_product_number
		, ibp_sales_units.sales_product_line_code
		, ibp_sales_units.units as ibp_sales_product_forecast_units
		, Sales_GRU_Gpsy.salesproduct_gru
		, (ibp_sales_units.units * sales_gru_gpsy.salesproduct_gru) as salesprod_grossrevenue
		, ibp_sales_units.base_product_number
		, ibp_sales_units.base_product_line_code
		, ibp_sales_units.base_prod_per_sales_prod_qty 
		, ibp_sales_units.base_product_amount_percent
		, ibp_sales_units.Base_Prod_fcst_Revenue_Units
		, (ibp_sales_units.units * sales_gru_gpsy.salesproduct_gru*ibp_sales_units.base_product_amount_percent)/100 as BaseProd_GrossRevenue
		, ibp_sales_units.region_5
		, sum(ibp_sales_units.Base_Prod_fcst_Revenue_Units) over (partition by ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2) as base_units
		, sum((ibp_sales_units.units * sales_gru_gpsy.salesproduct_gru*ibp_sales_units.base_product_amount_percent)/100) over (partition by ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2) as Base_GR
		, sum((ibp_sales_units.units * sales_gru_gpsy.salesproduct_gru*ibp_sales_units.base_product_amount_percent)/100) over (partition by ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2)/sum(ibp_sales_units.Base_Prod_fcst_Revenue_Units) over (partition by ibp_sales_units.base_product_number, ibp_sales_units.base_product_line_code, ibp_sales_units.cal_date, ibp_sales_units.region_5, ibp_sales_units.country_alpha2) as  base_gru
	    , (SELECT version from "prod"."version" WHERE record = 'LIST_PRICE_FILTERED'
				AND version = (SELECT MAX(version) FROM "prod"."version" WHERE record = 'LIST_PRICE_FILTERED')) AS version
	from
		"fin_stage"."lpf_01_ibp_combined" ibp_sales_units
		inner join
		"fin_stage"."forecast_Sales_GRU_staging" Sales_GRU_Gpsy
			on ibp_sales_units.sales_product_number = Sales_GRU_Gpsy.sales_product_number
			and ibp_sales_units.country_alpha2 = Sales_GRU_Gpsy.country_alpha2
		where 
		ibp_sales_units.cal_date = (select min(cal_date) from "fin_stage"."lpf_01_ibp_combined")
"""

query_list.append(["fin_prod.forecast_GRU_Sales_to_Base", forecast_GRU_Sales_to_Base, "append"])

# COMMAND ----------

list_price_dashboard = """


with  __dbt__CTE__lpp_06_list_price_APJ as (



select distinct
			'LIST_PRICE_GPSY' as record
			, sales_product_number
			, sales_product_line_code as "product line"
			, list_price_APJ.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code as currencycode_gpsy
			, price_term_code as "price term code"
			, price_start_effective_date as "price start effective date"
			, QB_Sequence_Number as "qb sequence number"
			, List_Price as "list price"
			, 0 as accountingrate
			, 0 as "listpriceusd"
			, 0 as "salesproduct_gru"
		from 
			"fin_stage"."lpf_04_list_price_apj" list_price_APJ
			inner join
			"mdm"."iso_country_code_xref" country_xref
				on list_price_APJ.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_07_list_price_EU as (



select distinct
			'LIST_PRICE_GPSY' as record
			, sales_product_number
			, sales_product_line_code as "product line"
			, list_price_EU.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code as currencycode_gpsy
			, price_term_code as "price term code"
			, price_start_effective_date as "price start effective date"
			, QB_Sequence_Number as "qb sequence number"
			, List_Price as "list price"
			, 0 as accountingrate
			, 0 as "listpriceusd"
			, 0 as "salesproduct_gru"
		from 
			"fin_stage"."lpf_03_list_price_eu" list_price_EU
			inner join
			"mdm"."iso_country_code_xref" country_xref
				on list_price_EU.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_08_list_price_LA as (


select distinct
			'LIST_PRICE_GPSY' as record
			, sales_product_number
			, sales_product_line_code as "product line"
			, list_price_LA.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code as currencycode_gpsy
			, price_term_code as "price term code"
			, price_start_effective_date as "price start effective date"
			, QB_Sequence_Number as "qb sequence number"
			, List_Price as "list price"
			, 0 as accountingrate
			, 0 as "listpriceusd"
			, 0 as "salesproduct_gru"
		from 
			"fin_stage"."lpf_05_list_price_la" list_price_LA
			inner join
			"mdm"."iso_country_code_xref" country_xref
				on list_price_LA.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_09_list_price_NA as (



select distinct
			'LIST_PRICE_GPSY' as record
			, sales_product_number
			, sales_product_line_code as "product line"
			, list_price_NA.country_alpha2
			, country_xref.country
			, country_xref.region_5
			, country_xref.region_3
			, country_xref.market10
			, currency_code as currencycode_gpsy
			, price_term_code as "price term code"
			, price_start_effective_date as "price start effective date"
			, QB_Sequence_Number as "qb sequence number"
			, List_Price as "list price"
			, 0 as accountingrate
			, 0 as "listpriceusd"
			, 0 as "salesproduct_gru"
		from 
			"fin_stage"."lpf_06_list_price_na" list_price_NA
			inner join
			"mdm"."iso_country_code_xref" country_xref
				on list_price_NA.country_alpha2 = country_xref.country_alpha2
),  __dbt__CTE__lpp_10_list_price_filtered as (



select distinct
			'LIST_PRICE_FILTERED' as record
			, sales_product_number
			, sales_product_line_code as "product line"
			, forecast_Sales_GRU.country_alpha2
			, iso_country_code_xref.country
			, iso_country_code_xref.region_5
			, iso_country_code_xref.region_3
			, iso_country_code_xref.market10
			, currency_code as currencycode_gpsy
			, price_term_code as "price term code"
			, price_start_effective_date as "price start effective date"
			, QB_Sequence_Number as "qb Sequence Number"
			, List_Price as "list price"
			, AccountingRate
			, List_Price_USD as "listpriceusd"
			, salesproduct_gru
		from
			"fin_stage"."forecast_sales_gru_staging" forecast_Sales_GRU
			inner join
			"mdm"."iso_country_code_xref" iso_country_code_xref
				on forecast_Sales_GRU.country_alpha2 = iso_country_code_xref.country_alpha2
),  __dbt__CTE__lpp_12_list_price_all as (


select 
			record
			, sales_product_number
			, "product line"
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price term code"
			, "price start effective date"
			, "qb sequence number"
			, "list price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
		from
			__dbt__CTE__lpp_06_list_price_APJ

		union

		select 
			record
			, sales_product_number
			, "product line"
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price term code"
			, "price start effective date"
			, "qb sequence number"
			, "list price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
		from
			__dbt__CTE__lpp_07_list_price_EU

		union

		select 
			record
			, sales_product_number
			, "product Line"
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price term code"
			, "price start effective date"
			, "qb sequence number"
			, "list price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
		from
			__dbt__CTE__lpp_08_list_price_LA

		union

		select 
			record
			, sales_product_number
			, "product Line"
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price term code"
			, "price start effective date"
			, "qb sequence number"
			, "list price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
		from
			__dbt__CTE__lpp_09_list_price_NA

		union

		select 
			record
			, sales_product_number
			, "product Line"
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price term code"
			, "price start effective date"
			, "qb sequence number"
			, "list price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
		from
			__dbt__CTE__lpp_10_list_price_filtered
)select 
			record
			, lp.sales_product_number
			, rdma.base_product_number
			, lp."product line"
			, rdma.base_product_line_code
			, country_alpha2
			, country
			, region_5
			, region_3
			, market10
			, currencycode_gpsy
			, "price term code"
			, "price start effective date"
			, "qb sequence number"
			, "list price"
			, accountingrate
			, "listpriceusd"
			, "salesproduct_gru"
			, (select acct_rates_version from "fin_stage"."list_price_version_staging") as Accounting_rate_version
			, (select lp_gpsy_version from "fin_stage"."list_price_version_staging" ) as Gpsy_version
			, (SELECT version from "prod".version WHERE record = 'LIST_PRICE_FILTERED' 
				AND version = (SELECT MAX(version) FROM "prod".version WHERE record = 'LIST_PRICE_FILTERED')) as Sales_GRU_version
		from
			__dbt__CTE__lpp_12_list_price_all lp
			left join
			"mdm"."rdma_base_to_sales_product_map" rdma
				on lp.sales_product_number = rdma.sales_product_number
"""

query_list.append(["fin_prod.list_price_dashboard", list_price_dashboard, "append"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list

# COMMAND ----------


