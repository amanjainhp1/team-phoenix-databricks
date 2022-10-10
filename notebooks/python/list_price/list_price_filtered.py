# Databricks notebook source
dbutils.widgets.text("accounting_eff_date",'2022-09-01')
dbutils.widgets.text("accounting_rate_version",'2022.09.01.1')
dbutils.widgets.text("forecast_record",'FORECAST_SALES_GRU')

# COMMAND ----------

from pyspark.sql.types import StringType , NullType
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

f_report_units_query = """
SELECT "Product Number" as product_number
        , "Country code" as country_code
        , "Currency code" as currency_code
        , "Price Term code" as price_term_code
        , "Price Start Effective Date" as price_start_effective_date
        , "QB Sequence Number" as "qbl_sequence_number"
        , "List Price" as list_price
        , "Product Line" as product_line
        , load_date
        , version
FROM ie2_prod.dbo.list_price_gpsy
"""

f_report_units = read_sql_server_to_df(configs) \
    .option("query", f_report_units_query) \
    .load()

write_df_to_redshift(configs, f_report_units, "prod.list_price_gpsy", "overwrite")

# COMMAND ----------

rdma = """
SELECT *
FROM ie2_prod.dbo.rdma
"""

f_report_units = read_sql_server_to_df(configs) \
    .option("query", rdma) \
    .load()

write_df_to_redshift(configs, f_report_units, "mdm.rdma", "overwrite")

# COMMAND ----------

f_report_units_query = """
SELECT *
FROM ie2_prod.dbo.rdma_base_to_sales_product_map
"""

f_report_units = read_sql_server_to_df(configs) \
    .option("query", f_report_units_query) \
    .load()

write_df_to_redshift(configs, f_report_units, "mdm.rdma_base_to_sales_product_map", "overwrite")

# COMMAND ----------

f_report_units_query = """
SELECT * FROM ie2_prod.dbo.working_forecast_country WHERE version = '2022.09.19.1'
"""

f_report_units = read_sql_server_to_df(configs) \
    .option("query", f_report_units_query) \
    .load()

write_df_to_redshift(configs, f_report_units, "prod.working_forecast_country", "overwrite")

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

##testing 

query = """
select count(*)
    , "actuals_supplies_baseprod" as record
from actuals_supplies_baseprod

UNION ALL

select count(*)
    , "actuals_supplies_salesprod" as record
from actuals_supplies_salesprod

UNION ALL

select count(*)
    , "working_forecast_country" as record
from working_forecast_country

UNION ALL

select count(*)
    , "ibp_supplies_forecast" as record
from ibp_supplies_forecast

UNION ALL

select count(*)
    , "list_price_gpsy" as record
from list_price_gpsy

UNION ALL

select count(*)
    , "acct_rates" as record
from acct_rates

UNION ALL

select count(*)
    , "calendar" as record
from calendar

UNION ALL

select count(*)
    , "product_line_xref" as record
from product_line_xref

UNION ALL

select count(*)
    , "hardware_xref" as record
from hardware_xref

UNION ALL

select count(*)
    , "rdma" as record
from rdma

UNION ALL
    
select count(*)
    , "iso_country_code_xref" as record
from iso_country_code_xref
    
UNION ALL
    
select count(*)
    , "list_price_term_codes" as record
from list_price_term_codes

UNION ALL
    
select count(*)
    , "list_price_eu_countrylist" as record
from list_price_eu_countrylist

UNION ALL
    
select count(*)
    , "country_currency_map" as record
from country_currency_map

UNION ALL
    
select count(*)
    , "list_price_eoq" as record
from list_price_eoq

UNION ALL

select count(*)
    , "rdma_base_to_sales_product_map" as record
from rdma_base_to_sales_product_map
"""

test_df = spark.sql(query)

# COMMAND ----------

test_df.display()

# COMMAND ----------

tables = ['fin_prod.actuals_supplies_baseprod', 'fin_prod.actuals_supplies_salesprod', 'prod.working_forecast_country', 'prod.ibp_supplies_forecast', 'prod.list_price_gpsy', 'prod.acct_rates' , 'mdm.calendar' , 'mdm.product_line_xref' , 'mdm.hardware_xref' , 'mdm.rdma' , 'mdm.iso_country_code_xref' , 'mdm.list_price_term_codes' , 'mdm.list_price_eu_countrylist' , 'mdm.country_currency_map' , 'prod.list_price_eoq' , 'mdm.rdma_base_to_sales_product_map' , 'mdm.iso_cc_rollup_xref']

for table in tables:
    # Define the input and output formats and paths and the table name.
    schema = table.split(".")[0]
    table_name = table.split(".")[1]
    write_format = 'delta'
    save_path = f'/tmp/delta/{schema}/{table_name}'
    
    # Load the data from its source.
    df = get_data_by_table(table)
        
    # Write the data to its target.
    df.write \
      .format(write_format) \
      .mode("overwrite") \
      .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Create the table.
    spark.sql("CREATE TABLE IF NOT EXISTS " + table + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table).createOrReplaceTempView(table_name)
    
    print(f'{table_name} loaded')

# COMMAND ----------

# # load S3 tables to df
# actuals_supplies_baseprod = read_redshift_to_df(configs) \
#     .option("query", "SELECT * FROM fin_prod.actuals_supplies_baseprod WHERE version = (SELECT max(version) from fin_prod.actuals_supplies_baseprod)") \
#     .load()
# actuals_supplies_salesprod = read_redshift_to_df(configs) \
#     .option("query", "SELECT * FROM fin_prod.actuals_supplies_salesprod WHERE version = (SELECT max(version) from fin_prod.actuals_supplies_salesprod)") \
#     .load()
# working_forecast_country = read_redshift_to_df(configs) \
#     .option("query", "SELECT * FROM prod.working_forecast_country WHERE version = (SELECT max(version) from prod.working_forecast_country)") \
#     .load()
# ibp_supplies_forecast = read_redshift_to_df(configs) \
#     .option("query", "SELECT * FROM prod.ibp_supplies_forecast WHERE version = (SELECT max(version) from prod.ibp_supplies_forecast)") \
#     .load()
# list_price_gpsy = read_redshift_to_df(configs) \
#     .option("query", "SELECT * FROM prod.list_price_gpsy WHERE version = (SELECT max(version) from prod.list_price_gpsy)") \
#     .load()
# acct_rates = read_redshift_to_df(configs) \
#     .option("query", "SELECT * FROM prod.acct_rates WHERE version = (SELECT max(version) from prod.acct_rates)") \
#     .load()
# calendar = read_redshift_to_df(configs) \
#     .option("dbtable", "mdm.calendar") \
#     .load()
# product_line_xref = read_redshift_to_df(configs) \
#     .option("dbtable", "mdm.product_line_xref") \
#     .load()
# hardware_xref = read_redshift_to_df(configs) \
#     .option("dbtable", "mdm.hardware_xref") \
#     .load()
# rdma = read_redshift_to_df(configs) \
#     .option("dbtable", "mdm.rdma") \
#     .load()
# iso_country_code_xref = read_redshift_to_df(configs) \
#     .option("dbtable", "mdm.iso_country_code_xref") \
#     .load()
# rdma_base_to_sales_map = read_redshift_to_df(configs) \
#     .option("dbtable", "mdm.rdma_base_to_sales_product_map") \
#     .load() 
# list_price_eoq = read_redshift_to_df(configs) \
#     .option("dbtable", "prod.list_price_eoq") \
#     .load()
# iso_cc_rollup_xref = read_redshift_to_df(configs) \
#     .option("dbtable", "mdm.iso_cc_rollup_xref") \
#     .load()
# list_price_term_codes = read_redshift_to_df(configs) \
#     .option("dbtable", "mdm.list_price_term_codes") \
#     .load()
# list_price_eu_countrylist = read_redshift_to_df(configs) \
#     .option("dbtable", "mdm.list_price_eu_countrylist") \
#     .load()
# country_currency_map = read_redshift_to_df(configs) \
#     .option("dbtable", "mdm.country_currency_map") \
#     .loadlist_price_filter_vars

# COMMAND ----------

list_price_filter_vars = f"""


SELECT 'ACTUALS_SUPPLIES_BASEPROD' AS record
    , MAX(version) AS version
FROM actuals_supplies_baseprod

UNION ALL

SELECT 'ACTUALS_SUPPLIES_SALESPROD' AS record
    , MAX(version) AS version
FROM actuals_supplies_salesprod

UNION ALL

SELECT 'IBP_SUPPLIES_FORECAST' AS record
    , MAX(version) AS version
FROM ibp_supplies_forecast

UNION ALL

SELECT 'INSIGHTS_UNITS' AS record
    , MAX(version) AS version
FROM working_forecast_country

UNION ALL

SELECT 'LIST_PRICE_GPSY' AS record
    , MAX(version) AS version
FROM list_price_gpsy

UNION ALL

SELECT 'ACCT_RATES' AS record
    , MAX(version) AS version
FROM acct_rates
"""

list_price_filter_vars = spark.sql(list_price_filter_vars)
list_price_filter_vars.createOrReplaceTempView("list_price_filter_vars")

# COMMAND ----------

list_price_filter_dates = f"""

SELECT
    'ACTUALS_SUPPLIES' AS record
    , add_months(min(cal_date) , -12) AS cal_date
FROM
    ibp_supplies_forecast                               
WHERE
    version = (SELECT version FROM list_price_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'IBP_SUPPLIES_FORECAST' AS record
    , MIN(cal_date) AS cal_date
FROM
    ibp_supplies_forecast                          
WHERE version = (SELECT version FROM list_price_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT
    'MAX_IBP_SUPPLIES_FORECAST' AS record
    , MAX(cal_date) AS cal_date
FROM
    ibp_supplies_forecast                           
WHERE
    version = (SELECT version FROM list_price_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST')

UNION ALL

SELECT 'EFFECTIVE_DATE_ACCT_RATES' AS record
    , MAX(effectivedate) AS cal_date
FROM
    acct_rates                                      

UNION ALL

SELECT 'EOQ' AS record
    , MAX(load_date) AS cal_date
FROM
    list_price_eoq
"""

list_price_filter_dates = spark.sql(list_price_filter_dates)
list_price_filter_dates.createOrReplaceTempView("list_price_filter_dates")

# COMMAND ----------

lpf_01_ibp_combined = f"""

WITH lpf_06_ibp_region as (
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
		ibp_supplies_forecast ibp_supplies_forecast       
		LEFT JOIN
		iso_country_code_xref iso_country_code_xref
			ON iso_country_code_xref.country_alpha2 = ibp_supplies_forecast.country_alpha2
		INNER JOIN
		rdma_base_to_sales_product_map rdma_base_to_sales_product_map
			ON rdma_base_to_sales_product_map.sales_product_number = ibp_supplies_forecast.sales_product_number
	WHERE
		ibp_supplies_forecast.version = (SELECT version FROM list_price_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST') 
),  lpf_03_actuals_baseprod AS (


SELECT
	actuals_supplies_baseprod.country_alpha2
	, actuals_supplies_baseprod.base_product_number
	, actuals_supplies_baseprod.pl AS base_product_line_code
	, SUM(actuals_supplies_baseprod.revenue_units) AS revenue_units
	, SUM(actuals_supplies_baseprod.gross_revenue) AS gross_rev
	, actuals_supplies_baseprod.version
FROM
    actuals_supplies_baseprod actuals_supplies_baseprod
WHERE
	actuals_supplies_baseprod.version =
	            (SELECT version FROM list_price_filter_vars WHERE record = 'ACTUALS_SUPPLIES_BASEPROD')
	AND actuals_supplies_baseprod.cal_date >
	            (SELECT cal_date FROM list_price_filter_dates WHERE record = 'ACTUALS_SUPPLIES')
GROUP BY
    actuals_supplies_baseprod.country_alpha2
    , actuals_supplies_baseprod.base_product_number
    , actuals_supplies_baseprod.pl
	, actuals_supplies_baseprod.version
),  lpf_04_actuals_salesprod AS (


SELECT
    country_alpha2
	, sales_product_number
	, pl AS sales_product_line_code
	, SUM(revenue_units) AS revenue_units
	, SUM(gross_revenue) AS gross_rev
	, version
FROM
    actuals_supplies_salesprod
WHERE
	version = (SELECT version FROM list_price_filter_vars WHERE record = 'ACTUALS_SUPPLIES_SALESPROD') AND
	   cal_date > (SELECT cal_date FROM list_price_filter_dates WHERE record = 'ACTUALS_SUPPLIES')
GROUP BY country_alpha2
    , sales_product_number
    , pl
    , version
),  lpf_07_ibp_actuals AS (


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
		lpf_03_actuals_baseprod actuals_base_prod
		INNER JOIN
		rdma_base_to_sales_product_map rdma_base_to_sales_product_map
			ON rdma_base_to_sales_product_map.base_product_number = actuals_base_prod.base_product_number
		INNER JOIN
		lpf_04_actuals_salesprod actuals_sales_prod
			ON actuals_sales_prod.sales_product_number = rdma_base_to_sales_product_map.sales_product_number
			AND actuals_base_prod.country_alpha2 = actuals_sales_prod.country_alpha2
		LEFT JOIN
		iso_country_code_xref iso_country_code_xref
			ON iso_country_code_xref.country_alpha2 = actuals_base_prod.country_alpha2
		JOIN
		 calendar calendar
		    ON 1 = 1
	WHERE
		actuals_sales_prod.revenue_units > 0
		AND actuals_base_prod.revenue_units > 0
		AND calendar.date >= (SELECT cal_date FROM list_price_filter_dates WHERE record = 'IBP_SUPPLIES_FORECAST')
		AND calendar.day_of_month = 1
		AND calendar.date <= (SELECT cal_date FROM list_price_filter_dates WHERE record = 'MAX_IBP_SUPPLIES_FORECAST')
		AND not exists (SELECT 1 FROM lpf_06_ibp_region ibp_region
						WHERE ibp_region.base_product_number = actuals_base_prod.base_product_number
						AND ibp_region.country_alpha2 = actuals_base_prod.country_alpha2
						AND ibp_region.sales_product_number = actuals_sales_prod.sales_product_number)
		AND EXISTS (SELECT 1 FROM working_forecast_country insights_units
					WHERE insights_units.base_product_number = actuals_base_prod.base_product_number
					AND insights_units.country = actuals_base_prod.country_alpha2
					AND insights_units.cal_date = calendar.date
					AND cal_date >= (SELECT cal_date FROM list_price_filter_dates WHERE record = 'IBP_SUPPLIES_FORECAST')
					AND version = (SELECT version FROM list_price_filter_vars WHERE record = 'INSIGHTS_UNITS')
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
        lpf_06_ibp_region ibp_region

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
	    lpf_07_ibp_actuals ibp_actuals
"""

lpf_01_ibp_combined = spark.sql(lpf_01_ibp_combined)
write_df_to_redshift(configs, lpf_01_ibp_combined, "fin_stage.lpf_01_ibp_combined", "overwrite")
lpf_01_ibp_combined.createOrReplaceTempView("lpf_01_ibp_combined")

# COMMAND ----------

#testing

f_report_units_query = """
SELECT country_alpha2
    , region_5
    , sales_product_number
    , sales_product_line_code
    , base_product_number
    , base_product_line_code
    , base_prod_per_sales_prod_qty
    , base_product_amount_percent
    , Base_Prod_fcst_Revenue_Units as base_prod_fcst_revenue_units
    , cal_date
    , units
    , version
    , source
FROM ie2_financials.dbt.lpf_08_ibp_combined
"""

f_report_units = read_sql_server_to_df(configs) \
    .option("query", f_report_units_query) \
    .load()

write_df_to_redshift(configs, f_report_units, "fin_stage.lpf_01_ibp_combined", "overwrite")

f_report_units.createOrReplaceTempView("lpf_01_ibp_combined")

# COMMAND ----------

#testing

version_count = read_redshift_to_df(configs) \
    .option("query", f"""SELECT * FROM fin_stage.lpf_01_ibp_combined """) \
    .load()

version_count.createOrReplaceTempView("lpf_01_ibp_combined")

# COMMAND ----------

lpf_02_list_price_all = f"""

WITH __dbt__CTE__lpf_09_ibp_grp as (


SELECT DISTINCT
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    lpf_01_ibp_combined ibp_combined
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
		list_price_term_codes price_term_codes
		LEFT JOIN
		iso_cc_rollup_xref iso_cc_rollup_xref
			ON iso_cc_rollup_xref.country_level_1 = price_term_codes.country_alpha2
			WHERE country_scenario = 'LIST_PRICE'
)
SELECT DISTINCT	list_price_gpsy.product_number AS sales_product_number
		, list_price_gpsy.product_line AS sales_product_line_code
		, list_price_gpsy.country_code AS country_alpha2
		, list_price_gpsy.currency_code AS currency_code
		, list_price_gpsy.price_term_code AS price_term_code
		, list_price_gpsy.price_start_effective_date AS price_start_effective_date
		, list_price_gpsy.qbl_sequence_number AS qb_sequence_number
		, list_price_gpsy.list_price AS list_price
		, case
			WHEN list_price_gpsy.price_term_code = 'DP' THEN 1
			WHEN list_price_gpsy.price_term_code = 'DF' THEN 2
			WHEN list_price_gpsy.price_term_code = 'IN' THEN 3
			WHEN list_price_gpsy.price_term_code = 'RP' THEN 4
			end AS price_term_code_priority
		, country_price_term_map.country_group
		, country_price_term_map.price_term_code AS ctry_grp_price_term_code
		, list_price_gpsy.version AS list_price_version
		, ibp_grp.version AS sales_unit_version
		, ibp_grp.source AS sales_unit_source
	FROM
	    list_price_gpsy list_price_gpsy
	    INNER JOIN
	    __dbt__CTE__lpf_09_ibp_grp ibp_grp
		    ON ibp_grp.sales_product_number = list_price_gpsy.product_number
		LEFT JOIN
		__dbt__CTE__lpf_11_country_price_term_map country_price_term_map
			ON country_price_term_map.country_code = list_price_gpsy.country_code
	WHERE
	    list_price_gpsy.version = (SELECT version FROM list_price_filter_vars WHERE record = 'LIST_PRICE_GPSY')
	    AND list_price_gpsy.currency_code <> 'CO'
		AND list_price_gpsy.price_term_code in ('DP', 'DF', 'IN', 'RP')
"""

lpf_02_list_price_all = spark.sql(lpf_02_list_price_all)
write_df_to_redshift(configs, lpf_02_list_price_all, "fin_stage.lpf_02_list_price_all", "overwrite")
lpf_02_list_price_all.createOrReplaceTempView("lpf_02_list_price_all")

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
	    lpf_01_ibp_combined ibp_combined
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
	lpf_02_list_price_all list_price_all
	INNER JOIN
	iso_country_code_xref iso_country_code_xref
		on iso_country_code_xref.region_5 = list_price_all.country_alpha2
	INNER JOIN
	__dbt__CTE__lpf_09_ibp_grp ibp_grp
		on ibp_grp.sales_product_number = list_price_all.sales_product_number
		and iso_country_code_xref.country_alpha2 = ibp_grp.country_alpha2
	INNER JOIN
	list_price_eu_countrylist list_price_eu_countrylist
		ON list_price_eu_countrylist.country_alpha2 = ibp_grp.country_alpha2
		AND
		    ((list_price_eu_countrylist.currency = 'EURO' AND list_price_all.currency_code = 'EC')
		    OR (list_price_eu_countrylist.currency = 'DOLLAR' AND list_price_all.currency_code = 'UD'))
WHERE
	(list_price_all.country_alpha2 = 'EU')
	AND price_term_code IN ('DP', 'DF')
	AND not exists
	(SELECT * FROM lpf_02_list_price_all list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		AND list_price_all.country_alpha2 = list_price_all2.country_alpha2
		AND list_price_all.currency_code = list_price_all2.currency_code
		AND list_price_all.price_term_code = list_price_all2.price_term_code
		AND (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			OR (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			AND list_price_all.qb_sequence_number < list_price_all2.qb_sequence_number)))
"""

lpf_03_list_price_eu = spark.sql(lpf_03_list_price_eu)
write_df_to_redshift(configs, lpf_03_list_price_eu, "fin_stage.lpf_03_list_price_eu", "overwrite")
lpf_03_list_price_eu.createOrReplaceTempView("lpf_03_list_price_eu")

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
	    lpf_01_ibp_combined ibp_combined
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
	 lpf_02_list_price_all list_price_all
WHERE
	 EXISTS (SELECT 1 FROM __dbt__CTE__lpf_09_ibp_grp ibp_grp WHERE ibp_grp.region_5 in ('AP', 'JP')
	 AND ibp_grp.sales_product_number = list_price_all.sales_product_number
		AND list_price_all.country_alpha2 = ibp_grp.country_alpha2)
	AND list_price_all.price_term_code <> 'RP'
	AND not exists
	(SELECT * FROM lpf_02_list_price_all list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		AND list_price_all.country_alpha2 = list_price_all2.country_alpha2
		AND list_price_all.price_term_code = list_price_all2.price_term_code
		AND (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			OR (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			AND list_price_all.qb_sequence_number < list_price_all2.qb_sequence_number)))
"""

lpf_04_list_price_apj = spark.sql(lpf_04_list_price_apj)
write_df_to_redshift(configs, lpf_04_list_price_apj, "fin_stage.lpf_04_list_price_apj", "overwrite")
lpf_04_list_price_apj.createOrReplaceTempView("lpf_04_list_price_apj")

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
	    lpf_01_ibp_combined ibp_combined
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
	lpf_02_list_price_all list_price_all
	INNER JOIN
	product_line_xref product_line_xref
		ON product_line_xref.pl = list_price_all.sales_product_line_code
WHERE
	 exists (SELECT 1 FROM __dbt__CTE__lpf_09_ibp_grp ibp_grp WHERE ibp_grp.region_5 = 'LA'
			AND ibp_grp.sales_product_number = list_price_all.sales_product_number
		AND list_price_all.country_alpha2 = ibp_grp.country_alpha2)
	AND list_price_all.price_term_code <> 'RP'
	AND product_line_xref.pl_category = 'SUP' 
	AND product_line_xref.technology in ('LASER', 'INK', 'PWA')
	AND not exists
	(SELECT * FROM lpf_02_list_price_all list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		AND list_price_all.country_alpha2 = list_price_all2.country_alpha2
		AND list_price_all.price_term_code = list_price_all2.price_term_code
		AND list_price_all.currency_code = list_price_all2.currency_code
		AND (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			OR (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			AND list_price_all.qb_sequence_number < list_price_all2.qb_sequence_number)))
"""

lpf_05_list_price_la = spark.sql(lpf_05_list_price_la)
write_df_to_redshift(configs, lpf_05_list_price_la, "fin_stage.lpf_05_list_price_la", "overwrite")
lpf_05_list_price_la.createOrReplaceTempView("lpf_05_list_price_la")

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
	    lpf_01_ibp_combined ibp_combined
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
	lpf_02_list_price_all list_price_all
WHERE
	 exists (SELECT 1 FROM __dbt__CTE__lpf_09_ibp_grp ibp_grp WHERE ibp_grp.region_5 = 'NA'
		AND ibp_grp.sales_product_number = list_price_all.sales_product_number
		AND list_price_all.country_alpha2 = ibp_grp.country_alpha2)
	AND list_price_all.price_term_code <> 'RP'
	AND not exists
	(SELECT * FROM lpf_02_list_price_all list_price_all2
		WHERE list_price_all.sales_product_number = list_price_all2.sales_product_number
		AND list_price_all.country_alpha2 = list_price_all2.country_alpha2
		AND list_price_all.price_term_code = list_price_all2.price_term_code
		AND (list_price_all.price_start_effective_date < list_price_all2.price_start_effective_date
			OR (list_price_all.price_start_effective_date = list_price_all2.price_start_effective_date
			AND list_price_all.qb_sequence_number < list_price_all2.qb_sequence_number)))
"""

lpf_06_list_price_na = spark.sql(lpf_06_list_price_na)
write_df_to_redshift(configs, lpf_06_list_price_na, "fin_stage.lpf_06_list_price_na", "overwrite")
lpf_06_list_price_na.createOrReplaceTempView("lpf_06_list_price_na")

# COMMAND ----------

forecast_sales_gru = f"""


with  __dbt__CTE__lpf_17_list_price_apj_filtered as (


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
	lpf_04_list_price_apj list_price_apj
WHERE
	list_price_apj.price_term_code = list_price_apj.selected_price_term_code
	AND list_price_apj.currency_code = list_price_apj.selected_currency_code
	AND not exists(
	SELECT 1 FROM lpf_04_list_price_apj list_price_apj_2
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
	lpf_05_list_price_la list_price_la
WHERE
	list_price_la.price_term_code = list_price_la.selected_price_term_code
	AND list_price_la.currency_code = list_price_la.selected_currency_code
	AND not exists(
	SELECT 1 FROM lpf_05_list_price_la list_price_la2
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
	lpf_06_list_price_na list_price_na
WHERE
	list_price_na.currency_code = list_price_na.selected_currency_code
	AND not exists(
	SELECT 1 FROM lpf_06_list_price_NA list_price_na2
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
	    lpf_01_ibp_combined ibp_combined
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
	lpf_03_list_price_eu list_price_eu
	INNER JOIN
	__dbt__CTE__lpf_09_ibp_grp ibp_grp
    	ON ibp_grp.sales_product_number = list_price_eu.sales_product_number
	    AND ibp_grp.country_alpha2 = list_price_eu.country_alpha2
WHERE
	not exists
	(
		SELECT 1 FROM
		lpf_03_list_price_eu list_price_eu2
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
		list_price_term_codes price_term_codes
		LEFT JOIN
		iso_cc_rollup_xref iso_cc_rollup_xref
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
	lpf_02_list_price_all list_price_all
WHERE
	((list_price_all.Price_term_code = 'RP'))
	AND not exists
	(SELECT * FROM lpf_02_list_price_all list_price_all2
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
    country_currency_map_landing.country_alpha2
    , acct_rates.currencycode
	, acct_rates.accountingrate
FROM
      country_currency_map country_currency_map_landing
	  INNER JOIN
	  acct_rates acct_rates
		ON acct_rates.isocurrcd = country_currency_map_landing.currency_iso_code
		AND acct_rates.effectivedate = '{dbutils.widgets.get("accounting_eff_date")}'
		AND acct_rates.version = '{dbutils.widgets.get("accounting_rate_version")}'
WHERE country_currency_map_landing.country_alpha2 is not null
),  __dbt__CTE__lpf_26_eoq as (


SELECT
		region_5
		, product_line AS product_line
		, eoq_discount_pct AS eoq_discount
		, load_date AS eoq_load_date
	FROM
	list_price_eoq
	WHERE
	load_date = (SELECT cal_date FROM list_price_filter_dates WHERE record = 'EOQ')
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
		, acct_rates.accountingrate
		, count(list_price) over (partition by list_price_dp_df_in_rp.sales_product_number, list_price_dp_df_in_rp.country_alpha2, list_price_dp_df_in_rp.price_term_code) as count_List_Price
		, list_price_eoq.eoq_discount
	FROM
		__dbt__CTE__lpf_24_list_price_DP_DF_IN_RP list_price_dp_df_in_rp
		LEFT JOIN
		(SELECT DISTINCT sales_product_number, sales_product_line_code FROM rdma_base_to_sales_product_map) rdma_base_to_sales_product_map
			on list_price_DP_DF_IN_RP.sales_product_number = rdma_base_to_sales_product_map.sales_product_number
		LEFT JOIN
		iso_country_code_xref iso_country_code_xref
			ON iso_country_code_xref.country_alpha2 = list_price_dp_df_in_rp.country_alpha2
		LEFT JOIN
		__dbt__CTE__lpf_25_country_currency_map country_currency_map
			ON country_currency_map.country_alpha2 = list_price_dp_df_in_rp.country_alpha2
			AND country_currency_map.currencycode = list_price_dp_df_in_rp.currency_code
		LEFT JOIN
		acct_rates acct_rates
			ON acct_rates.currencycode = list_price_dp_df_in_rp.currency_code
		LEFT JOIN
		__dbt__CTE__lpf_26_eoq list_price_eoq
			ON list_price_eoq.product_line = rdma_base_to_sales_product_map.sales_product_line_code
			AND iso_country_code_xref.region_5 = list_price_eoq.region_5
	WHERE 
	1=1
	AND acct_rates.version = '{dbutils.widgets.get("accounting_rate_version")}'
		AND acct_rates.effectivedate = '{dbutils.widgets.get("accounting_eff_date")}'
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
		, list_price_eoq.accountingrate as accounting_rate
		, list_price_eoq.list_price/list_price_eoq.accountingrate AS list_price_usd
		, 0 AS list_price_adder_lc
		, list_price_eoq.accountingrate AS currency_code_adder
		, 0 AS list_price_adder_usd
		, coalesce(eoq_discount, 0) AS eoq_discount
		, ((list_price/accountingrate) * (1-coalesce(eoq_discount, 0))) AS sales_product_gru
	FROM
		__dbt__CTE__lpf_27_list_price_eoq list_price_eoq
	WHERE
		(((count_list_price > 1) and (currency_country is not null)) or (count_list_price = 1))
"""
forecast_sales_gru = spark.sql(forecast_sales_gru)

forecast_sales_gru = forecast_sales_gru.withColumn("load_date" , lit(None).cast(StringType())) \
                .withColumn("version" , lit(None).cast(StringType()))
write_df_to_redshift(configs, forecast_sales_gru, "fin_stage.forecast_sales_gru", "overwrite")
forecast_sales_gru.createOrReplaceTempView("forecast_sales_gru")

# COMMAND ----------

list_price_version = f"""


with __dbt__CTE__lpf_09_ibp_grp as (


SELECT DISTINCT
		ibp_combined.sales_product_number
		, country_alpha2
		, region_5
		, ibp_combined.source
		, ibp_combined.version
	FROM
	    lpf_01_ibp_combined ibp_combined
),  __dbt__CTE__lpf_26_eoq as (


SELECT
		region_5
		, product_line AS product_line
		, eoq_discount_pct AS eoq_discount
		, load_date AS eoq_load_date
	FROM
	list_price_eoq
	WHERE
	load_date = (SELECT cal_date FROM list_price_filter_dates WHERE record = 'EOQ')
)SELECT DISTINCT
	'{dbutils.widgets.get("forecast_record")}' AS record
	, (SELECT version FROM list_price_filter_vars WHERE record = 'LIST_PRICE_GPSY') AS lp_gpsy_version
	, (SELECT version FROM list_price_filter_vars WHERE record = 'IBP_SUPPLIES_FORECAST') AS ibp_version
	, '{dbutils.widgets.get("accounting_rate_version")}' AS acct_rates_version
	, (SELECT cal_date FROM list_price_filter_dates WHERE record = 'EOQ') AS eoq_load_date

	FROM
		lpf_02_list_price_all list_price_all
		INNER JOIN 
		__dbt__CTE__lpf_26_eoq eoq
			ON list_price_all.sales_product_line_code = eoq.product_line
"""
list_price_version = spark.sql(list_price_version)

list_price_version = list_price_version.withColumn("load_date" , lit(None).cast(StringType())) \
                .withColumn("version" , lit(None).cast(StringType()))

write_df_to_redshift(configs, list_price_version, "fin_stage.list_price_version", "overwrite")
list_price_version.createOrReplaceTempView("list_price_version")
