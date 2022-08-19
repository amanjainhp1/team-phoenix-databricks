# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from functools import reduce

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

redshift_row_count = 0
try:
    redshift_row_count = read_redshift_to_df(configs) \
        .option("dbtable", "fin_prod.odw_sacp_actuals") \
        .load() \
        .count()
except:
    None

if redshift_row_count == 0:
    sacp_actuals_df = read_sql_server_to_df(configs) \
        .option("dbtable", "IE2_Financials.ms4.odw_sacp_actuals") \
        .load()
    
    write_df_to_redshift(configs, sacp_actuals_df, "fin_prod.odw_sacp_actuals", "append")

# COMMAND ----------

# mount S3 bucket
bucket = f"dataos-core-{stack}-team-phoenix"
bucket_prefix = "landing/ODW/odw_sacp_actuals/"
dbfs_mount = '/mnt/odw_sacp_actuals/'

s3_mount(f'{bucket}/{bucket_prefix}', dbfs_mount)

# COMMAND ----------

files = dbutils.fs.ls(dbfs_mount)

if len(files) >= 1:
    SeriesAppend=[]
    
    for f in files:
        odw_sacp_actuals_df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("inferSchema", "True") \
            .option("header","True") \
            .option("treatEmptyValuesAsNulls", "False") \
            .load(f.path)

        SeriesAppend.append(odw_sacp_actuals_df)

    df_series = reduce(DataFrame.unionAll, SeriesAppend)

# COMMAND ----------

# MAGIC %md
# MAGIC Latest File

# COMMAND ----------

sacp_actuals_latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

sacp_actuals_latest_file = sacp_actuals_latest_file.split("/")[len(sacp_actuals_latest_file.split("/"))-1]

print(sacp_actuals_latest_file)

# COMMAND ----------

if redshift_row_count > 0:
    sacp_actuals_df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{sacp_actuals_latest_file}")

    sacp_actuals_df = sacp_actuals_df.withColumn("load_date", current_timestamp())
    
    sacp_actuals_df = sacp_actuals_df.withColumnRenamed("Calendar Date","calendar_date") \
                        .withColumnRenamed("Fiscal Year/Period","fiscal_year_period") \
                        .withColumnRenamed("Profit Center Code","profit_center_code") \
                        .withColumnRenamed("Profit Center Name","profit_center_name") \
                        .withColumnRenamed("Segment Code","segment_code") \
                        .withColumnRenamed("Functional Area Code","functional_area_code") \
                        .withColumnRenamed("Functional Area Name","functional_area_name") \
                        .withColumnRenamed("Sign Flip Amount","sign_flip_amount") 

    write_df_to_redshift(configs, sacp_actuals_df, "fin_stage.odw_sacp_actuals", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC financials_odw_sacp_actuals

# COMMAND ----------

query_list = []

# COMMAND ----------

odw_sacp_actuals = """

WITH

webi_sacppl_data AS
(
SELECT "fiscal_year_period" as fiscal_year_period
      ,w."profit_center_code" as profit_center_code
      ,"profit_center_name" as profit_center_name
      ,"segment_code" as segment
      ,"functional_area_code" as fa_code
      ,CASE
		WHEN "functional_area_code" = 'SUPPLIES_U' THEN 'SUPPLIES_U'
		WHEN "functional_area_code" = 'OEM_UNITS' THEN 'OEM_UNITS'
		ELSE "functional_area_name"
	  END as fa_name
      ,SUM("sign_flip_amount") as amount
  FROM "fin_stage"."odw_sacp_actuals" w
  LEFT JOIN "mdm"."product_line_xref" pc ON w.profit_center_code = pc.profit_center_code
  WHERE 1=1
	AND fiscal_year_period is not null
	AND fiscal_year_period = (SELECT MAX(fiscal_year_period) FROM "fin_stage"."odw_sacp_actuals")
	AND w.profit_center_code IN (
		select profit_center_code
		from "mdm"."product_line_xref"
		where pl_category IN ('SUP', 'LLC')
		and technology in ('PWA', 'LASER', 'INK', 'LF', 'LLCS')
		or pl IN ('N6', 'AU')
		 )
  GROUP BY fiscal_year_period
      , w.profit_center_code
      , profit_center_name
      , segment_code
      , functional_area_code
      , functional_area_name
)
--select * from webi_sacppl_data where fa_code in ('OEM_UNITS', 'SUPPLIES_U')
,
change_date_and_profit_center_hierarchy AS
(
SELECT
	cal.date as cal_date,
	pl,
	segment,
	isnull(fa_code, 'F018') as fa_code,
	isnull(fa_name, 'ADMINISTRATIVE') as fa_name,
	sum(amount) as amount
FROM webi_sacppl_data w
LEFT JOIN "mdm"."calendar" cal ON ms4_fiscal_year_period = fiscal_year_period
LEFT JOIN "mdm"."product_line_xref" pc ON w.profit_center_code = pc.profit_center_code
WHERE 1=1
AND day_of_month = 1
GROUP BY cal.date
    , pl
    , segment
    , fa_code
    , fa_name
),
add_seg_hierarchy AS
(
SELECT
	cal_date,
	pl,
	segment,
	country_alpha2,
	region_3,
	region_5,
	fa_code,
	fa_name,
	SUM(amount) as amount
FROM change_date_and_profit_center_hierarchy w
LEFT JOIN "mdm"."profit_center_code_xref" s ON w.segment = s.profit_center_code
GROUP BY cal_date
    , pl
    , country_alpha2
    , region_3
    , region_5
    , segment
    , fa_name
    , fa_code
),
add_financial_hierarchy AS
(
SELECT
	cal_date,
	pl,
	country_alpha2,
	region_3,
	region_5,
	w.fa_code,
	fa_name,
	"func_area_hier_desc_level7"  as fa_level_7_description,
	"func_area_hier_desc_level8" fa_level_8_description,
	"func_area_hier_desc_level9" fa_level_9_description,
	"func_area_hier_desc_level12" fa_level_12_description,
	SUM(amount) as amount
FROM add_seg_hierarchy w
LEFT JOIN "mdm"."hp_simplify_functional_area_hierarchy_v1" f ON w.fa_code = f.functional_area_code
WHERE func_area_hier_desc_level6 IN ('NET REVENUES', 'TOTAL COST OF SALES')
GROUP BY cal_date
    , pl
    , country_alpha2
    , region_3
    , region_5
    , w.fa_code
    , fa_name
    , func_area_hier_desc_level7
    , func_area_hier_desc_level8
    , func_area_hier_desc_level9
    , func_area_hier_desc_level12
),
reclass_financials AS
(
SELECT
	cal_date,
	pl,
	country_alpha2,
	region_3,
	region_5,
	fa_code,
	fa_name,
	fa_level_7_description,
	fa_level_8_description,
	fa_level_9_description,
	fa_level_12_description,
	CASE
		WHEN fa_level_7_description = 'GROSS TRADE REVENUES' THEN 'GROSS REVENUE'
		WHEN fa_level_7_description = 'NET CURRENCY' THEN 'NET CURRENCY'
		WHEN fa_level_12_description = 'CONTRACTUAL DISCOUNTS' THEN 'CONTRACTUAL DISCOUNTS'
		WHEN fa_level_7_description = 'TRADE DISCOUNTS' AND fa_level_12_description <> 'CONTRACTUAL DISCOUNTS' THEN 'DISCRETIONARY DISCOUNTS'
		WHEN fa_level_8_description = 'WARRANTY' THEN 'WARRANTY'
		ELSE 'OTHER COS'	
	END AS finance11,
	SUM(amount) as amount
FROM add_financial_hierarchy
GROUP BY cal_date
    , pl
    , country_alpha2
    , region_3
    , region_5
    , fa_code
    , fa_name
    , fa_level_12_description
    , fa_level_7_description
    , fa_level_8_description
    , fa_level_9_description
),
webi_planet_like_data_converted AS
(
SELECT
	cal_date,
	region_5,
	pl,
	finance11,
	SUM(amount) as amount
FROM reclass_financials
GROUP BY
	cal_date,
	region_5,
	pl,
	finance11
),
webi_pl_targets AS 
(
SELECT
	cal_date,
	CASE
		WHEN region_5 is null THEN 'XW'
		ELSE region_5
	END AS region_5,
	pl,
	finance11,
	SUM(amount) as dollars
FROM webi_planet_like_data_converted
GROUP BY cal_date
    , region_5
    , pl
    , finance11
),
webi_financials_narrow AS
(
SELECT
	cal_date,
	region_5,
	pl,
	finance11,
	SUM(dollars) as dollars
FROM webi_pl_targets
GROUP BY
	cal_date,
	region_5,
	pl,
	finance11
),
webi_gross_revenue AS
(
SELECT
	cal_date,
	region_5,
	pl,
	SUM(dollars) as gross_revenue
FROM webi_financials_narrow
WHERE finance11 = 'GROSS REVENUE'
GROUP BY cal_date
    , region_5
    , pl
),
webi_net_currency AS
(
SELECT
	cal_date,
	region_5,
	pl,
	SUM(dollars) as net_currency
FROM webi_financials_narrow
WHERE finance11 = 'NET CURRENCY'
GROUP BY cal_date
    , region_5
    , pl
),
webi_contractual_discounts AS
(
SELECT
	cal_date,
	region_5,
	pl,
	SUM(dollars) as contractual_discounts
FROM webi_financials_narrow
WHERE finance11 = 'CONTRACTUAL DISCOUNTS'
GROUP BY cal_date
    , region_5
    , pl
),
webi_discretionary_discounts AS
(
SELECT
	cal_date,
	region_5,
	pl,
	SUM(dollars) as discretionary_discounts
FROM webi_financials_narrow
WHERE finance11 = 'DISCRETIONARY DISCOUNTS'
GROUP BY cal_date
    , region_5
    , pl
),
webi_warranty AS
(
SELECT
	cal_date,
	region_5,
	pl,
	SUM(dollars) as warranty
FROM webi_financials_narrow
WHERE finance11 = 'WARRANTY'
GROUP BY cal_date
    , region_5
    , pl
),
webi_other_cos AS
(
SELECT
	cal_date,
	region_5,
	pl,
	SUM(dollars) as other_cos
FROM webi_financials_narrow
WHERE finance11 = 'OTHER COS'
GROUP BY cal_date
    , region_5
    , pl
),
webi_gross_bind_currency AS
(
SELECT
	g.cal_date,
	g.region_5,
	g.pl,
	ISNULL(SUM(gross_revenue), 0) as gross_revenue,
	ISNULL(SUM(net_currency), 0) as net_currency
FROM webi_gross_revenue as g
LEFT JOIN webi_net_currency as c ON
	(
	g.cal_date = c.cal_date AND
	g.region_5 = c.region_5 AND
	g.pl = c.pl
	)
GROUP BY g.cal_date
    , g.region_5
    , g.pl
),
missing_currency AS
(
SELECT 
	c.cal_date,
	c.region_5,
	c.pl,
	SUM(gross_revenue) as gross_revenue,
	SUM(c.net_currency) as net_currency
FROM webi_net_currency as c
LEFT JOIN webi_gross_bind_currency as b ON
	(
	c.cal_date = b.cal_date AND
	c.region_5 = b.region_5 AND
	c.pl = b.pl
	)
WHERE gross_revenue IS NULL
GROUP BY c.cal_date
    , c.region_5
    , c.pl
),
add_back_missing_currency AS
(
SELECT
	cal_date,
	region_5,
	pl,
	0 as gross_revenue,
	SUM(net_currency) as net_currency,
	0 as contractual_discounts,
	0 as discretionary_discounts,
	0 as warranty,
	0 as other_cos
FROM missing_currency
GROUP BY cal_date
    , region_5
    , pl
),
webi_bind_contractual_discounts AS
(
SELECT
	g.cal_date,
	g.region_5,
	g.pl,
	ISNULL(SUM(gross_revenue), 0) as gross_revenue,
	ISNULL(SUM(net_currency), 0) as net_currency,
	ISNULL(SUM(contractual_discounts), 0) as contractual_discounts
FROM webi_gross_bind_currency as g
LEFT JOIN webi_contractual_discounts as c ON
	(
	g.cal_date = c.cal_date AND
	g.region_5 = c.region_5 AND
	g.pl = c.pl
	)
GROUP BY g.cal_date
    , g.region_5
    , g.pl
),
missing_contractual_discounts AS
(
SELECT 
	c.cal_date,
	c.region_5,
	c.pl,
	SUM(gross_revenue) as gross_revenue,
	--SUM(net_currency) as net_currency,
	SUM(c.contractual_discounts) as contractual_discounts
FROM webi_contractual_discounts as c
LEFT JOIN webi_bind_contractual_discounts as b ON
	(
	c.cal_date = b.cal_date AND
	c.region_5 = b.region_5 AND
	c.pl = b.pl
	)
WHERE gross_revenue IS NULL
GROUP BY c.cal_date
    , c.region_5
    , c.pl
),
add_back_missing_contractual_discounts AS
(
SELECT
	cal_date,
	region_5,
	pl,
	0 as gross_revenue,
	0 as net_currency,
	SUM(contractual_discounts) as contractual_discounts,
	0 as discretionary_discounts,
	0 as warranty,
	0 as other_cos
FROM missing_contractual_discounts
GROUP BY cal_date
    , region_5
    , pl
),
webi_bind_discretionary_discounts AS
(
SELECT
	g.cal_date,
	g.region_5,
	g.pl,
	ISNULL(SUM(gross_revenue), 0) as gross_revenue,
	ISNULL(SUM(net_currency), 0) as net_currency,
	ISNULL(SUM(contractual_discounts), 0) as contractual_discounts,
	ISNULL(SUM(discretionary_discounts), 0) as discretionary_discounts
FROM webi_bind_contractual_discounts as g
LEFT JOIN webi_discretionary_discounts as c ON
	(
	g.cal_date = c.cal_date AND
	g.region_5 = c.region_5 AND
	g.pl = c.pl
	)
GROUP BY g.cal_date
    , g.region_5
    , g.pl
),
missing_discretionary_discounts AS
(
SELECT 
	c.cal_date,
	c.region_5,
	c.pl,
	SUM(gross_revenue) as gross_revenue,
	--SUM(net_currency) as net_currency,
	--SUM(contractual_discounts) as contractual_discounts,
	SUM(c.discretionary_discounts) as discretionary_discounts
FROM webi_discretionary_discounts as c
LEFT JOIN webi_bind_discretionary_discounts as b ON
	(
	c.cal_date = b.cal_date AND
	c.region_5 = b.region_5 AND
	c.pl = b.pl
	)
WHERE gross_revenue IS NULL
GROUP BY c.cal_date
    , c.region_5
    , c.pl
),
add_back_missing_discretionary_discounts AS
(
SELECT
	cal_date,
	region_5,
	pl,
	0 as gross_revenue,
	0 as net_currency,
	0 as contractual_discounts,
	SUM(discretionary_discounts) as discretionary_discounts,
	0 as warranty,
	0 as other_cos
FROM missing_discretionary_discounts
GROUP BY cal_date
    , region_5
    , pl
),

webi_bind_warranty AS
(
SELECT
	g.cal_date,
	g.region_5,
	g.pl,
	ISNULL(SUM(gross_revenue), 0) as gross_revenue,
	ISNULL(SUM(net_currency), 0) as net_currency,
	ISNULL(SUM(contractual_discounts), 0) as contractual_discounts,
	ISNULL(SUM(discretionary_discounts), 0) as discretionary_discounts,
	ISNULL(SUM(warranty), 0) as warranty
FROM webi_bind_discretionary_discounts as g
LEFT JOIN webi_warranty as c ON
	(
	g.cal_date = c.cal_date AND
	g.region_5 = c.region_5 AND
	g.pl = c.pl
	)
GROUP BY g.cal_date
    , g.region_5
    , g.pl
),
missing_warranty AS
(
SELECT 
	c.cal_date,
	c.region_5,
	c.pl,
	SUM(gross_revenue) as gross_revenue,
	--SUM(net_currency) as net_currency,
	--SUM(contractual_discounts) as contractual_discounts,
	--SUM(discretionary_discounts) as discretionary_discounts,
	SUM(c.warranty) as warranty
FROM webi_warranty as c
LEFT JOIN webi_bind_warranty as b ON
	(
	c.cal_date = b.cal_date AND
	c.region_5 = b.region_5 AND
	c.pl = b.pl
	)
WHERE gross_revenue IS NULL
GROUP BY c.cal_date
    , c.region_5
    , c.pl
),
add_back_missing_warranty AS
(
SELECT
	cal_date,
	region_5,
	pl,
	0 as gross_revenue,
	0 as net_currency,
	0 as contractual_discounts,
	0 as discretionary_discounts,
	SUM(warranty) as warranty,
	0 as other_cos
FROM missing_warranty
GROUP BY cal_date
    , region_5
    , pl
),
webi_bind_other_cos AS
(
SELECT
	g.cal_date,
	g.region_5,
	g.pl,
	ISNULL(SUM(gross_revenue), 0) as gross_revenue,
	ISNULL(SUM(net_currency), 0) as net_currency,
	ISNULL(SUM(contractual_discounts), 0) as contractual_discounts,
	ISNULL(SUM(discretionary_discounts), 0) as discretionary_discounts,
	ISNULL(SUM(warranty), 0) as warranty,
	ISNULL(SUM(other_cos), 0) as other_cos
FROM webi_bind_warranty as g
LEFT JOIN webi_other_cos as c ON
	(
	g.cal_date = c.cal_date AND
	g.region_5 = c.region_5 AND
	g.pl = c.pl
	)
GROUP BY g.cal_date
    , g.region_5
    , g.pl
),
missing_other_cos AS
(
SELECT 
	c.cal_date,
	c.region_5,
	c.pl,
	SUM(gross_revenue) as gross_revenue,
	--SUM(net_currency) as net_currency,
	--SUM(contractual_discounts) as contractual_discounts,
	--SUM(discretionary_discounts) as discretionary_discounts,
	--SUM(warranty) as warranty,
	SUM(c.other_cos) as other_cos
FROM webi_other_cos as c
LEFT JOIN webi_bind_other_cos as b ON
	(
	c.cal_date = b.cal_date AND
	c.region_5 = b.region_5 AND
	c.pl = b.pl
	)
WHERE gross_revenue IS NULL
GROUP BY c.cal_date
    , c.region_5
    , c.pl
),
add_back_missing_other_cos AS
(
SELECT
	cal_date,
	region_5,
	pl,
	0 as gross_revenue,
	0 as net_currency,
	0 as contractual_discounts,
	0 as discretionary_discounts,
	0 as warranty,
	SUM(other_cos) as other_cos
FROM missing_other_cos
GROUP BY cal_date
    , region_5
    , pl
),
webi_data_wide AS
(
SELECT *
FROM webi_bind_other_cos
UNION ALL

SELECT *
FROM add_back_missing_currency

UNION ALL

SELECT *
FROM add_back_missing_contractual_discounts

UNION ALL

SELECT *
FROM add_back_missing_discretionary_discounts

UNION ALL

SELECT *
FROM add_back_missing_warranty

UNION ALL

SELECT *
FROM add_back_missing_other_cos
),
webi_data_wide2 AS
(
SELECT
	cal_date,
	region_5,
	pl,
	ISNULL(SUM(gross_revenue), 0) as gross_revenue,
	ISNULL(SUM(net_currency), 0) as net_currency, 
	ISNULL(SUM(contractual_discounts), 0) as contractual_discounts,
	ISNULL(SUM(discretionary_discounts), 0) as discretionary_discounts,
	ISNULL(SUM(warranty), 0) as warranty,
	ISNULL(SUM(other_cos), 0) as other_cos
FROM webi_data_wide
GROUP BY cal_date
    , region_5
    , pl
),
webi_data_wide3 AS
(
SELECT
	cal_date,
	region_5,
	pl,
	SUM(gross_revenue) as gross_revenue,
	SUM(net_currency) as net_currency,
	SUM(contractual_discounts) * -1 as contractual_discounts,
	SUM(discretionary_discounts) * -1 as discretionary_discounts,
	SUM(warranty) * -1 as warranty,
	SUM(other_cos) * -1 as other_cos,
		SUM(gross_revenue) + SUM(net_currency) + SUM(contractual_discounts) + SUM(discretionary_discounts) + SUM(warranty) + SUM(other_cos) as total
FROM webi_data_wide2
GROUP BY cal_date
    , region_5
    , pl
)
,
webi_data_wide4 AS
(
SELECT
	cal_date,
	region_5,
	pl,
	SUM(gross_revenue) as gross_revenue,
	SUM(net_currency) as net_currency,
	SUM(contractual_discounts) as contractual_discounts,
	SUM(discretionary_discounts) as discretionary_discounts,
	SUM(gross_revenue) + SUM(net_currency) - SUM(contractual_discounts) - SUM(discretionary_discounts) as net_revenue,
	SUM(warranty) as warranty,
	SUM(other_cos) as other_cos,
	SUM(warranty) + SUM(other_cos) as total_cos,
	SUM(gross_revenue) + SUM(net_currency) - SUM(contractual_discounts) - SUM(discretionary_discounts) - SUM(warranty) - SUM(other_cos) as gross_profit
FROM webi_data_wide3
WHERE total <> 0
GROUP BY cal_date
    , region_5
    , pl
)
SELECT
    'ACTUALS-ODW_SACP_ACTUALS' as record,
	cal_date,
	region_5,
	pl,	
	SUM(gross_revenue) as gross_revenue,
	SUM(net_currency) as net_currency,
	SUM(contractual_discounts) as contractual_discounts,
	SUM(discretionary_discounts) as discretionary_discounts,
	SUM(gross_revenue) + SUM(net_currency) - SUM(contractual_discounts) - SUM(discretionary_discounts) as net_revenue,
	SUM(warranty) as warranty,
	SUM(other_cos) as other_cos,
	SUM(warranty) + SUM(other_cos) as total_cos,
	SUM(gross_profit) as gross_profit,
    current_timestamp as load_date
FROM webi_data_wide4
GROUP BY cal_date
    , region_5
    , pl
"""

query_list.append(["fin_prod.odw_sacp_actuals", odw_sacp_actuals , "append"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list

# COMMAND ----------


