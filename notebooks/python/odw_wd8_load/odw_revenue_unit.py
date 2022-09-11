# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import col, current_date, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType, DecimalType

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %md
# MAGIC Initial SFAI Data Load

# COMMAND ----------

# define odw_revenue_units_sales_actuals schema
bucket = f"dataos-core-{stack}-team-phoenix-fin" 
bucket_prefix = "landing/odw/revenue_unit_sales_actuals/"
odw_revenue_units_sales_actuals_schema = StructType([ \
            StructField("fiscal_year_period", StringType(), True), \
            StructField("profit_center_hier_desc_level4", StringType(), True), \
            StructField("segment_hier_desc_level4", StringType(), True), \
            StructField("segment_code", StringType(), True), \
            StructField("segment_name", StringType(), True), \
            StructField("profit_center_code", StringType(), True), \
            StructField("material_number", StringType(), True), \
            StructField("unit_quantity_sign_flip", DecimalType(), True), \
            StructField("load_date", TimestampType(), True), \
            StructField("unit_reporting_code", StringType(), True), \
            StructField("unit_reporting_description", StringType(), True)
        ])

odw_revenue_units_sales_actuals_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), odw_revenue_units_sales_actuals_schema)

# COMMAND ----------

redshift_row_count = 0
try:
    redshift_row_count = read_redshift_to_df(configs) \
        .option("dbtable", "fin_prod.odw_revenue_units_sales_actuals") \
        .load() \
        .count()
except:
    None

if redshift_row_count == 0:
    revenue_unit_df = read_sql_server_to_df(configs) \
        .option("dbtable", "IE2_Landing.ms4.odw_revenue_units_sales_actuals_landing") \
        .load()
    
    odw_revenue_units_sales_actuals_df = odw_revenue_units_sales_actuals_df.union(revenue_unit_df)
    
    write_df_to_redshift(configs, odw_revenue_units_sales_actuals_df, "fin_prod.odw_revenue_units_sales_actuals", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC Complete Data

# COMMAND ----------

#Load all history data
# path = f"s3://{bucket}/{bucket_prefix}"
# files = dbutils.fs.ls(path)

# SeriesAppend=[]
# for f in files:
#     revenue_unit_complete_data_df = spark.read \
#         .format("com.crealytics.spark.excel") \
#         .option("inferSchema", "True") \
#         .option("header","True") \
#         .option("treatEmptyValuesAsNulls", "False") \
#         .load(f[0])

#     SeriesAppend.append(revenue_unit_complete_data_df)

# df_series = reduce(DataFrame.unionAll, SeriesAppend)

# COMMAND ----------

# MAGIC %md
# MAGIC Latest File

# COMMAND ----------

revenue_unit_latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

revenue_unit_latest_file = revenue_unit_latest_file.split("/")[len(revenue_unit_latest_file.split("/"))-1]

print(revenue_unit_latest_file)

# COMMAND ----------

# MAGIC %md
# MAGIC Revenue Unit Sales Actuals

# COMMAND ----------

if redshift_row_count > 0:
    revenue_unit_df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{revenue_unit_latest_file}")
    
    revenue_unit_df = revenue_unit_df \
        .withColumn("unit quantity (sign-flip)", revenue_unit_df["unit quantity (sign-flip)"].cast(DecimalType(38,6))) \
        .withColumn('unit quantity (sign-flip)', regexp_extract(col('unit quantity (sign-flip)'), '-?\d+\.\d{0,2}', 0))

    revenue_unit_df = revenue_unit_df \
        .withColumn("unit quantity (sign-flip)", revenue_unit_df["unit quantity (sign-flip)"].cast(DecimalType(38,2))) \
        .withColumn("load_date", current_date()) \
        .select("Fiscal Year/Period","Profit Center Hier Desc Level4","Segment Hier Desc Level4","Segment Code","Segment Name","Profit Center Code","Material Number","unit quantity (sign-flip)","load_date","Unit Reporting Code","Unit Reporting Description")

    revenue_unit_df = odw_revenue_units_sales_actuals_df.union(revenue_unit_df)    
    
#     write_df_to_redshift(configs, revenue_unit_df, "fin_prod.odw_revenue_units_sales_actuals", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC Revenue_unit_base_landing

# COMMAND ----------

query_list = []

# COMMAND ----------

revenue_units_base_actuals = f"""
WITH odw_sales_product_units AS (
SELECT 
    cal.date AS cal_date
    , profit_center_code
    , material_number
    , segment_code AS segment
    , SUM(unit_quantity_sign_flip) AS units
FROM "fin_prod"."odw_revenue_units_sales_actuals" w
LEFT JOIN "mdm"."calendar" cal 
    ON ms4_Fiscal_Year_Period = fiscal_year_period
WHERE 1=1
    AND material_number is not null
    AND unit_quantity_sign_flip <> 0
    AND unit_quantity_sign_flip is not null
    AND day_of_month = 1
    AND fiscal_year_period = ( SELECT MAX(fiscal_year_period ) FROM "fin_prod"."odw_revenue_units_sales_actuals" )
GROUP BY cal.date
    , profit_center_code
    , material_number
    , segment_code
), change_profit_center_hierarchy as (
SELECT
	cal_date
	, w.profit_center_code
	, pl
	, segment
	, material_number
	, ISNULL(SUM(units), 0) as units
FROM odw_sales_product_units w
LEFT JOIN mdm.product_line_xref plx 
    ON w.profit_center_code = plx.profit_center_code
WHERE 1=1
GROUP BY cal_date
    , pl
    , segment
    , material_number
    , w.profit_center_code
), add_seg_hierarchy as (
SELECT
	cal_date
	, pl
	, country_alpha2
	, region_3
	, region_5	
	, material_number
	, SUM(units) as units
FROM change_profit_center_hierarchy w
LEFT JOIN mdm.profit_center_code_xref s 
    ON w.segment = s.profit_center_code
GROUP BY cal_date
    , pl
    , country_alpha2
    , region_3
    , region_5
    , material_number
) 
-- translate material number (sales product with option) to sales product number
, sales_material_number as (
SELECT
	cal_date
	, pl
	, country_alpha2
	, region_3
	, region_5
	, material_number
	, CASE
		WHEN SUBSTRING(material_number,7,1) = '#' THEN SUBSTRING(material_number,1,6)
		WHEN SUBSTRING(material_number,8,1) = '#' THEN SUBSTRING(material_number,1,7)
		ELSE material_number
	END as sales_product_number
	, SUM(units) as units
FROM add_seg_hierarchy
GROUP BY cal_date
	, pl
	, country_alpha2
	, region_3
	, region_5
	, material_number
), sales_product_number as (
SELECT
	cal_date
	, pl
	, country_alpha2
	, region_3
	, region_5
	, sales_product_number
    , sales_prod_nr as rdma_sales_product_number
	, SUM(units) as units
FROM sales_material_number sp
LEFT JOIN mdm.rdma_sales_product rdma 
    ON sales_product_number = sales_prod_nr
GROUP BY cal_date
	, pl
	, country_alpha2
	, region_3
	, region_5
	, sales_product_number
	, sales_prod_nr
), base_product_number as (
SELECT
	cal_date
	, pl
	, sales_product_line_code
	, base_product_line_code
	, country_alpha2
	, region_3
	, region_5
	, sp.sales_product_number
	, base_product_number
	, SUM(units * isnull(base_prod_per_sales_prod_qty, 1)) as units
FROM sales_product_number sp
LEFT JOIN mdm.rdma_base_to_sales_product_map rdma 
    ON sp.sales_product_number = rdma.sales_product_number
GROUP BY cal_date
	, pl
	, country_alpha2
	, region_3
	, region_5
	, sp.sales_product_number
	, base_product_number
	, sales_product_line_code
	, base_product_line_code
), base_product_number2 as (
SELECT
	cal_date
	, pl
	, sales_product_line_code
    , base_product_line_code
	, country_alpha2
	, region_3
    , region_5
	, sales_product_number
	, CASE
		WHEN base_product_line_code is null THEN 'UNKN' + pl
		ELSE base_product_number
      END AS base_product_number
	, SUM(units) as units
FROM base_product_number sp
GROUP BY cal_date
	, pl 
	, country_alpha2
	, region_3
	, region_5
	, sales_product_number
	, base_product_number
	, sales_product_line_code
	, base_product_line_code
), odw_base_product_etl as (
SELECT
	cal_date
	, pl
	, country_alpha2
	, region_3
	, region_5
	, base_product_number
	, SUM(units) as units
FROM base_product_number2
GROUP BY
	cal_date
	, pl
	, country_alpha2
	, region_3
	, region_5
	, base_product_number
), final as (
SELECT
	cal_date
	, country_alpha2
	, region_3
	, region_5
	, base_product_number
	, pl
	, SUM(units) as base_quantity
	, current_timestamp as load_date
FROM odw_base_product_etl odw
GROUP BY cal_date,
	pl,
	country_alpha2,
	region_3,
	region_5,
	base_product_number
)SELECT cal_date
	, country_alpha2
	, region_3
	, region_5
	, base_product_number
	, pl
	, base_quantity
	, load_date
    , null as unit_reporting_code
    , null as unit_reporting_description
FROM final
"""

query_list = [["fin_prod.odw_revenue_units_base_actuals", revenue_units_base_actuals , "append"]]

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
