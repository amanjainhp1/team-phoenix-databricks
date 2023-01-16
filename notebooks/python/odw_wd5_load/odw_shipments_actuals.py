# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from functools import reduce
from pyspark.sql.functions import col, current_date, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType , BooleanType

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC 
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# define odw_revenue_units_sales_actuals schema
bucket = f"dataos-core-{stack}-team-phoenix-fin" 
bucket_prefix = "landing/odw/shipment_actuals/"
odw_actuals_deliveries_schema = StructType([ \
            StructField("record", StringType(), True), \
            StructField("cal_date", DateType(), True), \
            StructField("country_alpha2", StringType(), True), \
            StructField("market10", StringType(), True), \
            StructField("base_product_number", StringType(), True), \
            StructField("pl", StringType(), True), \
            StructField("trade_or_non_trade", StringType(), True), \
            StructField("base_quantity", DecimalType(), True), \
            StructField("official", IntegerType(), True), \
            StructField("load_date", TimestampType(), True), \
            StructField("version", IntegerType(), True), \
            StructField("unit_reporting_code", StringType(), True), \
            StructField("unit_reporting_description", StringType(), True), \
            StructField("bundled_qty", DecimalType(), True), \
            StructField("unbundled_qty", DecimalType(), True)
        ])

odw_actuals_deliveries_schema_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), odw_actuals_deliveries_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Initial SFAI Data Load

# COMMAND ----------

redshift_row_count = 0
try:
    redshift_row_count = read_redshift_to_df(configs) \
        .option("dbtable", "prod.odw_actuals_deliveries") \
        .load() \
        .count()
except:
    None

if redshift_row_count == 0:
    actuals_deliveries_df = read_sql_server_to_df(configs) \
        .option("dbtable", "IE2_Prod.ms4.actuals_deliveries") \
        .load()
    
    actuals_deliveries_df = odw_actuals_deliveries_schema_df.union(actuals_deliveries_df)
    
    write_df_to_redshift(configs, actuals_deliveries_df, "prod.odw_actuals_deliveries", "append")

# COMMAND ----------

#Load all history data
# path = f"s3://dataos-core-{stack}-team-phoenix-fin/landing/odw/shipment_actuals/"
# files = dbutils.fs.ls(path)

# if len(files) >= 1:
#     SeriesAppend=[]

#     for f in files:
#         odw_actuals_deliveries_df = spark.read \
#             .format("com.crealytics.spark.excel") \
#             .option("inferSchema", "True") \
#             .option("header","True") \
#             .option("treatEmptyValuesAsNulls", "False") \
#             .load(f[0])

#         SeriesAppend.append(odw_actuals_deliveries_df)

#     df_series = reduce(DataFrame.unionAll, SeriesAppend)

# COMMAND ----------

# MAGIC %md
# MAGIC Latest File

# COMMAND ----------

shipment_actuals_latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

shipment_actuals_latest_file = shipment_actuals_latest_file.split("/")[len(shipment_actuals_latest_file.split("/"))-1]

print(shipment_actuals_latest_file)

# COMMAND ----------

if redshift_row_count > 0:
    odw_actuals_deliveries_df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{shipment_actuals_latest_file}")

    odw_actuals_deliveries_df = odw_actuals_deliveries_df.withColumn("load_date", current_timestamp())

    odw_actuals_deliveries_df = odw_actuals_deliveries_df.withColumnRenamed("Fiscal Year Month","fiscal_year_period") \
                        .withColumnRenamed("Calendar Year Month","calendar_year_month") \
                        .withColumnRenamed("Unit Reporting Code","unit_reporting_code") \
                        .withColumnRenamed("Unit Reporting Description","unit_reporting_description") \
                        .withColumnRenamed("Parent Explosion","parent_explosion") \
                        .withColumnRenamed("Material Nr","material_nr") \
                        .withColumnRenamed("Material Desc","material_desc") \
                        .withColumnRenamed("Trade/Non-Trade","trade_or_non_trade") \
                        .withColumnRenamed("Original Profit Center Code","profit_center_code") \
                        .withColumnRenamed("Segment Code","segment_code") \
                        .withColumnRenamed("Segment Hier Desc Level2","segment_hier_desc_level2") \
                        .withColumnRenamed("Delivery Item Qty","delivery_item_qty") \
                        .withColumnRenamed("Delivery Item","delivery_item") \
                        .withColumnRenamed("Bundled Qty","bundled_qty") \
                        .withColumnRenamed("Unbundled Qty","unbundled_qty") 

    write_df_to_redshift(configs, odw_actuals_deliveries_df, "stage.odw_report_ships_deliveries_actuals", "append")

# COMMAND ----------

odw_actuals_deliveries = """

with odw_shipment_aka_deliveries_data as
(

SELECT calendar_year_month
	  , unit_reporting_code
	  , unit_reporting_description
      ,	CASE
		WHEN material_nr = 'G3Q47A#BGJ' THEN 'P2Q00'
		WHEN material_nr = '7KW64A#BGJ' THEN 'P2Q00'
		ELSE profit_center_code
	   END AS profit_center_code
	  , material_nr as material_number
	  , material_desc as material_description
	  , trade_or_non_trade
      , segment_code as segment
      ,SUM(delivery_item_qty) as units
	  ,SUM(bundled_qty) as bundled_qty
	  ,SUM(unbundled_qty) as unbundled_qty
  FROM stage.odw_report_ships_deliveries_actuals w
  where 1=1
  AND segment_code is not null -- all have zero values
  AND calendar_year_month = (SELECT MAX(calendar_year_month) FROM stage.odw_report_ships_deliveries_actuals)
  GROUP BY calendar_year_month
      , profit_center_code
      , material_nr
      , material_desc
      , trade_or_non_trade
      , segment_code
      , unit_reporting_code
      , unit_reporting_description
)
--select * from odw_shipment_aka_deliveries_data where profit_center_code is null and unit_reporting_code = 'S'
,
change_date_and_profit_center_hierarchy AS
(
SELECT
	cal.Date as cal_date,
	unit_reporting_code,
	unit_reporting_description,
	pl,
	segment,
	material_number,
	material_description,
	trade_or_non_trade,
	SUM(units) as units,
	SUM(bundled_qty) as bundled_qty,
	SUM(unbundled_qty) as unbundled_qty
FROM odw_shipment_aka_deliveries_data w
LEFT JOIN mdm.calendar cal ON w.calendar_year_month = cal.calendar_yr_mo
LEFT JOIN mdm.product_line_xref plx ON w.profit_center_code = plx.profit_center_code
WHERE 1=1
	AND day_of_month = 1
GROUP BY cal.Date
    , pl
    , segment
    , material_description
    , material_number
    , trade_or_non_trade
    , unit_reporting_code
    , unit_reporting_description
)
--select * from change_date_and_profit_center_hierarchy where pl is null
,
add_seg_hierarchy AS
(
SELECT
	cal_date,
	unit_reporting_code,
	unit_reporting_description,
	pl,
	country_alpha2,
	region_3,
	region_5,	
	material_number,
	material_description,
	trade_or_non_trade,
	SUM(units) as units,
	SUM(bundled_qty) as bundled_qty,
	SUM(unbundled_qty) as unbundled_qty
FROM change_date_and_profit_center_hierarchy w
LEFT JOIN mdm.profit_center_code_xref s ON w.segment = s.profit_center_code
GROUP BY cal_date
    , pl
    , country_alpha2
    , region_3
    , region_5
    , material_number
    , material_description
    , trade_or_non_trade
    , unit_reporting_code
    , unit_reporting_description
)
, odw_ships_translation as (
SELECT
	cal_date,
	unit_reporting_code,
	unit_reporting_description,
	pl,
	country_alpha2,
	region_3,
	region_5,
	material_number,
	material_description,
	trade_or_non_trade,
	units,
	bundled_qty,
	unbundled_qty
FROM add_seg_hierarchy
)
, 
--select * from #odw_ships_translation where pl = 'N0'


-- translate material number (sales product with option) to sales product number
 

ships_material_number AS
(
SELECT
	cal_date,
	unit_reporting_code,
	unit_reporting_description,
	pl,
	country_alpha2,
	region_3,
	region_5,
	material_number,
	CASE
		WHEN SUBSTRING(material_number,7,1) = '#' THEN SUBSTRING(material_number,1,6)
		WHEN SUBSTRING(material_number,8,1) = '#' THEN SUBSTRING(material_number,1,7)
		ELSE material_number
	END as sales_product_number,
	material_description,
	trade_or_non_trade,
	SUM(units) as units,
	SUM(bundled_qty) as bundled_qty,
	SUM(unbundled_qty) as unbundled_qty
FROM odw_ships_translation odw 
GROUP BY cal_date,
	pl,
	country_alpha2,
	region_3,
	region_5,
	material_number,
	material_description,
	trade_or_non_trade, 
	unit_reporting_code,
	unit_reporting_description
),
ships_sales_product_number AS
(
SELECT
	cal_date,
	unit_reporting_code,
	unit_reporting_description,
	pl,
	country_alpha2,
	region_3,
	region_5,
	sales_product_number,
	sales_prod_nr as rdma_sales_product_number,
	trade_or_non_trade,
	SUM(units) as units,
	SUM(bundled_qty) as bundled_qty,
	SUM(unbundled_qty) as unbundled_qty
FROM ships_material_number sp
LEFT JOIN mdm.rdma_sales_product rdma ON sales_product_number = sales_prod_nr
GROUP BY cal_date,
	pl,
	country_alpha2,
	region_3,
	region_5,
	sales_product_number,
	trade_or_non_trade,
	sales_prod_nr,
	unit_reporting_code,
	unit_reporting_description
),
ships_base_product_number AS
(
SELECT
	cal_date,
	unit_reporting_code,
	unit_reporting_description,
	pl,
	sales_product_line_code,
	base_product_line_code,
	country_alpha2,
	region_3,
	region_5,
	sp.sales_product_number,
	base_product_number,
	trade_or_non_trade,
	SUM(units * isnull(base_prod_per_sales_prod_qty, 1)) as units,
	SUM(bundled_qty * isnull(base_prod_per_sales_prod_qty, 1)) as bundled_qty,
	SUM(unbundled_qty * isnull(base_prod_per_sales_prod_qty, 1)) as unbundled_qty
FROM ships_material_number sp
LEFT JOIN mdm.rdma_base_to_sales_product_map rdma ON 
	sp.sales_product_number = rdma.sales_product_number
GROUP BY cal_date,
	pl,
	country_alpha2,
	region_3,
	region_5,
	sp.sales_product_number,
	trade_or_non_trade,
	base_product_number,
	sales_product_line_code,
	base_product_line_code,
	unit_reporting_code,
	unit_reporting_description
),
ships_base_product_number2 AS
(
SELECT
	cal_date,
	unit_reporting_code,
	unit_reporting_description,
	pl,
	sales_product_line_code,
	base_product_line_code,
	country_alpha2,
	region_3,
	region_5,
	sales_product_number,
	CASE
		WHEN base_product_line_code is null THEN 'UNKN' + pl
		ELSE base_product_number
	END AS base_product_number,
	trade_or_non_trade,
	SUM(units) as units,
	SUM(bundled_qty) as bundled_qty,
	SUM(unbundled_qty) as unbundled_qty
FROM ships_base_product_number sp
GROUP BY cal_date,
	pl,
	country_alpha2,
	region_3,
	region_5,
	sales_product_number,
	trade_or_non_trade,
	base_product_number,
	sales_product_line_code,
	base_product_line_code,
	unit_reporting_code,
	unit_reporting_description
),
fin_stage_odw_report_ships as (
SELECT
	cal_date,
	unit_reporting_code,
	unit_reporting_description,
	pl,
	country_alpha2,
	region_3,
	region_5,
	base_product_number,
	trade_or_non_trade,
	SUM(units) as units,
	SUM(bundled_qty) as bundled_qty,
	SUM(unbundled_qty) as unbundled_qty,
    current_timestamp as load_date
FROM ships_base_product_number2
GROUP BY
	cal_date,
	pl,
	country_alpha2,
	region_3,
	region_5,
	base_product_number,
	trade_or_non_trade,
	unit_reporting_code,
	unit_reporting_description
)SELECT
	'ACTUALS - ODW SUPPPLIES SHIPS' AS record,
	cal_date,
	odw.country_alpha2,
	market10,
	base_product_number,
	pl,
	trade_or_non_trade,
	SUM(units) as base_quantity,
	null as official,
	odw.load_date,
	null as version,
	unit_reporting_code,
	unit_reporting_description,
	SUM(bundled_qty) as bundled_qty,
	SUM(unbundled_qty) as unbundled_qty
FROM fin_stage_odw_report_ships odw
LEFT JOIN mdm.iso_country_code_xref iso ON odw.country_alpha2 = iso.country_alpha2
GROUP BY cal_date,
	unit_reporting_code,
	unit_reporting_description,
	odw.country_alpha2,
	market10,
	base_product_number,
	trade_or_non_trade,
	odw.load_date,
	pl
"""

# COMMAND ----------

if redshift_row_count > 0:
    dataDF = read_redshift_to_df(configs) \
            .option("query", odw_actuals_deliveries) \
            .load()

    dataDF = odw_actuals_deliveries_schema_df.union(dataDF)
    write_df_to_redshift(configs, dataDF, "prod.odw_actuals_deliveries", "append")
    write_df_to_sqlserver(configs=configs, df=dataDF, destination="IE2_Prod.ms4.actuals_deliveries", mode="append")

# COMMAND ----------

# copy back to SFAI
odw_report_ships_deliveries_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "stage.odw_report_ships_deliveries_actuals") \
    .load() \
    .select(col("fiscal_year_period").alias("Fiscal Year Month")
        , col("calendar_year_month").alias("Calendar Year Month")
        , col("unit_reporting_code").alias("Unit Reporting Code")
        , col("unit_reporting_description").alias("Unit Reporting Description")
        , col("parent_explosion").alias("Parent Explosion")
        , col("material_nr").alias("Material Nr")
        , col("material_desc").alias("Material Desc")
        , col("trade_or_non_trade").alias("Trade/Non-Trade")
        , col("profit_center_code").alias("Profit Center Code")
        , col("segment_code").alias("Segment Code")
        , col("segment_hier_desc_level2").alias("Segment Hier Desc Level2")
        , col("delivery_item_qty").alias("Delivery Item Qty")
        , col("load_date").alias("load_date")
        , col("delivery_item").alias("Delivery Item")
        , col("bundled_qty").alias("Bundled Qty")
        , col("unbundled_qty").alias("Unbundled Qty"))

# COMMAND ----------

tables = [
    ['IE2_Landing.ms4.odw_report_ships_deliveries_actuals_landing', odw_report_ships_deliveries_actuals, "overwrite"]
]

# COMMAND ----------

for t_name, df, mode in tables:
    write_df_to_sqlserver(configs, df, t_name, mode)
