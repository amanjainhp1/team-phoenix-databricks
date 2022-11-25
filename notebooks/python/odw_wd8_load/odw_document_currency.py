# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType , BooleanType

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# define odw_revenue_units_sales_actuals schema
bucket = f"dataos-core-{stack}-team-phoenix-fin" 
bucket_prefix = "landing/odw/document_currency/"
odw_document_currency_schema = StructType([ \
            StructField("record", StringType(), True), \
            StructField("cal_date", DateType(), True), \
            StructField("country_alpha2", StringType(), True), \
            StructField("region_5", StringType(), True), \
            StructField("pl", StringType(), True), \
            StructField("segment", StringType(), True), \
            StructField("document_currency_code", StringType(), True), \
            StructField("revenue", DecimalType(), True), \
            StructField("official", IntegerType(), True), \
            StructField("load_date", TimestampType(), True), \
            StructField("version", IntegerType(), True)
        ])

odw_document_currency_schema_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), odw_document_currency_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Initial SFAI Data Load

# COMMAND ----------

redshift_row_count = 1
# try:
#     redshift_row_count = read_redshift_to_df(configs) \
#         .option("dbtable", "fin_prod.odw_document_currency") \
#         .load() \
#         .count()
# except:
#     None

# if redshift_row_count == 0:
#     document_currency_df = read_sql_server_to_df(configs) \
#         .option("dbtable", "IE2_Financials.ms4.odw_document_currency") \
#         .load()
    
#     document_currency_df = odw_document_currency_schema_df.union(document_currency_df)
    
#     write_df_to_redshift(configs, document_currency_df, "fin_prod.odw_document_currency", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC Complete Data

# COMMAND ----------

# Load all history data
# path = f"s3://dataos-core-{stack}-team-phoenix-fin/landing/odw/document_currency/"
# files = dbutils.fs.ls(path)

# if len(files) >= 1:
#     SeriesAppend=[]
    
#     for f in files:
#         document_currency_df = spark.read \
#             .format("com.crealytics.spark.excel") \
#             .option("inferSchema", "True") \
#             .option("header","True") \
#             .option("treatEmptyValuesAsNulls", "False") \
#             .load(f[0])

#         SeriesAppend.append(document_currency_df)

#     df_series = reduce(DataFrame.unionAll, SeriesAppend)

# COMMAND ----------

# MAGIC %md
# MAGIC Latest File

# COMMAND ----------

document_currency_latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

document_currency_latest_file = document_currency_latest_file.split("/")[len(document_currency_latest_file.split("/"))-1]

print(document_currency_latest_file)

# COMMAND ----------

if redshift_row_count > 0:
    document_currency_df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{document_currency_latest_file}")

    document_currency_df = document_currency_df.withColumn("load_date", current_date()) \
        .withColumn("Fiscal Year/Period", (document_currency_df["Fiscal Year/Period"].cast(IntegerType())).cast(StringType()))
    
    write_df_to_redshift(configs, document_currency_df, "fin_stage.odw_document_currency", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC financials_odw_document_currency

# COMMAND ----------

odw_document_currency = """

with odw_currency_data as (


SELECT "Fiscal Year/Period" as ms4_fiscal_year_period      
      , "Segment Code" as segment
      , "Transaction Currency Code" as document_currency_code
      , "Profit Center Code" as profit_center_code
      , SUM("Net K$") * 1000 AS amount
  FROM  fin_stage.odw_document_currency w
  WHERE 1=1
	AND "Net K$" is not null
	AND "Net K$" <> 0
	AND "Func Area Hier Desc Level6" = 'NET REVENUES' -- edw report filters to revenue
  GROUP BY "Fiscal Year/Period", "Profit Center Code", "Segment Code","Transaction Currency Code"
), change_date_and_profit_center_hierarchy as (


SELECT
	cal.Date as cal_date
	, pl
	, segment
	, document_currency_code
	, sum(amount) as amount
FROM odw_currency_data w
LEFT JOIN "mdm"."calendar" cal ON w.ms4_fiscal_year_period = cal.ms4_fiscal_year_period
LEFT JOIN "prod"."ms4_profit_center_hierarchy" plx ON w.profit_center_code = plx.profit_center_code    
WHERE 1=1
	AND Day_of_Month = 1
GROUP BY cal.Date
    , pl
    , segment
    , document_currency_code
), add_seg_hierarchy as (


SELECT
	cal_date
	, pl
	, segment
	, iso.country_alpha2
	, iso.region_5
	, document_currency_code
	, SUM(amount) as amount
FROM change_date_and_profit_center_hierarchy w
LEFT JOIN "mdm"."profit_center_code_xref" s ON w.segment = s.profit_center_code
LEFT JOIN "mdm"."iso_country_code_xref" iso ON s.country_alpha2 = iso.country_alpha2
GROUP BY cal_date
    , pl
    , iso.country_alpha2
    , iso.region_5
    , document_currency_code
    , segment
)SELECT
    'ACTUALS - DOCUMENT CURRENCY' as record
    , cal_date
	, country_alpha2
	, region_5
	, pl
	, segment
	, document_currency_code
	, SUM(amount) as revenue
    , null as official
	, current_date as load_date
    , null as version
FROM add_seg_hierarchy
GROUP BY cal_date
    , country_alpha2
    , region_5
    , pl
    , segment
    , document_currency_code
"""

# COMMAND ----------

#Write df to redshift
if redshift_row_count > 0:
    dataDF = read_redshift_to_df(configs) \
            .option("query", odw_document_currency) \
            .load()

    dataDF = odw_document_currency_schema_df.union(dataDF)
    write_df_to_redshift(configs, dataDF, "fin_prod.odw_document_currency", "append")
