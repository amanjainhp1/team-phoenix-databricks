# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import current_date
from pyspark.sql.types import IntegerType, StringType

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

redshift_row_count = 0
try:
    redshift_row_count = read_redshift_to_df(configs) \
        .option("dbtable", "fin_prod.odw_document_currency") \
        .load() \
        .count()
except:
    None

if redshift_row_count == 0:
    document_currency_df = read_sql_server_to_df(configs) \
        .option("dbtable", "IE2_Financials.ms4.odw_document_currency") \
        .load()
    
    write_df_to_redshift(configs, document_currency_df, "fin_prod.odw_document_currency", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC Complete Data

# COMMAND ----------

# mount S3 bucket
bucket = f"dataos-core-{stack}-team-phoenix"
bucket_prefix = "landing/ODW/odw_document_currency"
dbfs_mount = '/mnt/odw_document_currency/'

s3_mount(f'{bucket}/{bucket_prefix}', dbfs_mount)

# COMMAND ----------

files = dbutils.fs.ls(dbfs_mount)

if len(files) >= 1:
    SeriesAppend=[]
    
    for f in files:
        document_currency_df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("inferSchema", "True") \
            .option("header","True") \
            .option("treatEmptyValuesAsNulls", "False") \
            .load(f.path)

        SeriesAppend.append(document_currency_df)

    df_series = reduce(DataFrame.unionAll, SeriesAppend)

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

    write_df_to_redshift(configs, document_currency_df, "fin_stage.odw_document_currency_report", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC financials_odw_document_currency

# COMMAND ----------

query_list = []

# COMMAND ----------

odw_document_currency = """

with odw_currency_data as (


SELECT "Fiscal Year/Period" as ms4_fiscal_year_period      
      , "Segment Code" as segment
      , "Transaction Currency Code" as document_currency_code
      , "Profit Center Code" as profit_center_code
      , SUM("Net K$") * 1000 AS amount
  FROM  fin_stage.odw_document_currency_report w
  WHERE 1=1
	AND "Net K$" is not null
	AND "Fiscal Year/Period" = (SELECT MAX("Fiscal Year/Period") FROM "fin_stage"."odw_document_currency_report")
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
GROUP BY cal.Date, pl, segment, document_currency_code
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
GROUP BY cal_date, pl, iso.country_alpha2, iso.region_5, document_currency_code, segment
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
GROUP BY cal_date, country_alpha2, region_5, pl, segment, document_currency_code
"""

query_list.append(["fin_prod.odw_document_currency", odw_document_currency , "append"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
