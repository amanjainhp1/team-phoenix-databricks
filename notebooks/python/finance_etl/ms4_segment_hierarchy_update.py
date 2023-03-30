# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import col, current_date, regexp_extract, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType, DecimalType, BooleanType

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# define ms4 segment hierarchy schema (mdm.profit center code xref where record = 'SEGMENT')
ms4_segment_hierarchy_schema = StructType([ \
            StructField("record", StringType(), True), \
            StructField("profit_center_code", StringType(), True), \
            StructField("profit_center_name", StringType(), True), \
            StructField("pc_level_0", StringType(), True), \
            StructField("pc_level_1", StringType(), True), \
            StructField("pc_level_2", StringType(), True), \
            StructField("pc_level_3", StringType(), True), \
            StructField("pc_level_4", StringType(), True), \
            StructField("pc_level_5", StringType(), True), \
            StructField("pc_level_6", StringType(), True), \
            StructField("country_alpha2", StringType(), True), \
            StructField("region_3", StringType(), True), \
            StructField("region_5", StringType(), True), \
            StructField("update_date", DateType(), True), \
            StructField("official", BooleanType(), True), \
            StructField("load_date", TimestampType(), True), \
            StructField("version", StringType(), True)
        ])

ms4_segment_hierarchy_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), ms4_segment_hierarchy_schema)

# COMMAND ----------

# mount S3 bucket
bucket = f"dataos-core-{stack}-team-phoenix-fin" 
bucket_prefix = "odw/ms4_segment_hierarchy"
dbfs_mount = '/mnt/ms4_segment_hierarchy/'

s3_mount(f'{bucket}/{bucket_prefix}', dbfs_mount)

# COMMAND ----------

ms4_latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

ms4_latest_file = ms4_latest_file.split("/")[len(ms4_latest_file.split("/"))-1]

print(ms4_latest_file)

# COMMAND ----------

   ms4_latest_segment_hierarchy_df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{ms4_latest_file}")

# COMMAND ----------

ms4_latest_segment_hierarchy_df = ms4_latest_segment_hierarchy_df.withColumn("load_date", current_date())

# COMMAND ----------

ms4_latest_segment_hierarchy_df = ms4_latest_segment_hierarchy_df.withColumn("official", col("official").cast(BooleanType()))

# COMMAND ----------

# load/join latest hierarchy into pre-set schema & load to redshift
ms4_latest_segment_hierarchy_df = ms4_segment_hierarchy_df.union(ms4_latest_segment_hierarchy_df) 

write_df_to_redshift(configs=configs, df=ms4_latest_segment_hierarchy_df, destination="fin_stage.ms4_segment_hierarchy", mode="append", postactions="", preactions="TRUNCATE fin_stage.ms4_segment_hierarchy")

# COMMAND ----------

# load S3 tables to df
ms4_segment_hierarchy = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.ms4_segment_hierarchy") \
    .load()
profit_center_code_xref = read_redshift_to_df(configs) \
    .option("query", "SELECT * FROM mdm.profit_center_code_xref WHERE record = 'PROFIT_CENTER'") \
    .load()

# COMMAND ----------

ms4_segment_hierarchy.createOrReplaceTempView("ms4_segment_hierarchy")
profit_center_code_xref.createOrReplaceTempView("profit_center_code_xref")

# COMMAND ----------

load_segment_to_profit_center_code_xref = """
SELECT profit_center_id, 
	record,
	profit_center_code,
	profit_center_name,
	pc_level_0,
	pc_level_1,
	pc_level_2,
	pc_level_3,
	pc_level_4,
	pc_level_5,
	pc_level_6,
	country_alpha2,
	region_3,
	region_5,
	CAST(update_date as date),
	official,
	load_date,
	version
FROM profit_center_code_xref

UNION ALL

SELECT '' as profit_center_id,
	record,
	profit_center_code,
	profit_center_name,
	pc_level_0,
	pc_level_1,
	pc_level_2,
	pc_level_3,
	pc_level_4,
	pc_level_5,
	pc_level_6,
	country_alpha2,
	region_3,
	region_5,
	CAST(update_date AS date),
	official,
	CAST(load_date AS timestamp),
	version
FROM ms4_segment_hierarchy
"""

load_segment_to_profit_center_code_xref = spark.sql(load_segment_to_profit_center_code_xref)
load_segment_to_profit_center_code_xref.createOrReplaceTempView("load_segment_to_profit_center_code_xref")

# COMMAND ----------

# define mdm.profit center code xref shema
profit_center_code_schema = StructType([ \
            StructField("profit_center_id", IntegerType()), \
            StructField("record", StringType(), True), \
            StructField("profit_center_code", StringType(), True), \
            StructField("profit_center_name", StringType(), True), \
            StructField("pc_level_0", StringType(), True), \
            StructField("pc_level_1", StringType(), True), \
            StructField("pc_level_2", StringType(), True), \
            StructField("pc_level_3", StringType(), True), \
            StructField("pc_level_4", StringType(), True), \
            StructField("pc_level_5", StringType(), True), \
            StructField("pc_level_6", StringType(), True), \
            StructField("country_alpha2", StringType(), True), \
            StructField("region_3", StringType(), True), \
            StructField("region_5", StringType(), True), \
            StructField("update_date", DateType(), True), \
            StructField("official", BooleanType(), True), \
            StructField("load_date", TimestampType(), True), \
            StructField("version", StringType(), True)
        ])

profit_center_code_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), profit_center_code_schema)

# COMMAND ----------

# load/join latest hierarchy into pre-set schema & load to redshift
load_segment_to_profit_center_code_xref2 = profit_center_code_df.union(load_segment_to_profit_center_code_xref) 

# COMMAND ----------

#LOAD TO DB
write_df_to_redshift(configs, load_segment_to_profit_center_code_xref2, "mdm.profit_center_code_xref", "append", postactions = "", preactions = "truncate mdm.profit_center_code_xref")

# COMMAND ----------


