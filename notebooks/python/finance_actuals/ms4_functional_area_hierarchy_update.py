# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import col, current_date, regexp_extract, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType, DecimalType

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# define ms4 table schema
ms4_functional_area_hierarchy_schema = StructType([ \
            StructField("functional_area_code", StringType(), True), \
            StructField("functional_area_name", StringType(), True), \
            StructField("func_area_hier_level0", StringType(), True), \
            StructField("func_area_hier_desc_level0", StringType(), True), \
            StructField("func_area_hier_level1", StringType(), True), \
            StructField("func_area_hier_desc_level1", StringType(), True), \
            StructField("func_area_hier_level2", StringType(), True), \
            StructField("func_area_hier_desc_level2", StringType(), True), \
            StructField("func_area_hier_level3", StringType(), True), \
            StructField("func_area_hier_desc_level3", StringType(), True), \
            StructField("func_area_hier_level4", StringType(), True), \
            StructField("func_area_hier_desc_level4", StringType(), True), \
            StructField("func_area_hier_level5", StringType(), True), \
            StructField("func_area_hier_desc_level5", StringType(), True), \
            StructField("func_area_hier_level6", StringType(), True), \
            StructField("func_area_hier_desc_level6", StringType(), True), \
            StructField("func_area_hier_level7", StringType(), True), \
            StructField("func_area_hier_desc_level7", StringType(), True), \
            StructField("func_area_hier_level8", StringType(), True), \
            StructField("func_area_hier_desc_level8", StringType(), True), \
            StructField("func_area_hier_level9", StringType(), True), \
            StructField("func_area_hier_desc_level9", StringType(), True), \
            StructField("func_area_hier_level10", StringType(), True), \
            StructField("func_area_hier_desc_level10", StringType(), True), \
            StructField("func_area_hier_level11", StringType(), True), \
            StructField("func_area_hier_desc_level11", StringType(), True), \
            StructField("func_area_hier_level12", StringType(), True), \
            StructField("func_area_hier_desc_level12", StringType(), True), \
            StructField("func_area_leaf_node", StringType(), True), \
            StructField("load_date", DateType(), True)
        ])

ms4_functional_hierarchy_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), ms4_functional_area_hierarchy_schema)

# COMMAND ----------

# mount S3 bucket
bucket = f"dataos-core-{stack}-team-phoenix-fin" 
bucket_prefix = "odw/ms4_functional_area_hierarchy"
dbfs_mount = '/mnt/ms4_functional_area_hierarchy/'

s3_mount(f'{bucket}/{bucket_prefix}', dbfs_mount)

# COMMAND ----------

ms4_latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

ms4_latest_file = ms4_latest_file.split("/")[len(ms4_latest_file.split("/"))-1]

print(ms4_latest_file)

# COMMAND ----------

   ms4_latest_functional_hierarchy_df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{ms4_latest_file}")

# COMMAND ----------

ms4_latest_functional_hierarchy_df = ms4_latest_functional_hierarchy_df.withColumn("load_date", current_date()).where("`Functional Area Code` is not null")

# COMMAND ----------

# load/join latest hierarchy into pre-set schema & load to redshift
ms4_latest_functional_hierarchy_df = ms4_functional_hierarchy_df.union(ms4_latest_functional_hierarchy_df) 

write_df_to_redshift(configs=configs, df=ms4_latest_functional_hierarchy_df, destination="mdm.ms4_functional_area_hierarchy", mode="append", postactions="", preactions="TRUNCATE mdm.ms4_functional_area_hierarchy")
