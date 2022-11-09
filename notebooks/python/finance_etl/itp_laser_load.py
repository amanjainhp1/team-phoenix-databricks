# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import col, current_date, regexp_extract, lit, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType, DecimalType

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# define table schema
itp_laser_schema = StructType([ \
            StructField("record", StringType(), True), \
            StructField("market10", StringType(), True), \
            StructField("sales_product", StringType(), True), \
            StructField("key_figure", StringType(), True), \
            StructField("cal_date", DateType(), True), \
            StructField("load_date", TimestampType(), True), \
            StructField("units", IntegerType(), True)
        ])

itp_laser_schema = spark.createDataFrame(spark.sparkContext.emptyRDD(), itp_laser_schema)

# COMMAND ----------

# mount S3 bucket
bucket = f"dataos-core-{stack}-team-phoenix" 
bucket_prefix = "product/supplies_itp_laser"
dbfs_mount = '/mnt/supplies_itp_laser/'

s3_mount(f'{bucket}/{bucket_prefix}', dbfs_mount)

# COMMAND ----------

itp_latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

itp_latest_file = itp_latest_file.split("/")[len(itp_latest_file.split("/"))-1]

print(itp_latest_file)

# COMMAND ----------

  itp_laser_df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{itp_latest_file}")

# COMMAND ----------

itp_laser_df = itp_laser_df.withColumn("cal_date", to_date("cal_date"))

# COMMAND ----------

itp_laser_df.createOrReplaceTempView("itp_laser_df")

# COMMAND ----------

# load/join latest hierarchy into pre-set schema & load to redshift
from pyspark.sql.functions import *

itp_laser_df = itp_laser_df.union(itp_laser_schema)

write_df_to_redshift(configs=configs, df=itp_laser_df, destination="fin_stage.itp_laser_landing", mode="append", postactions="", preactions="TRUNCATE fin_stage.itp_laser_landing")
