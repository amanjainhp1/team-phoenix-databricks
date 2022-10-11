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
ci_flash_for_insights_supplies_schema = StructType([ \
            StructField("fiscal_year_qtr", StringType(), True), \
            StructField("pl", StringType(), True), \
            StructField("business_description", StringType(), True), \
            StructField("market", StringType(), True), \
            StructField("channel_inv_k", DecimalType(), True), \
            StructField("ink_toner", StringType(), True)
        ])

ci_flash_for_insights_supplies_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), ci_flash_for_insights_supplies_schema)



# COMMAND ----------

revenue_flash_for_insights_supplies_schema = StructType([ \
            StructField("fiscal_year_qtr", StringType(), True), \
            StructField("pl", StringType(), True), \
            StructField("business_description", StringType(), True), \
            StructField("market", StringType(), True), \
            StructField("net_revenues_k", DecimalType(), True), \
            StructField("ink_toner", StringType(), True), \
            StructField("hedge_k", DecimalType(), True), \
            StructField("concatenate", StringType(), True)
        ])

revenue_flash_for_insights_supplies_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), revenue_flash_for_insights_supplies_schema)


# COMMAND ----------

# mount S3 bucket
bucket = f"dataos-core-{stack}-team-phoenix-fin" 
bucket_prefix = "flash_supplies_finance/channel_inventory"
dbfs_mount = '/mnt/channel_inventory/'

s3_mount(f'{bucket}/{bucket_prefix}', dbfs_mount)

# COMMAND ----------

ci_latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

ci_latest_file = ci_latest_file.split("/")[len(ci_latest_file.split("/"))-1]

print(ci_latest_file)

# COMMAND ----------

   channel_inventory_latest_df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{ci_latest_file}") \
        .select(col("Fiscal_Year_Qtr").alias("fiscal_year_qtr")
               , col("PL").alias("pl")
               , col("Business_description").alias("business_description")
               , col("Market").alias("market")
               , col("Channel Inv $K").alias("channel_inv_k")
               , col("Ink/Toner").alias("ink_toner")
               )

# COMMAND ----------

# load/join latest channel inventory into pre-set schema & load to redshift
channel_inventory_load = ci_flash_for_insights_supplies_df.union(channel_inventory_latest_df) 

write_df_to_redshift(configs=configs, df=channel_inventory_load, destination="fin_prod.ci_flash_for_insights_supplies_temp", mode="append", postactions="", preactions="TRUNCATE fin_prod.ci_flash_for_insights_supplies_temp")

# COMMAND ----------

bucket = f"dataos-core-{stack}-team-phoenix-fin" 
bucket_prefix = "flash_supplies_finance/reported_revenue"
dbfs_mount = '/mnt/reported_revenue/'

s3_mount(f'{bucket}/{bucket_prefix}', dbfs_mount)

# COMMAND ----------

rev_latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

rev_latest_file = rev_latest_file.split("/")[len(rev_latest_file.split("/"))-1]

print(rev_latest_file)

# COMMAND ----------

   revenue_latest_df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{rev_latest_file}") \
        .select(col("Fiscal_Year_Qtr").alias("fiscal_year_qtr")
               , col("PL").alias("pl")
               , col("Business_description").alias("business_description")
               , col("Market").alias("market")
               , col("Net_Revenues $K").alias("net_revenue_k")
               , col("Ink/Toner").alias("ink_toner")
               , col("Hedge $K").alias("hedge_k")
               , col("Concatenate").alias("concatenate")
               )

# COMMAND ----------

# load/join latest reported revenue into pre-set schema & load to redshift
revenue_load = revenue_flash_for_insights_supplies_df.union(revenue_latest_df) 

write_df_to_redshift(configs=configs, df=revenue_load, destination="fin_prod.rev_flash_for_insights_supplies_temp", mode="append", postactions="", preactions="TRUNCATE fin_prod.rev_flash_for_insights_supplies_temp")

# COMMAND ----------


