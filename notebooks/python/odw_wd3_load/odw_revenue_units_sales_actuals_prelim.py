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

# define odw_revenue_units_sales_actuals_prelim schema
bucket = f"dataos-core-{stack}-team-phoenix-fin" 
bucket_prefix = "landing/odw/revenue_units_sales_actuals_prelim/"
odw_revenue_units_sales_actuals_prelim_schema = StructType([ \
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

odw_revenue_units_sales_actuals_prelim_schema_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), odw_revenue_units_sales_actuals_prelim_schema)

# COMMAND ----------

redshift_sales_actuals_prelim_row_count = 0
try:
    redshift_sales_actuals_prelim_row_count = read_redshift_to_df(configs) \
        .option("dbtable", "fin_prod.odw_revenue_units_sales_actuals_prelim") \
        .load() \
        .count()
except:
    None

if redshift_sales_actuals_prelim_row_count == 0:
    revenue_unit_sales_df = read_sql_server_to_df(configs) \
        .option("dbtable", "IE2_Landing.ms4.odw_revenue_units_sales_actuals_prelim_landing") \
        .load()
    
    odw_revenue_units_sales_actuals_prelim_schema_df = odw_revenue_units_sales_actuals_prelim_schema_df.union(revenue_unit_sales_df)
    
    write_df_to_redshift(configs, odw_revenue_units_sales_actuals_prelim_schema_df, "fin_prod.odw_revenue_units_sales_actuals_prelim", "append")

# COMMAND ----------


