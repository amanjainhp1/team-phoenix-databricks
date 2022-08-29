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
        .option("dbtable", "fin_prod.odw_rac_product_financials_actuals") \
        .load() \
        .count()
except:
    None

if redshift_row_count == 0:
    odw_rac_product_financials_df = read_sql_server_to_df(configs) \
        .option("dbtable", "IE2_Landing.ms4.odw_report_rac_product_financials_actuals_landing") \
        .load()
    
    write_df_to_redshift(configs, odw_rac_product_financials_df, "fin_prod.odw_rac_product_financials_actuals", "append")

# COMMAND ----------

# mount S3 bucket
bucket = f"dataos-core-{stack}-team-phoenix-fin"
bucket_prefix = "landing/odw/rac_product_financials/"
dbfs_mount = '/mnt/odw_rac_product_financials/'

s3_mount(f'{bucket}/{bucket_prefix}', dbfs_mount)

# COMMAND ----------

files = dbutils.fs.ls(dbfs_mount)

if len(files) >= 1:
    SeriesAppend=[]
    
    for f in files:
        odw_rac_product_financials_df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("inferSchema", "True") \
            .option("header","True") \
            .option("treatEmptyValuesAsNulls", "False") \
            .load(f.path)

        SeriesAppend.append(odw_rac_product_financials_df)

    df_series = reduce(DataFrame.unionAll, SeriesAppend)

# COMMAND ----------

# MAGIC %md
# MAGIC Latest File

# COMMAND ----------

rac_product_financials_latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

rac_product_financials_latest_file = rac_product_financials_latest_file.split("/")[len(rac_product_financials_latest_file.split("/"))-1]

print(rac_product_financials_latest_file)

# COMMAND ----------

if redshift_row_count > 0:
    rac_product_financials_df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{rac_product_financials_latest_file}")

    rac_product_financials_df = rac_product_financials_df.withColumn("load_date", current_timestamp())

    rac_product_financials_df = rac_product_financials_df.withColumnRenamed("Fiscal Year/Period","fiscal_year_period") \
                        .withColumnRenamed("Material Number","material_number") \
                        .withColumnRenamed("Profit Center Code","profit_center_code") \
                        .withColumnRenamed("Segment Code","segment_code") \
                        .withColumnRenamed("Segment Name","segment_name") \
                        .withColumnRenamed("Gross Trade Revenues USD","gross_trade_revenues_usd") \
                        .withColumnRenamed("Contractual Discounts USD","contractual_discounts_usd") \
                        .withColumnRenamed("Discretionary Discounts USD","discretionary_discounts_usd") \
                        .withColumnRenamed("Net Currency USD","net_currency_usd") \
                        .withColumnRenamed("Net Revenues USD","net_revenues_usd") \
                        .withColumnRenamed("TOTAL COST OF SALES USD","total_cost_of_sales_usd") \
                        .withColumnRenamed("GROSS MARGIN USD","gross_margin_usd") 

    write_df_to_redshift(configs, rac_product_financials_df, "fin_prod.odw_rac_product_financials_actuals", "append")

# COMMAND ----------


