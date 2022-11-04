# Databricks notebook source
# 10/6/2022 - Brent Merrick
# You should receive an email from julio-cesar.quintero-cornejo@hp.com, with the currency hedge Excel workbook
# Before running this notebook, copy the "Oct FY21 Hedge G-L FX Impact HPI 03.10.2022_NoLINKS - Reviewed.xlsx" file
# to the /landing/currency_hedge/ bucket.
# The file listed above will not be that name, EXACTLY, but will be very close

# COMMAND ----------

import boto3

from datetime import datetime
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("spreadsheet_startdate", "")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

def get_dir_content(ls_path):
    dir_paths = dbutils.fs.ls(ls_path)
    subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
    flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
    return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths

paths = get_dir_content(constants["S3_BASE_BUCKET"][stack] + 'landing/currency_hedge/')
latest_file = paths[len(paths)-1]

# COMMAND ----------

#Location of Excel sheet
#sampleDataFilePath = "s3://dataos-core-dev-team-phoenix/landing/currency_hedge/Oct FY21 Hedge G-L FX Impact HPI 03.10.2022_NoLINKS - Reviewed.xlsx"
sampleDataFilePath = latest_file

#flags required for reading the excel
isHeaderOn = "true"
isInferSchemaOn = "false"

#sheetname of excel file
sample1Address = "'Revenue Currency Hedge'!A2"

#read excelfile
sample1DF = spark.read.format("com.crealytics.spark.excel") \
    .option("header", isHeaderOn) \
    .option("inferSchema", isInferSchemaOn) \
    .option("treatEmptyValuesAsNulls", "false") \
    .option("dataAddress", sample1Address) \
    .load(sampleDataFilePath)

# COMMAND ----------

# create dataframe of excel data

today = datetime.today()
today_as_string = f"{str(today.year)[2:4]}-{today.month}" if dbutils.widgets.get("spreadsheet_startdate") == "" else dbutils.widgets.get("spreadsheet_startdate")

sample1DF = sample1DF \
    .withColumnRenamed("Product Category", "product_category") \
    .withColumnRenamed("Currency", "currency") \
    .withColumnRenamed(sample1DF.columns[2], today_as_string) # messed up column will always be 3rd position and be current month and year e.g. Oct 

# COMMAND ----------

# create a list of the column names and filter out columns that won't be part of the unpivot process
col_list = sample1DF.columns
filtered_cols = [x for x in col_list if not x.startswith("_c")] #or what ever they start with
filtered_sample1DF = sample1DF.select(filtered_cols)

# COMMAND ----------

# remove first two columns from column list
months_list = list(filtered_sample1DF.columns[2:len(filtered_sample1DF.columns)])

# COMMAND ----------

# Unpivot the data

# unpivot
unpivotExpr = f"stack({len(months_list)}"

for month in months_list:
  unpivotExpr = unpivotExpr + f", '{month}', `{month}`"

unpivotExpr = unpivotExpr + ") as (month,revenue_currency_hedge)"

unpivotDF = filtered_sample1DF \
    .select("product_category", "currency", expr(unpivotExpr)) \
    .select("product_category", "currency", to_date(col("month"),"yy-MM").alias("month"), "revenue_currency_hedge")

# COMMAND ----------

unpivotDF.display()

# COMMAND ----------

# Add record to version table for 'CURRENCY_HEDGE'
max_info = call_redshift_addversion_sproc(configs, 'CURRENCY_HEDGE', 'Excel File')
max_version = max_info[0]
max_load_date = str(max_info[1])

# COMMAND ----------

# Add load_date and verison to Dataframe
unpivotDF_records = unpivotDF \
    .withColumn("load_date", lit(max_load_date).cast("timestamp")) \
    .withColumn("version", lit(max_version)) \
    .withColumn("revenue_currency_hedge",col("revenue_currency_hedge").cast("double")) \
    .filter(unpivotDF.product_category.isNotNull()) \
    .filter(unpivotDF.revenue_currency_hedge.isNotNull()) \
    .filter("revenue_currency_hedge <> 0")

# COMMAND ----------

# write the updated dataframe to the prod.acct_rates table
write_df_to_redshift(configs, unpivotDF_records, "prod.currency_hedge", "overwrite")

# COMMAND ----------

# output dataset to S3 for archival purposes
# s3a://dataos-core-dev-team-phoenix/product/currency_hedge/[version]/

# set the bucket name
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/currency_hedge/" + max_version

# write the data out in parquet format
write_df_to_s3(unpivotDF_records, s3_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# # Rename the columns to match SFAI
unpivotDF_records = unpivotDF_records \
    .withColumnRenamed("product_category","Product_Category") \
    .withColumnRenamed("currency","Currency") \
    .withColumnRenamed("month","Month") \
    .withColumnRenamed("revenue_currency_hedge","Revenue_Currency_Hedge")

# COMMAND ----------

# # Write to SFAI
write_df_to_sqlserver(configs, unpivotDF_records, "IE2_Prod.dbo.currency_hedge", "overwrite")

# COMMAND ----------

# copy the file from the landing bucket to an archive bucket, then delete from the landing bucket

working_file = latest_file.split("/")[len(latest_file.split("/"))-1]
file_to_delete = 'landing/currency_hedge/' + working_file
new_location = 'archive/currency_hedge/' + working_file

s3 = boto3.resource('s3')
s3.Object('dataos-core-prod-team-phoenix', new_location).copy_from(CopySource=f'dataos-core-{stack}-team-phoenix/' + file_to_delete)
s3.Object('dataos-core-prod-team-phoenix',file_to_delete).delete()
