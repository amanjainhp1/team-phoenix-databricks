# Databricks notebook source
# 10/6/2022 - Brent Merrick
# Refresh the Excel Sheet, "Forecast Template IBP_Test.xlsm"
# to the /landing/ibp_supplies_fcst/ S3 bucket


# COMMAND ----------

# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

def get_dir_content(ls_path):
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths

paths = get_dir_content('s3://dataos-core-prod-team-phoenix/landing/ibp_supplies_fcst/')
#[print(p) for p in paths]
for p in paths:
  latest_file = p


# COMMAND ----------

# MAGIC %md
# MAGIC ## TO DO

# COMMAND ----------



# TODO - this file has only pulled in the LLCs for NA and LA.  We still need to pull the entire supplies dataset from the main tab and process it



# COMMAND ----------

#Location of Excel sheet
sampleDataFilePath = latest_file

#flags required for reading the excel
isHeaderOn = "true"
isInferSchemaOn = "false"

#sheetname of excel file
sample1Address = "'LLC only'!I4"

#read excelfile
df = spark.read.format("com.crealytics.spark.excel") \
  .option("header", isHeaderOn) \
  .option("inferSchema", isInferSchemaOn) \
  .option("treatEmptyValuesAsNulls", "false") \
  .option("dataAddress", sample1Address) \
  .load(sampleDataFilePath)

# COMMAND ----------

# create dataframe of excel data
from datetime import datetime

df = df \
  .withColumnRenamed("Market", "market") \
  .withColumnRenamed("Primary Base Product", "base_product_number")
display(df)

# COMMAND ----------

# create a list of the column names and filter out columns that won't be part of the unpivot process
col_list = df.columns
filtered_cols = [x for x in col_list if not x.startswith("_c")] #or what ever they start with
filtered_df = df.select(filtered_cols)
filtered_df.display()

# COMMAND ----------

# remove first two columns from column list
months_list = list(filtered_df.columns[2:len(filtered_df.columns)])

print(months_list)

# COMMAND ----------

# Unpivot the data
from pyspark.sql.functions import *

# Unpivot the data
unpivotExpr = f"stack({len(months_list)}"

for month in months_list:
  unpivotExpr = unpivotExpr + f", '{month}', `{month}`"

unpivotExpr = unpivotExpr + ") as (month,units)"

unpivotDF = filtered_df \
    .select("market", "base_product_number", expr(unpivotExpr)) \
    .select("market", "base_product_number", "month", "units")

unpivotDF_filtered_1 = unpivotDF.where("market in ('LATIN AMERICA','NORTH AMERICA')")

# COMMAND ----------

unpivotDF_filtered_1.display()

# COMMAND ----------


import pandas as pd
from datetime import datetime

df1 = unpivotDF_filtered_1.toPandas()

format_data = "%y-%b"
 
for index, row in df1.iterrows():
    variable1 = row["month"]
    variable2 = datetime.strptime(variable1, format_data).date()
    df1.at[index,'month'] = variable2


# COMMAND ----------

df_spark = spark.createDataFrame(df1)

record = "SUPPLIES_STF"
version = "WORKING VERSION"
geography_grain = 'REGION_5'
official = 1
username = "BRENT.MERRICK@HP.COM"
# max_load_date = str(max_info[1])

df_spark_records = df_spark \
    .withColumn("record", lit(record)) \
    .withColumn("version", lit(version)) \
    .withColumn("geography_grain", lit(geography_grain)) \
    .withColumnRenamed("market", "geography") \
    .withColumn("units",col("units").cast("double")) \
    .withColumn('geography', regexp_replace('geography', 'LATIN AMERICA', 'LA')) \
    .withColumn('geography', regexp_replace('geography', 'NORTH AMERICA', 'NA')) \
    .withColumn("official", lit(official).cast("boolean")) \
    .withColumn("username", lit(username)) \
    .withColumnRenamed("month", "cal_date") \
    .withColumn("load_date",current_timestamp())

df_spark_records_reordered = df_spark_records.select("record","geography_grain","geography","base_product_number","cal_date","units","official","load_date","version","username")
df_spark_records_reordered.display()


# COMMAND ----------

# write the updated dataframe to the prod.acct_rates table
write_df_to_redshift(configs, unpivotDF_records, "stage.supplies_stf_landing", "append")

# COMMAND ----------

# copy the file from the landing bucket to an archive bucket, then delete from the landing bucket

import boto3

working_file = latest_file.split("/")[len(latest_file.split("/"))-1]
file_to_delete = 'landing/ibp_supplies_fcst/' + working_file
new_location = 'archive/ibp_supplies_fcst/' + working_file

s3_client = boto3.client('s3')

s3 = boto3.resource('s3')

s3.Object('dataos-core-prod-team-phoenix', new_location).copy_from(CopySource='dataos-core-prod-team-phoenix/' + file_to_delete)
s3.Object('dataos-core-prod-team-phoenix',file_to_delete).delete()

