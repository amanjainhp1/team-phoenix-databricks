# Databricks notebook source
# 2/23/2023 - Brent Merrick
# load data from the Forecast Template Nov22.xlsm, Merge1 sheet, make a copy and upload to the s3://dataos-core-prod-team-phoenix/landing/ibp_supplies_fcst/ bucket.


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

#Location of Excel sheet
#sampleDataFilePath = "s3://dataos-core-dev-team-phoenix/landing/currency_hedge/Oct FY21 Hedge G-L FX Impact HPI 03.10.2022_NoLINKS - Reviewed.xlsx"
sampleDataFilePath = latest_file

#flags required for reading the excel
isHeaderOn = "true"
isInferSchemaOn = "false"

#sheetname of excel file
sample1Address = "'Merge1'!A1"

#read excelfile
sample1DF = spark.read.format("com.crealytics.spark.excel") \
  .option("header", isHeaderOn) \
  .option("inferSchema", isInferSchemaOn) \
  .option("treatEmptyValuesAsNulls", "false") \
  .option("dataAddress", sample1Address) \
  .load(sampleDataFilePath)

sample1DF.cache()

# COMMAND ----------

sample1DF.show()

# COMMAND ----------

#from pyspark.sql import functions as F
from pyspark.sql.functions import *

df2 = sample1DF.withColumn("load_date", F.lit(datetime.now().strftime("%Y-%m-%d %H:%m:%S.%s")))

df2.display()

# COMMAND ----------

# write the updated dataframe to the stage.ibp_supplies_fcst_stage table
write_df_to_redshift(configs, df2, "stage.ibp_supplies_fcst_stage", "overwrite")

# COMMAND ----------

# add code to split region_5 to country level here

# COMMAND ----------

# copy new code to prod (prod_df_records)


# COMMAND ----------

# Add record to version table for 'CURRENCY_HEDGE'
max_info = call_redshift_addversion_sproc(configs, 'IBP_SUPPLIES_FCST', 'IBP Excel File')
max_version = max_info[0]
max_load_date = str(max_info[1])

# COMMAND ----------

# output dataset to S3 for archival purposes
# s3a://dataos-core-dev-team-phoenix/product/currency_hedge/[version]/

# set the bucket name
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/ibp_supplies_fcst/" + max_version

# write the data out in parquet format
write_df_to_s3(prod_df_records, s3_output_bucket, "parquet", "overwrite")


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


# COMMAND ----------


