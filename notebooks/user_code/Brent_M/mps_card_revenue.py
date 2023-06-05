# Databricks notebook source
# 05/10/2023 - Brent Merrick
# loading initial spreadsheet for mps_card_revenue

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

paths = get_dir_content('s3://dataos-core-prod-team-phoenix/landing/mps_card_revenue/')
#[print(p) for p in paths]
for p in paths:
  latest_file = p


# COMMAND ----------

#Location of Excel sheet
sampleDataFilePath = latest_file

#flags required for reading the excel
isHeaderOn = "true"
isInferSchemaOn = "false"

#sheetname of excel file
sample1Address = "'Sheet1'!A1"

#read excelfile
sample1DF = spark.read.format("com.crealytics.spark.excel") \
  .option("header", isHeaderOn) \
  .option("inferSchema", isInferSchemaOn) \
  .option("treatEmptyValuesAsNulls", "false") \
  .option("dataAddress", sample1Address) \
  .load(sampleDataFilePath)

# COMMAND ----------

sample1DF.display()

# COMMAND ----------

# Add record to version table for 'CURRENCY_HEDGE'
max_info = call_redshift_addversion_sproc(configs, 'MPS_CARD_REVENUE', 'Excel File')
max_version = max_info[0]
max_load_date = str(max_info[1])

# COMMAND ----------

# Add load_date and verison to Dataframe
from pyspark.sql.functions import *

mps_card_records = sample1DF \
    .withColumn("load_date", lit(max_load_date).cast("timestamp")) \
    .withColumn("version", lit(max_version))

# .withColumn("$ Amount", mps_card_records["$ Amount"].cast('decimal(12,4)')) \

mps_card_records.display()

# COMMAND ----------

# write the updated dataframe to the prod.acct_rates table
write_df_to_redshift(configs, mps_card_records, "stage.mps_card_revenue_stage", "overwrite")

# COMMAND ----------

# ETL and copy to prod table

# COMMAND ----------

# output dataset to S3 for archival purposes
# s3a://dataos-core-dev-team-phoenix/product/mps_card_revenue/[version]/

# set the bucket name
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/mps_card_revenue/" + max_version

# write the data out in parquet format
write_df_to_s3(mps_card_records, s3_output_bucket, "parquet", "overwrite")


# COMMAND ----------

# copy the file from the landing bucket to an archive bucket, then delete from the landing bucket

import boto3

working_file = latest_file.split("/")[len(latest_file.split("/"))-1]
file_to_delete = 'landing/mps_card_revenue/' + working_file
new_location = 'archive/mps_card_revenue/' + working_file

s3_client = boto3.client('s3')

s3 = boto3.resource('s3')

s3.Object('dataos-core-prod-team-phoenix', new_location).copy_from(CopySource='dataos-core-prod-team-phoenix/' + file_to_delete)
s3.Object('dataos-core-prod-team-phoenix',file_to_delete).delete()


# COMMAND ----------

# pull the data from the stage table and do a little bit of ETL
mps_card_query = """
SELECT
    c.date as cal_date,
    coalesce(left(a."country code",2), b.country_alpha2) as country_alpha2,
    "prod nb" as product_number,
    CAST("lc amount" as decimal(18,2)) as lc_amount,
    CAST("$ amount" as decimal(18,2)) as usd_amount,
    currency,
    "business model" as rtm,
    a."billing model" as billing_model,
    left("business area",2) as pl,
    a.load_date,
    a.version
FROM stage.mps_card_revenue_stage a
    LEFT JOIN mdm.iso_cc_rollup_xref b on a.country=b.country_level_1
        AND b.country_scenario = 'MPS_CARD'
    LEFT JOIN mdm.calendar c on a."billing month" = left(c.date_key,6)
        AND c.day_of_month = 1
WHERE 1=1
    AND "$ amount" IS NOT NULL
ORDER BY 2,3,1,5
"""

# execute query from stage table
mps_card_records = read_redshift_to_df(configs) \
    .option("query", mps_card_query) \
    .load()

# COMMAND ----------

# write the data to fin_prod database
write_df_to_redshift(configs, mps_card_records, "fin_prod.mps_card_revenue", "overwrite")
