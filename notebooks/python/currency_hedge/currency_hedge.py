# Databricks notebook source
# 10/6/2022 - Brent Merrick
# You should receive an email from Mokry, Mateusz <mokry.mateusz@hp.com>, with the currency hedge Excel workbook
# Before running this notebook, copy the "Print_Supplies_FX gain_loss_Dec'23_WD2.xlsb" file to *.xlsx to convert from Binary to xlsx then move
# The file listed above will not be that name, EXACTLY, but will be very close

#  Expand all rows and columns in the spreadsheet
#  Change the date format in column A to yyyy-mm-dd
#  copy/paste values from column A
#  change format of all the numbers from $xxx to number (xxx,xxx.00 and -xx,xxx.00)
# save to the /landing/currency_hedge/ bucket.



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
#sampleDataFilePath = "s3://dataos-core-dev-team-phoenix/landing/currency_hedge/Print_Supplies_FX gain_loss_Dec'23_WD2.xlsx"
sampleDataFilePath = latest_file

#flags required for reading the excel
isHeaderOn = "true"
isInferSchemaOn = "false"

#sheetname of excel file
sample1Address = "'Gain_Loss_Summary'!A4"

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
    .withColumnRenamed("Calendar month", "month") \
    .withColumnRenamed("Currency", "currency")

# COMMAND ----------

filtered_sample1DF = sample1DF.drop("Fiscal month", "Market", "Country", "Segment Code","Total","Segment used in load")

# COMMAND ----------

col_list = filtered_sample1DF.columns
filtered_cols = [x for x in col_list]
filtered_sample2DF = filtered_sample1DF.select(filtered_cols)

# filtered_sample2DF.display()

# COMMAND ----------

# remove first two columns from column list
profit_center_list = list(filtered_sample2DF.columns[2:len(filtered_sample2DF.columns)])
#print(profit_center_list)

# COMMAND ----------

# Unpivot the data

# unpivot
unpivotExpr = f"stack({len(profit_center_list)}"

for profit_center in profit_center_list:
  unpivotExpr = unpivotExpr + f", '{profit_center}', `{profit_center}`"

unpivotExpr = unpivotExpr + ") as (profit_center,revenue_currency_hedge)"

unpivotDF = filtered_sample1DF \
    .select("month", "currency", expr(unpivotExpr)) \
    .select("month", "currency", "profit_center", "revenue_currency_hedge")

unpivotDF_filtered_1 = unpivotDF.filter(unpivotDF.profit_center.startswith('P'))
unpivotDF_filtered_2 = unpivotDF_filtered_1.where("currency <> 'null'")
unpivotDF_filtered_3 = unpivotDF_filtered_2.where("profit_center not in ('PWP Supplies','PWI Packaging Supplies')")

unpivotDF_filtered_4 = unpivotDF_filtered_3.select("profit_center", "currency", "month", "revenue_currency_hedge")


# COMMAND ----------

unpivotDF_filtered_4.display()

# COMMAND ----------

# Add record to version table for 'CURRENCY_HEDGE'
max_info = call_redshift_addversion_sproc(configs, 'CURRENCY_HEDGE', 'Excel File')
max_version = max_info[0]
max_load_date = str(max_info[1])

# COMMAND ----------


unpivotDF_records = unpivotDF_filtered_4 \
    .withColumn("load_date", lit(max_load_date).cast("timestamp")) \
    .withColumn("version", lit(max_version)) \
    .withColumn("month",col("month").cast("DATE")) \
    .withColumn('revenue_currency_hedge', regexp_replace('revenue_currency_hedge', ',', '')) \
    .withColumn("revenue_currency_hedge",col("revenue_currency_hedge").cast("double"))

# COMMAND ----------

unpivotDF_records.display()

# COMMAND ----------


write_df_to_redshift(configs, unpivotDF_records, "stage.currency_hedge_stage", "append")

# COMMAND ----------

# pull the data from the stage table and promote to prod
currency_hedge_stage_query = """
select
    profit_center,
    currency,
    CAST(month as date) as "month",
    revenue_currency_hedge,
    load_date,
    version
from stage.currency_hedge_stage
where 1=1
    and CAST(month as date) >= add_months(date_trunc('month', CURRENT_DATE), 0)
    and revenue_currency_hedge <> 0
order by 3
"""

# execute query from stage table
currency_hedge_stage_records = read_redshift_to_df(configs) \
    .option("query", currency_hedge_stage_query) \
    .load()



# COMMAND ----------

currency_hedge_stage_records.display()

# COMMAND ----------

write_df_to_redshift(configs=configs, df=currency_hedge_stage_records, destination="fin_prod.currency_hedge", mode="append")

# COMMAND ----------

# output dataset to S3 for archival purposes
# s3a://dataos-core-dev-team-phoenix/product/currency_hedge/[version]/

# set the bucket name
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/currency_hedge/" + max_version

# write the data out in parquet format
write_df_to_s3(unpivotDF_records, s3_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# copy the file from the landing bucket to an archive bucket, then delete from the landing bucket

working_file = latest_file.split("/")[len(latest_file.split("/"))-1]
file_to_delete = 'landing/currency_hedge/' + working_file
new_location = 'archive/currency_hedge/' + working_file

s3 = boto3.resource('s3')
s3.Object('dataos-core-prod-team-phoenix', new_location).copy_from(CopySource=f'dataos-core-{stack}-team-phoenix/' + file_to_delete)
s3.Object('dataos-core-prod-team-phoenix',file_to_delete).delete()
