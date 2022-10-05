# Databricks notebook source
dbutils.widgets.text("spreadsheet_startdate", "")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

#Location of Excel sheet
sampleDataFilePath = "s3://dataos-core-dev-team-phoenix/landing/currency_hedge/Oct FY21 Hedge G-L FX Impact HPI 03.10.2022_NoLINKS - Reviewed.xlsx"

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
from datetime import datetime
today = datetime.today()
today_as_string = f"{str(today.year)[2:4]}-{today.month}" if dbutils.widgets.get("spreadsheet_startdate") == "" else dbutils.widgets.get("spreadsheet_startdate")

sample1DF = sample1DF \
  .withColumnRenamed("Product Category", "productcategory") \
  .withColumnRenamed("Currency", "currency") \
  .withColumnRenamed(sample1DF.columns[2], today_as_string) # messed up column will always be 3rd position and be current month and year e.g. Oct 
display(sample1DF)

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
from pyspark.sql.functions import *

# unpivot
unpivotExpr = f"stack({len(months_list)}"

for month in months_list:
  unpivotExpr = unpivotExpr + f", '{month}', `{month}`"
unpivotExpr = unpivotExpr + ") as (month,revenue_currency_hedge)"

unpivotDF = filtered_sample1DF \
  .select("productcategory", "currency", expr(unpivotExpr)) \
  .select("productcategory", "currency", to_date(col("month"),"yy-MM").alias("month"), "revenue_currency_hedge")

unpivotDF.display()

# COMMAND ----------

# To do:
# create version in version table
# load data to redshift database, with version and load_date from version table
# copy data to S3 bucket
# load back to SFAI
