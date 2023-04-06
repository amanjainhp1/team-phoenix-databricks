# Databricks notebook source
# 3/23/2023 - Brent Merrick
# load data from the "IBP Forecast Template.xlsx", "Fcst Data" sheet, make a copy and upload to the s3://dataos-core-prod-team-phoenix/landing/ibp_supplies_fcst/ bucket.


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
sampleDataFilePath = latest_file

#flags required for reading the excel
isHeaderOn = "true"
isInferSchemaOn = "false"

#sheetname of excel file
sample1Address = "'Fcst Data'!I4"

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

df = sample1DF \
  .withColumnRenamed("Market", "market") \
  .withColumnRenamed("Primary Base Product", "primary_base_product")

# df.display()

# COMMAND ----------

# create a list of the column names and filter out columns that won't be part of the unpivot process
col_list = df.columns
filtered_cols = [x for x in col_list if not x.startswith("_c")] #or what ever they start with
filtered_df = df.select(filtered_cols)

# COMMAND ----------

# remove first two columns from column list
months_list = list(filtered_df.columns[2:len(filtered_df.columns)])

# COMMAND ----------

# Unpivot the data
from pyspark.sql.functions import *

# unpivot
unpivotExpr = f"stack({len(months_list)}"

for month in months_list:
  unpivotExpr = unpivotExpr + f", '{month}', `{month}`"
unpivotExpr = unpivotExpr + ") as (month,units)"

unpivotDF = filtered_df \
  .select("market", "primary_base_product", expr(unpivotExpr)) \
  .select("market", "primary_base_product", "month", "units")

unpivotDF.display()

# COMMAND ----------


import pandas as pd
from datetime import datetime

df2 = unpivotDF.toPandas()

format_data = "%y-%b"
 
for index, row in df2.iterrows():
    variable1 = row["month"]
    variable2 = datetime.strptime(variable1, format_data).date()
    df2.at[index,'month'] = variable2
    
df2

# COMMAND ----------

df_spark = spark.createDataFrame(df2)

df_spark_records = df_spark \
    .withColumnRenamed("month", "cal_date") \
    .withColumn("units",col("units").cast("double")) \
    .withColumn("load_date",current_timestamp())

df_spark_records_reordered = df_spark_records.select("market","primary_base_product","load_date","cal_date","units")
df_spark_records_reordered.display()

# COMMAND ----------

# ibp_fcst_stage
# write the updated dataframe to the stage.ibp_fcst_stage table
write_df_to_redshift(configs, df_spark_records_reordered, "stage.ibp_fcst_stage", "overwrite")

# COMMAND ----------

# load to stf_landing table
stf_landing_query = """

with mapping as
(
    SELECT 'CENTRAL AND EASTERN EUROPE' as market, 'EU' as region_5
    UNION ALL
    SELECT 'GREATER ASIA' as market, 'AP' as region_5
    UNION ALL
    SELECT 'GREATER CHINA' as market, 'AP' as region_5
    UNION ALL
    SELECT 'INDIA B SL' as market, 'AP' as region_5
    UNION ALL
    SELECT 'LATIN AMERICA' as market, 'LA' as region_5
    UNION ALL
    SELECT 'NORTH AMERICA' as market, 'NA' as region_5
    UNION ALL
    SELECT 'NORTHWEST EUROPE' as market, 'EU' as region_5
    UNION ALL
    SELECT 'SOUTHERN EUROPE MIDDLE EAST AND AFRICA' as market, 'EU' as region_5
),

first_run as
(
    SELECT
        b.region_5 as geography,
        a.primary_base_product as base_product_number,
        a.cal_date,
        sum(a.units) as units,
        a.load_date,
        c.pl as pl_code
    FROM stage.ibp_fcst_stage a
        left join mapping b on a.market = b.market
        left join mdm.rdma c on a.primary_base_product=c.base_prod_number
    GROUP BY
        b.region_5, a.primary_base_product, a.cal_date, a.load_date, c.pl
)

SELECT
    'SUPPLIES_STF' as record,
    'REGION_5' as geography_grain,
    geography,
    base_product_number,
    cal_date,
    units,
    true as official,
    fr.load_date,
    'WORKING VERSION' as version,
    'BRENT.MERRICK@HP.COM' as username,
    pl_code
FROM first_run fr left join mdm.product_line_xref xref on fr.pl_code=xref.pl
WHERE 1=1
    AND NOT (xref.technology = 'LASER' AND fr.geography IN ('NA','LA'))
ORDER BY 1,2

"""


stf_landing_records = read_redshift_to_df(configs) \
    .option("query", stf_landing_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, stf_landing_records, "stage.supplies_stf_landing", "append")

# COMMAND ----------

# load over-rides from Natalia Navarro (email), using Shiny tool to load NA and LA data for LASER.
