# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# Download .zip file to S3
import urllib 

urllib.request.urlretrieve("http://polaris-pro-inc.austin.hp.com:8080/T0000047_o.zip","/tmp/T0000047_o.zip") 
dbutils.fs.mv("file:/tmp/T0000047_o.zip", "s3://dataos-core-prod-team-phoenix/landing/Accounting_Rates/")

# COMMAND ----------

# unzip all the .zip files in the folder
import boto3 
import zipfile 
from datetime import * 
from io import BytesIO 
import json 
import re 

dev_client = boto3.client('s3') 
dev_resource=boto3.resource('s3')        

S3_ZIP_FOLDER = 'landing/Accounting_Rates/' 
S3_UNZIPPED_FOLDER = 'landing/Accounting_Rates/unzipped/' 
S3_BUCKET = 'dataos-core-prod-team-phoenix' 
#S3_BUCKET = {s3_output_bucket}
ZIP_FILE='T0000047_o.zip'     

bucket_dev = dev_resource.Bucket(S3_BUCKET) 

zip_obj = dev_resource.Object(bucket_name=S3_BUCKET, key=f"{S3_ZIP_FOLDER}{ZIP_FILE}") 
print("zip_obj=",zip_obj) 

buffer = BytesIO(zip_obj.get()["Body"].read()) 
z = zipfile.ZipFile(buffer) 

# for each file within the zip 
for filename in z.namelist(): 
    file_info = z.getinfo(filename)   

    # Now copy the files to the 'unzipped' S3 folder 
    print(f"Copying file {filename} to {S3_BUCKET}/{S3_UNZIPPED_FOLDER}{filename}") 

    response = dev_client.put_object( 
        Body=z.open(filename).read() ,
        Bucket=S3_BUCKET, 
        Key=f'{S3_UNZIPPED_FOLDER}{filename}' 
    ) 

print(f"Done Unzipping {ZIP_FILE}") 

# COMMAND ----------

# load data from the TXT flat file into a dataframe
from pyspark.sql import SparkSession

# File location and type
file_location = "s3://dataos-core-prod-team-phoenix/landing/Accounting_Rates/unzipped/T0000047_O"
file_type = "txt"

spark = SparkSession.builder.appName("DataFrame").getOrCreate()
  
df = spark.read.text(file_location)

# COMMAND ----------

# split the data out into columns
df = df.withColumnRenamed("value","column0")

df = df.withColumn("CurrencyCode", df.column0.substr(0, 2))
df = df.withColumn("EffectiveDate", df.column0.substr(3, 6))
df = df.withColumn("AccountingRate", df.column0.substr(20, 10).cast("Numeric"))
df = df.withColumn("PricingRate", df.column0.substr(30, 10).cast("Numeric"))
df = df.withColumn("AcctRtMult", df.column0.substr(38, 10).cast("Numeric"))
df = df.withColumn("PrcRtMult", df.column0.substr(47, 10).cast("Numeric"))
df = df.withColumn("RespFiel", df.column0.substr(57, 1))
df = df.withColumn("MinAcctRt", df.column0.substr(58, 3).cast("Numeric"))
df = df.withColumn("MaxAcctRt", df.column0.substr(61, 3).cast("Numeric"))
df = df.withColumn("MinPrcRt", df.column0.substr(64, 3).cast("Numeric"))
df = df.withColumn("MaxPrcRt", df.column0.substr(67, 3).cast("Numeric"))
df = df.withColumn("PrefFmt", df.column0.substr(70, 1))
df = df.withColumn("DecNr", df.column0.substr(71, 1).cast("Numeric"))
df = df.withColumn("IsoCurrCd", df.column0.substr(72, 3))
df = df.withColumn("CurrCdDn", df.column0.substr(75, 20))

#This is supposed to replace NULL values with 0 in a particular column.  This isn't working, but not showing an error.  Circle back on this to figure out why
df.na.fill(value=0, subset=["PrcRtMult"])

df = df.drop('column0')

# COMMAND ----------

# display(df)

# COMMAND ----------

# write the data to a staging database
write_df_to_redshift(configs, df, "stage.acct_rates_stage", "overwrite")

# COMMAND ----------

# pull the data from the stage table and do a little bit of ETL
accounting_rates_query = """
SELECT 
	'ACCT_RATES' as record
    ,currencycode
	,CASE 
		WHEN LEFT(effectivedate,2) = '99' THEN CONCAT('19', CONCAT(LEFT(effectivedate,2), CONCAT('-', CONCAT(RIGHT(effectivedate,2),'-01'))))
		ELSE CONCAT('20', CONCAT(LEFT(effectivedate,2),CONCAT('-',CONCAT(RIGHT(effectivedate,2),'-01'))))
	END AS effectivedate
	,(accountingrate / 10000) as accountingrate
	,isocurrcd
	,currcddn
FROM stage.acct_rates_stage
"""

# execute query from stage table
redshift_accounting_rates_records = read_redshift_to_df(configs) \
    .option("query", accounting_rates_query) \
    .load()

# COMMAND ----------

# add version to the prod.version table and store the max values into variables (max_version, max_load_date)
max_info = call_redshift_addversion_sproc(configs, 'ACCT_RATES', 'Polaris flat file output')

max_version = max_info[0]
max_load_date = str(max_info[1])

# COMMAND ----------

#add load_date and version to the dataframe
from pyspark.sql.functions import trim, col, lit

redshift_accounting_rates_records2 = redshift_accounting_rates_records \
    .withColumn("load_date", lit(max_load_date).cast("timestamp")) \
    .withColumn("version", lit(max_version))

redshift_accounting_rates_records2.withColumn("effectivedate", redshift_accounting_rates_records2.effectivedate.cast("date"))

# COMMAND ----------

#INSERT SOME Q/A here, exit notebook if errors found

# COMMAND ----------

# write the updated dataframe to the prod.acct_rates table
write_df_to_redshift(configs, redshift_accounting_rates_records2, "prod.acct_rates", "overwrite")

# COMMAND ----------

# output dataset to S3 for archival purposes
# s3a://dataos-core-dev-team-phoenix/product/accounting_rates/[version]/

# set the bucket name
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/accounting_rates/" + max_version

# write the data out in parquet format
write_df_to_s3(redshift_accounting_rates_records2, s3_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# write the data back to SFAI for legacy processes

# re-order the dataframe to match the table schema in SFAI
sfai_acct_rates = redshift_accounting_rates_records2.select('currencycode','effectivedate','accountingrate','isocurrcd','currcddn','version','load_date', 'record')


# COMMAND ----------

# Rename the columns to match SFAI
sfai_acct_rates = sfai_acct_rates.withColumnRenamed("currencycode","CurrencyCode")
sfai_acct_rates = sfai_acct_rates.withColumnRenamed("effectivedate","EffectiveDate")
sfai_acct_rates = sfai_acct_rates.withColumnRenamed("accountingrate","AccountingRate")
sfai_acct_rates = sfai_acct_rates.withColumnRenamed("isocurrcd","IsoCurrCd")
sfai_acct_rates = sfai_acct_rates.withColumnRenamed("currcddn","CurrCdDn")
sfai_acct_rates = sfai_acct_rates.withColumnRenamed("load_date","upload_date")
sfai_acct_rates = sfai_acct_rates.withColumnRenamed("record","Record")

#display(sfai_acct_rates)

# COMMAND ----------

# Write to SFAI
write_df_to_sqlserver(configs, sfai_acct_rates, "IE2_Prod.dbo.acct_rates", "overwrite")

# COMMAND ----------

# This is used to build a variable to write out to the S3 archive folder
from datetime import datetime
date = datetime.today()
datestamp = date.strftime("%Y%m%d")

# COMMAND ----------

# move text file from landing S3 bucket to an archive bucket, then remove the source files

import boto3
s3 = boto3.resource('s3')
s3.Object('dataos-core-prod-team-phoenix','archive/acct_rates/' + datestamp + '/T0000047_O').copy_from(CopySource='dataos-core-prod-team-phoenix/landing/Accounting_Rates/unzipped/T0000047_O')
s3.Object('dataos-core-prod-team-phoenix','landing/Accounting_Rates/unzipped/T0000047_O').delete()
s3.Object('dataos-core-prod-team-phoenix','landing/Accounting_Rates/T0000047_o.zip').delete()
