# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# load data from the TXT flat file into a dataframe
from pyspark.sql import SparkSession

# File location and type
file_location = "/FileStore/tables/T0000047_O.txt"
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


df = df.drop('column0')

# COMMAND ----------


#display(df)
#display(df.filter((df.CurrencyCode=='AD') & (df.EffectiveDate==2006)))

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

# select CONCAT('Nested', CONCAT(' CONCAT', ' example!'));

redshift_accounting_rates_records = read_redshift_to_df(configs) \
    .option("query", accounting_rates_query) \
    .load()

# COMMAND ----------

#display(redshift_accounting_rates_records)
#display(redshift_accounting_rates_records.filter(redshift_accounting_rates_records.date<'2015-09-01'))

# COMMAND ----------

# add version and load_date to dataframe
max_info = call_redshift_addversion_sproc(configs, 'ACCT_RATES', 'Polaris flat file output')

max_version = max_info[0]
max_load_date = str(max_info[1])

# COMMAND ----------

from pyspark.sql.functions import trim, col, lit

redshift_accounting_rates_records2 = redshift_accounting_rates_records \
    .withColumn("load_date", lit(max_load_date).cast("timestamp")) \
    .withColumn("version", lit(max_version))

redshift_accounting_rates_records2.withColumn("effectivedate", redshift_accounting_rates_records2.effectivedate.cast("date"))

# COMMAND ----------

#display(redshift_accounting_rates_records2)

# COMMAND ----------

write_df_to_redshift(configs, redshift_accounting_rates_records2, "prod.acct_rates", "overwrite")

# COMMAND ----------

# output dataset to S3 for archival purposes
# write to parquet file in s3

# s3a://dataos-core-dev-team-phoenix/product/list_price_gpsy/

s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/accounting_rates/" + max_version

write_df_to_s3(redshift_accounting_rates_records2, s3_output_bucket, "parquet", "overwrite")
