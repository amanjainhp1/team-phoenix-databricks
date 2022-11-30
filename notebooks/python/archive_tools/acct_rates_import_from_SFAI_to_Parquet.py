# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

acct_rates_query = """

SELECT 
    UPPER([Record]) as record
    ,UPPER([CurrencyCode]) as currencycode
    ,[EffectiveDate] as effectivedate
    ,[AccountingRate] as accountingrate
    ,UPPER([IsoCurrCd]) as isocurrcd
    ,UPPER([CurrCdDn]) as currcddn
    ,[upload_date] as load_date
    ,[version] as version
from ie2_landing.dbo.acct_rates_landing
where 1=1
    and version = '2021.11.02.1'
"""

redshift_acct_rates_records = read_sql_server_to_df(configs) \
    .option("query", acct_rates_query) \
    .load()

#redshift_acct_rates_records.show()

# COMMAND ----------

#version = redshift_acct_rates_records.select('version').distinct()
version = redshift_acct_rates_records.select('version').distinct().head()[0]
#version.show()

# COMMAND ----------

# output dataset to S3 for archival purposes
# s3a://dataos-core-dev-team-phoenix/product/accounting_rates/[version]/

# set the bucket name
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/accounting_rates/" + version

# write the data out in parquet format
write_df_to_s3(redshift_acct_rates_records, s3_output_bucket, "parquet", "overwrite")
