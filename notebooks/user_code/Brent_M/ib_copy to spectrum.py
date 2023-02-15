# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

ib_version = '2023.01.18.2'
ib_version

# COMMAND ----------

ib_spectrum_query = f"""
SELECT
    record,
    cal_date,
    country_alpha2,
    platform_subset,
    customer_engagement,
    measure,
    units,
    official,
    load_date,
    version 
FROM prod.ib
WHERE version = '{ib_version}'
"""

ib_spectrum_records = read_redshift_to_df(configs) \
    .option("query", ib_spectrum_query) \
    .load()

ib_spectrum_records.display()

# COMMAND ----------

s3_ib_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/ib/" + ib_version

write_df_to_s3(ib_spectrum_records, s3_ib_output_bucket, "parquet", "overwrite")
