# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# pull the data from the table to backup
redshift_query = """
SELECT *
FROM prod.decay_m13
"""

# execute query from stage table
redshift_records = read_redshift_to_df(configs) \
    .option("query", redshift_query) \
    .load()

# COMMAND ----------

# output dataset to S3 for archival purposes
# s3a://dataos-core-dev-team-phoenix/product/accounting_rates/[version]/

# set the bucket name
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "archive/decay_m13/20230209/"

# write the data out in parquet format
write_df_to_s3(redshift_records, s3_output_bucket, "parquet", "overwrite")
