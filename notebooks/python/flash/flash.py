# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

archer_flash_record = read_sql_server_to_df(configs) \
    .option("query", "SELECT TOP 1 REPLACE(record, ' ', '') AS record FROM archer_prod.dbo.stf_flash_country_speedlic_vw WHERE record LIKE 'Flash%'") \
    .load()

archer_flash_record_str = archer_flash_record.head()[0].replace(" ", "").upper()

redshift_flash_record = read_redshift_to_df(configs) \
    .option("query", f"""SELECT DISTINCT source_name AS record FROM prod.flash_wd3 WHERE source_name LIKE ('{archer_flash_record_str}')""") \
    .load() \
    .count()

if redshift_flash_record > 0:
    dbutils.notebook.exit(archer_flash_record_str + " is already contained in Redshift prod.flash_wd3 table")

# COMMAND ----------

# retrieve archer_flash_records
archer_flash_records = read_sql_server_to_df(configs) \
    .option("query", "SELECT * FROM archer_prod.dbo.stf_flash_country_speedlic_vw WHERE record LIKE 'Flash%'") \
    .load()

# write to stage.flash_stage
write_df_to_redshift(configs, archer_flash_records, "stage.flash_stage", "overwrite")

# COMMAND ----------

#  --add record to version table for 'FLASH'

max_version_info = call_redshift_addversion_sproc(configs, 'FLASH', archer_flash_record_str)

max_version = max_version_info[0]
max_load_date = max_version_info[1]

# COMMAND ----------

from pyspark.sql.functions import col,lit

archer_flash_records = archer_flash_records \
    .withColumn("source_name", lit(archer_flash_record_str)) \
    .withColumn("record", lit("FLASH")) \
    .withColumn("load_date", lit(max_load_date)) \
    .withColumn("version", lit(max_version)) \
    .withColumnRenamed('geo','country_alpha2') \
    .withColumnRenamed('base_prod_number','base_product_number') \
    .withColumnRenamed('date', 'cal_date') \
    .select("record", "source_name", "country_alpha2", "base_product_number", "cal_date", "units", "load_date", "version")

# COMMAND ----------

# write to parquet file in s3

# s3a://dataos-core-dev-team-phoenix/product/flash/Flash-2022-03/
s3_flash_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/flash/" + archer_flash_record_str

write_df_to_s3(archer_flash_records, s3_flash_output_bucket, "parquet", "overwrite")

# append to prod.flash
write_df_to_redshift(configs, archer_flash_records, "prod.flash_wd3", "append")
