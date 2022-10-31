# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------
# for interactive sessions, create job_type which should take either "flash" or "wd3" value
dbutils.widgets.text("job_type", "")

# COMMAND ----------
job_type = dbutils.widgets.get("job_type").lower()

# COMMAND ----------

archer_record = read_sql_server_to_df(configs) \
    .option("query", f"SELECT TOP 1 REPLACE(record, ' ', '') AS record FROM archer_prod.dbo.stf_{job_type}_country_speedlic_vw WHERE record LIKE '{job_type}%'") \
    .load()

archer_record_str = archer_record.head()[0].replace(" ", "").upper()

redshift_record = read_redshift_to_df(configs) \
    .option("query", f"""SELECT DISTINCT source_name AS record FROM prod.flash_wd3 WHERE source_name LIKE ('{archer_record_str}')""") \
    .load() \
    .count()

if redshift_record > 0:
    raise Exception(archer_record_str + " is already contained in Redshift prod.flash_wd3 table")

# COMMAND ----------

# retrieve archer_records
archer_records = read_sql_server_to_df(configs) \
    .option("query", f"SELECT * FROM archer_prod.dbo.stf_{job_type}_country_speedlic_vw WHERE record LIKE '{job_type}%'") \
    .load()

# write to stage.flash_stage or stage.wd3_stage
write_df_to_redshift(configs, archer_records, f"stage.{job_type}_stage", "overwrite")

# COMMAND ----------

#  --add record to version table for 'FLASH'or 'WD3

max_version_info = call_redshift_addversion_sproc(configs, f'{job_type}'.upper(), archer_record_str)

max_version = max_version_info[0]
max_load_date = max_version_info[1]

# COMMAND ----------

from pyspark.sql.functions import col,lit

archer_records = archer_records \
    .withColumn("source_name", lit(archer_record_str)) \
    .withColumn("record", lit(f"{job_type}".upper())) \
    .withColumn("load_date", lit(max_load_date)) \
    .withColumn("version", lit(max_version)) \
    .withColumnRenamed('geo', 'country_alpha2') \
    .withColumnRenamed('base_prod_number', 'base_product_number') \
    .withColumnRenamed('date', 'cal_date') \
    .select("record", "source_name", "country_alpha2", "base_product_number", "cal_date", "units", "load_date", "version")

# COMMAND ----------

# write to parquet file in s3
# e.g. s3a://dataos-core-dev-team-phoenix/product/flash/Flash-2022-03/
s3_output_bucket = f'constants["S3_BASE_BUCKET"][stack]product/{job_type}/{archer_record_str}'

write_df_to_s3(archer_records, s3_output_bucket, "parquet", "overwrite")

# append to prod.flash_wd3 table
write_df_to_redshift(configs, archer_records, "prod.flash_wd3", "append")
