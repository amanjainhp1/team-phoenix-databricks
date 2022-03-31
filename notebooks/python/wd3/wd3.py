# Databricks notebook source
dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("stack", "")
dbutils.widgets.text("job_dbfs_path", "")

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

import json

with open(dbutils.widgets.get("job_dbfs_path").replace("dbfs:", "/dbfs") + "/configs/constants.json") as json_file:
  constants = json.load(json_file)

# COMMAND ----------

# retrieve secrets based on incoming/inputted secrets name - variables will be accessible across languages

redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")

sqlserver_secrets = secrets_get(dbutils.widgets.get("sqlserver_secrets_name"), "us-west-2")

# COMMAND ----------

configs = {}

configs["redshift_username"] = redshift_secrets["username"]
configs["redshift_password"] = redshift_secrets["password"]
configs["redshift_url"] = constants["REDSHIFT_URLS"][dbutils.widgets.get("stack")]
configs["redshift_port"] = constants["REDSHIFT_PORTS"][dbutils.widgets.get("stack")]
configs["redshift_dbname"] = constants["REDSHIFT_DATABASE"][dbutils.widgets.get("stack")]
configs["aws_iam_role"] = dbutils.widgets.get("aws_iam_role")
configs["redshift_temp_bucket"] =  "{}redshift_temp/".format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")])
configs["redshift_dev_group"] = constants["REDSHIFT_DEV_GROUP"][dbutils.widgets.get("stack")]

configs["sfai_username"] = sqlserver_secrets["username"]
configs["sfai_password"] = sqlserver_secrets["password"]
configs["sfai_url"] = constants["SFAI_URL"]

# COMMAND ----------

archer_wd3_record = read_sql_server_to_df(configs) \
  .option("query", "SELECT TOP 1 REPLACE(record, ' ', '') AS record FROM archer_prod.dbo.stf_wd3_country_speedlic_vw WHERE record LIKE 'WD3%'") \
  .load()

archer_wd3_record_str = archer_wd3_record.head()[0].replace(" ", "")

redshift_wd3_record = read_redshift_to_df(configs) \
  .option("query", "SELECT distinct source_name AS record FROM prod.flash_wd3") \
  .load()

if archer_wd3_record.exceptAll(redshift_wd3_record).count() == 0:
  dbutils.notebook.exit(archer_wd3_record_str + " is already contained in Redshift prod.flash_wd3 table")

# COMMAND ----------

# retrieve archer_wd3_records
archer_wd3_records = read_sql_server_to_df(configs) \
  .option("query", "SELECT * FROM archer_prod.dbo.stf_wd3_country_speedlic_vw WHERE record LIKE 'WD3%'") \
  .load()

# write to stage.wd3_stage
write_df_to_redshift(configs, archer_wd3_records, "stage.wd3_stage", "overwrite")

# COMMAND ----------

#  --add record to version table for 'wd3'

sql_query = f"""CALL prod.addversion_sproc('WD3', '{archer_wd3_record_str}');"""

submit_remote_query(dbutils.widgets.get("stack"), configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], sql_query)

# COMMAND ----------

version_query = f"""
SELECT
      record
	, MAX(version) AS version
	, MAX(load_date) as load_date
FROM prod.version
WHERE record = 'WD3' AND source_name = '{archer_wd3_record_str}'
GROUP BY record
"""

version = read_redshift_to_df(configs) \
  .option("query", version_query) \
  .load()

max_version = version.select("version").distinct().head()[0]
max_load_date = version.select("load_date").distinct().head()[0]

# COMMAND ----------

from pyspark.sql.functions import col,lit

archer_wd3_records = archer_wd3_records \
  .withColumn("source_name", lit(archer_wd3_record_str)) \
  .withColumn("record", lit("WD3")) \
  .withColumn("load_date", lit(max_load_date)) \
  .withColumn("version", lit(max_version)) \
  .withColumnRenamed('geo','country_alpha2') \
  .withColumnRenamed('base_prod_number','base_product_number') \
  .withColumnRenamed('date', 'cal_date') \
  .select("record", "source_name", "country_alpha2", "base_product_number", "cal_date", "units", "load_date", "version")

# COMMAND ----------

# write to parquet file in s3

# s3a://dataos-core-dev-team-phoenix/product/wd3/WD3-2022-03/
s3_wd3_output_bucket = constants["S3_BASE_BUCKET"][dbutils.widgets.get("stack")] + "product/wd3/" + archer_wd3_record_str

write_df_to_s3(archer_wd3_records, s3_wd3_output_bucket, "parquet", "overwrite")

# append to prod.wd3
write_df_to_redshift(configs, archer_wd3_records, "prod.flash_wd3", "append")

# COMMAND ----------


