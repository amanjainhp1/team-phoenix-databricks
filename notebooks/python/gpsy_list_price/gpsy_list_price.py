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

# %sh
# aws s3 ls s3://enrich-data-lake-restricted-prod/gpsy/

# COMMAND ----------

# load new AMS files
ams_s3 = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", ",") \
.load(f"s3a://enrich-data-lake-restricted-prod/gpsy/insight_product_price_ams*.bz2")

# ams_s3.display()

# COMMAND ----------

# load new APJ files
apj_s3 = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", ",") \
.load(f"s3a://enrich-data-lake-restricted-prod/gpsy/insight_product_price_apj*.bz2")

# COMMAND ----------

# load new EU files
eu_s3 = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", ",") \
.load(f"s3a://enrich-data-lake-restricted-prod/gpsy/insight_product_price_eu*.bz2")

# COMMAND ----------

#from pyspark.sql import functions as F
from pyspark.sql.functions import trim, col

# COMMAND ----------

# write to stage.gpsy_list_price_stage
ams_s3 = ams_s3.select([trim(col(c)).alias(c.strip().lower().replace(" ", "_")) for c in ams_s3.columns])
write_df_to_redshift(configs, ams_s3, "stage.gpsy_list_price_stage", "overwrite")

# COMMAND ----------

apj_s3 = apj_s3.select([trim(col(c)).alias(c.strip().lower().replace(" ", "_")) for c in apj_s3.columns])
write_df_to_redshift(configs, apj_s3, "stage.gpsy_list_price_stage", "append")

# COMMAND ----------

eu_s3 = eu_s3.select([trim(col(c)).alias(c.strip().lower().replace(" ", "_")) for c in eu_s3.columns])
write_df_to_redshift(configs, eu_s3, "stage.gpsy_list_price_stage", "append")

# COMMAND ----------

#  --add record to version table for 'LIST_PRICE'

sql_query = f"""CALL prod.addversion_sproc('LIST_PRICE', 'GPSY');"""

submit_remote_query(dbutils.widgets.get("stack"), configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], sql_query)


# COMMAND ----------

version_query = f"""
SELECT
      record
	, MAX(version) AS version
	, MAX(load_date) as load_date
FROM prod.version
WHERE record = 'LIST_PRICE'
GROUP BY record
"""

version = read_redshift_to_df(configs) \
  .option("query", version_query) \
  .load()

max_version = version.select("version").distinct().head()[0]
max_load_date = version.select("load_date").distinct().head()[0]



# COMMAND ----------

redshift_gpsy_list_price_records = read_redshift_to_df(configs) \
  .option("query", "select * from stage.gpsy_list_price_stage") \
  .load()

# COMMAND ----------

from pyspark.sql.functions import col,lit

redshift_gpsy_prod_list_price_records = read_redshift_to_df(configs) \
  .option("query", "select product_number, country_code, currency_code, price_term_code, cast(price_start_effective_date as date) as price_start_effective_date, qbl_sequence_number, list_price, product_line from stage.gpsy_list_price_stage") \
  .load()

redshift_gpsy_prod_list_price_records = redshift_gpsy_prod_list_price_records \
  .withColumn("qbl_sequence_number", col("qbl_sequence_number").cast("integer")) \
  .withColumn("list_price", col("list_price").cast("double")) \
  .withColumn("load_date", lit(max_load_date).cast("timestamp")) \
  .withColumn("version", lit(max_version)) \

write_df_to_redshift(configs, redshift_gpsy_prod_list_price_records, "prod.list_price_gpsy", "overwrite")
# redshift_gpsy_prod_list_price_records.show()

# COMMAND ----------

# output dataset to S3 for archival purposes
# write to parquet file in s3

# s3a://dataos-core-dev-team-phoenix/product/list_price_gpsy/
s3_flash_output_bucket = constants["S3_BASE_BUCKET"][dbutils.widgets.get("stack")] + "product/list_price_gpsy/" + max_version

write_df_to_s3(redshift_gpsy_prod_list_price_records, s3_flash_output_bucket, "parquet", "overwrite")



# COMMAND ----------

# write PROD dataset to SFAI

