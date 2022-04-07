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

ib_archive_query = """

SELECT
    record
    ,cal_date
    ,country as country_alpha2
    ,platform_subset
    ,customer_engagement
    ,measure
    ,units
    ,official
    ,load_date
    ,version
  FROM archive.dbo.ib_archive
  WHERE version = '2020.11.03.1'
  
"""

redshift_ib_archive_records = read_sql_server_to_df(configs) \
    .option("query", ib_archive_query) \
    .load()

# redshift_ib_archive_records.show()



# COMMAND ----------

# version = redshift_ib_archive_records.select('version').distinct()
version = redshift_ib_archive_records.select('version').distinct().head()[0]


# COMMAND ----------

# write to parquet file in s3

# s3a://dataos-core-dev-team-phoenix/product/flash/Flash-2022-03/
s3_flash_output_bucket = constants["S3_BASE_BUCKET"][dbutils.widgets.get("stack")] + "spectrum/ib/" + version

write_df_to_s3(redshift_ib_archive_records, s3_flash_output_bucket, "parquet", "overwrite")


