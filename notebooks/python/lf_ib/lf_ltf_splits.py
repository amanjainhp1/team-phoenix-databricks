# Databricks notebook source
dbutils.widgets.text("redshift_secret_name", "")
dbutils.widgets.dropdown("redshift_region_name", "us-west-2", ["us-west-2", "us-east-2"])
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("stack", "")

# COMMAND ----------

import json

with open(dbutils.widgets.get("job_dbfs_path").replace("dbfs:", "/dbfs") + "/configs/constants.json") as json_file:
  constants = json.load(json_file)

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

redshift_secret_name = dbutils.widgets.get("redshift_secret_name")
redshift_region_name = dbutils.widgets.get("redshift_region_name")

redshift_username = secrets_get(redshift_secret_name, redshift_region_name)["username"]
redshift_password = secrets_get(redshift_secret_name, redshift_region_name)["password"]

# COMMAND ----------

configs = {}
configs["redshift_url"] = constants['REDSHIFT_URLS'][dbutils.widgets.get("stack")]
configs["redshift_port"] = constants['REDSHIFT_PORTS'][dbutils.widgets.get("stack")]
configs["redshift_dbname"] = dbutils.widgets.get("stack")
configs["aws_iam_role"] =  dbutils.widgets.get("aws_iam_role")
configs["redshift_username"] = redshift_username
configs["redshift_password"] = redshift_password
configs["redshift_temp_bucket"] = "{}redshift_temp/".format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")])

# COMMAND ----------

submit_remote_query(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'], "TRUNCATE prod.lf_ltf_splits")

# COMMAND ----------

df_lf_ltf_splits = spark.read.format('csv').options(header='true', inferSchema='true').load('{}product/norm_ships/fcst/ltf/Large Format/lf_ltp_splits.csv'.format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")]))

# COMMAND ----------

write_df_to_redshift(configs, df_lf_ltf_splits, "prod.lf_ltf_splits", "append")
