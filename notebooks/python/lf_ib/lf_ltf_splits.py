# Databricks notebook source
dbutils.widgets.text("redshift_secret_name", "")
dbutils.widgets.dropdown("redshift_region_name", "us-west-2", ["us-west-2", "us-east-2"])
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("stack", "")

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
configs["redshift_url"] = "dataos-core-dev-team-phoenix.dev.hpdataos.com"
configs["redshift_port"] = "5439"
configs["redshift_dbname"] = dbutils.widgets.get("stack")
configs["aws_iam_role"] =  dbutils.widgets.get("aws_iam_role")
configs["redshift_username"] = redshift_username
configs["redshift_password"] = redshift_password
configs["redshift_temp_bucket"] = "s3://dataos-core-{}-team-phoenix/redshift_temp/".format(dbutils.widgets.get("stack"))

# COMMAND ----------

submit_remote_query(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'], "TRUNCATE prod.lf_ltf_splits")

# COMMAND ----------

df_lf_ltf_splits = spark.read.format('csv').options(header='true', inferSchema='true').load('s3://dataos-core-dev-team-phoenix/product/norm_ships/fcst/ltf/Large Format/lf_ltp_splits.csv')


# COMMAND ----------

write_df_to_redshift(configs, df_lf_ltf_splits, "prod.lf_ltf_splits", "append")
