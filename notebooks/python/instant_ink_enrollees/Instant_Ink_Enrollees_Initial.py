# Databricks notebook source
dbutils.widgets.text("REDSHIFT_SECRET_NAME", "")
dbutils.widgets.dropdown("REDSHIFT_REGION_NAME", "us-west-2", ["us-west-2", "us-east-2"])

# COMMAND ----------

import boto3
import io
import yaml
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# set common parameters
sfai_hostname = "sfai.corp.hpicloud.net"
sfai_port = 1433
sfai_url = f"jdbc:sqlserver://{sfai_hostname}:{sfai_port};database="
sfai_username = "databricks_user" # placeholder
sfai_password = "databricksdemo123" # placeholder

# COMMAND ----------

redshift_secret_name = dbutils.widgets.get("REDSHIFT_SECRET_NAME")
redshift_region_name = dbutils.widgets.get("REDSHIFT_REGION_NAME")

redshift_username = secrets_get(redshift_secret_name, redshift_region_name)["username"]
redshift_password = secrets_get(redshift_secret_name, redshift_region_name)["password"]

# COMMAND ----------

# set table specific parameters
database = "IE2_Prod"
schema = "dbo"
table = "instant_ink_enrollees"

# extract data from SFAI
iink_df = spark.read \
  .format("jdbc") \
  .option("url", sfai_url + database) \
  .option("dbTable", f"{schema}.{table}") \
  .option("user", sfai_username) \
  .option("password", sfai_password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()

# COMMAND ----------

iink_df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
  .option("dbtable", "stage.instant_ink_enrollees") \
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
  .option("user", redshift_username) \
  .option("password", redshift_password) \
  .mode("append") \
  .save()

