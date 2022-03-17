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
configs["sfai_username"] = sqlserver_secrets["username"]
configs["sfai_password"] = sqlserver_secrets["password"]
configs["sfai_url"] = constants["SFAI_URL"]

# COMMAND ----------

archer_wd3_record = read_sql_server_to_DF(configs) \
  .option("query", "SELECT TOP 1 record FROM archer_prod.dbo.stf_wd3_country_speedlic_vw WHERE record LIKE 'WD3%'") \
  .load()

archer_wd3_record_str = archer_wd3_record.head()[0].replace(" ", "")

redshift_wd3_record = read_redshift_to_df(configs) \
  .option("query", "SELECT distinct record FROM prod.wd3") \
  .load()

if archer_wd3_record.exceptAll(redshift_wd3_record).count() == 0:
  dbutils.notebook.exit(archer_wd3_record_str + " is already contained in Redshift prod.wd3 table")

# COMMAND ----------

# retrieve archer_wd3_records
archer_wd3_records = read_sql_server_to_DF(configs) \
  .option("query", "SELECT * FROM archer_prod.dbo.stf_wd3_country_speedlic_vw WHERE record LIKE 'WD3%'") \
  .load()

# write to parquet file in s3
# s3a://dataos-core-dev-team-phoenix/product/wd3/WD3-2022-03/
s3_wd3_output_bucket = constants["S3_BASE_BUCKET"][dbutils.widgets.get("stack")] + "product/wd3/" + archer_wd3_record_str

write_df_to_s3(archer_wd3_records, s3_wd3_output_bucket, "parquet", "overwrite")

# write to stage.wd3_stage
write_df_to_redshift(configs, )

# append to prod.wd3

# write_df_to_redshift(configs)
