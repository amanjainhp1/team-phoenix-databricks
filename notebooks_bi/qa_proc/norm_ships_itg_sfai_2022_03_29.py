# Databricks notebook source
# MAGIC %md
# MAGIC # Norm Ships - March 2022 Initial QC For New Redshift Process
# MAGIC 
# MAGIC Mark Middendorf, MS - Master Data Engineer
# MAGIC 
# MAGIC Phoenix | Normalized Shipments
# MAGIC 
# MAGIC **Notebook sections:**
# MAGIC  + review high-level v2v comparisons
# MAGIC  + review high-level technology comparisons (Laser, Ink/PWA)
# MAGIC  + deep dive into corner cases if needed

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Notebook setup

# COMMAND ----------

# Databricks notebook source
dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("stack", "")
dbutils.widgets.text("job_dbfs_path", "")

# COMMAND ----------

# MAGIC %run ../../notebooks/python/common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../../notebooks/python/common/database_utils

# COMMAND ----------

import os
os.listdir("../../notebooks/") # returns list

# COMMAND ----------

constants = {
    "SFAI_URL": "jdbc:sqlserver://sfai.corp.hpicloud.net:1433;",
    "SFAI_DRIVER": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "S3_BASE_BUCKET" : {
        "dev" : "s3a://dataos-core-dev-team-phoenix/",
        "itg" : "s3a://dataos-core-itg-team-phoenix/",
        "prod" : "s3a://dataos-core-prod-team-phoenix/"
    },
    "REDSHIFT_URLS" : {
        "dev" : "dataos-core-dev-team-phoenix.dev.hpdataos.com",
        "itg" : "dataos-core-team-phoenix-itg.hpdataos.com",
        "prod" : "dataos-core-team-phoenix.hpdataos.com",
        "reporting" : "dataos-core-team-phoenix-reporting.hpdataos.com"
    },
    "REDSHIFT_PORTS" : {
        "dev" : "5439",
        "itg" : "5439",
        "prod" : "5439",
        "reporting" : "5439"
    },
    "REDSHIFT_DATABASE" : {
        "dev" : "dev",
        "itg" : "itg",
        "prod" : "prod",
        "reporting" : "prod"
    }
}

# COMMAND ----------

# retrieve secrets based on incoming/inputted secrets name - variables will be accessible across languages
redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")
sqlserver_secrets = secrets_get(dbutils.widgets.get("sqlserver_secrets_name"), "us-west-2")

# COMMAND ----------

# configs dict setup
configs = {}
configs["redshift_username"] = redshift_secrets["username"]
configs["redshift_password"] = redshift_secrets["password"]
configs["redshift_url"] = constants["REDSHIFT_URLS"][dbutils.widgets.get("stack")]
configs["redshift_port"] = constants["REDSHIFT_PORTS"][dbutils.widgets.get("stack")]
configs["redshift_dbname"] = constants["REDSHIFT_DATABASE"][dbutils.widgets.get("stack")]
# configs["aws_iam_role"] = dbutils.widgets.get("aws_iam_role")
# configs["redshift_temp_bucket"] =  "{}redshift_temp/".format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")])
configs["sfai_username"] = sqlserver_secrets["username"]
configs["sfai_password"] = sqlserver_secrets["password"]
configs["sfai_url"] = constants["SFAI_URL"]

# COMMAND ----------

ns_sfai_records = read_sql_server_to_df(configs) \
  .option("query", "SELECT * FROM ie2_staging.dbt.norm_ships") \
  .load()

# COMMAND ----------

display(ns_sfai_records)
