# Databricks notebook source
import json

# COMMAND ----------

# MAGIC %run ./secrets_manager_utils

# COMMAND ----------

# retrieve stack from spark conf "custom tags" (defined as part of deployment internal/defaults.yml file)
stack = ""
custom_tags = json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags"))

for tag in custom_tags:
  if tag["key"] == "Custom3":
    stack = tag["value"].lower()

# COMMAND ----------

# define constants
constants = {
    "SFAI_URL": "jdbc:sqlserver://sfai.corp.hpicloud.net:1433;",
    "SFAI_DRIVER": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "SFAI_SECRET_NAME": {
        "dev": "arn:aws:secretsmanager:us-west-2:740156627385:secret:prod/sqlserver/team-phoenix/auto_databricks-TuXNHG",
        "itg": "",
        "prod": ""
    },
    "S3_BASE_BUCKET": {
        "dev": "s3a://dataos-core-dev-team-phoenix/",
        "itg": "s3a://dataos-core-itg-team-phoenix/",
        "prod": "s3a://dataos-core-prod-team-phoenix/"
    },
    "REDSHIFT_URL": {
        "dev": "dataos-core-dev-team-phoenix.dev.hpdataos.com",
        "itg": "dataos-core-team-phoenix-itg.hpdataos.com",
        "prod": "dataos-core-team-phoenix.hpdataos.com",
        "reporting": "dataos-core-team-phoenix-reporting.hpdataos.com"
    },
    "REDSHIFT_PORT": {
        "dev": "5439",
        "itg": "5439",
        "prod": "5439",
        "reporting": "5439"
    },
    "REDSHIFT_DATABASE": {
        "dev": "dev",
        "itg": "itg",
        "prod": "prod",
        "reporting": "prod"
    },
    "REDSHIFT_DEV_GROUP": {
        "dev": "dev_arch_eng",
        "itg": "dev_arch_eng",
        "prod": "phoenix_dev"
    },
    "REDSHIFT_SECRET_NAME": {
        "dev": "arn:aws:secretsmanager:us-west-2:740156627385:secret:dev/redshift/dataos-core-dev-01/auto_glue-dj6tOj",
        "itg": "",
        "prod": ""
    },
    "REDSHIFT_IAM_ROLE": {
        "dev": "arn:aws:iam::740156627385:role/team-phoenix-role",
        "itg": "",
        "prod": ""
    }
}

# COMMAND ----------

configs = {}

# redshift
redshift_secret = secrets_get(constants["REDSHIFT_SECRET_NAME"][stack], "us-west-2")
configs["redshift_username"] = redshift_secret["username"]
configs["redshift_password"] = redshift_secret["password"]
configs["redshift_url"] = constants["REDSHIFT_URL"][stack]
configs["redshift_port"] = constants["REDSHIFT_PORT"][stack]
configs["redshift_dev_group"] = constants["REDSHIFT_DEV_GROUP"][stack]
configs["redshift_dbname"] = constants["REDSHIFT_DATABASE"][stack]
configs["redshift_temp_bucket"] = constants["S3_BASE_BUCKET"][stack] + "/redshift_temp/"
configs["aws_iam_role"] = constants["REDSHIFT_IAM_ROLE"][stack]

# sqlserver
sqlserver_secret = secrets_get(constants["SFAI_SECRET_NAME"][stack], "us-west-2")
configs["sfai_username"] = sqlserver_secret["username"]
configs["sfai_password"] = sqlserver_secret["password"]
configs["sfai_url"] = constants["SFAI_URL"]
configs["sfai_driver"] = constants["SFAI_DRIVER"]
