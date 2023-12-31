# Databricks notebook source
import boto3
import json
import os
from typing import Dict

# COMMAND ----------

def create_session(role_arn: str, session_duration: int = 3600, set_env_vars: bool = True):
    # Create an STS client object that represents a live connection to the STS service
    sts_client = boto3.client('sts')

    # Call the assume_role method of the STSConnection object and pass the role
    # ARN and a role session name.
    assumed_role_object=sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName="AssumeRoleSession1",
        DurationSeconds=session_duration
    )

    # From the response that contains the assumed role, get the temporary 
    # credentials that can be used to make subsequent API calls
    credentials=assumed_role_object['Credentials']
    
    # If True, set AWS environment variables to temporary credentials (AWS CLI will default to these)
    if set_env_vars:
        os.environ['AWS_ACCESS_KEY_ID'] = credentials['AccessKeyId']
        os.environ['AWS_SECRET_ACCESS_KEY'] = credentials['SecretAccessKey']
        os.environ['AWS_SESSION_TOKEN'] = credentials['SessionToken']

    # Establish a session to be used in subsequent calls to AWS services
    session = boto3.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name='us-west-2'
    )
    
    return session

# COMMAND ----------

# Retrieve username and password from AWS Secrets Manager

def secrets_get(secret_name: str, region_name: str = "us-west-2", session: boto3.session.Session = None) -> Dict[str, str]:
    # Create a new session object if one has not been provided
    if not session:
        session = boto3.session.Session()

    # Create a Secrets Manager client
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name
    )

    # Get the secret value
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    return eval(get_secret_value_response["SecretString"])

# COMMAND ----------

# retrieve stack from spark conf "custom tags" (defined as part of deployment internal/defaults.yml file)
stack = ""
custom_tags = json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags"))

for tag in custom_tags:
    if tag["key"] == "Custom3":
        stack = tag["value"].lower()

# COMMAND ----------

# define constants
common_constants = {
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
    "REDSHIFT_IAM_ROLE": {
        "dev": "arn:aws:iam::740156627385:role/team-phoenix-role",
        "itg": "arn:aws:iam::740156627385:role/redshift-copy-unload-team-phoenix",
        "prod": "arn:aws:iam::828361281741:role/redshift-copy-unload-team-phoenix",
        "reporting": "arn:aws:iam::828361281741:role/redshift-copy-unload-team-phoenix"
    },
    "REDSHIFT_DEV_GROUP": {
        "dev": "dev_arch_eng",
        "itg": "dev_arch_eng",
        "prod": "phoenix_dev"
    },
    "REDSHIFT_SPECTRUM_SCHEMA": {
        "dev": "phoenix_spectrum",
        "itg": "phoenix_spectrum_itg",
        "prod": "phoenix_spectrum_prod",
        "reporting": "phoenix_spectrum_reporting"
    }
}

developer_constants = {
    "SFAI_URL": "jdbc:sqlserver://sfai.corp.hpicloud.net:1433;trustServerCertificate=true;",
    "SFAI_DRIVER": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "SFAI_SECRET_NAME": {
        "dev": "arn:aws:secretsmanager:us-west-2:740156627385:secret:prod/sqlserver/team-phoenix/auto_databricks-TuXNHG",
        "itg": "arn:aws:secretsmanager:us-west-2:740156627385:secret:prod/sqlserver/team-phoenix/auto_databricks-TuXNHG",
        "prod": "arn:aws:secretsmanager:us-west-2:828361281741:secret:prod/sqlserver/team-phoenix/auto_databricks-hChWVK",
        "reporting": "arn:aws:secretsmanager:us-west-2:828361281741:secret:prod/sqlserver/team-phoenix/auto_databricks-hChWVK"
    },
    "OZZY_SECRET_NAME": {
        "dev": "arn:aws:secretsmanager:us-west-2:740156627385:secret:dev/team-phoenix/ozzy-sAlNQG",
        "itg": "arn:aws:secretsmanager:us-west-2:740156627385:secret:itg/team-phoenix/ozzy-GmMlPe",
        "prod": "arn:aws:secretsmanager:us-west-2:828361281741:secret:prod/team-phoenix/ozzy-YdkEtR"
    },
    "S3_BASE_BUCKET": {
        "dev": "s3a://dataos-core-dev-team-phoenix/",
        "itg": "s3a://dataos-core-itg-team-phoenix/",
        "prod": "s3a://dataos-core-prod-team-phoenix/",
        "reporting": "s3a://dataos-core-prod-team-phoenix/"
    },
    "S3_FIN_BUCKET": {
        "dev": "s3a://dataos-core-dev-team-phoenix-fin/",
        "itg": "s3a://dataos-core-itg-team-phoenix-fin/",
        "prod": "s3a://dataos-core-prod-team-phoenix-fin/",
        "reporting": "s3a://dataos-core-prod-team-phoenix-fin/"
    },
    "REDSHIFT_SECRET_NAME": {
        "dev": "arn:aws:secretsmanager:us-west-2:740156627385:secret:dev/redshift/dataos-core-dev-01/auto_glue-dj6tOj",
        "itg": "arn:aws:secretsmanager:us-west-2:740156627385:secret:itg/redshift/team-phoenix/auto_glue-v6JOfZ",
        "prod": "arn:aws:secretsmanager:us-west-2:828361281741:secret:prod/redshift/phoenix/auto_glue-aDckNc",
        "reporting": "arn:aws:secretsmanager:us-west-2:828361281741:secret:prod/redshift/phoenix-reporting/auto_databricks-w7Xhqb",
        "reporting-readonly": "arn:aws:secretsmanager:us-west-2:828361281741:secret:prod/redshift/phoenix-reporting/auto_reporting-XnolN0"
    },
    "STS_IAM_ROLE": {
        "dev": "arn:aws:iam::740156627385:role/dataos-dev-databricks-phoenix-role",
        "itg": "arn:aws:iam::740156627385:role/dataos-itg-databricks-phoenix-role",
        "prod": "arn:aws:iam::828361281741:role/dataos-prod-databricks-phoenix-role"
    },
    "SESSION_DURATION": {
        "dev": 14400,
        "itg": 14400,
        "prod": 14400
    }
}

analyst_constants = {
    "S3_BASE_BUCKET": {
        "dev": "s3a://dataos-core-dev-team-phoenix/analyst/",
        "itg": "s3a://dataos-core-itg-team-phoenix/analyst/",
        "prod": "s3a://dataos-core-prod-team-phoenix/analyst/"
    },
    "REDSHIFT_SECRET_NAME": {
        "dev": "arn:aws:secretsmanager:us-west-2:740156627385:secret:dev/redshift/team-phoenix/auto_team_phoenix_analyst-LU2mBY",
        "itg": "arn:aws:secretsmanager:us-west-2:740156627385:secret:itg/redshift/team-phoenix/auto_team_phoenix_analyst-c3Hinm",
        "prod": "arn:aws:secretsmanager:us-west-2:828361281741:secret:prod/redshift/team-phoenix/auto_team_phoenix_analyst-W0FFkg"
    },
    "STS_IAM_ROLE": {
        "dev": "arn:aws:iam::740156627385:role/dataos-dev-databricks-phoenix-analyst-role",
        "itg": "arn:aws:iam::740156627385:role/dataos-dev-databricks-phoenix-analyst-role",
        "prod": "arn:aws:iam::828361281741:role/dataos-prod-databricks-phoenix-analyst-role"
    },
    "SESSION_DURATION": {
        "dev": 14400,
        "itg": 3600,
        "prod": 3600
    }
}

# COMMAND ----------

# determine which constants to use (e.g. developer or analyst)
role = 'developer'
sql_server_access = True
constants = {**common_constants, **developer_constants}
if 'analyst' in spark.conf.get('spark.databricks.clusterUsageTags.instanceProfileArn') and spark.conf.get('spark.databricks.clusterUsageTags.instanceProfileUsed'):
    role = 'analyst'
    sql_server_access = False
    constants = {**common_constants, **analyst_constants}

# COMMAND ----------

# assume role, retrieve credentials, create session
session = create_session(constants["STS_IAM_ROLE"][stack], constants["SESSION_DURATION"][stack])

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
configs["redshift_temp_bucket"] = constants["S3_BASE_BUCKET"][stack] + "redshift_temp/"
configs["aws_iam_role"] = constants["REDSHIFT_IAM_ROLE"][stack]

# sqlserver
if sql_server_access:
    sqlserver_secret = secrets_get(constants["SFAI_SECRET_NAME"][stack], "us-west-2")

    configs["sfai_username"] = sqlserver_secret["username"]
    configs["sfai_password"] = sqlserver_secret["password"]
    configs["sfai_url"] = constants["SFAI_URL"]
    configs["sfai_driver"] = constants["SFAI_DRIVER"]
