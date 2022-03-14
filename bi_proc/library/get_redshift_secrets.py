# Databricks notebook source
# Retrieve username and password from AWS secrets manager
import boto3
import io
from datetime import datetime

# Set variables
region_name = "us-west-2"
# TODO update to dbutil.widgets
secret_name = "arn:aws:secretsmanager:us-west-2:740156627385:secret:itg/redshift/team-phoenix/auto_glue-v6JOfZ"

def secrets_get(secret_name):
    endpoint_url = "https://secretsmanager.us-west-2.amazonaws.com"
    client = boto3.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return eval(get_secret_value_response['SecretString'])
  
username = secrets_get(secret_name)['username']
password = secrets_get(secret_name)['password']

# COMMAND ----------

spark.conf.set('username',username)
spark.conf.set('password',password)
