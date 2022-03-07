# Databricks notebook source
import boto3
import io
import yaml
from datetime import datetime

# COMMAND ----------

# Set variables
region_name = "us-west-2"
secret_name = "arn:aws:secretsmanager:us-west-2:740156627385:secret:dev/redshift/dataos-core-dev-01/auto_glue-dj6tOj"
s3_bucket = "dataos-core-dev-team-phoenix"

# COMMAND ----------

def secrets_get(secret_name):
    endpoint_url = "https://secretsmanager.us-west-2.amazonaws.com"
    client = boto3.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return eval(get_secret_value_response['SecretString'])

# COMMAND ----------

username = secrets_get(secret_name)['username']
password = secrets_get(secret_name)['password']
host = 'dataos-core-dev-01.ctxslfwrp6pj.us-west-2.redshift.amazonaws.com'

# COMMAND ----------

df_decay_pro = spark.read.format('csv').options(header='true', inferSchema='true').load('s3://dataos-core-dev-team-phoenix/product/mdm/decay/Large Format/lf_decay_pro.csv')
df_decay_design = spark.read.format('csv').options(header='true', inferSchema='true').load('s3://dataos-core-dev-team-phoenix/product/mdm/decay/Large Format/lf_decay_design.csv')

df_decay_pro.createOrReplaceTempView("decay_pro")
df_decay_design.createOrReplaceTempView("decay_design")

# COMMAND ----------

import json
import boto3
import psycopg2 


credential = {
'dbname' : 'dev',
'port' : '5439',
'user' : 'auto_glue',
'password' : '7Wn5jq9Io5COIfXd9YI7ZWGw3qg22U8j',
'host_url':'dataos-core-dev-team-phoenix.dev.hpdataos.com',
'sslmode':'require'
}


    
conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
    .format(credential['dbname'],credential['port'],credential['user'],credential['password'],credential['host_url'])
con = psycopg2.connect(conn_string)
cur = con.cursor()
cur.execute("truncate table stage.lf_decay_temp")
con.commit()
cur.close()

# COMMAND ----------

df_decay_pro.write \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
  .option("dbtable", "stage.lf_decay_temp") \
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
  .option("tempformat", "CSV") \
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
  .option("user", username) \
  .option("password", password) \
  .mode("append") \
  .save()

df_decay_design.write \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
  .option("dbtable", "stage.lf_decay_temp") \
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
  .option("tempformat", "CSV") \
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
  .option("user", username) \
  .option("password", password) \
  .mode("append") \
  .save()

# COMMAND ----------

import json
import boto3
import psycopg2 


credential = {
'dbname' : 'dev',
'port' : '5439',
'user' : 'auto_glue',
'password' : '7Wn5jq9Io5COIfXd9YI7ZWGw3qg22U8j',
'host_url':'dataos-core-dev-team-phoenix.dev.hpdataos.com',
'sslmode':'require'
}


    
conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
    .format(credential['dbname'],credential['port'],credential['user'],credential['password'],credential['host_url'])
con = psycopg2.connect(conn_string)
cur = con.cursor()
cur.execute("call prod.p_lf_decay()")
con.commit()
cur.close()
