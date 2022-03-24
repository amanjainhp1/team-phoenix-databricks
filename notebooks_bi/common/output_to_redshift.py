# Databricks notebook source
# MAGIC %md
# MAGIC # Output Tables to Redshift Using SQL Queries

# COMMAND ----------

# imports
import json
import sys
import boto3
import psycopg2 
import pyspark.sql.functions as func
from pyspark.sql.functions import *

# COMMAND ----------

# globals
secrets_url = dbutils.widgets.text("arn:aws:secretsmanager:us-west-2:740156627385:secret:dev/redshift/dataos-core-dev-01/auto_glue-dj6tOj", "")
spark_url = dbutils.widgets.text("jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None", "")
redshift_url = dbutils.widgets.text("dataos-core-dev-team-phoenix.dev.hpdataos.com", "")
spark_temp_dir = dbutils.widgets.text("s3a://dataos-core-dev-team-phoenix/redshift_temp/", "")
aws_iam = dbutils.widgets.text("arn:aws:iam::740156627385:role/team-phoenix-role", "")
spark_format = dbutils.widgets.text("com.databricks.spark.redshift", "")

# COMMAND ----------

# MAGIC %run ../../notebooks/python/common/secrets_manager_utils

# COMMAND ----------

# Secrets Variables
# redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")
redshift_secrets = secrets_get(secrets_url, "us-west-2")
spark.conf.set("username", redshift_secrets["username"])
spark.conf.set("password", redshift_secrets["password"])
username = spark.conf.get("username")
password = spark.conf.get("password")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Redshift Class
# MAGIC Parameters: username, password, table name, query
# MAGIC 
# MAGIC Methods:
# MAGIC 
# MAGIC getData() Retrieves query data and returns a dataframe
# MAGIC saveTable() Uses dataframe parameter to create Redshift Table

# COMMAND ----------

class RedshiftOut:
  def get_data(self, username, password, table_name, query):
     dataDF = spark.read \
        .format(dbutils.widgets.get(spark_format)) \
        .option("url", dbutils.widgets.get(spark_url)) \
        .option("tempdir", dbutils.widgets.get(spark_temp_dir)) \
        .option("aws_iam_role", dbutils.widgets.get(aws_iam)) \
        .option("user", username) \
        .option("password", password) \
        .option("query", query) \
        .load()
     
     return(dataDF)
  
  
  def save_table(self, dataDF):
      dataDF.write \
      .format(dbutils.widgets.get(spark_format)) \
      .option("url", dbutils.widgets.get(spark_url)) \
      .option("dbtable", table_name) \
      .option("tempdir", dbutils.widgets.get(spark_temp_dir)) \
      .option("aws_iam_role", dbutils.widgets.get(aws_iam)) \
      .option("user", username) \
      .option("password", password) \
      .mode("overwrite") \
      .save()
      
  
  # from Matt Koson, Data Engineer
  def submit_remote_query(self, dbname, port, user, password, host, sql_query):  
      conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
          .format(dbname, port, user, password, host)
      
      con = psycopg2.connect(conn_string)
      cur = con.cursor()
      cur.execute(sql_query)
      con.commit()
      cur.close()
  


# COMMAND ----------

for obj in query_list:
  table_name = obj[0]
  query = obj[1]
  read_obj = RedshiftOut()
  data_df = read_obj.get_data(username, password, table_name, query)
  read_obj.save_table(data_df)
  read_obj.submit_remote_query("dev", "5439", username, password, redshift_url, f'GRANT ALL ON {table_name} TO group dev_arch_eng')
