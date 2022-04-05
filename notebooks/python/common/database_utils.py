# Databricks notebook source
import boto3
import json
from pyspark.sql import functions as f
import psycopg2

# COMMAND ----------

def submit_remote_query(dbname, port, user, password, host, sql_query):  
  conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
      .format(dbname, port, user, password, host)
  con = psycopg2.connect(conn_string)
  cur = con.cursor()
  cur.execute(sql_query)
  con.commit()
  cur.close()

# COMMAND ----------

def read_redshift_to_df(configs):
  df = spark.read \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://{}:{}/{}?ssl_verify=None".format(configs["redshift_url"], configs["redshift_port"], configs["redshift_dbname"])) \
  .option("aws_iam_role", configs["aws_iam_role"]) \
  .option("user", configs["redshift_username"]) \
  .option("password", configs["redshift_password"]) \
  .option("tempdir", configs["redshift_temp_bucket"])
  return df

# COMMAND ----------

def write_df_to_redshift(configs, df, destination, mode, postactions = ""):
  for column in df.dtypes:
    if column[1] == 'string':
        df = df.withColumn(column[0], f.upper(f.col(column[0])))
  try:
    df.write \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://{}:{}/{}?ssl_verify=None".format(configs["redshift_url"], configs["redshift_port"], configs["redshift_dbname"])) \
    .option("dbtable", destination) \
    .option("tempdir", configs["redshift_temp_bucket"]) \
    .option("tempformat", "CSV") \
    .option("aws_iam_role", configs["aws_iam_role"]) \
    .option("user", configs["redshift_username"]) \
    .option("password", configs["redshift_password"]) \
    .option("postactions", "GRANT ALL ON {} TO GROUP {};{}".format(destination, configs["redshift_dev_group"], postactions)) \
    .option("extracopyoptions", "TIMEFORMAT 'auto'") \
    .mode(mode) \
    .save()
  except Exception as error:
      print ("An exception has occured:", error)
      print ("Exception Type:", type(error))

# COMMAND ----------

def read_sql_server_to_df(configs):
  df = spark.read \
  .format("jdbc") \
  .option("url",  configs["sfai_url"]) \
  .option("user", configs["sfai_username"]) \
  .option("password",  configs["sfai_password"])
  return df

# COMMAND ----------

def write_df_to_sqlserver(configs, df, destination, mode):
  try:
    df.write \
    .format("jdbc") \
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("url",  configs["sfai_url"]) \
    .option("user", configs["sfai_username"]) \
    .option("password",  configs["sfai_password"]) \
    .option("dbtable", destination) \
    .mode(mode) \
    .save()
  except Exception as error:
      print ("An exception has occured:", error)
      print ("Exception Type:", type(error))

# COMMAND ----------

def write_df_to_s3(df, destination, format, mode):
  df.write \
  .format(format) \
  .mode(mode) \
  .save(destination)
