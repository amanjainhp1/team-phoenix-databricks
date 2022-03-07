# Databricks notebook source
import json
import boto3
import psycopg2 

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
  df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://{}:{}/{}?ssl_verify=None".format(configs["redshift_url"], configs["redshift_port"], configs["redshift_dbname"])) \
  .option("dbtable", destination) \
  .option("tempdir", configs["redshift_temp_bucket"]) \
  .option("tempformat", "CSV") \
  .option("aws_iam_role", configs["aws_iam_role"]) \
  .option("user", configs["redshift_username"]) \
  .option("password", configs["redshift_password"]) \
  .option("postactions", "GRANT ALL ON {} TO GROUP dev_arch_eng;{}".format(destination, postactions)) \
  .mode(mode) \
  .save()
