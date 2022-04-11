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

# MAGIC %run ../../notebooks/python/common/configs

# COMMAND ----------

# globals
spark_format = "com.databricks.spark.redshift"
secrets_url = constants["REDSHIFT_SECRET_NAME"][stack]
redshift_url = configs["redshift_url"]
spark_temp_dir = configs["redshift_temp_bucket"]
aws_iam = configs["aws_iam_role"]
redshift_port = configs["redshift_port"]
redshift_dbname = configs["redshift_dbname"]

# COMMAND ----------

# MAGIC %run ../../notebooks/python/common/secrets_manager_utils

# COMMAND ----------

# Secrets Variables
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
            .format(spark_format) \
            .option("url", "jdbc:redshift://{}:{}/{}?ssl_verify=None".format(redshift_url, redshift_port, redshift_dbname)) \
            .option("tempdir", spark_temp_dir) \
            .option("aws_iam_role", aws_iam) \
            .option("user", username) \
            .option("password", password) \
            .option("query", query) \
            .load()
        
        return(dataDF)
  
  
    def save_table(self, dataDF):
        dataDF.write \
            .format(spark_format) \
            .option("url", "jdbc:redshift://{}:{}/{}?ssl_verify=None".format(redshift_url, redshift_port, redshift_dbname)) \
            .option("dbtable", table_name) \
            .option("tempdir", spark_temp_dir) \
            .option("aws_iam_role", aws_iam) \
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
    query_name = table_name.split('.')[1]
    try:
        read_obj = RedshiftOut()
        data_df = read_obj.get_data(username, password, table_name, query)
        print("Query " + query_name + " retrieved.")
    except Exception(e):
        print("Error, query " + query_name + " not retrieved.")
        print(e)
    
    try:
        read_obj.save_table(data_df)
        read_obj.submit_remote_query(redshift_dbname, redshift_port, username, password, redshift_url, f'GRANT ALL ON {table_name} TO group dev_arch_eng')
        print("Table " + table_name + " created.\n")
    except Exception(e):
        print("Error, table " + table_name + " not created.\n")
        print(e)
    


# COMMAND ----------


