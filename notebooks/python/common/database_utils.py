# Databricks notebook source
from sqlite3 import Timestamp
import boto3
from datetime import datetime
import json
from pyspark.sql import functions as f
import psycopg2
from typing import Union
from pyspark.sql.dataframe import DataFrame

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

def read_redshift_to_df(configs: dict) -> DataFrame:
    df = spark.read \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://{}:{}/{}?ssl_verify=None".format(configs["redshift_url"], configs["redshift_port"], configs["redshift_dbname"])) \
    .option("aws_iam_role", configs["aws_iam_role"]) \
    .option("user", configs["redshift_username"]) \
    .option("password", configs["redshift_password"]) \
    .option("tempdir", configs["redshift_temp_bucket"])
    return df

# COMMAND ----------

def write_df_to_redshift(configs: dict, df: DataFrame, destination: str = "", mode: str = "append", postactions: str = "", preactions: str = "") -> None:
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
            .option("preactions", preactions) \
            .option("postactions", "GRANT ALL ON {} TO GROUP {};{}".format(destination, configs["redshift_dev_group"], postactions)) \
            .option("extracopyoptions", "TIMEFORMAT 'auto'") \
            .mode(mode) \
            .save()
    except Exception as error:
        print ("An exception has occured:", error)
        print ("Exception Type:", type(error))

# COMMAND ----------

def read_sql_server_to_df(configs: dict) -> DataFrame:
    df = spark.read \
        .format("jdbc") \
        .option("url",  configs["sfai_url"]) \
        .option("user", configs["sfai_username"]) \
        .option("password",  configs["sfai_password"])
    return df

# COMMAND ----------

def write_df_to_sqlserver(configs: dict, df: DataFrame, destination: str = "", mode: str = "append") -> None:
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

def write_df_to_s3(df: DataFrame, destination: str = "", format: str = "parquet", mode: str = "append"):
    df.write \
        .format(format) \
        .mode(mode) \
        .save(destination)

# COMMAND ----------

def call_redshift_addversion_sproc(configs: dict, record: str, source_name: str) -> Union[str, datetime]:
    
    call_addversion_sproc_query = "CALL prod.addversion_sproc('{}', '{}');".format(record, source_name)
    
    retrieve_version_query = """
    SELECT 
        MAX(version) AS version,
        MAX(load_date) AS load_date
    FROM prod.version
    WHERE record = '{}'
    AND source_name = '{}'
    GROUP BY record, source_name
    """.format(record, source_name)

    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'" \
        .format(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"])
    con = psycopg2.connect(conn_string)
    cur = con.cursor()
    
    cur.execute(call_addversion_sproc_query)
    con.commit()
    
    cur.execute(retrieve_version_query)
    output = cur.fetchone()
    
    cur.close()
    
    return output
