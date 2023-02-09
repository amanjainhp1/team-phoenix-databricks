# Databricks notebook source
import boto3
import json
import psycopg2
import uuid

from datetime import datetime
from functools import singledispatch
from pyspark.sql.functions import col,upper
from pyspark.sql.dataframe import DataFrame
from sqlite3 import Timestamp
from typing import Union

# COMMAND ----------

def submit_remote_query_inner_func(conn_string: str, sql_query: str):
    con = psycopg2.connect(conn_string)
    cur = con.cursor()
    cur.execute(sql_query)
    con.commit()
    cur.close()    

@singledispatch
def submit_remote_query(dbname: str, port: str, user: str, password: str, host: str, sql_query: str):
    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
        .format(dbname, port, user, password, host)
    submit_remote_query_inner_func(conn_string, sql_query)

@submit_remote_query.register
def _(configs: dict, sql_query: str):
    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
        .format(configs["redshift_dbname"], configs["redshift_port"], \
                configs["redshift_username"], configs["redshift_password"], configs["redshift_url"])
    submit_remote_query_inner_func(conn_string, sql_query)

# COMMAND ----------

def read_redshift_to_df(configs: dict) -> DataFrame:
    df = spark.read \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://{}:{}/{}?ssl_verify=None".format(configs["redshift_url"], configs["redshift_port"], configs["redshift_dbname"])) \
    .option("temporary_aws_access_key_id", configs["session"].get_credentials().access_key) \
    .option("temporary_aws_secret_access_key", configs["session"].get_credentials().secret_key) \
    .option("temporary_aws_session_token", configs["session"].get_credentials().token) \
    .option("user", configs["redshift_username"]) \
    .option("password", configs["redshift_password"]) \
    .option("tempdir", configs["redshift_temp_bucket"])
    return df

# COMMAND ----------

def df_strings_to_upper(df: DataFrame) -> DataFrame:
    for column in df.dtypes:
        if column[1] == 'string':
            df = df.withColumn(column[0], upper(col=(column[0])))
    return df

# COMMAND ----------

def write_df_to_redshift(configs: dict, df: DataFrame, destination: str = "", mode: str = "append", postactions: str = "", preactions: str = "") -> None:
    
    df = df_strings_to_upper(df)
    
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
        raise Exception(error)

# COMMAND ----------

def read_sql_server_to_df(configs: dict) -> DataFrame:
    df = spark.read \
        .format("jdbc") \
        .option("url",  configs["sfai_url"]) \
        .option("user", configs["sfai_username"]) \
        .option("password",  configs["sfai_password"])
    return df

# COMMAND ----------

def write_df_to_sqlserver(configs: dict, df: DataFrame, destination: str = "", mode: str = "append", postactions: str = "", preactions: str = "") -> None:
    try:
        df.write \
            .format("jdbc") \
            .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("url",  configs["sfai_url"]) \
            .option("user", configs["sfai_username"]) \
            .option("password",  configs["sfai_password"]) \
            .option("dbtable", destination) \
            .option("preactions", preactions) \
            .option("postactions", postactions) \
            .mode(mode) \
            .save()
    except Exception as error:
        print ("An exception has occured:", error)
        print ("Exception Type:", type(error))
        raise Exception(error)

# COMMAND ----------

def write_df_to_s3(df: DataFrame, destination: str = "", format: str = "parquet", mode: str = "append", upper_strings: bool = False) -> None:
    try:
        if upper_strings:
            df = df_strings_to_upper(df)
        df.write \
            .format(format) \
            .mode(mode) \
            .save(destination)
    except Exception as error:
        print ("An exception has occured:", error)
        print ("Exception Type:", type(error))
        raise Exception(error)

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

# COMMAND ----------

def read_redshift_to_spark_df(configs: dict, query: str) -> DataFrame:
    # generate uuid and append to redshift_temp_bucket
    redshift_temp_bucket = configs['redshift_temp_bucket'] + str(uuid.uuid4())  + "/"
    # redshift cannot unload to s3a or s3n path, so we clean the path
    clean_s3_path = redshift_temp_bucket.replace("s3a://", "s3://").replace("s3n://", "s3://")
    # construct unload query from query provided
    unload_query = "UNLOAD('{}') TO '{}' WITH CREDENTIALS 'aws_iam_role={}' FORMAT AS PARQUET;".format(query, clean_s3_path, configs["aws_iam_role"])
    submit_remote_query(configs, unload_query)
    # read data into Spark dataframe
    df = spark.read.parquet(redshift_temp_bucket) 
    return df
