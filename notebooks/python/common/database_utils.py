# Databricks notebook source
import boto3
import concurrent.futures
import json
import os
import psycopg2
import re
import uuid

from datetime import datetime
from functools import singledispatch
from pyspark.sql.functions import col, upper
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField
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

# function to read redshift to rows
def read_redshift_to_rows(configs: dict, sql_query: str):
    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
        .format(configs["redshift_dbname"], configs["redshift_port"], \
                configs["redshift_username"], configs["redshift_password"], configs["redshift_url"])
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute(sql_query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# COMMAND ----------

def read_redshift_to_df(configs: dict) -> DataFrame:
    df = spark.read \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://{}:{}/{}?ssl_verify=None".format(configs["redshift_url"], configs["redshift_port"], configs["redshift_dbname"])) \
    .option("temporary_aws_access_key_id", os.getenv("AWS_ACCESS_KEY_ID")) \
    .option("temporary_aws_secret_access_key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .option("temporary_aws_session_token", os.getenv("AWS_SESSION_TOKEN")) \
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

# alternate function to read from redshift to spark via unload and read
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

# COMMAND ----------

# write dataframe to delta table
# expected input is a list of ['schema_name.table_name', dataframe]

# nDatabricks will not allow us to easily write out a table with non-nullable columns using a DataFrame,
#  so ignore that field when comparing schemas
def ignore_nullable(schema):
    modified_fields = []
    for field in schema.fields:
        modified_fields.append(StructField(field.name, field.dataType, nullable=True))
    return StructType(modified_fields)

def write_df_to_delta_inner_function(table: list, rename_cols: bool):
        # Extract the schema and table names.
        full_table_name = table[0]
        view_name = full_table_name.split(".")[1]
        
        # Load the data from its source.
        df = table[1]
        print(f'loading {full_table_name}...')

        # Rename columns with restricted characters
        if rename_cols:
            for column in df.dtypes:
                column_wo_parentheses = re.sub('\)|\(', '', column[0].lower())
                column_w_underscores = re.sub('-| ', '_', column_wo_parentheses)
                column_wo_dollarsign = re.sub('\$', '_dollars', column_w_underscores)
                df = df.withColumnRenamed(column[0], column_wo_dollarsign)

        # Create the table from DataFrame and corresponding view
        df.createOrReplaceTempView(view_name)
        ## Check if delta table exists
        try:
            delta_table_schema = spark.sql(f"SELECT * FROM {full_table_name} LIMIT 1").schema
            print(f'{full_table_name} exists')
            ## Check if schemas match, then a) truncate and append, else b) drop and re-create table using DataFrame schema
            if ignore_nullable(df.schema) == ignore_nullable(delta_table_schema):
                print(f'DataFrame and table schemas match. Truncating {full_table_name}')
                spark.sql(f"TRUNCATE TABLE {full_table_name}")
                spark.sql(f"INSERT INTO {full_table_name} SELECT * FROM {view_name}")
            else:
                print(f'DataFrame and table schemas do not match. Re-creating {full_table_name}')
                spark.sql(f"DROP TABLE {full_table_name}")
                spark.sql(f"CREATE TABLE {full_table_name} SELECT * FROM {view_name}")
        except:
            print(f'creating {full_table_name}')
            spark.sql(f"CREATE TABLE {full_table_name} SELECT * FROM {view_name}")
        print(f'{full_table_name} loaded')

def write_df_to_delta(tables: list, rename_cols: bool = False, threads: int = 1) -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_table = {}
        for table in tables:
            future = executor.submit(write_df_to_delta_inner_function, table, rename_cols)
            future_to_table[future] = table[0]
        
        for future in concurrent.futures.as_completed(future_to_table):
            table = future_to_table[future]
            result = future.result()
