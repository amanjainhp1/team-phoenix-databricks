# Databricks notebook source
# MAGIC %md
# MAGIC # Output Tables to Redshift Using SQL Queries

# COMMAND ----------

# imports
import pyspark.sql.functions as func
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ./configs

# COMMAND ----------

# MAGIC %run ./database_utils

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
    def get_data(self, configs, table_name, query):
        dataDF = read_redshift_to_df(configs) \
            .option("query", query) \
            .load()

        return(dataDF)


    def save_table(self, dataDF, table_name, write_mode):
        write_df_to_redshift(configs, dataDF, table_name, write_mode)

# COMMAND ----------

for obj in query_list:
    table_name = obj[0]
    query = obj[1]
    write_mode = obj[2]
    query_name = table_name.split('.')[1]
    try:
        read_obj = RedshiftOut()
        data_df = read_obj.get_data(configs, table_name, query)
        print("Query " + query_name + " retrieved.")
    except Exception(e):
        print("Error, query " + query_name + " not retrieved.")
        print(e)
    
    try:
        read_obj.save_table(data_df, table_name, write_mode)
        print("Table " + table_name + " created.\n")
    except Exception(e):
        print("Error, table " + table_name + " not created.\n")
        print(e)