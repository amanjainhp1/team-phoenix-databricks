# Databricks notebook source
import pyspark.sql.functions as f
import time
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

drop_testcases_table= f""" drop table if exists qa.test_cases;"""
submit_remote_query(configs,drop_testcases_table )

# COMMAND ----------

# create test case table
testcases_create_query=f"""CREATE TABLE qa.test_cases (test_case_id int IDENTITY(1,1),project varchar(200) NULL,server_name varchar(200) NULL,database_name varchar(200) NULL,test_category varchar(200) NULL,module_name varchar(200) NULL,test_case_name varchar(500) NULL,schema_name varchar(200) NULL,table_name varchar(200) NULL,element_name varchar(200) NULL,test_query varchar(5000) NULL,query_path varchar(200) NULL,min_threshold varchar(200) NULL,max_threshold varchar(200) NULL,severity varchar(200) NULL, workflow_name varchar(200) NULL,test_case_creation_date timestamp NULL,test_case_created_by varchar(200) NULL,enabled bool null);"""

# COMMAND ----------

submit_remote_query(configs,testcases_create_query ) # create test cases table

# COMMAND ----------

read_testcases_data = read_redshift_to_df(configs).option("query", "SELECT * FROM qa.test_cases").load()

# COMMAND ----------

read_testcases_data.show()
