# Databricks notebook source
import pyspark.sql.functions as f
import time
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

drop_testresults_table= f""" drop table if exists stage.test_results;"""
submit_remote_query(configs,drop_testresults_table )

# COMMAND ----------

# create test results table
testresults_create_query=f"""create table stage.test_results (test_result_id int identity(1,1),
test_case_id int ,
version_id VARCHAR (200),
test_rundate DATETIME,
test_run_by VARCHAR (200),
test_result_detail VARCHAR (200),
test_result VARCHAR (200),
test_run_id int null
);GRANT ALL ON TABLE stage.test_results TO group dev_arch_eng"""

# COMMAND ----------

submit_remote_query(configs,testresults_create_query ) # create test results table

# COMMAND ----------

read_testresults_data = read_redshift_to_df(configs).option("query", "SELECT * FROM stage.test_results").load()

# COMMAND ----------

read_testresults_data.show()

# COMMAND ----------

drop_testresults_detail_table= f""" drop table if exists stage.test_results_detail;"""
submit_remote_query(configs,drop_testresults_detail_table )

# COMMAND ----------

# create test results detail table
testresults_detail_create_query=f"""create table stage.test_results_detail (test_result_detail_id int identity(1,1),
test_case_id int ,
test_result_id int ,
detail_value super
);GRANT ALL ON TABLE stage.test_results_detail TO group dev_arch_eng"""

# COMMAND ----------

submit_remote_query(configs,testresults_detail_create_query ) # create test results table
