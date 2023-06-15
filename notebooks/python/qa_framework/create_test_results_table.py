# Databricks notebook source
import pyspark.sql.functions as f
import time
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

drop_testresults_table= "DROP TABLE IF EXISTS stage.test_results;"
submit_remote_query(configs, drop_testresults_table)

# COMMAND ----------

# create test results table
testresults_create_query=f"""
CREATE TABLE stage.test_results (
  test_result_id INT identity(1,1),
  test_case_id INT ,
  version_id VARCHAR (200),
  test_rundate DATETIME,
  test_run_by VARCHAR (200),
  test_result_detail VARCHAR (200),
  test_result VARCHAR (200),
  test_run_id INT NULL
);
"""

# COMMAND ----------

submit_remote_query(configs, testresults_create_query) # create test results table

# COMMAND ----------

read_testresults_data = read_redshift_to_df(configs).option("query", "SELECT * FROM stage.test_results").load()

# COMMAND ----------

read_testresults_data.show()

# COMMAND ----------

drop_testresults_detail_table= "DROP TABLE IF EXISTS stage.test_results_detail;"
submit_remote_query(configs, drop_testresults_detail_table)

# COMMAND ----------

# create test results detail table
testresults_detail_create_query=f"""
CREATE TABLE stage.test_results_detail (
  test_result_detail_id INT identity(1,1),
  test_case_id INT ,
  test_result_id INT ,
  detail_value SUPER
);
"""

# COMMAND ----------

submit_remote_query(configs, testresults_detail_create_query) # create test results table

# COMMAND ----------

testresults_detail_vov_create_query=f"""
CREATE TABLE IF NOT EXISTS stage.test_results_detail_vov
(
	measure VARCHAR(100)   
	,latestversion VARCHAR(100)   
	,prevversion VARCHAR(100)   
	,country_alpha2 VARCHAR(100)   
	,platform_subset VARCHAR(500)   
	,cal_date DATE   
	,newunits DOUBLE PRECISION   
	,prevunits DOUBLE PRECISION   
	,diff DOUBLE PRECISION   
	,chgpct DOUBLE PRECISION   
	,customer_engagement VARCHAR(100)   
	,module_name VARCHAR(100)   
)
;"""

# COMMAND ----------

submit_remote_query(configs, testresults_detail_vov_create_query) # create test results vov table
