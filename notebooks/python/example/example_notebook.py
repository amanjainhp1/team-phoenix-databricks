# Databricks notebook source
task = dbutils.widgets.get("task")

# COMMAND ----------

print("this task is: " + task)

# COMMAND ----------

import dataos_splunk 
import os

data = {"hello": "from niraj"} 
metadata = {"source": "disk"} 

dataos_splunk.send_to_splunk(metadata, data) 

# COMMAND ----------

dbutils.notebook.exit("SUCCESS: " + task)
