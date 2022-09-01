# Databricks notebook source
task = dbutils.widgets.get("task")

# COMMAND ----------

print("this task is: " + task)

# COMMAND ----------

import os
import dataos_splunk

def splunk_logger(level, message, sourceType="databricks"):
    data = {"level": level,
            "message": message}
    metadata = {"source": "databricks", "sourcetype": sourceType}
    dataos_splunk.send_to_splunk(metadata, data)

# COMMAND ----------

splunk_logger(level='INFO', message=f'team-phoenix example-multitask-dev {task} successful')

# COMMAND ----------

dbutils.notebook.exit("SUCCESS: " + task)
