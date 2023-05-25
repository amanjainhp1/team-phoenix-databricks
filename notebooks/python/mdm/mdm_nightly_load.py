# Databricks notebook source
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from time import sleep
import json

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/splunk_logging_utils

# COMMAND ----------

job_data = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
splunk_data = log_job_start(app=job_data['tags']['jobName'], run_id=job_data['tags']['runId'])

# COMMAND ----------

notebooks = []

try:
    tables = json.loads(dbutils.widgets.get("tables"))
except:
    raise Exception("ERROR: No tables provided in the 'tables' parameter. Please provide a valid value.")
    
date = datetime.today()

datestamp = date.strftime("%Y%m%d")
timestamp= str(int(date.timestamp()))

for table in tables:
    widgets = tables[table]

    widgets["datestamp"] = datestamp
    widgets["timestamp"] = timestamp

    widgets['load_large_tables'] = dbutils.widgets.get('load_large_tables')

    notebooks = notebooks + [["move_sfai_data_to_redshift", 0, widgets]]

# COMMAND ----------

successful_jobs = []
failed_jobs = []

def run_notebook(notebook: list) -> tuple:
    try:
        result = dbutils.notebook.run(notebook[0], timeout_seconds=notebook[1], arguments=notebook[2])
    except:
        result = "FAILED"
    sleep(1)
    return(notebook[2]["destination_table"], result)

with ThreadPoolExecutor(max_workers = 4) as executor:thread = executor.map(run_notebook, notebooks)

for result in thread:
    if "SUCCESS" in result:
        successful_jobs.append(result[0])
    elif "FAILED" in result:
        failed_jobs.append(result[0])

# COMMAND ----------

if len(failed_jobs) > 0:
    log_job_end(splunk_data, "FAILED")
    raise Exception("Job failed. " + str(failed_jobs) + " contains a FAILED status.")
else:
    log_job_end(splunk_data, "SUCCESS")
