# Databricks notebook source
from datetime import datetime
import json
import dataos_splunk

# COMMAND ----------

# example log:
# app: mdm-nightly-load-dev
# job:{
#     end_timestamp: 2022-09-02 16:45:34
#     overall_status: SUCCESS
#     run_id: 1234567890
#     start_timestamp: 2022-09-02 17:45:34
# }

# retrieve job data from Databricks
job_data = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()

# define function used to retrieve a timestamp
def get_timestamp():
    return(str(datetime.today().strftime("%Y-%m-%d %H:%M:%S")))

def create_splunk_data(app: str, run_id: str) -> dict:
    data = {}
    data["app"] = app
    data["run_id"] = run_id
    return(data)

def push_to_splunk(data: dict, source_type="databricks") -> None:
    metadata = {"source": "databricks", "sourcetype": source_type}
    dataos_splunk.send_to_splunk(metadata, json.dumps(data, indent=4))

def log_job_start(app: str, run_id: str) -> dict:
    timestamp = get_timestamp()
    data = create_splunk_data(app, run_id)
    data["job"]["start_timestamp"] = timestamp
    return(data)

def log_job_end(data: dict, status: str) -> None:
    timestamp = get_timestamp()
    data["job"]["end_timestamp"] = timestamp
    data["job"]["overall_status"] = status
    push_to_splunk(data)