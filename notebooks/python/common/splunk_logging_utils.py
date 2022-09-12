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

# define function used to retrieve a timestamp
def get_timestamp() -> str:
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
    data = create_splunk_data(app, run_id)
    data["job"]["start_timestamp"] = get_timestamp()
    return(data)

def log_job_end(data: dict, status: str) -> None:
    data["job"]["end_timestamp"] = get_timestamp()
    data["job"]["overall_status"] = status
    push_to_splunk(data)
