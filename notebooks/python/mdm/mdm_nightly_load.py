# Databricks notebook source
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from time import sleep
import json

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

notebooks = []

try:
    tables = json.loads(dbutils.widgets.get("tables"))
except:
    tables = json.loads("""
        {
            "calendar": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "calendar",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "mdm",
                "destination_table": "calendar"
            },
            "ce_splits": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "ce_splits",
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "prod", 
                "destination_table": "ce_splits"
            },
            "decay": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod", 
                "source_schema": "dbo", 
                "source_table": "decay",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "prod", 
                "destination_table": "decay"
            },
            "hardware_xref": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "hardware_xref", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "mdm", 
                "destination_table": "hardware_xref"
            },
            "instant_ink_enrollees": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "instant_ink_enrollees", 
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "prod", 
                "destination_table": "instant_ink_enrollees"
            },
            "instant_ink_enrollees_ltf": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "instant_ink_enrollees_ltf", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "prod", 
                "destination_table": "instant_ink_enrollees_ltf"
            },
            "iso_cc_rollup_xref": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo", 
                "source_table": "iso_cc_rollup_xref", 
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "mdm", 
                "destination_table": "iso_cc_rollup_xref"
            },
            "iso_country_code_xref": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo", 
                "source_table": "iso_country_code_xref", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "mdm", 
                "destination_table": "iso_country_code_xref"
            },
            "printer_lag": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "printer_lag", 
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "mdm",
                "destination_table": "printer_lag"
            },
            "product_line_xref": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "product_line_xref",
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "mdm", 
                "destination_table": "product_line_xref"
            },
            "profit_center_code_xref": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod",
                "source_schema": "dbo", 
                "source_table": "profit_center_code_xref", 
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "mdm",
                "destination_table": "profit_center_code_xref"
            },
            "supplies_hw_mapping": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "supplies_hw_mapping",
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "mdm",
                "destination_table": "supplies_hw_mapping"
            },
            "supplies_xref": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "supplies_xref",
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "mdm",
                "destination_table": "supplies_xref"
            },
            "yield": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "yield",
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "mdm",
                "destination_table": "yield"
            }
        }
    """)
    
date = datetime.today()

datestamp = date.strftime("%Y%m%d")
timestamp= str(int(date.timestamp()))

for table in tables:
    widgets = tables[table]

    widgets["datestamp"] = datestamp
    widgets["timestamp"] = timestamp
    
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
    dbutils.notebook.exit("Job failed. " + str(failed_jobs) + " contains a FAILED status.")
