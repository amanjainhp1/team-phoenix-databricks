# Databricks notebook source
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from time import sleep

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

notebooks = []

try:
    tables = dbutils.widgets.get("tables")
except:
    tables = [
        [["sqlserver", "IE2_Prod", "dbo", "calendar"], ["redshift", stack, "mdm", "calendar"]],
        [["sqlserver", "IE2_Prod", "dbo", "ce_splits"], ["redshift", stack, "prod", "ce_splits"]],
        [["sqlserver", "IE2_Prod", "dbo", "decay"], ["redshift", stack, "prod", "decay"]],
        [["sqlserver", "IE2_Prod", "dbo", "hardware_xref"], ["redshift", stack, "mdm", "hardware_xref"]],
        [["sqlserver", "IE2_Prod", "dbo", "instant_ink_enrollees"], ["redshift", stack, "prod", "instant_ink_enrollees"]],
        [["sqlserver", "IE2_Prod", "dbo", "instant_ink_enrollees_ltf"], ["redshift", stack, "prod", "instant_ink_enrollees_ltf"]],
        [["sqlserver", "IE2_Prod", "dbo", "iso_cc_rollup_xref"], ["redshift", stack, "mdm", "iso_cc_rollup_xref"]],
        [["sqlserver", "IE2_Prod", "dbo", "iso_country_code_xref"], ["redshift", stack, "mdm", "iso_country_code_xref"]],
        [["sqlserver", "IE2_Prod", "dbo", "printer_lag"], ["redshift", stack, "mdm", "printer_lag"]],
        [["sqlserver", "IE2_Prod", "dbo", "product_line_xref"], ["redshift", stack, "mdm", "product_line_xref"]],
        [["sqlserver", "IE2_Prod", "dbo", "profit_center_code_xref"], ["redshift", stack, "mdm", "profit_center_code_xref"]],
        [["sqlserver", "IE2_Prod", "dbo", "supplies_hw_mapping"], ["redshift", stack, "mdm", "supplies_hw_mapping"]],
        [["sqlserver", "IE2_Prod", "dbo", "supplies_xref"], ["redshift", stack, "mdm", "supplies_xref"]],
        [["sqlserver", "IE2_Prod", "dbo", "yield"], ["redshift", stack, "mdm", "yield"]]
    ]
    
date = datetime.today()

datestamp = date.strftime("%Y%m%d")
timestmap= str(int(date.timestamp()))

for table in tables:
    widgets = {}
    
    widgets["datestamp"] = datestamp
    widgets["timestamp"] = timestamp
    
    widgets["source_system"] = table[0][0]
    widgets["source_database"] = table[0][1]
    widgets["source_schema"] = table[0][2]
    widgets["source_table"] = table[0][3]
    
    widgets["destination_system"] = table[1][0]
    widgets["destination_database"] = table[1][1]
    widgets["destination_schema"] = table[1][2]
    widgets["destination_table"] = table[1][3]
    
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

# COMMAND ----------


