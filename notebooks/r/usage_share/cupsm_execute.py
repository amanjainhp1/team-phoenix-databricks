# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from time import time, sleep

# COMMAND ----------

# MAGIC %run ../../python/common/datetime_utils

# COMMAND ----------

# all notebook parameters
dbutils.widgets.text("datestamp", "")
dbutils.widgets.text("timestamp", "")

# cuspm_execute parameters
dbutils.widgets.text("tasks", "")
dbutils.widgets.text("technology", "")

# cupsm_retrieve_inputs parameters
dbutils.widgets.text("bdtbl", "")
dbutils.widgets.text("ib_version", "")

# toner_usage_total parameters
# toner_usage_color parameters

# toner_share parameters
dbutils.widgets.text("cutoff_dt", "")
dbutils.widgets.text("outnm_dt", "")

# COMMAND ----------

# retrieve tasks from widgets/parameters
tasks = dbutils.widgets.get("tasks").split(";")

# define all possible tasks
possible_tasks = ["all", "retrieve_inputs", "usage_total", "usage_color", "share"]

# error and exit if task list contains any erroneous value
for task in tasks:
    if task not in possible_tasks:
        dbutils.notebook.exit("ERROR: tasks list contains at least one erroneous value. Accepted values are {} with a semicolon (;) delimiter.".format(", ".join(possible_tasks)))

# COMMAND ----------

# set default parameters if not provided by user
date = Date()
datestamp = date.getDatestamp() if dbutils.widgets.get("datestamp") == "" else dbutils.widgets.get("datestamp")
timestamp = date.getTimestamp() if dbutils.widgets.get("timestamp") == "" else dbutils.widgets.get("timestamp")

# eligible values: ink, toner
technology = dbutils.widgets.get("technology")

if technology == 'toner':
    bdtbl = "cumulus_prod04_dashboard.dashboard.print_share_usage_agg_stg" if dbutils.widgets.get("bdtbl") == "" else dbutils.widgets.get("bdtbl")
elif technology == 'ink':
    bdtbl = "cumulus_prod02_biz_trans.biz_trans.v_print_share_usage_forecasting" if dbutils.widgets.get("bdtbl") == "" else dbutils.widgets.get("bdtbl")

# COMMAND ----------

# specify notebook/task and notebook path relative to this notebook
notebooks = {
    "toner": {
        "retrieve_inputs": {
            "precedence": 1,
            "notebook": "./cupsm_retrieve_inputs"
        },
        "usage_total": {
            "precedence": 2,
            "notebook": "./toner_usage_total"
        },
        "usage_color": {
            "precedence": 3,
            "notebook": "./toner_usage_color"
        },
        "share": {
            "precedence": 4,
            "notebook": "./toner_share"}
    },
    "ink": {
        "retrieve_inputs": {
            "precedence": 1,
            "notebook": "./cupsm_retrieve_inputs"
        },
        "usage_total": {
            "precedence": 2,
            "notebook": "./ink_usage_total"
        },
        "usage_color": {
            "precedence": 2,
            "notebook": "./ink_usage_color"
        },
        "share": {
            "precedence": 3,
            "notebook": "./ink_share"}
    }
}

# create dict of args
notebook_args = {
            "tasks": f"{dbutils.widgets.get('tasks')}",
            "technology": f"{technology}",
            "datestamp": f"{datestamp}",
            "timestamp": f"{timestamp}",
            "bdtbl": f"{bdtbl}",
            "ib_version": f"{dbutils.widgets.get('ib_version')}",
            "cutoff_dt": f"{dbutils.widgets.get('cutoff_dt')}",
            "outnm_dt": f"{dbutils.widgets.get('outnm_dt')}"
}

# COMMAND ----------

def get_precedences(notebooks: dict, technology: str) -> set:
    precedences = set()
    for notebook_label, notebook_info in notebooks[technology].items():
        precedences.add(notebook_info["precedence"])
    return precedences

def filter_notebooks_by_precedence(precedence: int, technology: str, notebooks: dict, notebook_args: dict) -> dict:
    filtered_notebooks = {}
    for key, value in notebooks[technology].items():
        if value["precedence"] == precedence:
            filtered_notebooks[key] = {"notebook_path": value["notebook"], "notebook_args": notebook_args}
    return(filtered_notebooks)

def run_notebook(notebook: list) -> None:
    try:
        start_time = time()
        print(f"LOG: running {notebook[3]} notebook with parameters {notebook[2]}")
        result = dbutils.notebook.run(notebook[0], timeout_seconds=notebook[1], arguments=notebook[2])
        completion_time = str(round((time() - start_time)/60, 1))
        print(f"LOG: {notebook[3]} notebook completed in {completion_time} minutes")
    except Exception as e:
        print(f"LOG: {notebook[3]} contains a FAILED status")
        result = "FAILED"
    sleep(1)
    return result

# loop through each notebook in order and run
# if a task fails, exit
def run_notebooks(notebooks: dict, technology: str):
    precedences = get_precedences(notebooks, technology)
    
    for precedence in precedences:
        print("LOG: running notebooks with precedence value: " + str(precedence))
        
        filtered_notebooks = filter_notebooks_by_precedence(precedence, technology, notebooks, notebook_args)
        
        if any(task in tasks for task in ["all"]+(list(filtered_notebooks.keys()))):
            
            filtered_notebook_list = []
            for key, value in filtered_notebooks.items():
                filtered_notebook_list.append([value['notebook_path'], 0, value['notebook_args'], key])
            
            with ThreadPoolExecutor(max_workers = 4) as executor: thread = executor.map(run_notebook, filtered_notebook_list)
            if "FAILED" in thread: return

run_notebooks(notebooks, technology)

# COMMAND ----------


