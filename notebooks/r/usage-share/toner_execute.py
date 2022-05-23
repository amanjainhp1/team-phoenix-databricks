# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from time import time, sleep

# COMMAND ----------

# toner_execute parameters
dbutils.widgets.text("tasks", "")

# all toner_* parameters
dbutils.widgets.text("ib_version", "")
dbutils.widgets.text("bdtbl", "")
dbutils.widgets.text("outnm_dt", "")
dbutils.widgets.text("ib_version", "")

# toner_usage_color & toner_share parameters
dbutils.widgets.text("upm_date", "")

# toner_share parameters
dbutils.widgets.text("bdtbl", "")
dbutils.widgets.text("cutoff_dt", "")
dbutils.widgets.text("upm_date_color", "")
dbutils.widgets.text("writeout", "")

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

# set default dates if not specified
today = str(date.today())

upm_date = today if dbutils.widgets.get("upm_date") == "" else dbutils.widgets.get("upm_date")
upm_date_color = today if dbutils.widgets.get("upm_date_color") == "" else dbutils.widgets.get("upm_date_color")

bdtbl = "cumulus_prod04_dashboard.dashboard.print_share_usage_agg_stg" if  dbutils.widgets.get("bdtbl")=="" else dbutils.widgets.get("bdtbl")

writeout = "false" if dbutils.widgets.get("writeout") == "" else dbutils.widgets.get("writeout")

# COMMAND ----------

# specify notebook/task and notebook path relative to this notebook
notebooks = {
    "retrieve_inputs": {
        "precedence": "1",
        "notebook": "./toner_retrieve_inputs"
    },
    "usage_total": {
        "precedence": "2",
        "notebook": "./toner_usage_total"
    },
    "usage_color": {
        "precedence": "3",
        "notebook": "./toner_usage_color"
    },
    "share": {
        "precedence": "4",
        "notebook": "./toner_share"}
}

# create dict of args
notebook_args = {
            "tasks": f"{dbutils.widgets.get('tasks')}",
            "ib_version": f"{dbutils.widgets.get('ib_version')}",
            "upm_date": f"{upm_date}",
            "upm_color_date": f"{upm_date_color}",
            "bdtbl": f"{bdtbl}",
            "cutoff_dt": f"{dbutils.widgets.get('cutoff_dt')}",
            "outnm_dt": f"{dbutils.widgets.get('outnm_dt')}",
            "writeout": f"{writeout}"
}

# COMMAND ----------

def get_precedences(notebooks: dict) -> set:
    precedences = set()
    for key, value in notebooks.items():
        precedences.add(value["precedence"])
    return precedences

def filter_notebooks_by_precedence(precedence: int, notebooks: dict, notebook_args: dict) -> dict:
    filtered_notebooks = {}
    for key, value in notebooks.items():
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
def run_notebooks(notebooks: dict):
    precedences = get_precedences(notebooks)
    
    for precedence in precedences:
        print("LOG: running notebooks with precedence value: " + str(precedence))
        
        filtered_notebooks = filter_notebooks_by_precedence(precedence, notebooks, nmotebook_args)
        
        if any(task in tasks for task in ["all", f"{list(filtered_notebooks.keys())}"]):
            
            filtered_notebook_list = []
            for key, value in filtered_notebooks.items():
                filtered_notebook_list.append([value['notebook_path'], 0, value['notebook_args'], key])
            
            with ThreadPoolExecutor(max_workers = 4) as executor: thread = executor.map(run_notebook, filtered_notebook_list)
            if "FAILED" in thread: return

run_notebooks(notebooks)
