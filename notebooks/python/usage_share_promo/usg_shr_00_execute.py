# Databricks notebook source
import time

# COMMAND ----------

dbutils.widgets.text("tasks", "")
dbutils.widgets.text("version", "")

# COMMAND ----------

# retrieve tasks from widgets/parameters
tasks = dbutils.widgets.get("tasks").split(";")

# define all possible tasks
possible_tasks = ["all", "landing", "npi", "matures", "adjusts", "final"]

# error and exit if task list contains any erroneous value
for task in tasks:
    if task not in possible_tasks:
        dbutils.notebook.exit("ERROR: tasks list contains at least one erroneous value. Accepted values are {} with a semicolon (;) delimiter.".format(", ".join(possible_tasks)))

# COMMAND ----------

# specify notebook/task and notebook path relative to this notebook
notebooks = {
    "landing": "./usg_shr_01_landing",
    "npi": "./usg_shr_02_npi",
    "matures": "./usg_shr_03_matures",
    "adjusts": "./usg_shr_04_adjusts",
    "demand": "./usg_shr_05_demand",
    "final": "./usg_shr_06_final"
}

# loop through each installed-base related notebook in order and run
# if a task fails, exit
for key, value in notebooks.items():
    notebook_path = value
    notebook = notebook_path.split("/")[-1]
    
    if any(task in tasks for task in ["all", f"{key}"]):
        print(f"LOG: running {notebook} notebook")
        
        notebook_args = {"version": f"{dbutils.widgets.get("version")}"}

        try:
            start_time = time.time()
            dbutils.notebook.run(notebook_path, 0, notebook_args)
            completion_time = str(round((time.time() - start_time)/60, 1))
            print(f"LOG: {notebook} notebook completed in {completion_time} minutes")
        except Exception as e:
            raise dbutils.notebook.exit(e)
