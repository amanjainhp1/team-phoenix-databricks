# Databricks notebook source
dbutils.widgets.text("tasks", "")

# COMMAND ----------

# retrieve tasks from widgets/parameters
tasks = dbutils.widgets.get("tasks").split(";")

# error and exit if task list contains any erroneous value
for task in tasks:
    if task not in ["all", "norm-ships", "ib-base", "ib-promo"]:
        dbutils.notebook.exit("ERROR: tasks list contains at least one erroneous value. Accepted values are all, norm-ships, ib-base, and ib-promo with a semicolon (;) delimiter.")

# COMMAND ----------

import time

# specify notebook/task and notebook path relative to this notebook
notebooks = {
    "norm-ships": "./base/norm_shipments/norm_ships",
    "ib-base": "./base/ib/installed_base_full",
    "ib-promo": "./base_promotion/ib_promo_full",
}

# loop through each installed-base related notebook in order and run
# if a task fails, exit
for key, value in notebooks.items():
    notebook_path = value
    notebook = notebook_path.split("/")[-1]
    
    if any(task in tasks for task in ["all", f"{key}"]):
        print(f"LOG: running {notebook} notebook")
        
        notebook_args = {}

        try:
            start_time = time.time()
            dbutils.notebook.run(notebook_path, 0, notebook_args)
            completion_time = str(round((time.time() - start_time)/60, 1))
            print(f"LOG: {notebook} notebook completed in {completion_time} minutes")
        except Exception as e:
            raise dbutils.notebook.exit(e)
