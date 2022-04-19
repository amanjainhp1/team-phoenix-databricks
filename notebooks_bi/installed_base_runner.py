# Databricks notebook source
# retrieve tasks from widgets/parameters
tasks = dbutils.widgets.get("tasks").split(";")

# error and exit if task list contains any erroneous value
for task in tasks:
    if task not in ["all", "norm-ships", "ib-base", "ib-scenario", "ib-promo"]:
        dbutils.notebook.exit("ERROR: tasks list contains at least one erroneous value. Accepted values are all, norm-ships, ib-base, ib-scenario, and ib-promo with a semicolon (;) delimiter.")

# COMMAND ----------

import time

# specify notebook/task and notebook path relative to this notebook
notebooks = {
    "norm-ships": "./base/norm_shipments/norm_ships",
    "ib-base": "./base/ib/installed_base_full",
    "ib-scenario": "./base_promotion/insert_into_scenario_ns_ib_records",
    "ib-promo": "./base_promotion/ib_promo_full",
}

# loop through each installed-base related notebook in order and run
# if a task fails, exit
for key, value in notebooks.items():
    notebook_path = value
    notebook = notebook_path.split("/")[-1]
    
    if any(task in tasks for task in ["all", f"{key}"]):
        print(f"LOG: running {notebook} notebook")
        try:
            start_time = time.time()
            dbutils.notebook.run(notebook_path, 0)
            completion_time = str(int(time.time() - start_time))
            print(f"LOG: {notebook} notebook completed in {completion_time} seconds")
        except WorkflowException as e:
           dbutils.notebook.exit(e)

# COMMAND ----------


