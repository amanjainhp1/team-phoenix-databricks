# Databricks notebook source
from datetime import date

# COMMAND ----------

# MAGIC %run ../common/datetime_utils

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

# *_share parameters
dbutils.widgets.text("writeout", "")
dbutils.widgets.text("cutoff_dt", "")
dbutils.widgets.text("outnm_dt", "")

# COMMAND ----------

# retrieve tasks from widgets/parameters
tasks = dbutils.widgets.get("tasks").split(";")

# define all possible tasks
possible_tasks = ["all", "retrieve_inputs", "usage_total", "usage_color", "share", "proxy_locking"]

# error and exit if task list contains any erroneous value
for task in tasks:
    if task not in possible_tasks:
        raise Exception("ERROR: tasks list contains at least one erroneous value. Accepted values are {} with a semicolon (;) delimiter.".format(", ".join(possible_tasks)))

# COMMAND ----------

# set default parameters if not provided by user
date = Date()
datestamp = date.getDatestamp() if dbutils.widgets.get("datestamp") == "" else dbutils.widgets.get("datestamp")
timestamp = date.getTimestamp() if dbutils.widgets.get("timestamp") == "" else dbutils.widgets.get("timestamp")

# eligible values: ink, toner
technology = dbutils.widgets.get("technology")

bdtbl = ""
if technology == 'toner':
    bdtbl = "cumulus_prod04_dashboard.dashboard.print_share_usage_agg_stg" if dbutils.widgets.get("bdtbl") == "" else dbutils.widgets.get("bdtbl")
elif technology == 'ink':
    bdtbl = "cumulus_prod02_biz_trans.biz_trans.v_print_share_usage_forecasting" if dbutils.widgets.get("bdtbl") == "" else dbutils.widgets.get("bdtbl")

# COMMAND ----------

# create dict of args
common_notebook_args = {
            "tasks": f"{dbutils.widgets.get('tasks')}",
            "technology": f"{technology}",
            "datestamp": f"{datestamp}",
            "timestamp": f"{timestamp}",
            "bdtbl": f"{bdtbl}",
            "ib_version": f"{dbutils.widgets.get('ib_version')}",
            "cutoff_dt": f"{dbutils.widgets.get('cutoff_dt')}",
            "outnm_dt": f"{dbutils.widgets.get('outnm_dt')}",
            "writeout": f"{dbutils.widgets.get('writeout')}"
}

# for jobs pass args via task values
dbutils.jobs.taskValues.set(key = "args", value = common_notebook_args)