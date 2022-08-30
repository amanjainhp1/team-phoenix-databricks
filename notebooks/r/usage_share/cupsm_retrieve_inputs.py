# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

dbutils.widgets.text("bdtbl", "")
dbutils.widgets.text("ib_version", "")
dbutils.widgets.text("tasks", "")
dbutils.widgets.text("technology", "")
dbutils.widgets.text("datestamp", "")
dbutils.widgets.text("timestamp", "")

# COMMAND ----------

# retrieve tasks from widgets/parameters
tasks = dbutils.widgets.get("tasks") if dbutils.widgets.get("tasks") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["tasks"]
tasks = tasks.split(";")

# define all relevenat task parameters to this notebook
relevant_tasks = ["all", "retrieve_inputs"]

# exit if tasks list does not contain a relevant task i.e. "all" or "retrieve_inputs"
for task in tasks:
    if task not in relevant_tasks:
        dbutils.notebook.exit("EXIT: Tasks list does not contain a relevant value i.e. {}.".format(", ".join(relevant_tasks)))

# COMMAND ----------

# set vars equal to widget vals for interactive sessions, else retrieve task values 
bdtbl = dbutils.widgets.get("bdtbl") if dbutils.widgets.get("bdtbl") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["bdtbl"]
ib_version = dbutils.widgets.get("ib_version") if dbutils.widgets.get("ib_version") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["ib_version"]
technology = dbutils.widgets.get("technology") if dbutils.widgets.get("technology") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["technology"]
datestamp = dbutils.widgets.get("datestamp") if dbutils.widgets.get("datestamp") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["datestamp"]
timestamp = dbutils.widgets.get("timestamp") if dbutils.widgets.get("timestamp") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["timestamp"]

# COMMAND ----------

# given a task list, retrieve all needed data
# e.g. usage_color and usage_total

# construct a lookup list of tables, with relevant tasks
all_tables = {
    'toner': {
        'bdtbl': {
            'tasks': ['all'],
            'query': f'SELECT * FROM {bdtbl}'
        },
        'calendar':{
            'tasks': ['all', 'share'],
            'query': f'SELECT * FROM mdm.calendar'
        },
        'hardware_xref': {
            'tasks': ['all'],
            'query': f'SELECT * FROM mdm.hardware_xref'
        },
        'ib': {
            'tasks': ['all'],
            'query': f"""SELECT * FROM prod.ib WHERE version = '{ib_version}'"""
        },
        'iso_cc_rollup_xref': {
            'tasks': ['all'],
            'query': f'SELECT * FROM mdm.iso_cc_rollup_xref'
        },
        'iso_country_code_xref': {
            'tasks': ['all'],
            'query': f'SELECT * FROM mdm.iso_country_code_xref'
        },
        'tri_printer_ref_landing': {
            'tasks': ['all', 'usage_total'],
            'query': f'SELECT * FROM prod.tri_printer_ref_landing'
        }
    },
    'ink': {
        'bdtbl': {
            'tasks': ['all'],
            'query': f'SELECT * FROM {bdtbl}'
        },
        'hardware_xref': {
            'tasks': ['all'],
            'query': f'SELECT * FROM mdm.hardware_xref'
        },
        'ib': {
            'tasks': ['all'],
            'query': f"""SELECT * FROM prod.ib WHERE version = '{ib_version}'"""
        },
        'iso_cc_rollup_xref': {
            'tasks': ['all'],
            'query': f'SELECT * FROM mdm.iso_cc_rollup_xref'
        },
        'iso_country_code_xref': {
            'tasks': ['all'],
            'query': f'SELECT * FROM mdm.iso_country_code_xref'
        },
    }
}

# COMMAND ----------

# create a list of saved tables, and add table each time we go through, then check list with each iteration so as to avoid re-saving a table with more than one task
saved_tables = []

# for all tables, loop through and see if we have passed in relevant tasks
# if so, read in data and save out to parquet files in S3
# e.g. 's3a://dataos-core-dev-team-phoenix/cupsm_inputs/20220520/123456789/iso_country_code_xref/'
for key, value in all_tables[technology].items():
    for task in tasks:
        if task in value['tasks'] and key not in saved_tables:
            saved_tables.append(key)

            df = read_redshift_to_df(configs) \
                .option("query", value['query']) \
                .load()

            df = df_strings_to_upper(df)

            df.write.parquet("{}cupsm_inputs/{}/{}//{}/{}/".format(constants["S3_BASE_BUCKET"][stack], technology, datestamp, timestamp, key), mode = "overwrite")
