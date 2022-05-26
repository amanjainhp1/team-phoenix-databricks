# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# given a task list, retrieve all needed data
# e.g. usage_color and usage_total

# construct a lookup list of tables, with relevant tasks
all_tables = {
    'bdtbl': {
        'tasks': ['all'],
        'query': f'SELECT * FROM {dbutils.widgets.get("bdtbl")}'
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
        'query': f"""SELECT * FROM prod.ib WHERE version = '{dbutils.widgets.get("ib_version")}'"""
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
}

# COMMAND ----------

# create a list of saved tables, and add table each time we go through, then check list with each iteration so as to avoid re-saving a table with more than one task
saved_tables = []

# for all tables, loop through and see if we have passed in relevant tasks
# if so, read in data and save out to parquet files in S3
# e.g. 's3a://dataos-core-dev-team-phoenix/cupsm_inputs/20220520/123456789/iso_country_code_xref/'
for key, value in all_tables.items():
    for task in dbutils.widgets.get('tasks').split(';'):
        if task in value['tasks'] and key not in saved_tables:
            saved_tables.append(key)
            
            df = read_redshift_to_df(configs) \
              .option("query", value['query']) \
              .load()
                    
            for column in df.dtypes:
                if column[1] == 'string':
                    df = df.withColumn(column[0], f.upper(f.col(column[0])))
            
            df.write.parquet("{}cupsm_inputs/{}/{}//{}/{}/".format(constants["S3_BASE_BUCKET"][stack], "toner", dbutils.widgets.get("datestamp"), dbutils.widgets.get("timestamp"), key), mode = "overwrite")
