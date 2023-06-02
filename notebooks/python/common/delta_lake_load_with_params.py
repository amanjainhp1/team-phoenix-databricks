# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Loader
# MAGIC 
# MAGIC The purpose of this script is to take a tables list variable and load the table(s) specified by these variables to the delta lake.
# MAGIC example variable: tables[table name with schema, dataframe to load, mode]

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

import re

for table in tables:
    # Define the input and output formats and paths and the table name.
    full_table_name = table[0]
    mode = table[2]

    # Load the data from its source.
    df = table[1]
    print(f'loading {table[0]}...')
    
    for column in df.dtypes:
        column_wo_parentheses = re.sub('\(|\)', '', column[0].lower())
        column_w_underscores = re.sub('-| |/', '_', column_wo_parentheses)
        renamed_column = re.sub('\$', '_dollars', column_w_underscores)
        df = df.withColumnRenamed(column[0], renamed_column)   
    
    # Write the data to its target.
    df.write \
      .format('delta') \
      .mode(mode) \
      .option("overwriteSchema", "true")\
      .saveAsTable(table[0])

    print(f'{table[0]} loaded')
