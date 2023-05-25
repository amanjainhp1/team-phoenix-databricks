# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Loader
# MAGIC 
# MAGIC The purpose of this script is to take a tables list variable and load the table(s) specified by these variables to the delta lake.
# MAGIC example variable: tables[table name with schema, dataframe to load, mode]

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

import re

for table in tables:
    # Define the input and output formats and paths and the table name.
    full_table_name = table[0]
    schema = table[0].split(".")[0]
    table_name = table[0].split(".")[1]
    mode = table[2]
    write_format = 'delta'

    # Load the data from its source.
    df = table[1]
    print(f'loading {table[0]}...')

    for column in df.dtypes:
        renamed_column = re.sub('\)', '', re.sub('\(', '', re.sub('-', '_', re.sub('/', '_', re.sub('\$', '_dollars', re.sub(' ', '_', column[0])))))).lower()
        df = df.withColumnRenamed(column[0], renamed_column)   
    
    # Write the data to its target.
    df.write \
      .format(write_format) \
      .mode(mode) \
      .option("overwriteSchema", "true")\
      .saveAsTable(table[0])

    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')
