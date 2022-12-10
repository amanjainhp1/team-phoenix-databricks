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
    schema = table[0].split(".")[0]
    table_name = table[0].split(".")[1]
    mode = table[2]
    write_format = 'delta'
    save_path = f'/tmp/delta/{schema}/{table_name}'
    
    # Load the data from its source.
    df = table[1]
    renamed_column = re.sub('\)', '', re.sub('\(', '', re.sub('-', '_', re.sub('/', '_', re.sub('\$', '_dollars', re.sub(' ', '_', column[0])))))).lower()
    renamed_df = df.withColumnRenamed(column[0], renamed_column)
    print(f'loading {table[0]}...')
    # Write the data to its target.
    renamed_df.write \
      .format(write_format) \
      .mode(mode) \
      .option("overwriteSchema", "true")\
      .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Create the table.
    spark.sql("CREATE TABLE IF NOT EXISTS " + table[0] + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')
