# Databricks notebook source
import pyspark.sql.functions as f
import time
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

max_version_ib = "2023.02.16.1"

source = "prod.ib"
destination = "IE2_Prod.dbo.ib"
mode = "append"

destination_df = read_sql_server_to_df(configs) \
    .option("dbtable", destination) \
    .load()

destination_cols = destination_df.columns

source_df = read_redshift_to_df(configs) \
    .option("dbtable", source) \
    .load()

source_df = source_df.filter(f"version = '{max_version_ib}'")

source_df = source_df.withColumnRenamed("country_alpha2", "country")

source_df = source_df.select(destination_cols)

# source_df.show()

# COMMAND ----------

    rows = source_df.count()
    n_partitions = rows//1048576
    source_df = source_df.repartition(n_partitions+1)  
    
    write_df_to_sqlserver(configs, source_df, destination, mode)
