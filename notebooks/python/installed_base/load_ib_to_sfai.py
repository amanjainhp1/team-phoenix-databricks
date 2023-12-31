# Databricks notebook source
import pyspark.sql.functions as f
import time
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

tables = {
    "version": {
        "source": "prod.version",
        "destination": "IE2_Prod.dbo.version",
        "action": "append"
    },
    "norm_shipments":{
        "source": "prod.norm_shipments",
        "destination": "IE2_Prod.dbo.norm_shipments",
        "action": "append"
    },
    "ib": {
        "source": "prod.ib",
        "destination": "IE2_Prod.dbo.ib",
        "action": "append"
    },
    "norm_ships_ce": {
        "source": "prod.norm_shipments_ce",
        "destination": "IE2_Prod.dbo.norm_shipments_ce",
        "action": "append"
    },
    "scenario": {
        "source": "prod.scenario",
        "destination": "IE2_Prod.dbo.scenario",
        "action": "append"
    },
    "ib_datamart_source": {
        "source": "prod.ib_datamart_source_vw",
        "destination": "IE2_Prod.dbo.ib_datamart_source",
        "action": "overwrite"
    }
}

# COMMAND ----------

max_version_ib = ""
max_version_ns = ""

for table in tables.items():
    start_time = time.time()

    source = table[1]["source"]
    destination = table[1]["destination"]
    mode = table[1]["action"]
    
    print("LOG: loading {} to {}".format(source, destination))
    
    destination_df = read_sql_server_to_df(configs) \
        .option("dbtable", destination) \
        .load()

    destination_cols = destination_df.columns

    source_df = read_redshift_to_df(configs) \
        .option("dbtable", source) \
        .load()

    # for prod.version, select latest record for IB & latest record for NORM_SHIPMENTS
    if "version" in source:
        w = Window.partitionBy('record')
        source_df = source_df.withColumn('max_version', f.max('version').over(w)) \
            .where('record IN ("IB", "NORM_SHIPMENTS")') \
            .where(f.col('version') == f.col('max_version'))
        
        max_version_ib = source_df.where('record = "IB"') \
            .select('max_version') \
            .head()[0]
        
        max_version_ns = source_df.where('record = "NORM_SHIPMENTS"') \
            .select('max_version') \
            .head()[0]
        
        print("LOG: max_version_ib: " + max_version_ib)
        print("LOG: max_version_ns: " + max_version_ns)
        
        source_df = source_df.drop('max_version') \
            .withColumn('sub_version', f.lit(1).cast('integer'))
    
    # for prod.scenario, select all records grouped by latest load_date
    elif "scenario" in source:
        w = Window.partitionBy('record')
        source_df = source_df \
            .filter((col("record") != "ALLOCATED FLASH PLUS LTF")) \
            .withColumn('max_load_date', f.max('load_date').over(w)) \
            .where(f.col('load_date') == f.col('max_load_date')) \
            .drop('max_load_date') \
            .distinct()
        
        # do a left outer join with SFAI data to prevent loading of duplicate data (primary key violation)
        cond = [source_df.scenario_name == destination_df.scenario_name, source_df.record == destination_df.record, source_df.version == destination_df.version]
        source_df = source_df \
            .alias("sd") \
            .join(destination_df.alias("dd"), on = cond, how = 'left_outer') \
            .filter("dd.scenario_name IS NULL") \
            .select("sd.scenario_name", "sd.record", "sd.version", "sd.load_date")
    # else if norm_ships, filter to latest version
    elif "norm_ship" in source:
        source_df = source_df.filter(f"version = '{max_version_ns}'")
    # else if ib, ib_datamart_source, filter to latest_version
    elif table[0] in ['ib', 'ib_datamart_source']:
        source_df = source_df.filter(f"version = '{max_version_ib}'")
    
    # if ib, rename country_alpha2 col
    if table[0] == "ib":
        source_df = source_df.withColumnRenamed("country_alpha2", "country")
    
    source_df = source_df.select(destination_cols)
    source_df.show()
    
    # re-partition the data to get as close as to 1048576 rows per partition as possible
    # see https://devblogs.microsoft.com/azure-sql/partitioning-on-spark-fast-loading-clustered-columnstore-index/
    rows = source_df.count()
    n_partitions = rows//1048576
    source_df = source_df.repartition(n_partitions+1)  

    if mode == "append":
        write_df_to_sqlserver(configs, source_df, destination, mode)
    elif mode == "overwrite":
        write_df_to_sqlserver(configs, source_df, destination, "append", "", f"truncate {destination}")

    completion_time = str(round((time.time()-start_time)/60, 1))
    print("LOG: loaded {} to {} in {} minutes".format(source, destination, completion_time))
