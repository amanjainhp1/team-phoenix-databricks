# Databricks notebook source
dbutils.widgets.text("version", "")

# COMMAND ----------

# MAGIC %run ../notebooks/python/common/configs

# COMMAND ----------

# MAGIC %run ../notebooks/python/common/database_utils

# COMMAND ----------

tables = {
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
    "ib_datamart_source": {
        "source": "prod.ib_datamart_source",
        "destination": "IE2_Prod.dbo.ib_datamart_source",
        "action": "overwrite"
    },
    "norm_ships_split_lag": {
        "source": "prod.norm_ships_split_lag",
        "destination": "IE2_Prod.dbo.norm_ships_split_lag",
        "action": "append"
    },
    "norm_ships_ce": {
        "source": "prod.norm_shipments_ce",
        "destination": "IE2_Prod.dbo.norm_shipments_ce",
        "action": "append"
    }
}

# COMMAND ----------

for table in tables.items():

    source = table[1]["source"]
    destination = table[1]["destination"]
    mode = table[1]["action"]
    
    print("LOG: loading {} to {}".format(source, destination))
    
    destination_cols = read_sql_server_to_df(configs) \
        .option("dbtable", destination) \
        .load() \
        .columns

    source_df = read_redshift_to_df(configs) \
        .option("dbtable", source) \
        .load() \
        .filter(f"version = '{dbutils.widgets.get('version')}'") \
        .select(destination_cols)
    
    # re-partition the data to get as close as to 1048576 rows per partition as possible
    # see https://devblogs.microsoft.com/azure-sql/partitioning-on-spark-fast-loading-clustered-columnstore-index/
    rows = source_df.count()
    n_partitions = rows//1048576
    source_df = source_df.repartition(n_partitions+1)  

    if mode == "append":
        write_df_to_sqlserver(configs, destination, mode)
    elif mode == "overwrite":
        write_df_to_sqlserver(configs, destination "append", "", f"truncate {destination}")

    print("LOG: loaded {} to {}".format(source, destination))