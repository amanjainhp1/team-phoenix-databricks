dbutils.widgets.text("ib_version", "") # set ib version to mark as official
dbutils.widgets.text("ns_version", "") # set norm_shipments version to mark as official

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

ib_version = dbutils.widgets.get("ib_version")
if ib_version == "":
    ib_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM prod.version WHERE record = 'IB'") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()

ns_version = dbutils.widgets.get("ns_version")
if ns_version == "":
    ns_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM prod.version WHERE record = 'NORM_SHIPMENTS'") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

submit_remote_query(configs, "UPDATE prod.ib SET official = 1 WHERE version = {ib_version};") # update ib table
submit_remote_query(configs, "UPDATE prod.version SET official = 1 WHERE version = {ib_version} AND record = 'IB';") # update version table
submit_remote_query(configs, "UPDATE prod.version SET official = 1 WHERE version = {ns_version} AND record = 'NORM_SHIPMENTS';") # update version table

# COMMAND ----------

import pymssql

def submit_remote_sqlserver_query(configs, db, query):
    conn = pymssql.connect(server="sfai.corp.hpicloud.net", user=configs["sfai_username"], password=configs["sfai_password"], database=db)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()

submit_remote_sqlserver_query(configs, "IE2_Prod", "UPDATE IE2_Prod.dbo.version SET official = 1 WHERE version = {ib_version} AND record = 'IB';")
submit_remote_sqlserver_query(configs, "IE2_Prod", "UPDATE IE2_Prod.dbo.version SET official = 1 WHERE version = {ns_version} AND record = 'NORM_SHIPMENTS';")
