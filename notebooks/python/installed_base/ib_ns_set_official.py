# Databricks notebook source
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
        .rdd.flatMap(lambda x: x).collect()[0]

ns_version = dbutils.widgets.get("ns_version")
if ns_version == "":
    ns_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM prod.version WHERE record = 'NORM_SHIPMENTS'") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]

# COMMAND ----------

submit_remote_query(configs, f"UPDATE prod.ib SET official = 1 WHERE version = '{ib_version}';") # update ib table
submit_remote_query(configs, f"UPDATE prod.version SET official = 1 WHERE version = '{ib_version}' AND record = 'IB';") # update version table
submit_remote_query(configs, f"UPDATE prod.version SET official = 1 WHERE version = '{ns_version}' AND record = 'NORM_SHIPMENTS';") # update version table

# COMMAND ----------

ib_spectrum_query = f"""
SELECT
    record,
    cal_date,
    country_alpha2,
    platform_subset,
    customer_engagement,
    measure,
    units,
    official,
    load_date,
    version 
FROM prod.ib
WHERE version = '{ib_version}'
"""

ib_spectrum_records = read_redshift_to_df(configs) \
    .option("query", ib_spectrum_query) \
    .load()

s3_ib_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/ib/" + ib_version

write_df_to_s3(ib_spectrum_records, s3_ib_output_bucket, "parquet", "overwrite")

# COMMAND ----------

import pymssql

def submit_remote_sqlserver_query(configs, db, query):
    conn = pymssql.connect(server="sfai.corp.hpicloud.net", user=configs["sfai_username"], password=configs["sfai_password"], database=db)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()

submit_remote_sqlserver_query(configs, "IE2_Prod", f"UPDATE IE2_Prod.dbo.version SET official = 1 WHERE version = '{ns_version}' AND record = 'NORM_SHIPMENTS';")
