# Databricks notebook source
from pyspark.sql.functions import trim, col, lit

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# load new AMS files
ams_s3 = spark.read.csv(path="s3a://enrich-data-lake-restricted-prod/gpsy/insight_product_price_ams*.bz2", header=True)

# COMMAND ----------

# load new APJ files
apj_s3 = spark.read.csv(path="s3a://enrich-data-lake-restricted-prod/gpsy/insight_product_price_apj*.bz2", header=True)

# COMMAND ----------

# load new EU files
eu_s3 = spark.read.csv(path="s3a://enrich-data-lake-restricted-prod/gpsy/insight_product_price_eu*.bz2", header=True)

# COMMAND ----------

# write to stage.gpsy_list_price_stage
ams_s3 = ams_s3.select([trim(col(c)).alias(c.strip().lower().replace(" ", "_")) for c in ams_s3.columns])
apj_s3 = apj_s3.select([trim(col(c)).alias(c.strip().lower().replace(" ", "_")) for c in apj_s3.columns])
eu_s3 = eu_s3.select([trim(col(c)).alias(c.strip().lower().replace(" ", "_")) for c in eu_s3.columns])

write_df_to_redshift(configs, ams_s3.union(apj_s3).union(eu_s3), "stage.gpsy_list_price_stage", "overwrite")

# COMMAND ----------

#  --add record to version table for 'LIST_PRICE'

max_info = call_redshift_addversion_sproc(configs, 'LIST_PRICE', 'GPSY')

max_version = max_info[0]
max_load_date = str(max_info[1])

# COMMAND ----------

redshift_gpsy_list_price_records = read_redshift_to_df(configs) \
    .option("dbtable", "stage.gpsy_list_price_stage") \
    .load()

# COMMAND ----------

stage_gpsy_query = """
SELECT
    product_number
    , country_code
    , currency_code
    , price_term_code
    , CAST(price_start_effective_date AS date) AS price_start_effective_date
    , qbl_sequence_number
    , list_price
    , product_line
FROM stage.gpsy_list_price_stage
"""

redshift_gpsy_prod_list_price_records = read_redshift_to_df(configs) \
    .option("query", stage_gpsy_query) \
    .load()

redshift_gpsy_prod_list_price_records = redshift_gpsy_prod_list_price_records \
    .withColumn("qbl_sequence_number", col("qbl_sequence_number").cast("integer")) \
    .withColumn("list_price", col("list_price").cast("double")) \
    .withColumn("load_date", lit(max_load_date).cast("timestamp")) \
    .withColumn("version", lit(max_version))

write_df_to_redshift(configs, redshift_gpsy_prod_list_price_records, "prod.list_price_gpsy", "overwrite")

# COMMAND ----------

# output dataset to S3 for archival purposes
# write to parquet file in s3

# s3a://dataos-core-dev-team-phoenix/product/list_price_gpsy/
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "spectrum/list_price_gpsy_historical/" + max_version

write_df_to_s3(redshift_gpsy_prod_list_price_records, s3_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# write PROD dataset to SFAI - 2xlarge 2node - 58.37 seconds vs 19 mins on a large 2 node

write_df_to_sqlserver(configs, redshift_gpsy_prod_list_price_records, "ie2_landing.dbo.list_price_gpsy_landing_temp", "overwrite")

# COMMAND ----------

import pymssql

def submit_remote_sqlserver_query(configs, db, query):
    conn = pymssql.connect(server="sfai.corp.hpicloud.net", user=configs["sfai_username"], password=configs["sfai_password"], database=db)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()

submit_remote_sqlserver_query(configs, "ie2_prod", "EXEC ie2_prod.dbo.p_load_list_price_gpsy;")
