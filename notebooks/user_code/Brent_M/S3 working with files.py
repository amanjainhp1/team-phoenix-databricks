# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run  ../../python/common/database_utils

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# list files in a location
dbutils.fs.ls("s3a://enrich-data-lake-restricted-prod/gpsy")

# COMMAND ----------

# read files into a dataframe, with a wildcard (*)
# load new AMS files
ams_s3 = spark.read.csv(path="s3a://enrich-data-lake-restricted-prod/gpsy/insight_product_price_ams*.bz2", header=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC #list file details in an s3 bucket
# MAGIC 
# MAGIC aws s3 ls s3://enrich-data-lake-restricted-prod/gpsy/

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC aws s3 ls s3://hp-bigdata-prod-enrichment/ie2_deliverables/rdma/

# COMMAND ----------

import boto3
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as func
from pyspark.sql.types import DoubleType

# COMMAND ----------

def retrieve_latest_s3_object_by_prefix(bucket, prefix):
    s3 = boto3.resource('s3')
    objects = list(s3.Bucket(bucket).objects.filter(Prefix=prefix))
    objects.sort(key=lambda o: o.last_modified)
    return objects[-1].key

#rdma_base_bucket = "hp-bigdata-prod-enrichment"
gpsy_base_bucket = "enrich-data-lake-restricted-prod"

#rdma_base_to_sales_latest_file = retrieve_latest_s3_object_by_prefix(rdma_base_bucket, "ie2_deliverables/rdma/rdma_base_to_sales")
#rdma_base_to_sales_latest_file = rdma_base_to_sales_latest_file.split("/")[len(rdma_base_to_sales_latest_file.split("/"))-1]
#print(rdma_base_to_sales_latest_file)

gpsy_latest_file = retrieve_latest_s3_object_by_prefix(gpsy_base_bucket, "/gpsy")
gpsy_latest_file = gpsy_latest_file.split("/")[len(gpsy_latest_file.split("/"))-1]
print(gpsy_latest_file)
