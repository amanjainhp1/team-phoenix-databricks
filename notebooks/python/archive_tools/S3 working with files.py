# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

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
