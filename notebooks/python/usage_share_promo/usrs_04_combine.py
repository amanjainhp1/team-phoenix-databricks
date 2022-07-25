# Databricks notebook source
# MAGIC %md
# MAGIC # Usage Share Refactor Step 04
# MAGIC - Combine data

# COMMAND ----------

# for interactive sessions, define a version widget
dbutils.widgets.text("version", "")

# COMMAND ----------

# for interactive sessions, define a version widget
dbutils.widgets.text("version", "")

# COMMAND ----------

import pandas as pd
import numpy as mp
import psycopg2 as ps

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# Read in Current data
#current_table=need to get step 01 results
#current_table.createOrReplaceTempView("current_table")

# COMMAND ----------

# Read in Mature data
matures_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}/usage_share_promo/matures_norm_final_landing.parquet")
matures_table.createOrReplaceTempView("matures_table")

# COMMAND ----------

# Read in NPI data
npi_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}/usage_share_promo/npi_norm_final_landing.parquet")
npi_table.createOrReplaceTempView("npi_table")

# COMMAND ----------

# Get product lifecycle status information
hardware_info = read_redshift_to_df(configs) \
  .option("query","""
    SELECT platform_subset, product_lifecycle_status, product_lifecycle_status_usage, product_lifecycle_status_share
    FROM "mdm"."hardware_xref"
    """) \
  .load()
hardware_info.createOrReplaceTempView("hardware_info")

# COMMAND ----------

current_1 = """

SELECT c.record
      ,c.cal_date
      ,c.geography_grain
      ,c.country_alpha2
      ,c.platform_subset
      ,c.customer_engagement
      ,c.forecast_process_note
      ,c.post_processing_note
      ,c.data_source
      ,c.version
      ,c.measure
      ,c.units
      ,c.proxy_used
      ,c.ib_version
      ,c.load_date
      ,h.product_lifecycle_status
      ,h.product_lifecycle_status_usage
      ,h.product_lifecycle_status_share
FROM current_table c
LEFT JOIN hardware_info
    ON c.platform_subset=h.platform_subset
"""

current_1=spark.sql(current_1)
current_1.createOrReplaceTempView("current_1")
