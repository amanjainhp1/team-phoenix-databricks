# Databricks notebook source
# MAGIC %md
# MAGIC # Usage Share Refactor Step 04
# MAGIC - Add NPIs into data
# MAGIC - This portion will be run each time an update is required

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
current_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/matures_current_landing")
current_table.createOrReplaceTempView("current_table")

# COMMAND ----------

# Read in NPI data
npi_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/npi_norm_final_landing")
npi_table.createOrReplaceTempView("npi_table")

# COMMAND ----------

# Get product lifecycle status information
hardware_info = read_redshift_to_df(configs) \
  .option("query","""
    SELECT platform_subset, product_lifecycle_status, product_lifecycle_status_usage, product_lifecycle_status_share, epa_family
    FROM "mdm"."hardware_xref"
    """) \
  .load()
hardware_info.createOrReplaceTempView("hardware_info")

# COMMAND ----------

current_1 = """

SELECT c.record
      ,c.cal_date
      ,c.geography_grain
      ,c.geography
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
      ,CONCAT(c.platform_subset,c.customer_engagement,c.geography,c.cal_date) as grp_id
FROM current_table c
LEFT JOIN hardware_info h
    ON c.platform_subset=h.platform_subset
"""

current_1=spark.sql(current_1)
current_1.createOrReplaceTempView("current_1")

# COMMAND ----------

npi_1 = """

SELECT c.record
      ,c.cal_date
      ,c.geography_grain
      ,c.geography
      ,c.platform_subset
      ,c.customer_engagement
      ,c.forecast_process_note
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
      ,CONCAT(c.platform_subset,c.customer_engagement,c.geography,c.cal_date) as grp_id
FROM npi_table c
LEFT JOIN hardware_info h
    ON c.platform_subset=h.platform_subset
WHERE c.units >0 
    AND h.product_lifecycle_status = 'N' or h.product_lifecycle_status_usage='N' or product_lifecycle_status_share='N'
"""

npi_1=spark.sql(npi_1)
npi_1.createOrReplaceTempView("npi_1")

# COMMAND ----------

display(npi_1)

# COMMAND ----------

overlap_1 = """

---Find overlap between current and mature
SELECT c.record
      ,c.cal_date
      ,c.geography_grain
      ,c.geography
      ,c.platform_subset
      ,c.customer_engagement
      ,c.forecast_process_note
      ,c.post_processing_note
      ,CASE WHEN c.product_lifecycle_status_share='N' AND c.measure='HP_SHARE' THEN m.data_source
            WHEN c.product_lifecycle_status_usage='N' AND c.measure like '%USAGE%' THEN m.data_source
            ELSE c.data_source
            END AS data_source
      ,c.version
      ,c.measure
      ,CASE WHEN c.product_lifecycle_status_share='N' AND c.measure='HP_SHARE' THEN m.units
            WHEN c.product_lifecycle_status_usage='N' AND c.measure like '%USAGE%' THEN m.units
            ELSE c.units
            END AS units
      ,c.proxy_used
      ,c.ib_version
      ,c.load_date
      ,c.grp_id
FROM current_1 c
INNER JOIN npi_1 m
    ON 1=1
    AND c.platform_subset=m.platform_subset
    AND c.customer_engagement=m.customer_engagement
    AND c.geography=m.geography
    AND c.measure=m.measure
    AND c.cal_date=m.cal_date
"""
overlap_1=spark.sql(overlap_1)
overlap_1.createOrReplaceTempView("overlap_1")

# COMMAND ----------

combine_1 = """
with cur_1 as (SELECT record
      ,cal_date
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,forecast_process_note
      ,post_processing_note
      ,data_source
      ,version
      ,measure
      ,units
      ,proxy_used
      ,ib_version
      ,load_date
FROM current_1
 WHERE grp_id not in (select distinct grp_id from overlap_1))
, ovr_1 as (SELECT record
      ,cal_date
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,forecast_process_note
      ,post_processing_note
      ,data_source
      ,version
      ,measure
      ,units
      ,proxy_used
      ,ib_version
      ,load_date
FROM overlap_1)
, npi_1 as (SELECT record
      ,cal_date
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,forecast_process_note
      ,post_processing_note
      ,data_source
      ,version
      ,measure
      ,units
      ,proxy_used
      ,ib_version
      ,load_date
FROM npi_1
WHERE grp_id not in (select distinct grp_id from overlap_1))
, combine as (
SELECT * FROM mat_1
UNION ALL
SELECT * FROM ovr_1
UNION ALL
SELECT * FROM npi_1
)


"""

combine_1=spark.sql(combine_1)
combine_1.createOrReplaceTempView("combine_1")

# COMMAND ----------

#write_df_to_redshift(configs: config(), df: matures_norm_final_landing, destination: "stage"."usrs_matures_norm_final_landing", mode: str = "overwrite")
#write_df_to_s3(df=combine_1, destination=f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/usage_share_country", format="parquet", mode="overwrite", upper_strings=True)
