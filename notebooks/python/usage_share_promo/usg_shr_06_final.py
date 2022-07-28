# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share Staging
# MAGIC (final output)

# COMMAND ----------

query_list = []

# COMMAND ----------

usage_share_staging = f"""
SELECT record
      , adj.cal_date
      , adj.geography_grain
      , adj.geography
      , adj.platform_subset
      , adj.customer_engagement
      , adj.measure
      , adj.units
      , adj.ib_version
      , adj.source
      , adj.version
      , adj.load_date
FROM "stage"."uss_03_adjusts" adj

UNION ALL

SELECT record
      , dmd.cal_date
      , dmd.geography_grain
      , dmd.geography
      , dmd.platform_subset
      , dmd.customer_engagement
      , dmd.measure
      , dmd.units
      , dmd.version as ib_version
      , dmd.source
      , dmd.version
      , dmd.load_date
FROM "stage"."usage_share_demand" AS dmd
"""

query_list.append(["stage.usage_share_staging", usage_share_staging, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Out Usage Share Staging

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
