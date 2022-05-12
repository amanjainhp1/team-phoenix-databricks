# Databricks notebook source
# MAGIC %md
# MAGIC # Installed Base Promotion

# COMMAND ----------

# Global Variables
try:
    version = dbutils.widgets.get("version")
except Exception(e):
    print("ERROR: version parameter is not set\n")
    print(e)

query_list = []

# COMMAND ----------

norm_ships = f"""


with ib_promo_01_filter_vars as (


SELECT record
    , version
    , source_name
    , load_date
    , official
FROM "prod"."version"
WHERE record in ('IB', 'NORM_SHIPMENTS')
    AND version = '{version}'
)SELECT ns.record
    , ns.cal_date
    , ns.region_5
    , ns.country_alpha2
    , ns.platform_subset
    , ns.units
    , vars.load_date
    , vars.version
FROM "stage"."norm_ships" AS ns
CROSS JOIN ib_promo_01_filter_vars AS vars
WHERE vars.record = 'NORM_SHIPMENTS'
"""

query_list.append(["prod.norm_shipments", norm_ships, "overwrite"])

# COMMAND ----------

ib = f"""


with ib_promo_01_filter_vars as (


SELECT record
    , version
    , source_name
    , load_date
    , official
FROM "prod"."version"
WHERE record in ('IB', 'NORM_SHIPMENTS')
    AND version = '{version}'
) SELECT ib.record
    , ib.month_begin AS cal_date
    , ib.country_alpha2
    , ib.platform_subset
    , UPPER(ib.split_name) AS customer_engagement
    , 'IB' AS measure
    , ib.ib AS units
    , 0 AS official
    , vars.load_date
    , vars.version
FROM "stage"."ib_staging" AS ib
JOIN ib_promo_01_filter_vars AS vars
    ON vars.record = ib.record
WHERE 1=1
    AND ib.record = 'IB'
"""

query_list.append(["prod.ib", ib, "overwrite"])

# COMMAND ----------

norm_shipments_ce = f"""


with ib_promo_01_filter_vars as (


SELECT record
    , version
    , source_name
    , load_date
    , official
FROM "prod"."version"
WHERE record in ('IB', 'NORM_SHIPMENTS')
    AND version = '{version}'
)SELECT 'NORM_SHIPS_CE' AS record
    , ns.month_begin AS cal_date
    , ns.region_5 AS region_5
    , ns.country_alpha2
    , ns.platform_subset
    , UPPER(ns.split_name) AS customer_engagement
    , ns.split_value
    , ns.units AS units
    , vars.load_date
    , vars.version
FROM "stage"."ib_04_units_ce_splits_pre" AS ns
CROSS JOIN ib_promo_01_filter_vars AS vars
WHERE 1=1
    AND vars.record = 'NORM_SHIPMENTS'
"""

query_list.append(["prod.norm_shipments_ce", norm_shipments_ce, "append"])

# COMMAND ----------

# MAGIC %run "../../common/output_to_redshift" $query_list=query_list