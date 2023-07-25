# Databricks notebook source
# MAGIC %md
# MAGIC # Installed Base Promotion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Versions for Norm Ships and IB to Scenario Table

# COMMAND ----------

from datetime import date

# COMMAND ----------

# MAGIC %run ../../common/configs

# COMMAND ----------

# MAGIC %run ../../common/database_utils

# COMMAND ----------

# Global Variables
query_list = []

redshift_dbname = configs["redshift_dbname"]
redshift_port = configs["redshift_port"]
username = configs["redshift_username"] 
password = configs["redshift_password"]
redshift_url = configs["redshift_url"]

cur_date = date.today().strftime("%Y.%m.%d")
ns_source = 'NS - DBT build - ' + cur_date
ib_source = 'IB - DBT build - ' + cur_date
ns_record = 'NORM_SHIPMENTS'
ib_record = 'IB'

# COMMAND ----------

ns_record_max_version_info = call_redshift_addversion_sproc(configs, ns_record, ns_source)
ib_record_max_version_info = call_redshift_addversion_sproc(configs, ib_record, ib_source)

version = ns_record_max_version_info[0]
print(version)

# COMMAND ----------

insert_ns_ib = f"""
WITH filters AS
(
    SELECT record
        , version
        , source_name
        , load_date
        , official
    FROM prod.version
    WHERE record in ('IB', 'NORM_SHIPMENTS')
        AND version = '{version}'
),
scenario_setup as
(
	-- bring in norm_shipments sub-class inputs
    SELECT vars.source_name AS scenario_name
        , inputs.record
        , inputs.version
        , CAST(vars.load_date AS date) AS load_date
    FROM filters AS vars
    LEFT JOIN stage.norm_ships_inputs AS inputs
        ON 1=1
    WHERE vars.record = 'NORM_SHIPMENTS'

    UNION ALL

	-- bring in ib sub-class inputs
    SELECT vars.source_name AS scenario_name
        , inputs.record
        , inputs.version
        , CAST(vars.load_date AS date) AS load_date
    FROM filters AS vars
    LEFT JOIN stage.ib_staging_inputs AS inputs
        ON 1=1
    WHERE vars.record IN ('IB')
        AND inputs.record <> 'LFD_DECAY'

    UNION ALL

	-- bring in norm_shipments sub-class as an IB input
    SELECT ib.source_name AS scenario_name
        , vars.record
        , vars.version
        , CAST(vars.load_date AS date) AS load_date
    FROM filters AS vars
    CROSS JOIN (SELECT DISTINCT source_name FROM filters WHERE record = 'IB') AS ib
    WHERE vars.record = 'NORM_SHIPMENTS'

)
SELECT scenario_name
	, record
	, version
	, load_date
FROM scenario_setup
where record not in ('INSTANT TONER', 'LF_LAG')
order by 1,2
"""

query_list.append(["prod.scenario", insert_ns_ib, "append"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promote norm_shipments, ib, and norm_shipments_ce

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
FROM "stage"."norm_ships_lf" AS ns
CROSS JOIN ib_promo_01_filter_vars AS vars
WHERE vars.record = 'NORM_SHIPMENTS'
"""

query_list.append(["prod.norm_shipments", norm_ships, "append"])

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

query_list.append(["prod.ib", ib, "append"])

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
)

SELECT 
    ns.record
    , ns.cal_date
    , ns.region_5
    , ns.country_alpha2
    , ns.platform_subset
    , ns.customer_engagement
    , ns.split_value
    , ns.units AS units
    , vars.load_date
    , vars.version
FROM "stage"."norm_shipments_ce" AS ns
CROSS JOIN ib_promo_01_filter_vars AS vars
WHERE 1=1
    AND vars.record = 'NORM_SHIPMENTS'
"""

query_list.append(["prod.norm_shipments_ce", norm_shipments_ce, "append"])

# COMMAND ----------

# MAGIC %run "../../common/output_to_redshift" $query_list=query_list
