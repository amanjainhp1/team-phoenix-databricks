# Databricks notebook source
# MAGIC %md
# MAGIC ## Add Versions for Norm Ships and IB to Scenario Table

# COMMAND ----------

from datetime import date

# COMMAND ----------

# MAGIC %run ../../notebooks/python/common/configs

# COMMAND ----------

try:
    version = dbutils.widgets.get("version")
except Exception(e):
    print("ERROR: version parameter is not set\n")
    print(e)

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

# MAGIC %run ../../notebooks/python/common/database_utils

# COMMAND ----------

call_redshift_addversion_sproc(configs, ns_record, ns_source)
call_redshift_addversion_sproc(configs, ib_record, ib_source)

# COMMAND ----------

insert_ns_ib = f"""
with filters as
(
    SELECT record
        , version
        , sub_version
        , source_name
        , load_date
        , official
    FROM prod.version
    WHERE record in ('IB', 'NORM_SHIPMENTS')
        AND version = {version}
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
INSERT INTO prod.scenario
SELECT scenario_name
	, record
	, version
	, load_date
FROM scenario_setup
where record not in ('INSTANT TONER', 'LF_LAG')
order by 1,2
"""

# COMMAND ----------

submit_remote_query(redshift_dbname, redshift_port, username, password, redshift_url, insert_ns_ib)
