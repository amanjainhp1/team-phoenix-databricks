# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share 01 
# MAGIC - Proxy Locking

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# for interactive sessions, define widgets
dbutils.widgets.text("usage_share_current_version", "")
dbutils.widgets.text("usage_share_locked_version", "")
dbutils.widgets.text("datestamp", "")

# COMMAND ----------

# retrieve tasks from widgets/parameters
tasks = dbutils.widgets.get("tasks").split(";")

# define all relevenat task parameters to this notebook
relevant_tasks = ["all", "proxy_locking"]
# exit if tasks list does not contain a relevant task i.e. "all" or "proxy_locking"
for task in tasks:
    if task not in relevant_tasks:
        dbutils.notebook.exit("EXIT: Tasks list does not contain a relevant value i.e. {}.".format(", ".join(relevant_tasks)))

# COMMAND ----------

# set vars equal to widget vals for job/interactive sessions
usage_share_locked_version = dbutils.widgets.get("usage_share_locked_version")

# set vars equal to widget vals for job/interactive sessions, else retrieve task values 
usage_share_current_version = dbutils.widgets.get("usage_share_current_version") if dbutils.widgets.get("usage_share_current_version") != "" else dbutils.jobs.taskValues.get(taskKey = "toner_share", key = "args")["usage_share_current_version"]
datestamp = dbutils.widgets.get("datestamp") if dbutils.widgets.get("datestamp") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["datestamp"]

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# (Country Usage-Page Share Monthly) CUPSM Processing

# This portion of the code tried to quantify changes in incoming raw, CUPSM data and the cause of those changes i.e. BD, modeling changes, or proxying changes. 

# Data is labeled as such:
# -BD if there is telemetry data for that cal_date
# -Modelled if there is not telemetry for that cal_date but the time series for that platform has some data
# -Proxied if there is no data for the time series for that platform

# Share and usage are treated separately by the CUPSM process so the change is quantified for each metric separately. The data is generated at the platform/geo level (a single time series for each platform/geo). A reference data set is used (the previous DUPSM that was reported off). This DUPSM is used to 

# Label each data point as either not changing; or a restatement of either BD, modelled, or proxied data; or a flag change (this is where a platform/geo is flagged as directional, or looses the flag); or a dropped/added platform/geo.


# Load Current run of Usage/Share
usage_share_current_df = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}spectrum/cupsm/{usage_share_current_version}/toner*") 
usage_share_current_df.createOrReplaceTempView("usage_share_current_df")

# COMMAND ----------

# Load Proxy Constant Usage/Share
usage_share_locked_df = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}spectrum/cupsm/{usage_share_locked_version}/toner*")
usage_share_locked_df.createOrReplaceTempView("usage_share_locked_df")

# COMMAND ----------

# Get list of platform_subset/customer_engagement/geographys that use Big Data directly
bdcurrent = spark.sql("""
    SELECT DISTINCT geography, platform_subset, customer_engagement, measure, 'BD' as group1
    FROM usage_share_current_df
    WHERE 1=1
      AND upper(data_source) in ('HAVE DATA','DASHBOARD')
""")

bdcurrent.createOrReplaceTempView("bdcurrent")

# COMMAND ----------

#add flag to show if telemetry directly used
cupsm_current_raw = spark.sql("""
    with step1 AS (
      SELECT usc.record, usc.cal_date, usc.geography_grain, usc.geography, usc.platform_subset, usc.customer_engagement, usc.forecast_process_note, usc.forecast_created_date, usc.data_source, usc.version,   usc.units, usc.proxy_used, usc.ib_version, usc.load_date, usc.measure, bdl.group1
      FROM usage_share_current_df usc
      LEFT JOIN bdcurrent bdl
        ON usc.geography=bdl.geography AND usc.platform_subset=bdl.platform_subset AND usc.customer_engagement=bdl.customer_engagement AND usc.measure=bdl.measure
      WHERE 1=1)
    SELECT record,cal_date, geography_grain, geography, platform_subset,customer_engagement, forecast_process_note, forecast_created_date, data_source, version, units, proxy_used, ib_version
         , load_date, measure, group1
         , CASE WHEN group1='BD' THEN 
             CASE WHEN upper(data_source) IN ('HAVE DATA','DASHBOARD') THEN 'BD'
               ELSE 'MODELLED'
               END
              WHEN data_source IN ('N') THEN 'N'
              WHEN group1 is NULL THEN 'PROXIED'
            END AS proxy_flag
     FROM step1
""")

cupsm_current_raw.createOrReplaceTempView("cupsm_current_raw")

# COMMAND ----------

usage_share_locked_df = spark.sql("""
    with step1 AS (SELECT usc.record, usc.cal_date, upper(usc.geography_grain) as geography_grain, upper(usc.geography) as geography, upper(usc.platform_subset) as platform_subset
                   , upper(usc.customer_engagement) as customer_engagement, usc.forecast_process_note, usc.forecast_created_date, upper(usc.data_source) as data_source
                   , usc.version, usc.units, upper(usc.proxy_used) as proxy_used, usc.ib_version
        , usc.load_date, upper(usc.measure) as measure
    FROM usage_share_locked_df usc)
    ,stepchk AS (SELECT distinct measure from step1)
    ,step2 AS (SELECT usc.record, usc.cal_date, upper(usc.geography_grain) as geography_grain, upper(usc.geography) as geography, upper(usc.platform_subset) as platform_subset
                   , upper(usc.customer_engagement) as customer_engagement, usc.forecast_process_note, usc.forecast_created_date, upper(usc.data_source) as data_source
                   , usc.version, usc.units, upper(usc.proxy_used) as proxy_used, usc.ib_version
        , usc.load_date, 'USAGE' as measure
    FROM usage_share_locked_df usc
    WHERE upper(measure)='K_USAGE')
      --SELECT * FROM step1
        SELECT * FROM step1 UNION SELECT * FROM step2
      ;
    
""")

usage_share_locked_df.createOrReplaceTempView("usage_share_locked_df")

# COMMAND ----------

#get list of platform_subset/customer_engagement/geographys that use Big Data directly
bdlocked = spark.sql("""
    SELECT DISTINCT upper(geography) as geography, upper(platform_subset) as platform_subset, upper(customer_engagement) as customer_engagement, upper(measure) as measure, 'BD' as group1, upper(data_source) as data_source
    FROM usage_share_locked_df
    WHERE 1=1
      AND upper(data_source) IN ('HAVE DATA','DASHBOARD')
    """)

bdlocked.createOrReplaceTempView("bdlocked")

# COMMAND ----------

#add flag to show if telemetry directly used
cupsm_locked_raw = spark.sql("""
    with step1 AS (
      SELECT usc.record, usc.cal_date, upper(usc.geography_grain) as geography_grain, upper(usc.geography) as geography, upper(usc.platform_subset) as platform_subset
                   , upper(usc.customer_engagement) as customer_engagement, usc.forecast_process_note, usc.forecast_created_date, upper(usc.data_source) as data_source
                   , usc.version, usc.units, upper(usc.proxy_used) as proxy_used, usc.ib_version
                   , usc.load_date, upper(usc.measure) as measure, bdl.group1
      FROM usage_share_locked_df usc
      LEFT JOIN bdlocked bdl
        ON upper(usc.geography)=upper(bdl.geography) AND upper(usc.platform_subset)=upper(bdl.platform_subset) AND upper(usc.customer_engagement)=upper(bdl.customer_engagement) AND upper(usc.measure)=upper(bdl.measure)
      WHERE 1=1)
    SELECT record,cal_date, geography_grain, geography, platform_subset,customer_engagement, forecast_process_note, forecast_created_date, data_source, version, units, proxy_used, ib_version
        , load_date, measure, group1
        , CASE WHEN group1='BD' THEN 
                    CASE WHEN upper(data_source) IN ('HAVE DATA','DASHBOARD') THEN 'BD'
                         ELSE 'MODELLED'
                         END
               WHEN upper(data_source) IN ('N') THEN 'N'
               WHEN group1 is NULL THEN 'PROXIED'
               END AS proxy_flag
     FROM step1
""")

cupsm_current_raw.createOrReplaceTempView("cupsm_current_raw")
cupsm_locked_raw.createOrReplaceTempView("cupsm_locked_raw")

# COMMAND ----------

#create data set that has locked values where proxied, and new values where based directly on telemetry
cupsm_combined = spark.sql("""
  SELECT  CASE WHEN cur.record IS NOT NULL THEN cur.record ELSE loc.record END AS record
        , CASE WHEN cur.cal_date IS NOT NULL THEN cur.cal_date ELSE loc.cal_date END AS cal_date
        , CASE WHEN cur.geography_grain IS NOT NULL THEN cur.geography_grain ELSE loc.geography_grain END AS geography_grain
        , CASE WHEN cur.geography IS NOT NULL THEN cur.geography ELSE loc.geography END AS geography
        , CASE WHEN cur.platform_subset IS NOT NULL THEN cur.platform_subset ELSE loc.platform_subset END AS platform_subset
        , CASE WHEN cur.customer_engagement IS NOT NULL THEN cur.customer_engagement ELSE loc.customer_engagement END AS customer_engagement
        , CASE WHEN cur.forecast_process_note IS NOT NULL THEN cur.forecast_process_note ELSE loc.forecast_process_note END AS forecast_process_note
        , CASE WHEN cur.forecast_created_date IS NOT NULL THEN cur.forecast_created_date ELSE loc.forecast_created_date END AS forecast_created_date
        , CASE WHEN cur.version IS NOT NULL THEN cur.version ELSE loc.version END AS version
        , CASE WHEN cur.ib_version IS NOT NULL THEN cur.ib_version ELSE loc.ib_version END AS ib_version
        , CASE WHEN cur.load_date IS NOT NULL THEN cur.load_date ELSE loc.load_date END AS load_date
        , CASE WHEN cur.measure IS NOT NULL THEN cur.measure ELSE loc.measure END AS measure
        , CASE WHEN cur.proxy_flag='PROXIED' THEN loc.units
               WHEN cur.proxy_flag in ('BD','MODELLED') THEN cur.units
               --WHEN loc.proxy_flag is NULL THEN cur.units
               ELSE loc.units --dropped values
               END AS units
        , CASE WHEN cur.proxy_flag='PROXIED' THEN loc.data_source
               WHEN cur.proxy_flag in ('BD','MODELLED') THEN cur.data_source
               ELSE loc.data_source
               END AS data_source
       , CASE WHEN cur.proxy_flag='PROXIED' THEN loc.proxy_used
               WHEN cur.proxy_flag in ('BD','MODELLED') THEN cur.proxy_used
               ELSE loc.proxy_used
               END AS proxy_used
       , CASE WHEN cur.proxy_flag = loc.proxy_flag THEN 
               CASE WHEN (cur.measure='hp_share' AND round(cur.units-loc.units,4)=0) THEN 'NO CHANGE'
                      WHEN round(cur.units-loc.units,-1)=0 THEN 'NO CHANGE'
                      ELSE concat(cur.proxy_flag,'_RESTATEMENT')
                      END
                 WHEN cur.proxy_flag IS NULL THEN 'DROPPED'
                 WHEN loc.proxy_flag IS NULL THEN 'NEW'
                 ELSE 'FLAG_CHANGE'
                 END AS status
       , cur.data_source AS data_source_cur
       , cur.proxy_used AS proxy_used_cur
       , cur.proxy_flag AS proxy_flag_cur
       , cur.units AS units_cur
  FROM cupsm_current_raw cur   
    FULL JOIN cupsm_locked_raw loc
  ON  upper(cur.geography)=upper(loc.geography) AND upper(cur.platform_subset)=upper(loc.platform_subset) AND upper(cur.customer_engagement)=upper(loc.customer_engagement) AND upper(cur.measure)=upper(loc.measure) AND cur.cal_date=loc.cal_date
  WHERE upper(cur.measure) in ('USAGE','K_USAGE','USAGE_N','HP_SHARE','SHARE_N','COLOR_USAGE')
  
""")

cupsm_combined.createOrReplaceTempView("cupsm_combined")

# COMMAND ----------

output = spark.sql("""
      SELECT 
        record
        , cal_date
        , geography_grain
        , geography
        , platform_subset
        , customer_engagement
        , forecast_process_note
        , forecast_created_date
        , case
           when status in ('NEW','NO CHANGE','BD_RESTATEMENT') then data_source_cur
           else data_source
           end as data_source
        , version
        , measure
        , case
           when status in ('NEW','NO CHANGE','BD_RESTATEMENT') then units_cur
           else units
           end as units
        , case 
          when status in ('NEW','NO CHANGE','BD_RESTATEMENT') then concat(status,";",proxy_used_cur,';',units_cur)
          else concat(status,";",proxy_used,';',units_cur)
          end as proxy_used
        , ib_version
        , load_date
      FROM cupsm_combined
      WHERE measure in ('COLOR_USAGE', 'COLOR_USAGE_C', 'COLOR_USAGE_M', 'COLOR_USAGE_Y', 'HP_SHARE', 'K_USAGE', 'USAGE', 'USAGE_N', 'SHARE_N')
""")

# COMMAND ----------

#usage_share_proxy
output.write.parquet(f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/{datestamp}/toner_locked", mode="overwrite")
