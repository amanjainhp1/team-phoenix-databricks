# Databricks notebook source
# MAGIC %md
# MAGIC # Usage Share Refactor Step 03
# MAGIC - NPI

# COMMAND ----------

# for interactive sessions, define a version widget
dbutils.widgets.text("version", "")

# COMMAND ----------

# retrieve version from widget
version = dbutils.widgets.get("version")

# COMMAND ----------

import pandas as pd
import numpy as mp
import psycopg2 as ps

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

npi_in = read_redshift_to_df(configs) \
  .option("query","""
    SELECT *
    FROM "prod"."usage_share_override_npi"
    """) \
  .load()
npi_in.createOrReplaceTempView("npi_in")

# COMMAND ----------

npi_in_r5 = spark.sql("""select * from npi_in where upper(geography_grain) ='REGION_5' """)
npi_in_r5.createOrReplaceTempView("npi_in_r5")
npi_in_m10 = spark.sql("""select * from npi_in where upper(geography_grain) ='MARKET10' """)
npi_in_m10.createOrReplaceTempView("npi_in_m10")

# COMMAND ----------

# MAGIC %md
# MAGIC #Region 5 to Country

# COMMAND ----------

country_info = read_redshift_to_df(configs) \
  .option("query","""
    SELECT distinct region_5, market10, country_alpha2
    FROM "mdm"."iso_country_code_xref"
    """) \
  .load()
country_info.createOrReplaceTempView("country_info")

ib_info = read_redshift_to_df(configs) \
  .option("query",f"""
      SELECT distinct country_alpha2, platform_subset, customer_engagement
      FROM "prod"."ib"
      WHERE 1=1
      AND version = '{version}'
    """) \
 .load()
ib_info.createOrReplaceTempView("ib_info")

ib_info_r5 = read_redshift_to_df(configs) \
  .option("query",f"""
      SELECT cc.region_5, ib.platform_subset, ib.customer_engagement, min(ib.cal_date) as ib_strt_dt
      FROM "prod"."ib" ib
      LEFT JOIN "mdm"."iso_country_code_xref" cc
        ON ib.country_alpha2=cc.country_alpha2
      WHERE 1=1
      AND ib.version = '{version}'
      GROUP BY 
        cc.region_5, ib.platform_subset, ib.customer_engagement
    """) \
 .load()
ib_info_r5.createOrReplaceTempView("ib_info_r5")

npi_helper_1 = """
 with stp1 as (SELECT npi.record
      ,npi.min_sys_dt
      ,ibdt.ib_strt_dt
      ,npi.month_num
      ,'Country' as geography_grain
      ,cc.country_alpha2
      ,upper(npi.platform_subset) as platform_subset
      ,upper(npi.customer_engagement) as customer_engagement
      ,upper(npi.forecast_process_note) as forecast_process_note
      ,upper(npi.post_processing_note) as post_processing_note
      ,upper(npi.data_source) as data_source
      ,npi.version
      ,upper(npi.measure) as measure
      ,npi.units
      ,npi.proxy_used
      ,npi.ib_version
      ,npi.load_date
FROM npi_in_r5 npi
LEFT JOIN  country_info cc
  ON upper(npi.geography)=upper(cc.region_5)
LEFT JOIN ib_info_r5 ibdt
  ON upper(npi.geography)=upper(ibdt.region_5)
  ),
stp2 as (select stp1.*
FROM stp1 
INNER JOIN ib_info ib
  ON stp1.country_alpha2=ib.country_alpha2 and stp1.platform_subset=ib.platform_subset and stp1.customer_engagement=ib.customer_engagement
  )
  select * from stp2
"""

npi_helper_1=spark.sql(npi_helper_1)
npi_helper_1.createOrReplaceTempView("npi_helper_1")

# COMMAND ----------

display(country_info)

# COMMAND ----------

# MAGIC %md
# MAGIC # NPI Market10 to Country

# COMMAND ----------

ib_info_m10 = read_redshift_to_df(configs) \
  .option("query",f"""
      SELECT cc.market10, ib.platform_subset, ib.customer_engagement, min(ib.cal_date) as ib_strt_dt
      FROM "prod"."ib" ib
      LEFT JOIN "mdm"."iso_country_code_xref" cc
        ON ib.country_alpha2=cc.country_alpha2
      WHERE 1=1
      AND ib.version = '{version}'
      GROUP BY 
        cc.market10, ib.platform_subset, ib.customer_engagement
    """) \
 .load()
ib_info_m10.createOrReplaceTempView("ib_info_m10")


npi_helper_2 ="""
 with stp1 as (SELECT npi.record
      ,npi.min_sys_dt
      ,ibdt.ib_strt_dt
      ,npi.month_num
      ,'Country' as geography_grain
      ,cc.country_alpha2
      ,upper(npi.platform_subset) as platform_subset
      ,upper(npi.customer_engagement) as customer_engagement
      ,upper(npi.forecast_process_note) as forecast_process_note
      ,upper(npi.post_processing_note) as post_processing_note
      ,upper(npi.data_source) as data_source
      ,npi.version
      ,upper(npi.measure) as measure
      ,npi.units
      ,npi.proxy_used
      ,npi.ib_version
      ,npi.load_date
FROM npi_in_m10 npi
LEFT JOIN  country_info cc
  ON upper(npi.geography)=upper(cc.market10)
LEFT JOIN ib_info_m10 ibdt
  ON upper(npi.geography)=upper(ibdt.market10)
  ),
stp2 as (select stp1.*
FROM stp1 
INNER JOIN ib_info ib
  ON stp1.country_alpha2=ib.country_alpha2 and stp1.platform_subset=ib.platform_subset and stp1.customer_engagement=ib.customer_engagement
  )
  select * from stp2
"""

npi_helper_2=spark.sql(npi_helper_2)
npi_helper_2.createOrReplaceTempView("npi_helper_2")

# COMMAND ----------

display(npi_helper_2)

# COMMAND ----------

npi_helper_3 = """
SELECT * FROM npi_helper_2
UNION
SELECT * FROM npi_helper_1
"""

npi_helper_3=spark.sql(npi_helper_3)
npi_helper_3.createOrReplaceTempView("npi_helper_3")

# COMMAND ----------

# MAGIC %md
# MAGIC # Usage Share NPI Month Num to Date

# COMMAND ----------

overrides_norm_landing = """

--create dates from min_sys_date and month_num
SELECT distinct record
      ,add_months(ib_strt_dt, month_num) as cal_date
      ,geography_grain
      ,country_alpha2
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
FROM npi_helper_3
"""

npi_helper_4=spark.sql(overrides_norm_landing)
npi_helper_4.createOrReplaceTempView("npi_helper_4")


# COMMAND ----------

# MAGIC %md
# MAGIC # Fill in missing NPI data

# COMMAND ----------

npi_fill_missing_ib_data = read_redshift_to_df(configs) \
  .option("query",f"""
--Get dates by platform_subset and customer_engagement from IB
SELECT ib.platform_subset
    , country_alpha2
    , customer_engagement
    , CAST(min(cal_date) AS DATE) AS min_ib_date
    , CAST(max(cal_date) AS DATE) AS max_ib_date
FROM "prod"."ib" ib
LEFT JOIN "mdm"."hardware_xref" hw
    ON ib.platform_subset=hw.platform_subset
WHERE 1=1
	AND ib.version = '{version}'
	AND measure = 'IB'
	AND (hw.product_lifecycle_status = 'N')
    AND units>0
GROUP BY ib.platform_subset
    , customer_engagement
    , country_alpha2

""") \
 .load()
npi_fill_missing_ib_data.createOrReplaceTempView("npi_fill_missing_ib_data")

# COMMAND ----------

npi_fill_missing_us_data = """

--create dates from min_sys_date and month_num
SELECT record
      ,geography_grain
      ,country_alpha2
      ,platform_subset
      ,customer_engagement
      ,CAST(min(cal_date) AS DATE) AS min_us_date
      ,CAST(max(cal_date) AS DATE) AS max_us_date
FROM npi_helper_4
GROUP BY 
record
      ,geography_grain
      ,country_alpha2
      ,platform_subset
      ,customer_engagement
"""

npi_fill_missing_us_data=spark.sql(npi_fill_missing_us_data)
npi_fill_missing_us_data.createOrReplaceTempView("npi_fill_missing_us_data")

npi_fill_missing_dates = """
---Combine data
SELECT 
      a.country_alpha2
      ,a.platform_subset
      ,a.customer_engagement
      ,b.min_ib_date
      ,a.min_us_date
      ,b.max_ib_date
      ,a.max_us_date
      , months_between(a.min_us_date, b.min_ib_date) AS min_diff
      , months_between(b.max_ib_date, a.max_us_date) AS max_diff
FROM npi_fill_missing_us_data a
LEFT JOIN npi_fill_missing_ib_data b
	ON a.platform_subset=b.platform_subset
	AND a.country_alpha2=b.country_alpha2
	AND a.customer_engagement=b.customer_engagement
WHERE b.min_ib_date is not null
"""

npi_fill_missing_dates=spark.sql(npi_fill_missing_dates)
npi_fill_missing_dates.createOrReplaceTempView("npi_fill_missing_dates")

# COMMAND ----------

npi_dates_list = read_redshift_to_df(configs) \
  .option("query",f"""
--Get dates
SELECT DISTINCT date
FROM "mdm"."calendar"
WHERE Day_of_Month = 1
""") \
 .load()
npi_dates_list.createOrReplaceTempView("npi_dates_list")

npi_dates_fill = """
SELECT platform_subset
    , country_alpha2
    , UPPER(customer_engagement) AS customer_engagement
    , date AS cal_date
    , case when date > max_us_date then "F"
      else "B"
      end as fore_back
FROM npi_fill_missing_dates
CROSS JOIN npi_dates_list
WHERE 1=1
AND (date > max_us_date
AND date <= max_ib_date)
OR (date < min_us_date
AND date >= min_ib_date)

"""
npi_dates_fill=spark.sql(npi_dates_fill)
npi_dates_fill.createOrReplaceTempView("npi_dates_fill")

# COMMAND ----------

fill_forecast = """
--get last value for flatlining forecast
SELECT a.platform_subset
    , UPPER(a.customer_engagement) AS customer_engagement
    , a.country_alpha2
    , a.measure
    , a.units
FROM npi_helper_4 a
INNER JOIN npi_fill_missing_dates b
    ON a.platform_subset = b.platform_subset
        AND a.customer_engagement = b.customer_engagement
        AND a.country_alpha2 = b.country_alpha2
        AND a.cal_date = b.max_us_date
WHERE b.max_us_date < b.max_ib_date
"""
fill_forecast=spark.sql(fill_forecast)
fill_forecast.createOrReplaceTempView("fill_forecast")

# COMMAND ----------

# MAGIC %md
# MAGIC # Usage Share Overrides NPI Landing Final

# COMMAND ----------

combine_data = """
SELECT 'USAGE_SHARE_MATURES' As record
    , CAST(a.cal_date AS DATE) AS cal_date
    , a.country_alpha2 AS geography
    , a.platform_subset
    , a.customer_engagement
    , 'NPI OVERRIDE' AS forecast_process_note
    , 'NONE' AS post_processing_note
    , CAST(current_date() AS DATE) AS forecast_created_date
    , 'DATA_SOURCE' AS data_source
    , 'VERSION' AS version
    , b.measure
    , b.units
    , NULL AS proxy_used
    , 'IB_VERSION' AS ib_version
    , current_date() AS load_date
FROM npi_dates_fill a
LEFT JOIN fill_forecast b
    ON a.platform_subset=b.platform_subset
        AND a.customer_engagement = b.customer_engagement
        AND a.country_alpha2 = b.country_alpha2
WHERE 1=1
"""
combine_data=spark.sql(combine_data)
combine_data.createOrReplaceTempView("combine_data")

# COMMAND ----------

npi_norm_final_landing = f"""
SELECT nl.record
    , nl.cal_date
    , 'COUNTRY' as geography_grain
    , nl.country_alpha2 as geography
    , nl.platform_subset
    , nl.customer_engagement
    , nl.forecast_process_note
    , CAST(current_date() AS DATE) AS forecast_created_date
    , nl.data_source
    , nl.version
    , nl.measure
    , nl.units
    , nl.proxy_used
    , nl.ib_version
    , nl.load_date
FROM npi_helper_4 nl
UNION ALL
SELECT fl.record
    , fl.cal_date
    , 'COUNTRY' as geography_grain
    , fl.geography
    , fl.platform_subset
    , fl.customer_engagement
    , fl.forecast_process_note
    , CAST(current_date() AS DATE) AS forecast_created_date
    , fl.data_source
    , fl.version
    , fl.measure
    , fl.units
    , fl.proxy_used
    , fl.ib_version
    , fl.load_date
FROM combine_data fl

"""
npi_norm_final_landing=spark.sql(npi_norm_final_landing)
npi_norm_final_landing.createOrReplaceTempView("npi_norm_final_landing")

# COMMAND ----------

#check for duplicates
npi_norm_final_landing \
    .groupby(['cal_date', 'platform_subset', 'customer_engagement', 'geography', 'measure']) \
    .count() \
    .where('count > 1') \
    .sort('count', ascending=False) \
    .show()

# COMMAND ----------

#write_df_to_redshift(configs: config(), df: npi_norm_final_landing, destination: "stage"."usrs_npi_norm_final_landing", mode: str = "overwrite")

