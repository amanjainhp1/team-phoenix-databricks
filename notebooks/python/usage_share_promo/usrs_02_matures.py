# Databricks notebook source
# MAGIC %md
# MAGIC # Usage Share Refactor Step 02
# MAGIC - Matures

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

# MAGIC %md # Mature to Country

# COMMAND ----------

matures_table_toner = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}/spectrum/matures/2022.07.14.1/toner.parquet")
matures_table_ink = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}/spectrum/matures/2022.07.14.1/ink.parquet")
#test1 = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}/spectrum/matures/2022.07.14.1/test1.parquet")
matures=matures_table_toner.union(matures_table_ink)
#test1 = spark.read.parquet("s3://dataos-core-dev-team-phoenix/spectrum/cupsm/2022.06.23.1/ink_ink_test_2022.1")
matures.createOrReplaceTempView("matures")

# COMMAND ----------

mature_tst=spark.sql("""select * from matures where platform_subset ='KOALA' """)
mature_tst.createOrReplaceTempView("mature_tst")

# COMMAND ----------

display(mature_tst)

# COMMAND ----------

country_info = read_redshift_to_df(configs) \
  .option("query","""
    SELECT distinct country_alpha2, market10
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

mature_helper_1 ="""
 with stp1 as (SELECT distinct mat.record
      ,mat.min_sys_dt
      ,mat.month_num
      ,'Country' as geography_grain
      ,cc.country_alpha2
      ,upper(mat.platform_subset) as platform_subset
      ,upper(mat.customer_engagement) as customer_engagement
      ,upper(mat.forecast_process_note) as forecast_process_note
      ,upper(mat.post_processing_note) as post_processing_note
      ,upper(mat.data_source) as data_source
      ,mat.version
      ,upper(mat.measure) as measure
      ,mat.units
      ,mat.proxy_used
      ,mat.ib_version
      ,mat.load_date
FROM matures mat
LEFT JOIN  country_info cc
  ON upper(mat.geography)=upper(cc.market10)
  ),
stp2 as (select stp1.*
FROM stp1 
INNER JOIN ib_info ib
  ON stp1.country_alpha2=ib.country_alpha2 and stp1.platform_subset=ib.platform_subset and stp1.customer_engagement=ib.customer_engagement
  )
  select * from stp2
"""

mature_helper_1=spark.sql(mature_helper_1)
#query_list.append(["stage.usrs_matures_helper_1", mature_helper_1, "overwrite"])

# COMMAND ----------

test2 = read_redshift_to_df(configs) \
  .option("query",f"""
      SELECT distinct country_alpha2, platform_subset, customer_engagement
      FROM "prod"."ib"
      WHERE 1=1
      AND version = '{version}'
      AND platform_subset='KOALA'
    """) \
 .load()
test2.createOrReplaceTempView("test2")

# COMMAND ----------

display(test2)

# COMMAND ----------

mature_helper_1.createOrReplaceTempView("mature_helper_1")
test3=spark.sql("""select * from mature_helper_1 where platform_subset='KOALA' and country_alpha2='PK'""")

# COMMAND ----------

display(test3)

# COMMAND ----------

display(mature_helper_1)

# COMMAND ----------

# can write to helper file
#write_df_to_redshift(configs: config(), df: mature_helper_1, destination: "stage"."usrs_matures_helper_1", mode: str = "overwrite")

# COMMAND ----------

mature_helper_1.createOrReplaceTempView("mature_helper_1")

# COMMAND ----------

# MAGIC %md
# MAGIC # Usage Share Matures Month Num to Date

# COMMAND ----------

overrides_norm_landing = """

--create dates from min_sys_date and month_num
SELECT distinct record
      ,add_months(min_sys_dt, month_num) as cal_date
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
FROM mature_helper_1
--"stage"."usrs_matures_helper_1"
"""

mature_helper_2=spark.sql(overrides_norm_landing)
#query_list.append(["stage.usrs_matures_normalized", overrides_norm_landing, "overwrite"])

# COMMAND ----------

mature_helper_2.createOrReplaceTempView("mature_helper_2")
test4 = spark.sql("select * from mature_helper_2 where platform_subset='KOALA' and country_alpha2='PK' and measure='HP_SHARE'")

# COMMAND ----------

display(test4)

# COMMAND ----------

display(mature_helper_2)

# COMMAND ----------

# can write to helper file
#write_df_to_redshift(configs: config(), df: mature_helper_2, destination: "stage"."usrs_matures_helper_2", mode: str = "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC # Fill in missing Mature data

# COMMAND ----------

matures_fill_missing_ib_data = read_redshift_to_df(configs) \
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
	AND (hw.product_lifecycle_status = 'M' OR (hw.product_lifecycle_status = 'C' AND hw.epa_family like 'SEG%'))
    AND units>0
GROUP BY ib.platform_subset
    , customer_engagement
    , country_alpha2

""") \
 .load()
matures_fill_missing_ib_data.createOrReplaceTempView("matures_fill_missing_ib_data")


# COMMAND ----------

matures_fill_missing_us_data = """

--create dates from min_sys_date and month_num
SELECT record
      ,geography_grain
      ,country_alpha2
      ,platform_subset
      ,customer_engagement
      ,CAST(min(cal_date) AS DATE) AS min_us_date
      ,CAST(max(cal_date) AS DATE) AS max_us_date
FROM mature_helper_2
GROUP BY 
record
      ,geography_grain
      ,country_alpha2
      ,platform_subset
      ,customer_engagement
"""

matures_fill_missing_us_data=spark.sql(matures_fill_missing_us_data)
matures_fill_missing_us_data.createOrReplaceTempView("matures_fill_missing_us_data")

matures_fill_missing_dates = """
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
FROM matures_fill_missing_us_data a
LEFT JOIN matures_fill_missing_ib_data b
	ON a.platform_subset=b.platform_subset
	AND a.country_alpha2=b.country_alpha2
	AND a.customer_engagement=b.customer_engagement
WHERE b.min_ib_date is not null
"""

matures_fill_missing_dates=spark.sql(matures_fill_missing_dates)
matures_fill_missing_dates.createOrReplaceTempView("matures_fill_missing_dates")


# COMMAND ----------

test1 = spark.sql("select * from matures_fill_missing_dates where platform_subset='KOALA'")

test1.createOrReplaceTempView("test1")
display(test1)

# COMMAND ----------

matures_dates_list = read_redshift_to_df(configs) \
  .option("query",f"""
--Get dates
SELECT DISTINCT date
FROM "mdm"."calendar"
WHERE Day_of_Month = 1
""") \
 .load()
matures_dates_list.createOrReplaceTempView("matures_dates_list")

matures_dates_fill = """
SELECT platform_subset
    , country_alpha2
    , UPPER(customer_engagement) AS customer_engagement
    , date AS cal_date
    , case when date > max_us_date then "F"
      else "B"
      end as fore_back
FROM matures_fill_missing_dates
CROSS JOIN matures_dates_list
WHERE 1=1
AND (date > max_us_date
AND date <= max_ib_date)
OR (date < min_us_date
AND date >= min_ib_date)

"""
matures_dates_fill=spark.sql(matures_dates_fill)
matures_dates_fill.createOrReplaceTempView("matures_dates_fill")



# COMMAND ----------

display(matures_dates_fill)

# COMMAND ----------

fill_forecast = """
--get last value for flatlining forecast
SELECT a.platform_subset
    , UPPER(a.customer_engagement) AS customer_engagement
    , a.country_alpha2
    , a.measure
    , a.units
FROM mature_helper_2 a
INNER JOIN matures_fill_missing_dates b
    ON a.platform_subset = b.platform_subset
        AND a.customer_engagement = b.customer_engagement
        AND a.country_alpha2 = b.country_alpha2
        AND a.cal_date = b.max_us_date
WHERE b.max_us_date < b.max_ib_date
"""
fill_forecast=spark.sql(fill_forecast)
fill_forecast.createOrReplaceTempView("fill_forecast")

fill_backfill = """
--get last value for flatlining forecast
SELECT a.platform_subset
    , UPPER(a.customer_engagement) AS customer_engagement
    , a.country_alpha2
    , a.measure
    , a.units
FROM mature_helper_2 a
INNER JOIN matures_fill_missing_dates b
    ON a.platform_subset = b.platform_subset
        AND a.customer_engagement = b.customer_engagement
        AND a.country_alpha2 = b.country_alpha2
        AND a.cal_date = b.min_us_date
WHERE b.min_ib_date < b.min_us_date
"""
fill_backfill=spark.sql(fill_backfill)
fill_backfill.createOrReplaceTempView("fill_backfill")


# COMMAND ----------

display(fill_forecast)

# COMMAND ----------

display(fill_backfill)

# COMMAND ----------

# MAGIC %md
# MAGIC # Usage Share Overrides Mature Landing Final

# COMMAND ----------

combine_data = """
SELECT 'USAGE_SHARE_MATURES' As record
    , CAST(a.cal_date AS DATE) AS cal_date
    , a.country_alpha2 AS geography
    , a.platform_subset
    , a.customer_engagement
    , 'MATURE OVERRIDE' AS forecast_process_note
    , 'NONE' AS post_processing_note
    , CAST(current_date() AS DATE) AS forecast_created_date
    , 'DATA_SOURCE' AS data_source
    , 'VERSION' AS version
    , b.measure
    , b.units
    , NULL AS proxy_used
    , 'IB_VERSION' AS ib_version
    , current_date() AS load_date
FROM matures_dates_fill a
LEFT JOIN fill_forecast b
    ON a.platform_subset=b.platform_subset
        AND a.customer_engagement = b.customer_engagement
        AND a.country_alpha2 = b.country_alpha2
WHERE 1=1
"""

combine_data_b = """
SELECT 'USAGE_SHARE_MATURES' As record
    , CAST(a.cal_date AS DATE) AS cal_date
    , a.country_alpha2 AS geography
    , a.platform_subset
    , a.customer_engagement
    , 'MATURE OVERRIDE' AS forecast_process_note
    , 'NONE' AS post_processing_note
    , CAST(current_date() AS DATE) AS forecast_created_date
    , 'DATA_SOURCE' AS data_source
    , 'VERSION' AS version
    , b.measure
    , b.units
    , NULL AS proxy_used
    , 'IB_VERSION' AS ib_version
    , current_date() AS load_date
FROM matures_dates_fill a
LEFT JOIN fill_backfill b
    ON a.platform_subset=b.platform_subset
        AND a.customer_engagement = b.customer_engagement
        AND a.country_alpha2 = b.country_alpha2
WHERE 1=1
"""
combine_data=spark.sql(combine_data)
combine_data.createOrReplaceTempView("combine_data")
combine_data_b=spark.sql(combine_data_b)
combine_data_b.createOrReplaceTempView("combine_data_b")


# COMMAND ----------

display(combine_data)

# COMMAND ----------

matures_norm_final_landing = f"""
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
FROM mature_helper_2 nl
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
UNION ALL
SELECT bl.record
    , bl.cal_date
    , 'COUNTRY' as geography_grain
    , bl.geography
    , bl.platform_subset
    , bl.customer_engagement
    , bl.forecast_process_note
    , CAST(current_date() AS DATE) AS forecast_created_date
    , bl.data_source
    , bl.version
    , bl.measure
    , bl.units
    , bl.proxy_used
    , bl.ib_version
    , bl.load_date
FROM combine_data_b bl
"""
matures_norm_final_landing=spark.sql(matures_norm_final_landing)
matures_norm_final_landing.createOrReplaceTempView("matures_norm_final_landing")

# COMMAND ----------

display(matures_norm_final_landing)

# COMMAND ----------

#check for duplicates
matures_norm_final_landing \
    .groupby(['cal_date', 'platform_subset', 'customer_engagement', 'geography', 'measure']) \
    .count() \
    .where('count > 1') \
    .sort('count', ascending=False) \
    .show()

# COMMAND ----------

#write_df_to_redshift(configs: config(), df: matures_norm_final_landing, destination: "stage"."usrs_matures_norm_final_landing", mode: str = "overwrite")
write_df_to_s3(df=matures_norm_final_landing, destination=f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/matures_norm_final_landing", format="parquet", mode="overwrite", upper_strings=True)
