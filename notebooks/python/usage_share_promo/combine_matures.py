# Databricks notebook source
# MAGIC %md
# MAGIC # Usage Share Refactor Step 04
# MAGIC - Combine data
# MAGIC - This portion will be run only when there is a new usage/share run (ie Quarter End)

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# for interactive sessions, define a version widget
dbutils.widgets.text("ink_current_version", "")
dbutils.widgets.text("toner_locked_version", "")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

datestamp = dbutils.jobs.taskValues.get(taskKey = "matures", key = "datestamp")
ink_current_version = dbutils.widgets.get('ink_current_version')
toner_locked_version = dbutils.widgets.get('toner_locked_version')

print("datestamp: " + datestamp)
print("ink_current_version: " + ink_current_version)
print("toner_locked_version: " + toner_locked_version)

# COMMAND ----------

# Read in Current data
#current_table=need to get step 01 results
toner_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/{toner_locked_version}/toner_locked*")
toner_table.createOrReplaceTempView("toner_table")

ink_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}spectrum/cupsm/{ink_current_version}/ink*")
ink_table.createOrReplaceTempView("ink_table")

# COMMAND ----------

#combine ink and toner (need to add LF)
current_table = """
SELECT * FROM toner_table
UNION ALL
SELECT * FROM ink_table
"""

current_table=spark.sql(current_table)
current_table.createOrReplaceTempView("current_table")

# COMMAND ----------

# Read in Mature data
matures_table = spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}usage_share_promo/{datestamp}/matures_norm_final_landing")
matures_table.createOrReplaceTempView("matures_table")

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

#Add group ID, and product lifecycle status information to current data
current_1 = """

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
      ,h.epa_family
      ,CONCAT(c.platform_subset,c.customer_engagement,c.geography,c.cal_date) as grp_id
FROM current_table c
LEFT JOIN hardware_info h
    ON c.platform_subset=h.platform_subset
"""

current_1=spark.sql(current_1)
current_1.createOrReplaceTempView("current_1")

# COMMAND ----------

#Add group ID, and product lifecycle status information to mature data
mature_1 = """

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
      ,h.epa_family
      ,CONCAT(c.platform_subset,c.customer_engagement,c.geography,c.cal_date) as grp_id
FROM matures_table c
LEFT JOIN hardware_info h
    ON c.platform_subset=h.platform_subset
WHERE c.units >0 
    AND h.product_lifecycle_status != 'E'
"""

mature_1=spark.sql(mature_1)
mature_1.createOrReplaceTempView("mature_1")

# COMMAND ----------

# get where data is in both current and matures
overlap_1 = """

---Find overlap between current and mature
SELECT c.record
      ,c.cal_date
      ,c.geography_grain
      ,c.geography
      ,c.platform_subset
      ,c.customer_engagement
      ,c.forecast_process_note
      ,CASE WHEN c.product_lifecycle_status_share='M' AND c.measure='HP_SHARE' THEN m.data_source
            WHEN c.product_lifecycle_status_usage='M' AND c.measure like '%USAGE%' THEN m.data_source
            ELSE c.data_source
            END AS data_source
      ,c.version
      ,c.measure
      ,CASE WHEN c.product_lifecycle_status_share='M' AND c.measure='HP_SHARE' THEN m.units
            WHEN c.product_lifecycle_status_usage='M' AND c.measure like '%USAGE%' THEN m.units
            ELSE c.units
            END AS units
      ,c.proxy_used
      ,c.ib_version
      ,c.load_date
      ,c.grp_id
FROM current_1 c
INNER JOIN mature_1 m
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
      ,data_source
      ,version
      ,measure
      ,units
      ,proxy_used
      ,ib_version
      ,load_date
FROM overlap_1)
, mat_1 as (SELECT record
      ,cal_date
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,forecast_process_note
      ,data_source
      ,version
      ,measure
      ,units
      ,proxy_used
      ,ib_version
      ,load_date
FROM mature_1
WHERE grp_id not in (select distinct grp_id from overlap_1))
, combine as (
SELECT * FROM mat_1
UNION ALL
SELECT * FROM ovr_1
UNION ALL
SELECT * FROM cur_1
)
SELECT * FROM combine

"""

combine_1=spark.sql(combine_1)
combine_1.createOrReplaceTempView("overlap_1")

# COMMAND ----------

from datetime import date

# retrieve current date
cur_date = date.today().strftime("%Y.%m.%d")

#execute stored procedure to create new version and load date
record = 'BASE_USAGE_SHARE'
source_name = f'{record} - {cur_date}'
max_version_info = call_redshift_addversion_sproc(configs=configs, record=record, source_name=source_name)

max_version = max_version_info[0]
max_load_date = max_version_info[1]

# COMMAND ----------

# retrieve ink and toner record names
ink_record_name = read_redshift_to_df(configs) \
    .option('query', f"SELECT record FROM prod.version WHERE record LIKE ('INK_Q%') AND version = '{ink_current_version}'") \
    .load() \
    .rdd.flatMap(lambda x: x).collect()[0]

toner_record_name = read_redshift_to_df(configs) \
    .option('query', f"SELECT record FROM prod.version WHERE record LIKE ('TONER_LOCKED_VERSION') AND version = '{toner_locked_version}'") \
    .load() \
    .rdd.flatMap(lambda x: x).collect()[0]

# insert records into scenario table to link demand back to underlying CUPSM datasets
insert_query = f"""
INSERT INTO prod.scenario VALUES
('{source_name}', '{ink_record_name}', '{ink_current_version}', '{max_load_date}'),
('{source_name}', '{toner_record_name}', '{toner_locked_version}', '{max_load_date}');
"""
submit_remote_query(configs, insert_query)

# COMMAND ----------

#write_df_to_redshift(configs: config(), df: matures_norm_final_landing, destination: "stage"."usrs_matures_norm_final_landing", mode: str = "overwrite")
write_df_to_s3(df=combine_1, destination=f"{constants['S3_BASE_BUCKET'][stack]}spectrum/base_usage_share/{max_version}", format="parquet", mode="overwrite", upper_strings=True)
