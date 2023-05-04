# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

df_iink_stf = spark.read.format('csv').options(header='true', inferSchema='true').load('{}product/ib/instant_ink_enrollees/instant_ink_enrollees_stf/i_ink_stf.csv'.format(constants['S3_BASE_BUCKET'][stack]))

# COMMAND ----------

write_df_to_redshift(configs, df_iink_stf, "stage.instant_ink_enrollees_stf", "overwrite")

# COMMAND ----------

 query_set_previous_official_zero = """
 UPDATE prod.instant_ink_enrollees_stf 
 SET official  = 0 
 """

# COMMAND ----------

submit_remote_query(configs, query_set_previous_official_zero)

# COMMAND ----------

query_add_version = """
CALL prod.addversion_sproc('IINK_ENROLLEES_STF', 'FORECASTER INPUT');  
"""

# COMMAND ----------

submit_remote_query(configs, query_add_version)

# COMMAND ----------

final_iink_enrollees_stf = """
SELECT
 cast(calendar_month_date as date) cal_date
,fiscal_qtr
,fiscal_yr
,country
,market10
,region_3
,program_type
,program_type2
,sub_brand
,series_name
,cast(series_number as int) series_number
,vol_val
,mono_color
,platform_subset
,hp_plus_eligible_printer
,ink_platform
,hw_sellto
,p1_enrollees
,p2_enrollees
,partial_enrollees
,enroll_customer
,enroll_replacement
,gross_new_enrollees
,p1_cancels
,p2_cancels
,partial_cancels
,cancel_customer
,cancel_replacement
,gross_cancels
,net_enrollees
,net_p1_enrollees
,net_p2_enrollees
,cumulative_enrollees
,cumulative_p1_enrollees
,cumulative_p2_enrollees
,prior_cum
,qtr_cumulative_enrollees
,fcst_type
,data_type
,fcst_cycle
,cast(build_date as date) build_date
,NULL load_date
,NULL version
,1 official
FROM stage.instant_ink_enrollees_stf
"""

df_final_iink_enrollees_stf = read_redshift_to_df(configs).option("query", final_iink_enrollees_stf).load()

# COMMAND ----------

write_df_to_redshift(configs, df_final_iink_enrollees_stf, "prod.instant_ink_enrollees_stf", "append")

# COMMAND ----------

 query_update_latest_version = """
 UPDATE prod.instant_ink_enrollees_stf
 SET version  = (SELECT MAX(version) FROM prod.version WHERE record = 'IINK_ENROLLEES_STF'),
     load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'IINK_ENROLLEES_STF')
 WHERE version IS NULL
 """

# COMMAND ----------

submit_remote_query(configs, query_update_latest_version)
