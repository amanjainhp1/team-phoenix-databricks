# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

df_iink_stf = spark.read.format('csv').options(header='true', inferSchema='true').load('{}product/ib/instant_ink_enrollees/instant_ink_enrollees_stf/i_ink_stf.csv'.format(constants['S3_BASE_BUCKET'][stack]))

df_iink_stf.createOrReplaceTempView("instant_ink_enrollees_stf")

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
 LEFT(calendar_month,4) || '-' || RIGHT(calendar_month,2) || '-01' cal_date
,fiscal_qtr
,fiscal_yr
,country
,market10
,region_3
,program_type
,program_type2
,sub_brand
,series_name
,series_number
,vol_val
,mono_color
,platform_subset
,hp_plus_eligible_printer
,hw_sellto
,p1_enrollees
,p2_enrollees
,p1_net_enrollees
,p2_net_enrollees
,cancellations
,printer_replacements
,gross_new_enrollees
,net_enrollees
,cumulative_enrollees
,prior_cum
,fcst_type
,data_type
,fcst_cycle
,build_date
,cum_enrollee_qtr
,churn_rate
,NULL load_date
,NULL version
,1 official
FROM instant_ink_enrollees_stf
"""

df_final_iink_enrollees_stf = read_redshift_to_df(configs).option("query", final_iink_enrollees_stf).load()

# COMMAND ----------

write_df_to_redshift(configs, df_final_iink_enrollees_stf, "prod.instant_ink_enrollees_stf", "append")

# COMMAND ----------

 query_update_latest_version = 
 """
 UPDATE prod.instant_ink_enrollees_stf
 SET version  = (SELECT MAX(version) FROM prod.version WHERE record = 'IINK_ENROLLEES_STF'),
     load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'IINK_ENROLLEES_STF')
 WHERE version IS NULL
 """

# COMMAND ----------

submit_remote_query(configs, query_update_latest_version)
