# Databricks notebook source
dbutils.widgets.text("redshift_secrets_name", "arn:aws:secretsmanager:us-west-2:740156627385:secret:dev/redshift/dataos-core-dev-01/auto_glue-dj6tOj")
dbutils.widgets.dropdown("redshift_secrets_region_name", "us-west-2", ["us-west-2", "us-east-2"])
dbutils.widgets.text("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role")
dbutils.widgets.text("stack", "dev")
dbutils.widgets.text("job_dbfs_path","")

# COMMAND ----------

import json

with open(dbutils.widgets.get("job_dbfs_path").replace("dbfs:", "/dbfs") + "/configs/constants.json") as json_file:
  constants = json.load(json_file)

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

redshift_secrets_name = dbutils.widgets.get("redshift_secrets_name")
redshift_secrets_region_name = dbutils.widgets.get("redshift_secrets_region_name")

redshift_username = secrets_get(redshift_secrets_name, redshift_secrets_region_name)["username"]
redshift_password = secrets_get(redshift_secrets_name, redshift_secrets_region_name)["password"]

# COMMAND ----------

configs = {}
configs["redshift_url"] = constants['REDSHIFT_URLS'][dbutils.widgets.get("stack")]
configs["redshift_port"] = constants['REDSHIFT_PORTS'][dbutils.widgets.get("stack")]
configs["redshift_dbname"] = dbutils.widgets.get("stack")
configs["aws_iam_role"] =  dbutils.widgets.get("aws_iam_role")
configs["redshift_username"] = redshift_username
configs["redshift_password"] = redshift_password
configs["redshift_temp_bucket"] = "{}redshift_temp/".format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")])
configs["redshift_dev_group"] = constants["REDSHIFT_DEV_GROUP"][dbutils.widgets.get("stack")]

# COMMAND ----------

submit_remote_query(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'], """UPDATE prod.instant_ink_enrollees
SET official = 0
WHERE UPPER(data_source) = 'FCST'""")

# COMMAND ----------

submit_remote_query(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'], """TRUNCATE TABLE stage.instant_ink_enrollees_staging""")

# COMMAND ----------

iink_enrollees_query = """

SELECT
 program_type as record
,country
,country_group_co as country_expanded  -- iso country code
,platform_subset
,sub_brand
,year_month
,year_fiscal
,quarter_fiscal
,yrqtr_fiscal
,data_source
,yr_mo_max_for_actuals
,cartridge_tech_n_name
,ctg_type
,ink_platform
,region_5 as region
,printer_sell_out_units
,printer_sell_out_units_participating
,enroll_card_units
,all_enrollments_customer
,all_enrollments_replacement
,p1_kit_enrollments
,p1_kitless_enrollments
,p2_kitless_enrollments
,cancellations_customer
,cancellations_replacement
,cum_enrollees_month
,cum_enrollees_quarter
,total_gross_new_enrollments
,NULL load_date
,NULL version
,1 official
FROM app_bm_instant_ink_bi.app_bm_instant_ink_bi.fcst_work_6f_summary_output_PR
WHERE record = 'Instant Ink'  AND year_month > (select case when cast(date_part(month,cast(max(year_month) as date)) as integer) < 10 THEN date_part(year,cast(max(year_month) as date)) || '0' || date_part(month,cast(max(year_month) as date))
when cast(date_part(year,cast(max(year_month) as date)) as integer) >=10 then date_part(year,cast(max(year_month) as date)) || date_part(month,cast(max(year_month) as date)) end mon
from prod.instant_ink_enrollees WHERE official = 1 AND data_source = 'Act')
"""

final_iink_enrollees = read_redshift_to_df(configs) \
  .option("query",iink_enrollees_query) \
  .load()

# COMMAND ----------

add_version_sproc = """
call prod.addversion_sproc('iink_ib', 'forecaster_input');  
"""

iink_proc = """

UPDATE stage.instant_ink_enrollees_staging
SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'iink_ib'),
version = (SELECT MAX(version) FROM prod.version WHERE record = 'iink_ib');

INSERT INTO prod.instant_ink_enrollees (record, country, platform_subset, sub_brand, year_month, year_fiscal, quarter_fiscal, yrqtr_fiscal, data_source, yr_mo_max_for_actuals, cartridge_tech_n_name, ctg_type, ink_platform, region, printer_sell_out_units, printer_sell_out_units_participating, enroll_card_units, all_enrollments_customer, all_enrollments_replacement, p1_kit_enrollments, p1_kitless_enrollments, p2_kitless_enrollments, cancellations_customer, cancellations_replacement, cum_enrollees_month, cum_enrollees_quarter, total_gross_new_enrollments, official, load_date, version)
SELECT 
 iel.record
,iel.country
,iel.platform_subset
,ISNULL(sub_brand,'UNKNOW') sub_brand
,LEFT(year_month,4) || '-' || RIGHT(year_month,2) || '-01' cal_date
,year_fiscal
,quarter_fiscal
,yrqtr_fiscal
,data_source
,yr_mo_max_for_actuals
,cartridge_tech_n_name
,ctg_type
,ink_platform
,region
,printer_sell_out_units
,printer_sell_out_units_participating
,enroll_card_units
,all_enrollments_customer
,all_enrollments_replacement
,p1_kit_enrollments
,p1_kitless_enrollments
,p2_kitless_enrollments
,cancellations_customer
,cancellations_replacement
,cum_enrollees_month
,cum_enrollees_quarter
,total_gross_new_enrollments
,iel.official
,iel.load_date
,iel.version
FROM stage.instant_ink_enrollees_staging iel;

"""

# COMMAND ----------

write_df_to_redshift(configs,final_iink_enrollees, "stage.instant_ink_enrollees_staging", "append", add_version_sproc + "\n" + iink_proc)
