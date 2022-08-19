# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

submit_remote_query(configs,"""UPDATE prod.instant_toner_enrollees
SET official = 0
WHERE UPPER(data_source) = 'FCST'""")

# COMMAND ----------

try:
    row_count = read_redshift_to_df(configs).option("dbtable", "stage.instant_toner_enrollees_staging").load().count()
    if row_count > 0:
        submit_remote_query(configs, """TRUNCATE TABLE stage.instant_toner_enrollees_staging""")
except Exception as error:
    print ("An exception has occured:", error)

# COMMAND ----------

itoner_enrollees_query = """
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
WHERE UPPER(program_type) = 'INSTANT TONER'  AND year_month > (select case when cast(date_part(month,cast(max(year_month) as date)) as integer) < 10 THEN date_part(year,cast(max(year_month) as date)) || '0' || date_part(month,cast(max(year_month) as date))
when cast(date_part(year,cast(max(year_month) as date)) as integer) >=10 then date_part(year,cast(max(year_month) as date)) || date_part(month,cast(max(year_month) as date)) end mon
from prod.instant_toner_enrollees WHERE official = 1 AND UPPER(data_source) = 'ACT')
"""

final_itoner_enrollees = read_redshift_to_df(configs) \
  .option("query",itoner_enrollees_query) \
  .load()

# COMMAND ----------

add_version_sproc = "call prod.addversion_sproc('IINK_IB', 'FORECASTER_INPUT');"

itoner_proc = """
UPDATE stage.instant_ink_enrollees_staging
SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'IINK_IB'),
version = (SELECT MAX(version) FROM prod.version WHERE record = 'IINK_IB');

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
FROM stage.instant_toner_enrollees_staging iel;
"""

# COMMAND ----------

write_df_to_redshift(configs,final_itoner_enrollees, "stage.instant_toner_enrollees_staging", "append", add_version_sproc + "\n" + itoner_proc)
