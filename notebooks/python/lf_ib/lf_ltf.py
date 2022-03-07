# Databricks notebook source
dbutils.widgets.text("redshift_secret_name", "")
dbutils.widgets.dropdown("redshift_secrets_region_name", "us-west-2", ["us-west-2", "us-east-2"])
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("stack", "")

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

# COMMAND ----------

submit_remote_query(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'], "TRUNCATE stage.lf_ltf_temp")

# COMMAND ----------

df_lf_ltf_design = spark.read.format('csv').options(header='true', inferSchema='true').load('{}product/norm_ships/fcst/ltf/Large Format/lf_ltp_design.csv'.format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")]))
df_lf_ltf_pro = spark.read.format('csv').options(header='true', inferSchema='true').load('{}product/norm_ships/fcst/ltf/Large Format/lf_ltp_pro.csv'.format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")]))

df_lf_ltf = df_lf_ltf_design.union(df_lf_ltf_pro)

# COMMAND ----------

add_version_sproc = """
call prod.addversion_sproc('hw_ltf_lf', 'forecaster_input');  
"""

lf_ltf_sproc = """

 SELECT 'hw_ltf_lf' record,region_3,platform_subset ltf_platform_subset, LEFT(FY,4) || '-' || RIGHT(FY,2) || '-01'  cal_date, cast(val as double precision) value
into #ltf_unpvt_wo_splits
 FROM 
 (
SELECT region_3, platform_subset, "202111", "202112", "202201", "202202", "202203", "202204", "202205", "202206", "202207", "202208", "202209", "202210", "202211", "202212", "202301", 
"202302", "202303", "202304", "202305", "202306", "202307", "202308", "202309", "202310", "202311", "202312", "202401", "202402", "202403", "202404", "202405", "202406", "202407", "202408", 
"202409", "202410", "202411", "202412", "202501", "202502", "202503", "202504", "202505", "202506", "202507", "202508", "202509", "202510", "202511", "202512", "202601", "202602", "202603", 
"202604", "202605", "202606", "202607", "202608", "202609", "202610"
FROM stage.lf_ltf_temp
) 
UNPIVOT
(val FOR FY IN (
 "202111", "202112", "202201", "202202", "202203", "202204", "202205", "202206", "202207", "202208", "202209", "202210", "202211", "202212", "202301", 
"202302", "202303", "202304", "202305", "202306", "202307", "202308", "202309", "202310", "202311", "202312", "202401", "202402", "202403", "202404", "202405", "202406", "202407", "202408", 
"202409", "202410", "202411", "202412", "202501", "202502", "202503", "202504", "202505", "202506", "202507", "202508", "202509", "202510", "202511", "202512", "202601", "202602", "202603", 
"202604", "202605", "202606", "202607", "202608", "202609", "202610" )
 );


--------------------APPLY LTF SPLITS TO CONVERT FROM FIJI NAMES TO OE NAMES---------------------------

select t.record,t.region_3,sp.ie_platform_subset platform_subset,t.cal_date,(t.value * sp.split_rate) value
into #ltf_w_splits
from #ltf_unpvt_wo_splits t 
LEFT JOIN prod.lf_ltf_splits sp on sp.ltf_platform_subset = t.ltf_platform_subset;

----------------------set previous version to official = 0------------------------------
update prod.hardware_ltf  
set official = 0 
where record  = 'hw_ltf_lf';

-----------------------------------------------------------------CONVERT TO COUNTRY GRANULARITY--------------------------------------------

------------------TOTAL QTY---------------------------------------  

SELECT platform_subset,c.region_3,SUM(base_quantity) Total_qty
INTO #rgn_3_total
FROM prod.actuals_hw ac
LEFT JOIN mdm.iso_country_code_xref c on c.country_alpha2 = ac.country_alpha2
WHERE ac.cal_date BETWEEN '2019-11-01' AND '2020-10-31' and ac.record  = 'actuals_lf' and ac.official  = 1
GROUP BY platform_subset,c.region_3;
   

----------------QTY PER COUNTRY------------------------------  

SELECT Platform_Subset,ac.country_alpha2,c.region_3,SUM(base_quantity) Cntry_qty
INTO #country_total
FROM prod.actuals_hw ac
LEFT JOIN mdm.iso_country_code_xref c on c.country_alpha2 = ac.country_alpha2
WHERE ac.cal_date BETWEEN '2019-11-01' AND '2020-10-31' and ac.record  = 'actuals_lf' and ac.official  = 1
GROUP BY Platform_Subset,ac.country_alpha2,c.region_3;


----------------------------------COUNTRY MIX---------------------------------------------------------------------

SELECT n.Platform_Subset,n.country_alpha2,n.region_3,CASE WHEN SUM(d.Total_qty) = 0 THEN 0 ELSE SUM(n.Cntry_qty)/SUM(d.Total_qty) END Cntry_mix
INTO #cntry_mix
FROM #country_total n 
INNER JOIN #rgn_3_total d on n.platform_subset = d.Platform_Subset and n.region_3 = d.region_3
GROUP BY n.Platform_Subset,n.country_alpha2,n.region_3;

--------------------------------APPLY MIX TO LTF-------------------------------------------------------------------------------

SELECT ltf.cal_date,c.country_alpha2,ltf.region_3,ltf.platform_subset,value*c.Cntry_mix value
INTO #cntry_ltf
FROM #ltf_w_splits ltf
INNER JOIN #cntry_mix c ON c.Platform_Subset = ltf.platform_subset AND c.region_3 = ltf.region_3
WHERE c.country_alpha2 IS NOT NULL

UNION

SELECT ltf.cal_date,c.country_alpha2,ltf.region_3,ltf.platform_subset,value*c.Cntry_mix value
FROM #ltf_w_splits ltf
LEFT JOIN mdm.hardware_xref hw on hw.platform_subset = ltf.platform_subset
LEFT JOIN #cntry_mix c ON c.Platform_Subset = hw.predecessor AND c.region_3 = ltf.region_3
LEFT JOIN #cntry_mix c1 ON c1.Platform_Subset = ltf.platform_subset AND c1.region_3 = ltf.region_3
WHERE c1.country_alpha2 IS null;

-----------------------------INSERT TO PROD--------------------------------

insert into prod.hardware_ltf (record,forecast_name,cal_date,country_alpha2,platform_subset,base_product_number,units,official)
select 'hw_ltf_lf' record,'Large Format' forecast_name,to_date(cal_date,'YYYY-MM-DD') ,country_alpha2,platform_subset ,'Not Available' base_product_number ,value,1 official 
from #cntry_ltf;

----------------UPDATE version--------------------------

update  prod.hardware_ltf
set load_date = (select max(load_date) from prod.version where record = 'hw_ltf_lf'),
version = (select max(version) from prod.version where record = 'hw_ltf_lf')
where version is null and record  = 'hw_ltf_lf';
"""

# COMMAND ----------

write_df_to_redshift(configs, df_lf_ltf, "stage.lf_ltf_temp", "append", add_version_sproc + "\n" + lf_ltf_sproc)
