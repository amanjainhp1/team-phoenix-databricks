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

submit_remote_query(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'], "TRUNCATE stage.lf_decay_temp")

# COMMAND ----------

df_decay_pro = spark.read.format('csv').options(header='true', inferSchema='true').load('{}product/mdm/decay/Large Format/lf_decay_pro.csv'.format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")]))
df_decay_design = spark.read.format('csv').options(header='true', inferSchema='true').load('{}product/mdm/decay/Large Format/lf_decay_design.csv'.format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")]))

df_lf_decay = df_decay_design.union(df_decay_pro)

# COMMAND ----------

add_version_sproc = """
call prod.addversion_sproc('hw_decay_lf', 'forecaster_input');  
"""

lf_decay_sproc = """
	
call prod.addversion_sproc('hw_decay_lf', 'forcaster input');

	----------------------------------
 SELECT 'hw_decay_lf' record,platform_subset platform_subset,region, pens,'TRAD' split_name,"Year",val value
 into #decay_unpvt
 FROM 
 (
SELECT record, region, platform_subset, pens, year_1, year_2, year_3, year_4, year_5, year_6, year_7, year_8, year_9, year_10, year_11, year_12, year_13, year_14, year_15
FROM stage.lf_decay_temp
  ) A
  UNPIVOT
  (val FOR "Year" IN (
 year_1, year_2, year_3, year_4, year_5, year_6, year_7, year_8, year_9, year_10, year_11, year_12, year_13, year_14, year_15)
  ) AS Unpvt;

---------------------SET PREVIOUS VERSION TO 0 ------------------------------
  
  UPDATE prod.decay 
  set official  = 0
  where record  = 'hw_decay_lf';
  
  
 ------------------------INSERT TO PROD---------------------------------
  
  insert into prod.decay(record,platform_subset,geography_grain,geography,"year",split_name,value,official)
  select record ,platform_subset,'region_3' geogoraphy_grain,  case when region  = 'Asia Pacific' then 'AP' when region  = 'Europe' then 'EU' 
  	when region  = 'Japan' then 'JP' when region  = 'Latin America' then 'LA' when region  = 'North America' then 'NA' end as geography 
  ,"year",split_name ,value,1 official 
  from #decay_unpvt;
-----------------udpate version and load date---------------------------
  
update  prod.ib
set load_date = (select max(load_date) from prod.version where record = 'hw_decay_lf'),
version = (select max(version) from prod.version where record = 'hw_decay_lf')
where version is null;

"""

# COMMAND ----------

write_df_to_redshift(configs, df_lf_decay, "stage.lf_decay_temp", "append", add_version_sproc + "\n" + lf_decay_sproc)
