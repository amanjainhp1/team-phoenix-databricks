# Databricks notebook source
dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.dropdown("redshift_secrets_region_name", "us-west-2", ["us-west-2", "us-east-2"])
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("stack", "")
dbutils.widgets.text("job_dbfs_path", "")

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

df_decay_pro = spark.read.format('csv').options(header='true', inferSchema='true').load('{}product/mdm/decay/large_format/lf_decay_pro.csv'.format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")]))
df_decay_design = spark.read.format('csv').options(header='true',inferSchema='true').load('{}product/mdm/decay/large_format/lf_decay_design.csv'.format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")]))

df_lf_decay = df_decay_design.union(df_decay_pro)

# COMMAND ----------

# if table exists, truncate, else print exception message
try:
    row_count = read_redshift_to_df(configs).option("dbtable", "stage.lf_decay_temp").load().count()
    if row_count > 0:
        submit_remote_query(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'], "TRUNCATE stage.lf_decay_temp")
except Exception as error:
  print ("An exception has occured:", error)

# COMMAND ----------

add_version_sproc = """
call prod.addversion_sproc('HW_DECAY_LF', 'FORECASTER INPUT');  
"""

lf_decay_sproc = """

	----------------------------------
 SELECT 'HW_DECAY_LF' record,platform_subset platform_subset,region,'TRAD' split_name,"Year",val value
 into #decay_unpvt
 FROM 
 (
SELECT  region,platform_subset, year_1, year_2, year_3, year_4, year_5, year_6, year_7, year_8, year_9, year_10, year_11, year_12, year_13, year_14, year_15
FROM stage.lf_decay_temp
  ) A
  UNPIVOT
  (val FOR "Year" IN (
 year_1, year_2, year_3, year_4, year_5, year_6, year_7, year_8, year_9, year_10, year_11, year_12, year_13, year_14, year_15)
  ) AS Unpvt;

---------------------SET PREVIOUS VERSION TO 0 ------------------------------
  
  UPDATE prod.decay 
  set official  = 0
  where record  = 'HW_DECAY_LF';
  
  
 ------------------------INSERT TO PROD---------------------------------
  
  insert into prod.decay(record,platform_subset,geography_grain,geography,"year",split_name,value,official)
  select record ,platform_subset,'region_3' geogoraphy_grain,  case when region  = 'Asia Pacific' then 'AP' when region  = 'Europe' then 'EU' 
  	when region  = 'Japan' then 'JP' when region  = 'Latin America' then 'LA' when region  = 'North America' then 'NA' end as geography 
  ,"year",split_name ,value,1 official 
  from #decay_unpvt;
-----------------udpate version and load date---------------------------
  
update  prod.ib
set load_date = (select max(load_date) from prod.version where record = 'HW_DECAY_LF'),
version = (select max(version) from prod.version where record = 'HW_DECAY_LF')
where version is null;

"""

# COMMAND ----------

write_df_to_redshift(configs, df_lf_decay, "stage.lf_decay_temp", "append", add_version_sproc + "\n" + lf_decay_sproc)
