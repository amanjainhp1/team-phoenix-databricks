# Databricks notebook source
import pyspark.sql.functions as f
import time
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

#truncate test case table 
truncate_testcases_table_query =f""" delete from stage.test_cases where module_name='US';"""
submit_remote_query(configs,truncate_testcases_table_query )

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES 
('Phoenix - QA','','','Data availability check','US','Check if ib is available for share data','prod','prod.usage_share_country','share','','QA Framework/usageandshare/shareebutnoIB.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Duplicate Check','US','Check for duplicates','prod','prod.usage_share_country','US','','QA Framework/usageandshare/Duplicate_check.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Data availability check','US','Drop in usage or share','prod','prod.usage_share_country','US','','QA Framework/usageandshare/Usage_share_drop.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Data availability check','US','Check if color usage is available for MONO platform','prod','prod.usage_share_country','ib','','QA Framework/usageandshare/Colorusageformonoplatform.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Data availability check','US','Check for measures for color platform','prod','prod.usage_share_country','US','','QA Framework/usageandshare/colorplatformmeasurescheck.sql','1','1',getdate(),'admin',1,'Medium')
;"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES 
('Phoenix - QA','','','MDM Check','US'
,'Check if geography is available in mdm','prod','prod.usage_share_country'
,'geography','select * from 
(select geography from prod.usage_share_country  where geography is not null except select country_alpha2  from mdm.iso_country_code_xref  )','','1','1'
,getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','MDM Check','US'
,'Check if platform is available in mdm','prod','prod.usage_share_country'
,'platform_subset','select * from 
(select platform_subset from prod.usage_share_country  where platform_subset is not null except select platform_subset from mdm.hardware_xref )','','1','1'
,getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Data availability check','US','Check if share is available for ib data','prod','prod.usage_share_country'
,'share','','QA Framework/usageandshare/Ib but no share.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Data availability check','US'
,'Check if platform is available in usage share','prod','prod.usage_share_country'
,'platform_subset','select * from 
(select platform_subset from prod.usage_share_country  where platform_subset is not null except select platform_subset from prod.usage_share )','','1','1'
,getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Duplicate check','US'
,'Check if source is duplicate','prod','prod.usage_share_country'
,'platform_subset','select record,cal_date,geography_grain,geography,platform_subset,customer_engagement,measure,count(distinct source)
from prod.usage_share_country
group by record,cal_date,geography_grain,geography,platform_subset,customer_engagement,measure
having count(distinct source)>1','','1','1'
,getdate(),'admin',1,'Medium')
;"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES 
('Phoenix - QA','','','Duplicate check','US'
,'Check if source is duplicate','prod','prod.usage_share_country'
,'platform_subset','select record,cal_date,geography_grain,geography,platform_subset,customer_engagement,measure,count(distinct source)
from prod.usage_share_country
group by record,cal_date,geography_grain,geography,platform_subset,customer_engagement,measure
having count(distinct source)>1','','1','1'
,getdate(),'admin',1,'Medium')
;"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES 
('Phoenix - QA','','','VOV Check','US'
,'Proxy - cycle over cycle comparisons','phoenix_spectrum_prod','phoenix_spectrum_prod.cupsm'
,'proxy','','QA Framework/usageandshare/Proxycoccomparison.sql','1','1'
,getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Data Check','US'
,'Start date comparsion between usage and IB','prod','prod.usage_share_country'
,'cal_date','','QA Framework/usageandshare/startdateval_IBvsUS.sql','1','1'
,getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','VOV Check','US'
,'Usage Share VOV check',' phoenix_spectrum_prod',' phoenix_spectrum_prod.usage_share_country'
,'units','','QA Framework/usageandshare/vovtesting.sql','1','1'
,getdate(),'admin',1,'Medium')
;"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table
