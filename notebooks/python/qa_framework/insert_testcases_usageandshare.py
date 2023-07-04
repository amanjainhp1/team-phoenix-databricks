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
truncate_testcases_table_query =f""" delete from qa.test_cases where module_name='US';"""
submit_remote_query(configs,truncate_testcases_table_query )

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO qa.test_cases
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
INSERT INTO qa.test_cases
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
INSERT INTO qa.test_cases
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

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO qa.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES 
('Phoenix - QA','','','Data Check','US'
,'K usage should not be greater than usage','prod','prod.usage_share_country'
,'units','select platform_subset , geography ,cal_date ,customer_engagement,sum(usaget) usaget,sum(kusaget) kusaget
from 
(
select platform_subset , geography ,cal_date ,customer_engagement ,measure
,case when measure=''USAGE'' then sum(units) end usaget,
case when measure=''K_USAGE'' then sum(units) end kusaget  from prod.usage_share_country usc 
where measure in (''USAGE'',''K_USAGE'')
group by platform_subset , geography ,cal_date ,customer_engagement ,measure
)group by platform_subset , geography ,cal_date ,customer_engagement
having sum(kusaget)::decimal(18,2)>sum(usaget)::decimal(18,2)','','1','1'
,getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Data Check','US'
,'Color usage should not be greater than usage for color printers','prod','prod.usage_share_country'
,'units','select platform_subset , geography ,cal_date ,customer_engagement,sum(usaget) usaget,sum(colorusaget) colorusaget
from 
(
select usc.platform_subset , geography ,cal_date ,customer_engagement ,measure
,case when measure=''USAGE'' then sum(units) end usaget,
case when measure=''COLOR_USAGE'' then sum(units) end colorusaget  from prod.usage_share_country usc 
left join 
mdm.hardware_xref hxref 
on usc.platform_subset=hxref.platform_subset
where mono_color=''COLOR'' AND measure in (''USAGE'',''COLOR_USAGE'')
group by usc.platform_subset , geography ,cal_date ,customer_engagement ,measure
) a group by platform_subset , geography ,cal_date ,customer_engagement
having sum(colorusaget)::decimal(18,2)>sum(usaget)::decimal(18,2)','','1','1'
,getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Data availability Check','US'
,'Drop out between US and IB','prod','prod.usage_share_country'
,'US','select  country_alpha2, a.platform_subset, count(1)
from prod.ib a left join mdm.hardware_xref b on a.platform_subset=b.platform_subset
where country_alpha2||UPPER(a.platform_subset) in
(
select country_alpha2||UPPER(platform_subset) from prod.ib where version
=(select max(ib_version) from prod.usage_share_country where version=(select max(version) from prod.usage_share_country ))
and units>0
except
select geography||UPPER(platform_subset) from prod.usage_share_country
)
group by country_alpha2,a.platform_subset','','1','1'
,getdate(),'admin',1,'Medium')
;"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO qa.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES 
('Phoenix - QA','','','Range Check','US','Usage should not be less than 0','prod','prod.usage_share_country','usage','select * from prod.usage_share_country where measure=''USAGE'' and  units<0','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Range Check','US','Share should be between 0-1','prod','prod.usage_share_country','share','select * from prod.usage_share_country where measure=''HP_SHARE'' and  (units<0 or units>1) ','','1','1',getdate(),'admin',1,'Medium');"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO qa.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES 
('Phoenix - QA','','','Biz Validation testing','US','Sum of k usage and color usage equals usage','prod','prod.usage_share_country','usage','with a as 
(select  geography ,platform_subset ,customer_engagement ,coalesce (sum(units),0) k_color_usage_sum from prod.usage_share_country usc 
where measure in ( ''K_USAGE'',''COLOR_USAGE'') group by  geography ,platform_subset ,customer_engagement )
, c as  (select  geography ,platform_subset ,customer_engagement  , coalesce (sum(units),0) usage_sum
from prod.usage_share_country usc where measure = ''USAGE'' group by  geography ,platform_subset ,customer_engagement )
select  a.geography ,a.platform_subset ,a.customer_engagement , round(k_color_usage_sum,4) ,round(usage_sum,4) ,hx.product_lifecycle_status_usage 
from c left join a on  a.geography = c.geography and a.platform_subset = c.platform_subset and a.customer_engagement = c.customer_engagement
left join mdm.hardware_xref hx on  a.platform_subset = hx.platform_subset
where coalesce(round(k_color_usage_sum,4),0)<> coalesce(round(usage_sum,4),0)','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','US','IB units equals US units','prod','prod.usage_share_country','units','with a as
(select record, cal_date, country_alpha2, platform_subset, customer_engagement,version, sum(units) ib_units from prod.ib where measure =''IB''
and version = (select max(ib_version) from prod.usage_share_country where version = (select max(version) from prod.usage_share_country))
group by record, cal_date, country_alpha2, platform_subset, customer_engagement,version)
,b as (select record, cal_date, geography, platform_subset, customer_engagement,- sum(units) us_units
from prod.usage_share_country usc where measure =''IB''
and ib_version = (select max(ib_version) from prod.usage_share_country where version = (select max(version) from prod.usage_share_country))
group by record, cal_date, geography, platform_subset, customer_engagement)
select a.record,b.record, a.cal_date,b.cal_date, a.country_alpha2,b.geography, a.platform_subset,b.platform_subset,
a.customer_engagement,b.customer_engagement,ib_units,us_units
from a left join b
on 
-- record = b.record
a.cal_date = b.cal_date
and a.country_alpha2 = b.geography
and a. platform_subset = b.platform_subset
and a.customer_engagement=b.customer_engagementwhere ib_units <> us_units','','1','1',getdate(),'admin',1,'Medium')

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table
