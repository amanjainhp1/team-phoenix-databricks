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
truncate_testcases_table_query =f""" delete from stage.test_cases;"""
submit_remote_query(configs,truncate_testcases_table_query )

# COMMAND ----------

# insert records into test case table
insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category,module_name,test_case_name, schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES('Phoenix - QA','','ITG','Data availability check','Actuals HW/ Actuals LF','Check if base product number is available','stage','stage.actuals_hw','base_product_number','select * from stage.actuals_hw where base_product_number is null or base_product_number= '''' ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','Actuals HW/ Actuals LF','Check if base quantity is available','stage','stage.actuals_hw','base_quantity','select * from stage.actuals_hw where base_quantity = 0 or base_quantity is NULL','','1','1',getdate(),'admin',1,'Very Low')
,('Phoenix - QA','','ITG','Data availability check','Actuals HW/ Actuals LF','Check if platform subset is available','stage','stage.actuals_hw','platform_subset','select * from stage.actuals_hw where platform_subset is NULL','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','Actuals HW/ Actuals LF','Check if country is available','stage','stage.actuals_hw','country_alpha2','select * from stage.actuals_hw where country_alpha2 is NULL','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','Actuals HW/ Actuals LF','Check for missing country values in mdm','stage','stage.actuals_hw','country_alpha2','select * from(select country_alpha2  from stage.actuals_hw  where country_alpha2  is not null except select country_alpha2  from mdm.iso_country_code_xref )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','Actuals HW/ Actuals LF','Check for missing platform subset in mdm','stage','stage.actuals_hw','platform_subset','select * from (select platform_subset from stage.actuals_hw  where platform_subset is not null except select platform_subset from mdm.hardware_xref )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','Actuals HW/ Actuals LF','Check if platform subset value is GENERIC','stage','stage.actuals_hw','platform_subset','select * from stage.actuals_hw where UPPER(platform_subset) like ''%GENERIC%'' ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','FLASH','Check if units is available','stage','stage.flash_stage','units','select * from stage.flash_stage where units is null or units = 0','','1','1',getdate(),'admin',1,'Very Low')
,('Phoenix - QA','','ITG','Data availability check','FLASH','Check if base product number is available','stage','stage.flash_stage','base_prod_number','select * from stage.flash_stage where base_prod_number  is null or base_prod_number ='''' ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','FLASH','Check if country is available','stage','stage.flash_stage','geo','select * from stage.flash_stage where geo is null','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','FLASH','Check for missing country in mdm','stage','stage.flash_stage','geo','select * from (select geo  from stage.flash_stage  where geo  is not null except select country_alpha2  from mdm.iso_country_code_xref ) ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','FLASH','Check for missing platform subset in mdm','stage','stage.flash_stage','platform_subset','select f.* from stage.flash_stage f left join mdm.rdma rdma on f.base_prod_number = rdma.base_prod_number LEFT JOIN mdm.hardware_xref AS hw ON hw.platform_subset = rdma.platform_subset where hw.platform_subset is null','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Duplicate check','FLASH','Check for duplicate records','stage','stage.flash_stage','units','select geo,base_prod_number,date,yeti_type,count(1) from stage.flash_stage group by geo,base_prod_number,yeti_type,date having count(1)>1 ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','WD3','Check if units is available','stage','stage.wd3_stage','units','select * from stage.wd3_stage where units is null','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','ITG','Data availability check','WD3','Check if base product number is available','stage','stage.wd3_stage','base_prod_number','select * from stage.wd3_stage where base_prod_number  is null or base_prod_number ='''' ','','1','1',getdate(),
'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','WD3','Check for missing country in mdm','stage','stage.wd3_stage','geo','select * from (select geo  from stage.wd3_stage  where geo  is not null except select country_alpha2  from mdm.iso_country_code_xref  )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','WD3','Check for missing platform subset in mdm','stage','stage.wd3_stage','platform_subset','select f.* from stage.wd3_stage f left join mdm.rdma rdma on f.base_prod_number = rdma.base_prod_number LEFT JOIN mdm.hardware_xref AS hw ON hw.platform_subset = rdma.platform_subset where hw.platform_subset is null','','1','1',getdate(),'admin',1,'Critical' )
,('Phoenix - QA','','ITG','Duplicate check','WD3','Check for duplicate records','stage','stage.wd3_stage','units','select geo,base_prod_number,date,yeti_type,count(1) from stage.wd3_stage group by geo,base_prod_number,date,yeti_type having count(1)>1 ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','HARDWARE LTF','Check if base product number is available','prod','prod.hardware_ltf','base_product_number','select * from prod.hardware_ltf where base_product_number is null or base_product_number= '''' and official=1 ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','HARDWARE LTF','Check if platform subset is available','prod','prod.hardware_ltf','platform_subset','select * from prod.hardware_ltf where platform_subset is NULL and official=1','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','HARDWARE LTF','Check if country is available','prod','prod.hardware_ltf','country_alpha2','select * from prod.hardware_ltf where country_alpha2 is NULL and official=1','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','HARDWARE LTF','Check for missing country values in mdm','prod','prod.hardware_ltf','country_alpha2','select * from (select country_alpha2  from prod.hardware_ltf  where country_alpha2  is not null except select country_alpha2  from mdm.iso_country_code_xref )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','HARDWARE LTF','Check for missing platform subset values in mdm','prod','prod.hardware_ltf','platform_subset','select * from (select platform_subset from prod.hardware_ltf  where platform_subset is not null except select platform_subset from mdm.hardware_xref )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','HARDWARE LTF','Check for GENERIC values in platform subset','prod','prod.hardware_ltf','platform_subset','select * from prod.hardware_ltf where UPPER(platform_subset) like ''%GENERIC%'' and official=1 ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Duplicate check','HARDWARE LTF','Check for duplicate values','prod','prod.hardware_ltf','units','select cal_date,platform_subset,base_product_number,country_alpha2,version,record,count(1) from prod.hardware_ltf where official=1 group by cal_date,platform_subset,base_product_number,country_alpha2,version,record having count(1)>1 ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Duplicate check','Actuals HW/ Actuals LF','Check for duplicate values','prod','prod.actuals_hw','base quantity','select version,country_alpha2, platform_subset,base_product_number,cal_date,count(1) from prod.actuals_hw group by  version,country_alpha2, platform_subset,base_product_number,cal_date having count(1)>1 ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','VOV check','Actuals HW/ Actuals LF','Check for VoV change','prod','prod.actuals_hw','base quantity','','QA Framework/actuals_hw_vov_check.sql','1','1',getdate(),'admin',1,'Medium')
;"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES ('Phoenix - QA','','ITG','VOV check','HARDWARE LTF','Check for vov change','prod','prod.hardware_ltf','units','','QA Framework/hardware_ltf_vov_check.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','ITG','MDM Check','Instant Ink Enrollees','Check for missing country values in MDM','prod','prod.instant_ink_enrollees','country','select * from (select country from prod.instant_ink_enrollees except select country_alpha2  from mdm.iso_country_code_xref ) ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','Instant Ink Enrollees','Check for missing platform subset values in MDM','prod','prod.instant_ink_enrollees','platform_subset','select * from (select platform_subset  from prod.instant_ink_enrollees except select platform_subset from mdm.hardware_xref ) ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','Instant Toner Enrollees','Check for missing country values in MDM','prod','prod.instant_toner_enrollees','platform_subset','select * from (select platform_subset  from prod.instant_toner_enrollees except select platform_subset from mdm.hardware_xref ) ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','Instant Toner Enrollees','Check for missing platform subset values in MDM','prod','prod.instant_toner_enrollees','country','select * from (select country from prod.instant_toner_enrollees except select country_alpha2  from mdm.iso_country_code_xref ) ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','CE Splits','Check for missing country values in MDM','prod','prod.ce_splits','country_alpha2','select * from (select country_alpha2 from prod.ce_splits except select country_alpha2  from mdm.iso_country_code_xref )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','CE Splits','Check for missing platform subset values in MDM','prod','prod.ce_splits','platform_subset','select * from (select platform_subset  from prod.ce_splits except select platform_subset  from mdm.hardware_xref hx)','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Duplicate check','CE Splits','Check for duplicate records','prod','prod.ce_splits','value','select record, platform_subset,region_5,country_alpha2,business_model,month_begin,split_name,version,count(1) from prod.ce_splits where official=1 group by record, platform_subset,region_5,country_alpha2,business_model,month_begin,split_name,version having count(1)>1 ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','CE Splits','Check if value is available','prod','prod.ce_splits','value','select * from prod.ce_splits where value=0','','1','1',getdate(),'admin',1,'Very Low')
,('Phoenix - QA','','ITG','MDM Check','Norm Ships','Check for missing country values in MDM','stage','stage.norm_ships','country_alpha2','select * from (select country_alpha2 from stage.norm_ships except select country_alpha2  from mdm.iso_country_code_xref )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','MDM Check','Norm Ships','Check for missing platform subset values in MDM','stage','stage.norm_ships','platform_subset','select * from (select platform_subset  from stage.norm_ships except select platform_subset  from mdm.hardware_xref hx )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Duplicate check','Norm Ships','Check for duplicate values','stage','stage.norm_ships','units','select region_5,record,cal_date,country_alpha2,platform_subset,version,count(1) from stage.norm_ships group by region_5,record,cal_date,country_alpha2,platform_subset,version having count(1)>1','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','Norm Ships','Check if unit value is available','stage','stage.norm_ships','units','select * from stage.norm_ships where units=0','','1','1',getdate(),'admin',1,'Very Low')
,('Phoenix - QA','','ITG','VOV check','Norm Ships','Check for vov change','prod','prod.norm_shipments','units','','QA Framework/norm_shipments_vov_check.sql','1','1',getdate(),'admin',1,'Medium')
;"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES ('Phoenix - QA','','ITG','Data availability check','IB','Check if ib is available','prod','prod.ib','units','select * from prod.ib
where (units is null or units =0) and official =1','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','ITG','Duplicate check','IB','Check for duplicate records','prod','prod.ib','units','select cal_date,country_alpha2,platform_subset,customer_engagement,version from prod.ib
where official=1
group by cal_date,country_alpha2,platform_subset,customer_engagement,version 
having count(1)>1','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Data availability check','IB','Check if ib is available','prod','prod.ib','platform_subset','select * from prod.ib where platform_subset like ''%GENERIC%'' ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','ITG','Biz Validation testing','IB','IB Greater than HW','prod','prod.ib','ib','','QA Framework/ib_bizval_IBGreaterthanHW.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','ITG','Biz Validation testing','IB','IB late','prod','prod.ib','ib','','QA Framework/ib_bizval_IBLate.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','ITG','Biz Validation testing','IB','IB Before NS','prod','prod.ib','ib','','QA Framework/ib_bizval_IBBeforeNS.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','ITG','Biz Validation testing','IB','IB Printer validation','prod','prod.ib','ib','','QA Framework/ib_bizval_Printervalidation.sql','1','1',getdate(),'admin',1,'Medium')
;"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases 
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES ('Phoenix - QA','','','MDM check','Norm Ships','Check for missing country values in MDM','prod','prod.norm_shipments','country_alpha2','select * from (select country_alpha2 from prod.norm_shipments except select country_alpha2  from mdm.iso_country_code_xref )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','MDM check','Norm Ships','Check for missing platform subset values in MDM','prod','prod.norm_shipments','platform_subset','select * from (select platform_subset  from prod.norm_shipments except select platform_subset  from mdm.hardware_xref hx )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Duplicate check','Norm Ships','Check for duplicate values','prod','prod.norm_shipments','units','select region_5,record,cal_date,country_alpha2,platform_subset,version,count(1) from prod.norm_shipments group by region_5,record,cal_date,country_alpha2,platform_subset,version having count(1)>1','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Data availability check','Norm Ships','Check if unit value is available','prod','prod.norm_shipments','units','select * from prod.norm_shipments where units=0','','1','1',getdate(),'admin',1,'Very Low')
;"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES 
('Phoenix - QA','','','STGVSPROD Check','IB','IB Stage vs PROD','prod','prod.ib','ib','','QA Framework/ib_stgvsprod.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','STGVSPROD Check','Actuals HW/ Actuals LF','Actuals Stage vs PROD','prod','prod.actuals_hw','units','','QA Framework/actuals_hw_stgvsprod.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','STGVSPROD Check','Norm Ships','Norm Ships Stage vs PROD','prod','prod.norm_shipments','units','','QA Framework/norm_ships_stgvsprod.sql','1','1',getdate(),'admin',1,'Medium')
;"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES 
('Phoenix - QA','','','Data availability check','IB','Check if platform subset value is GENERIC','stage','stage.ib_staging','platform_subset','select * from stage.ib_staging where UPPER(platform_subset) like ''%GENERIC%'' ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Duplicate check','IB','Check for duplicate values','stage','stage.ib_staging','units','select month_begin,country_alpha2,platform_subset,split_name,hps_ops ,version from stage.ib_staging group by month_begin,country_alpha2,platform_subset,split_name,hps_ops,version 
having count(1)>1','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','MDM Check','IB','Check for missing platform subset values in MDM','stage','stage.ib_staging','platform_subset','select * from (select platform_subset from stage.ib_staging except select platform_subset from mdm.hardware_xref ) ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','MDM check','IB','Check for missing country values in MDM','stage','stage.ib_staging','country_alpha2','select * from (select country_alpha2 from stage.ib_staging except select country_alpha2  from mdm.iso_country_code_xref )','','1','1',getdate(),'admin',1,'Critical') 
,('Phoenix - QA','','','Data availability check','IB','Drop out between IB and NS','stage','stage.ib_staging','IB','select * from (select record, country_alpha2, a.platform_subset, count(1) from stage.norm_ships a left join mdm.hardware_xref b on a.platform_subset=b.platform_subset 
where country_alpha2||UPPER(a.platform_subset) in ( select country_alpha2||UPPER(platform_subset) from stage.norm_ships except select country_alpha2||UPPER(platform_subset) from stage.ib_staging ) group by record,country_alpha2,a.platform_subset )','','1','1',getdate(),'admin',1,'Medium') 
,('Phoenix - QA','','','Biz Validation testing','IB','I-Ink date before Trad date','prod','prod.ib','ib','','QA Framework/iinkdatelessthantrad.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','IB','I-Ink and Trad IB greater than HW units','prod','prod.ib','ib','select ib.* ,ns_units from ( select platform_subset, country_alpha2, max(units) units from prod.ib where measure in (''IB'') and customer_engagement in (''TRAD'',''I-INK'') 
and version = (select max(version) from prod.ib where official = 1) group by platform_subset, country_alpha2) ib left join (select platform_subset, country_alpha2, sum(units) ns_units from prod.norm_shipments_ce where customer_engagement in (''TRAD'',''I-INK'') and version = (select max(version) from prod.ib where official = 1)
group by platform_subset, country_alpha2) ns on ib.platform_subset =ns.platform_subset and ib.country_alpha2 = ns.country_alpha2 where round(units,2) > round(ns_units,2)','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','IB','IB having all 1 values','prod','prod.ib','ib','select platform_subset,country_alpha2,customer_engagement,sum(reccntwith1ib)reccntwith1ib,sum(recordcount)recordcount from (select  platform_subset,country_alpha2,customer_engagement,cal_date,case when units=1 then 1 else 0 end as reccntwith1ib,1 recordcount from prod.ib where version=(select max(version) from prod.ib where official=1)
)a group by platform_subset,country_alpha2,customer_engagement having sum(reccntwith1ib)=sum(recordcount)','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','MDM Check','IB','Check for missing platform subset values in MDM','prod','prod.ib','platform_subset','select * from (select platform_subset from prod.ib except select platform_subset from mdm.hardware_xref ) ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','MDM check','IB','Check for missing country values in MDM','prod','prod.ib','country_alpha2','select * from (select country_alpha2 from prod.ib except select country_alpha2  from mdm.iso_country_code_xref )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','VOV Check','IB','IB VOV Check','prod','prod.ib','ib','','QA Framework/ib_bizval_vov.sql','1','1',getdate(),'admin',1,'Medium')
;"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES
('Phoenix - QA','','','Duplicate check','Actuals HW/ Actuals LF','Check for duplicate values','stage','stage.actuals_hw','units','select version,country_alpha2, platform_subset,base_product_number,cal_date,count(1) from stage.actuals_hw group by  version,country_alpha2, platform_subset,base_product_number,cal_date having count(1)>1 ','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','VOV Check','Norm Ships CE','Check for VoV change','prod','prod.norm_shipments_ce','units','','QA Framework/norm_shipments_ce_vov_check.sql','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','MDM check','Norm Ships CE','Check for missing country values in MDM','prod','prod.norm_shipments_ce','country_alpha2','select * from (select country_alpha2 from prod.norm_shipments_ce except select country_alpha2  from mdm.iso_country_code_xref )','','1','1',getdate(),'admin',1,'Critical') 
,('Phoenix - QA','','','MDM check','Norm Ships CE','Check for missing platform subset values in MDM','prod','prod.norm_shipments_ce','country_alpha2','select * from (select platform_subset  from prod.norm_shipments_ce except select platform_subset  from mdm.hardware_xref)','','1','1',getdate(),'admin',1,'Critical') 
,('Phoenix - QA','','','Duplicate check','Norm Ships CE','Check for duplicate values','prod','prod.norm_shipments_ce','units','select region_5,record,cal_date,country_alpha2,platform_subset,version,customer_engagement ,count(1) from prod.norm_shipments_ce where version =(select max(version) from prod.norm_shipments_ce nsc) group by region_5,record,cal_date,country_alpha2,platform_subset,version, customer_engagement  having count(1)>1','','1','1',getdate(),'admin',1,'Critical') 
,('Phoenix - QA','','','STGVSPROD Check','Norm Ships CE','Norm shipments CE Stage vs PROD','prod','prod.norm_shipments_ce','units','','QA Framework/norm_shipments_ce_stgvsprod.sql','1','1',getdate(),'admin',1,'Medium');"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table
