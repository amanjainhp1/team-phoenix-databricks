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
#truncate_testcases_table_query =f""" delete from stage.test_cases where module_name='Actuals Revenue';"""
#submit_remote_query(configs,truncate_testcases_table_query )

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES 
('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Revenue without IB baseprod','fin_prod','fin_prod.actuals_supplies_baseprod','units','select platform_subset,country_alpha2  from fin_prod.actuals_supplies_baseprod
group by platform_subset,country_alpha2 having sum(net_revenue)<>0 except select platform_subset,country_alpha2 from prod.ib where version in (select max(version) from prod.ib where official=1)
group by platform_subset,country_alpha2 having max(units)<>0','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Negative revenue test baseprod','fin_prod','fin_prod.actuals_supplies_baseprod','units','select platform_subset,country_alpha2  from fin_prod.actuals_supplies_baseprod
group by platform_subset,country_alpha2 having sum(net_revenue)<0','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Negative revenue test salesprod','fin_prod','fin_prod.actuals_supplies_salesprod','units','select country_alpha2,sales_product_number  from fin_prod.actuals_supplies_salesprod
group by country_alpha2,sales_product_number having sum(net_revenue)<0','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Revenue without units salesprod','fin_prod','fin_prod.actuals_supplies_salesprod','units','select country_alpha2,sales_product_number  from fin_prod.actuals_supplies_salesprod
group by country_alpha2,sales_product_number having sum(revenue_units)=0 and sum(net_revenue)>0','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Revenue without units baseprod','fin_prod','fin_prod.actuals_supplies_baseprod','units','select platform_subset,country_alpha2,base_product_number  from fin_prod.actuals_supplies_baseprod
group by platform_subset,country_alpha2,base_product_number having sum(revenue_units)=0 and sum(net_revenue)>0','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Missed product number odw/salesprod','fin_prod','fin_prod.odw_actuals_supplies_salesprod','sales_product_number','select distinct sales_product_number from  fin_prod.odw_actuals_supplies_salesprod
except select distinct sales_product_number from fin_prod.actuals_supplies_salesprod','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Missed product number edw/salesprod','fin_prod','fin_prod.edw_actuals_supplies_salesprod','sales_product_number','select distinct sales_product_number from  fin_prod.edw_actuals_supplies_salesprod
except select distinct sales_product_number from fin_prod.actuals_supplies_salesprod','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Missed product number odw/baseprod','fin_prod','fin_prod.odw_actuals_supplies_baseprod','base_product_number','select distinct base_product_number from  fin_prod.odw_actuals_supplies_baseprod
except select distinct base_product_number from fin_prod.actuals_supplies_baseprod','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Missed product number edw/baseprod','fin_prod','fin_prod.edw_actuals_supplies_baseprod','base_product_number','select distinct base_product_number from  fin_prod.edw_actuals_supplies_baseprod
except select distinct base_product_number from fin_prod.actuals_supplies_baseprod','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','MDM check','Actuals Revenue','Check for missing country values in MDM','fin_prod','fin_prod.actuals_supplies_baseprod','country_alpha2',
'select * from (select country_alpha2 from fin_prod.actuals_supplies_baseprod except select country_alpha2 from mdm.iso_country_code_xref iccx )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','MDM check','Actuals Revenue','Check for missing country values in MDM','fin_prod','fin_prod.actuals_supplies_salesprod','country_alpha2',
'select * from (select country_alpha2 from fin_prod.actuals_supplies_salesprod except select country_alpha2 from mdm.iso_country_code_xref iccx )','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Data availability check','Actuals Revenue','Check if base product number is available','fin_prod','fin_prod.actuals_supplies_baseprod','base_product_number',
'select * from fin_prod.actuals_supplies_baseprod asb where base_product_number is null or base_product_number=''''','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Data availability check','Actuals Revenue','Check if sales product number is available','fin_prod','fin_prod.actuals_supplies_salesprod','sales_product_number',
'select * from fin_prod.actuals_supplies_salesprod asb where sales_product_number is null or sales_product_number=''''','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Data availability check','Actuals Revenue','Check if product line is available','fin_prod','fin_prod.actuals_supplies_baseprod','pl',
'select * from fin_prod.actuals_supplies_baseprod ass where pl is null','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Data availability check','Actuals Revenue','Check if product line is available','fin_prod','fin_prod.actuals_supplies_salesprod','pl',
'select * from fin_prod.actuals_supplies_salesprod ass where pl is null','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Duplicate check','Actuals Revenue','Check for duplicate records','fin_prod','fin_prod.actuals_supplies_baseprod','units','select cal_date ,country_alpha2, market10 , pl ,platform_subset, base_product_number,customer_engagement  ,version,  count(1) count_dup
from fin_prod.actuals_supplies_baseprod asb group by cal_date ,country_alpha2, market10 , pl ,platform_subset, base_product_number,version ,customer_engagement having count_dup > 1','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Duplicate check','Actuals Revenue','Check for duplicate records','fin_prod','fin_prod.actuals_supplies_salesprod','units','select cal_date ,country_alpha2, currency, market10  , sales_product_number , pl,customer_engagement ,version,  count(1) count_dup
from fin_prod.actuals_supplies_salesprod group by cal_date ,country_alpha2, currency, market10  , sales_product_number , pl,customer_engagement ,version having count_dup > 1','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','MDM check','Actuals Revenue','Check for missing base product number values in MDM','fin_prod',' fin_prod.actuals_supplies_baseprod','units',
'select distinct base_product_number,platform_subset ,country_alpha2 ,pl  from fin_prod.actuals_supplies_baseprod where base_product_number in (select distinct base_product_number from fin_prod.actuals_supplies_baseprod except select distinct base_product_number from mdm.rdma_base_to_sales_product_map)','','1','1',getdate(),'admin',1,'Critical')
"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table

# COMMAND ----------

insert_testqueries_in_testcases_table_query= f""" 
INSERT INTO stage.test_cases
(project, server_name, database_name, test_category, module_name, test_case_name,schema_name, table_name, element_name, test_query, query_path, min_threshold, max_threshold, test_case_creation_date, test_case_created_by,enabled,severity)
VALUES 
('Phoenix - QA','','','MDM check','Actuals Revenue','Check for missing sales product number values in MDM','fin_prod',' fin_prod.actuals_supplies_salesprod','units',
'select distinct sales_product_number,country_alpha2 ,pl  from fin_prod.actuals_supplies_salesprod where sales_product_number in (select distinct sales_product_number from fin_prod.actuals_supplies_salesprod except select distinct sales_product_number from mdm.rdma_base_to_sales_product_map)','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Revenue without US','fin_prod','fin_prod.actuals_supplies_baseprod','units','select platform_subset,country_alpha2  from fin_prod.actuals_supplies_baseprod
group by platform_subset,country_alpha2 having sum(net_revenue)<>0 except select platform_subset,geography from prod.usage_share_country where version in (select max(version) from  prod.usage_share_country )
group by platform_subset,geography having max(units)<>0','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Drop in PS_Country between Usage Share and Actuals revenue','fin_prod','fin_prod.actuals_supplies_baseprod','units','select platform_subset,geography from prod.usage_share_country where version in (select max(version) from  prod.usage_share_country )
and cal_date <= ''2023-05-01'' group by platform_subset,geography having max(units)<>0 except select platform_subset,country_alpha2  from fin_prod.actuals_supplies_baseprod
group by platform_subset,country_alpha2 having sum(net_revenue)<>0','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Biz Validation testing','Actuals Revenue','Drop in PS_Country between IB and Actuals revenue','fin_prod','fin_prod.actuals_supplies_baseprod','units','select platform_subset,country_alpha2 from prod.ib where version in (select max(version) from  prod.ib where official=1 )
and cal_date <= ''2023-05-01'' group by platform_subset,country_alpha2 having max(units)<>0 except select platform_subset,country_alpha2  from fin_prod.actuals_supplies_baseprod
group by platform_subset,country_alpha2 having sum(net_revenue)<>0','','1','1',getdate(),'admin',1,'Medium')
,('Phoenix - QA','','','Duplicate check','Actuals Revenue','Check for duplicate records','prod','prod.actuals_supplies','units',' select cal_date , country_alpha2, market10, platform_subset, customer_engagement ,version, count(1) count_dup  from prod.actuals_supplies as2  where version = (select max(version) from prod.actuals_supplies as2 where official = 1)
 group by cal_date , country_alpha2, market10, platform_subset, customer_engagement,version  having count_dup >1','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Data availability check','Actuals Revenue','Check if base quantity is negative','prod','prod.actuals_supplies','base_quantity',
'select * from prod.actuals_supplies where base_quantity < 0 and version = (select max(version) from prod.actuals_supplies as2 where official = 1)','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Data availability check','Actuals Revenue','Check if base product number is available','prod','prod.actuals_supplies','base_product_number',
'select * from prod.actuals_supplies where base_product_number is null or base_product_number=''''','','1','1',getdate(),'admin',1,'Critical')
,('Phoenix - QA','','','Data availability check','Actuals Revenue','Check if base quantity is available','prod','prod.actuals_supplies','base_quantity',
'select * from prod.actuals_supplies where base_quantity = 0','','1','1',getdate(),'admin',1,'Critical')
"""

# COMMAND ----------

submit_remote_query(configs,insert_testqueries_in_testcases_table_query ) # insert into test cases table9
