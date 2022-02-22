# Databricks notebook source
dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("stack", "")

# COMMAND ----------

import json
with open('/dbfs/dataos-pipeline-springboard/dev/ce-split/green/configs/constants.json') as json_file:
  data=json.load(json_file)

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

import pyspark.sql.functions as func
from pyspark.sql.functions import *
 
redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")
spark.conf.set("redshift_username", redshift_secrets["username"])
spark.conf.set("redshift_password", redshift_secrets["password"])

sqlserver_secrets = secrets_get(dbutils.widgets.get("sqlserver_secrets_name"), "us-west-2")
spark.conf.set("sfai_username", sqlserver_secrets["username"])
spark.conf.set("sfai_password", sqlserver_secrets["password"])

# COMMAND ----------

dev_rs_url=data['REDSHIFT_URLS'][dbutils.widgets.get("stack")]
dev_rs_dbname=dbutils.widgets.get("stack")
dev_rs_user_ref=spark.conf.get("redshift_username")
dev_rs_pw_ref=spark.conf.get("redshift_password")
dev_jdbc_url_ref = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(dev_rs_url, dev_rs_dbname, dev_rs_user_ref, dev_rs_pw_ref)
s3_bucket = "s3a://dataos-core-{}-team-phoenix/redshift_temp".format(dev_rs_dbname)
dev_iam = dbutils.widgets.get("aws_iam_role")

def getDataByTable(table): 
  return  (spark.read.format("com.databricks.spark.redshift") \
          .option("forward_spark_s3_credentials","true")\
          .option("url", dev_jdbc_url_ref)\
          .option("dbtable",table)\
          .option("tempdir",s3_bucket)\
          .load())

# COMMAND ----------

instant_ink_enrollees_df = getDataByTable('prod.instant_ink_enrollees')
country_code_xref_df = getDataByTable('mdm.iso_country_code_xref')
hardware_xref_df = getDataByTable('mdm.hardware_xref')
norm_shipments_df = getDataByTable('prod.norm_shipments')
calendar_df = getDataByTable('mdm.calendar')

instant_ink_enrollees_df.createOrReplaceTempView('instant_ink_enrollees_df_view')
country_code_xref_df.createOrReplaceTempView('country_code_xref_df_view')
hardware_xref_df.createOrReplaceTempView('hardware_xref_df_view')
norm_shipments_df.createOrReplaceTempView('norm_shipments_df_view')
calendar_df.createOrReplaceTempView('calendar_df_view')

# COMMAND ----------

query ='''
  SELECT 
    'ce_splits_i-ink' as record
    ,platform_subset
    ,c.region_5
    ,i.country
    ,CASE WHEN c.developed_emerging = 'Developed' THEN 'DM' WHEN c.developed_emerging = 'Emerging' THEN 'EM' END em_dm
    ,'I-Ink' business_model
    ,year_month
    ,'I-INK' split_name 
    ,'PRE' pre_post_flag
    ,case when SUM(printer_sell_out_units) OVER(PARTITION BY platform_subset,i.country ORDER BY year_month
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) <= 0 THEN 0 ELSE  cum_enrollees_month/SUM(printer_sell_out_units) OVER(PARTITION BY platform_subset,i.country ORDER BY year_month
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) END AS  ce_split 
    ,1 active
    ,NULL active_at
    ,NULL inactive_at
    ,current_date() load_date
    ,'2022.01.14.1' version
    ,1 offical 
  FROM  instant_ink_enrollees_df_view i
  LEFT JOIN country_code_xref_df_view c on c.country_alpha2 = i.country
  WHERE i.official = 1 
         
   UNION

  SELECT 
  'ce_splits_i-ink' record
  ,platform_subset
  ,c.region_5
  ,i.country
  ,CASE WHEN c.developed_emerging = 'Developed' THEN 'DM' WHEN c.developed_emerging = 'Emerging' THEN 'EM' END em_dm
  ,'I-Ink' business_model
  ,year_month
  ,'TRAD' split_name 
  ,'PRE' pre_post_flag
  ,case when SUM(printer_sell_out_units) OVER(PARTITION BY platform_subset,i.country ORDER BY year_month
   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) <= 0 THEN 0 ELSE 1 - cum_enrollees_month/SUM(printer_sell_out_units) OVER(PARTITION BY platform_subset,i.country ORDER BY year_month
   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) END AS  ce_split
  ,1 active
  ,NULL active_at
  ,NULL inactive_at
  ,current_date() load_date
  ,'2022.01.14.1' version
  ,1 offical 
  FROM  instant_ink_enrollees_df_view i
  LEFT JOIN country_code_xref_df_view c on c.country_alpha2 = i.country
  WHERE i.official = 1  
         '''
initial_ce_split_df=spark.sql(query)
initial_ce_split_df.createOrReplaceTempView('initial_ce_split_df_view')
#code remaning

# COMMAND ----------

######################GET PLATFORM SUBSETS BY COUNTRY WHERE CE SPLITS FROM  I-INK ARE LESS THAN 0#####################
query ='''
  select distinct platform_subset,country,year_month
  from initial_ce_split_df_view
  where ce_split <= 0 and split_name = 'I-INK'
  '''
ce_less_zero_df=spark.sql(query)
ce_less_zero_df.createOrReplaceTempView('ce_less_zero_df_view')

# COMMAND ----------

query ='''
  select t.*
  from initial_ce_split_df_view t
  inner join ce_less_zero_df_view t1
  on t.platform_subset = t1.platform_subset and t.country = t1.country and t.year_month = t1.year_month
  '''

common_df = spark.sql(query)
common_df.createOrReplaceTempView('common_df_view')

# COMMAND ----------

initial_ce_split_df=initial_ce_split_df.join(common_df, (initial_ce_split_df.platform_subset == common_df.platform_subset) & (initial_ce_split_df.country == common_df.country) & (initial_ce_split_df.year_month == common_df.year_month), how='left_anti')
initial_ce_split_df.createOrReplaceTempView('initial_ce_split_df_view')

# COMMAND ----------


############################GET ALL PLATFORM SUBSETS WHERE I-INK GREATER THAN 1###########################
from pyspark.sql.functions import lit
query = '''
  select distinct platform_subset,country,year_month
  from initial_ce_split_df_view
  where ce_split > 1 and split_name = 'I-INK'
  '''
ce_greater_one_df = spark.sql(query)
ce_greater_one_df.createOrReplaceTempView('ce_greater_one_df_view')

# COMMAND ----------

ce_greater_one_df=(ce_greater_one_df
                                  .withColumnRenamed('platform_subset','platform_subsett')
                                  .withColumnRenamed('country','countryy')
                                  .withColumnRenamed('year_month','year_monthh'))

# COMMAND ----------

temp = initial_ce_split_df.join(ce_greater_one_df, (initial_ce_split_df.platform_subset == ce_greater_one_df.platform_subsett) & (initial_ce_split_df.country == ce_greater_one_df.countryy) & (initial_ce_split_df.year_month==ce_greater_one_df.year_monthh),'inner').select(initial_ce_split_df.record,initial_ce_split_df.platform_subset,initial_ce_split_df.region_5,initial_ce_split_df.country,initial_ce_split_df.em_dm,initial_ce_split_df.business_model,initial_ce_split_df.year_month,initial_ce_split_df.split_name,initial_ce_split_df.pre_post_flag,initial_ce_split_df.ce_split,initial_ce_split_df.active,initial_ce_split_df.active_at,initial_ce_split_df.inactive_at,initial_ce_split_df.load_date,initial_ce_split_df.version,initial_ce_split_df.offical).where(initial_ce_split_df.split_name == 'TRAD')
temp= temp.withColumn('ce_split',lit(0.0))
temp=temp.withColumnRenamed('split_name','split_namee')

# COMMAND ----------

initial_ce_split_df=initial_ce_split_df.withColumn('demo_split_name',when(initial_ce_split_df.split_name=='TRAD',lit('TRAD')).otherwise(lit('other')))

# COMMAND ----------

initial_ce_split_df=initial_ce_split_df.join(temp,(initial_ce_split_df.platform_subset==temp.platform_subset)&(initial_ce_split_df.country==temp.country)&(initial_ce_split_df.year_month==temp.year_month) & (initial_ce_split_df.demo_split_name==temp.split_namee),'leftanti').select(initial_ce_split_df.record,initial_ce_split_df.platform_subset,initial_ce_split_df.region_5,initial_ce_split_df.country,initial_ce_split_df.em_dm,initial_ce_split_df.business_model,initial_ce_split_df.year_month,initial_ce_split_df.split_name,initial_ce_split_df.pre_post_flag,initial_ce_split_df.ce_split,initial_ce_split_df.active,initial_ce_split_df.active_at,initial_ce_split_df.inactive_at,initial_ce_split_df.load_date,initial_ce_split_df.version,initial_ce_split_df.offical)

# COMMAND ----------

temp=temp.withColumnRenamed('split_namee','split_name')

# COMMAND ----------

initial_ce_split_df=initial_ce_split_df.union(temp)

# COMMAND ----------

initial_ce_split_df=initial_ce_split_df.withColumn("ce_split",when((col("ce_split")>1) & (col("split_name")=='I-INK'),lit('1.0')).otherwise(col("ce_split")))

# COMMAND ----------

initial_ce_split_df.createOrReplaceTempView('initial_ce_split_df_view')

# COMMAND ----------

query = '''
  Select distinct ce.platform_subset,ce.split_name, ce.country, min(ce.year_month) as min_date
  from initial_ce_split_df_view ce
  left join hardware_xref_df_view hw on ce.platform_subset=hw.platform_subset
  where hw.technology ='Ink' and offical = 1 
  group by ce.platform_subset,ce.split_name,ce.country
  '''

ce=spark.sql(query)
ce.createOrReplaceTempView('ce_view')

# COMMAND ----------

query1 = '''
  Select  norm.platform_subset, norm.country_alpha2, min(norm.cal_date) as min_date, sum(norm.units) as units
from norm_shipments_df_view norm
left join hardware_xref_df_view hw on hw.platform_subset =norm.platform_subset
where norm.version = (select max(version) from norm_shipments_df_view)
and hw.technology = 'Ink' 
and (norm.platform_subset like '%i-ink' or norm.platform_subset like '%yet%')
group by  norm.platform_subset, norm.country_alpha2
  '''
norm =spark.sql(query1)
norm.createOrReplaceTempView('norm_view')

# COMMAND ----------

query2 = '''
  Select distinct norm.platform_subset, hw.predecessor, iso.region_5, norm.country_alpha2, norm.min_date, round(norm.units,0) as units 
  from norm_view norm
  Full outer join ce_view ce on ce.platform_subset=norm.platform_subset and ce.country=norm.country_alpha2
  left join country_code_xref_df_view iso on iso.country_alpha2=norm.country_alpha2
  left join hardware_xref_df_view hw on hw.platform_subset=norm.platform_subset
  where ce.platform_subset is null and
  norm.min_date > current_date()
  group by norm.platform_subset, hw.predecessor, iso.region_5, norm.country_alpha2, norm.min_date, norm.units,ce.platform_subset,ce.country
  '''

tot =spark.sql(query2)
tot.createOrReplaceTempView('tot_view')

# COMMAND ----------

from pyspark.sql.functions import *
query3 = '''
  Select tot.platform_subset, tot.predecessor, tot.region_5, tot.country_alpha2, tot.min_date as npi_start_date, tot.units, ce.split_name, ce.min_date as pred_start_date,  months_between(ce.min_date,tot.min_date) as month_offset
  from tot_view tot
  left join ce_view ce on ce.platform_subset=tot.predecessor and ce.country=tot.country_alpha2
  '''

comp =spark.sql(query3)
comp.createOrReplaceTempView('comp_view')

# COMMAND ----------

##################################LOAD CE SPLITS FROM ENROLEES AND NPI INTO ONE TEMP TABLE######################################
query = '''
  Select 'ce_splits_i-ink' record, comp.platform_subset, comp.region_5, comp.country_alpha2, ces.em_dm, ces.business_model, 
add_months(ces.year_month,comp.month_offset) as month_begin, comp.split_name, ces.pre_post_flag, ces.ce_split value,
 ces.active, null as active_at, null as inactive_at, current_date() as load_date, '2022.01.14.1' as version, 1 as official 
From comp_view as comp
inner join initial_ce_split_df_view ces on ces.platform_subset=comp.predecessor and comp.country_alpha2=ces.country and comp.split_name=ces.split_name
where ces.record='ce_splits_i-ink'

union 

select 'ce_splits_i-ink' record,platform_subset,region_5,country,em_dm,business_model,year_month,split_name
,pre_post_flag,ce_split,active,null as active_at, null as inactive_at, current_date() as load_date, '2022.01.14.1' as version, 1 as official 
from initial_ce_split_df_view 
'''
npi_plus_enrolles_ce_splits =spark.sql(query)
npi_plus_enrolles_ce_splits.createOrReplaceTempView('npi_plus_enrolles_ce_splits_view')

# COMMAND ----------

######################################CALCULATE FUTURE 15 YEARS CE SPLITS##############################################

####################################GET MAX YEAR MONTH BY PLATFORM, SPLIT NAME AND COUNTRY#####################################

query = '''
  SELECT MAX(month_begin) max_year_month,platform_subset,country_alpha2,split_name
  FROM npi_plus_enrolles_ce_splits_view
  GROUP BY platform_subset,country_alpha2,split_name
  '''
max_month_per_ps_country=spark.sql(query)
max_month_per_ps_country.createOrReplaceTempView('max_month_per_ps_country_view')

# COMMAND ----------

#####################GET CORRESPONDING VALUE AND OTHER ATRIBUTES FOR THE LATEST MONTH FOR PLATFORM SUBSTE BY COUNTRY AND SPLIT NAME################
query = '''
    SELECT d.max_year_month,d.platform_subset,d.country_alpha2,d.split_name,t.value,t.em_dm,t.business_model,t.pre_post_flag,t.version,1 as official,t.region_5
  FROM max_month_per_ps_country_view d
  LEFT JOIN npi_plus_enrolles_ce_splits_view t on t.platform_subset = d.platform_subset and t.country_alpha2 = d.country_alpha2
  and t.split_name = d.split_name and t.month_begin = d.max_year_month
'''

ce_ps_max_per_month_country = spark.sql(query)
ce_ps_max_per_month_country.createOrReplaceTempView('ce_ps_max_per_month_country_view')

# COMMAND ----------

####################LOAD ALL FUTURE CE SPLITS BY PLATFROM SUBSET AND COUNTRY###################################
query ='''
  select c.Date,d.platform_subset,d.country_alpha2,d.split_name,d.value,d.em_dm,d.business_model,d.pre_post_flag,d.version,d.official,region_5
  from ce_ps_max_per_month_country_view d 
  left join calendar_df_view c on 1 = 1
  WHERE c.Date>d.max_year_month and c.Date<add_months(max_year_month,15*12) and c.Day_of_Month = 1
  ORDER BY platform_subset,country_alpha2,split_name,date
'''

ce_future = spark.sql(query)
ce_future.createOrReplaceTempView('ce_future_view')

# COMMAND ----------

######################FINAL CE##############################
query = '''
  select 'ce_splits_i-ink' record,platform_subset,region_5,country_alpha2,em_dm,business_model,month_begin,split_name,pre_post_flag,value,1 active,
  null as active_at, null as inactive_at, current_date() as load_date, '2022.01.14.1' as version, 1 as official 
  from npi_plus_enrolles_ce_splits_view

  union

  select 'ce_splits_i-ink' record,platform_subset,region_5,country_alpha2,em_dm,business_model,Date,split_name,pre_post_flag,value,1 active,
  null as active_at, null as inactive_at, current_date() as load_date, '2022.01.14.1' as version, 1 as official 
  from ce_future_view
  '''
final_ce = spark.sql(query)
final_ce.createOrReplaceTempView('final_ce_view')

# COMMAND ----------

final_ce=final_ce.select('record','platform_subset','region_5','country_alpha2','em_dm','business_model','month_begin','split_name','pre_post_flag','value','official','load_date','version')

# COMMAND ----------

url = """jdbc:redshift://{}:5439/{}?ssl_verify=None""".format(data['REDSHIFT_URLS'][dbutils.widgets.get("stack")],dbutils.widgets.get("stack"))
spark.conf.set('url', url)

# COMMAND ----------

# write data to redshift
final_ce.write \
  .format("com.databricks.spark.redshift") \
  .option("url", url) \
  .option("dbtable", "prod.ce_splits") \
  .option("tempdir", f"""s3a://dataos-core-{dbutils.widgets.get("stack")}-team-phoenix/redshift_temp/""") \
  .option("aws_iam_role", dbutils.widgets.get("aws_iam_role")) \
  .option("tempformat", "CSV") \
  .option("user", dev_rs_user_ref) \
  .option("password", dev_rs_pw_ref) \
  .option("postactions","GRANT ALL ON prod.ce_splits TO group dev_arch_eng") \
  .mode("append") \
  .save()
