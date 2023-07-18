# Databricks notebook source
# MAGIC %md
# MAGIC ### QA Framework

# COMMAND ----------

# MAGIC %pip install xlsxwriter

# COMMAND ----------

import pyspark.sql.functions as f
import time
from pyspark.sql import Window, SparkSession
from pyspark.sql.functions import col, substring, to_timestamp, when
import re
import smtplib
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate
from email import encoders
import pandas as pd
import numpy as np
import io
from IPython.core.display import HTML
from IPython.display import HTML
from io import BytesIO

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

dbutils.widgets.text("skip_notebook","true")

# COMMAND ----------

if dbutils.widgets.get("skip_notebook")=="true":
    dbutils.notebook.exit()

# COMMAND ----------

#s3_bucket ='dataos-core-itg-team-phoenix'
s3_bucket1=constants['S3_BASE_BUCKET'][stack]
s3_bucket=s3_bucket1[6:-1]
print(s3_bucket)
print(s3_bucket1)

# COMMAND ----------

username=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
print(username)

# COMMAND ----------

#load testcases data into df
read_testcases_data = read_redshift_to_df(configs).option("query", "SELECT * FROM qa.test_cases where enabled=1").load()

# COMMAND ----------

read_testrun_data = read_redshift_to_df(configs).option("query", "SELECT coalesce(max(test_run_id),0) test_run_id FROM qa.test_results").load()
test_run_id=read_testrun_data.first()['test_run_id']+1
print(test_run_id)

# COMMAND ----------

read_testcases_data.show()

# COMMAND ----------

read_testcases_data.createOrReplaceTempView("test_cases_table")

# COMMAND ----------

table_name_options = spark.sql("select distinct (table_name) from test_cases_table").rdd.map(lambda row : row[0]).collect()
table_name_options.sort()
module_name_options = spark.sql("select distinct (module_name) from test_cases_table").rdd.map(lambda row : row[0]).collect()
module_name_options.sort()
schema_name_options = spark.sql("select distinct (schema_name) from test_cases_table").rdd.map(lambda row : row[0]).collect()
schema_name_options.sort()
test_category_options = spark.sql("select distinct (test_category) from test_cases_table").rdd.map(lambda row : row[0]).collect()
test_category_options.sort()

# COMMAND ----------

dbutils.widgets.multiselect(name="table_name_multiselect_filter", defaultValue="All",choices= [str(x) for x in table_name_options]+ ["None",  "All"])
dbutils.widgets.multiselect(name="module_name_multiselect_filter", defaultValue="All",choices= [str(x) for x in module_name_options]+ ["None",  "All"])
dbutils.widgets.multiselect(name="schema_name_multiselect_filter", defaultValue="All",choices= [str(x) for x in schema_name_options]+ ["None",  "All"])
dbutils.widgets.multiselect(name="test_category_multiselect_filter", defaultValue="All",choices= [str(x) for x in test_category_options]+ ["None",  "All"])

# COMMAND ----------

# Save the multi-select widget value into a variable
multiselect_filter_value_table_name = dbutils.widgets.get("table_name_multiselect_filter")
print({multiselect_filter_value_table_name})
multiselect_filter_value_module_name = dbutils.widgets.get("module_name_multiselect_filter")
print({multiselect_filter_value_module_name})
multiselect_filter_value_schema_name = dbutils.widgets.get("schema_name_multiselect_filter")
print({multiselect_filter_value_schema_name})
multiselect_filter_value_test_category = dbutils.widgets.get("test_category_multiselect_filter")
print({multiselect_filter_value_test_category})

# COMMAND ----------

# Save the multi-select widget value into a variable
multiselect_filter_value_table_name = dbutils.widgets.get("table_name_multiselect_filter").split(",")
print(multiselect_filter_value_table_name)
multiselect_filter_value_module_name = dbutils.widgets.get("module_name_multiselect_filter").split(",")
print(multiselect_filter_value_module_name)
multiselect_filter_value_schema_name = dbutils.widgets.get("schema_name_multiselect_filter").split(",")
print(multiselect_filter_value_schema_name)
multiselect_filter_value_test_category = dbutils.widgets.get("test_category_multiselect_filter").split(",")
print(multiselect_filter_value_test_category)

# COMMAND ----------

choices_table_name=[str(x) for x in table_name_options]
if 'All' in multiselect_filter_value_table_name:
    table_name_values=choices_table_name
else:
    table_name_values=multiselect_filter_value_table_name
print(table_name_values)

# COMMAND ----------

choices_module_name=[str(x) for x in module_name_options]
if 'All' in multiselect_filter_value_module_name:
    module_name_values=choices_module_name
else:
    module_name_values=multiselect_filter_value_module_name
print(module_name_values)

# COMMAND ----------

choices_schema_name=[str(x) for x in schema_name_options]
if 'All' in multiselect_filter_value_schema_name:
    schema_name_values=choices_schema_name
else:
    schema_name_values=multiselect_filter_value_schema_name
print(schema_name_values)

# COMMAND ----------

choices_test_category=[str(x) for x in test_category_options]
if 'All' in multiselect_filter_value_test_category:
    test_category_values=choices_test_category
else:
    test_category_values=multiselect_filter_value_test_category
print(test_category_values)

# COMMAND ----------

table_name_values_str = ','.join(table_name_values)
module_name_values_str = ','.join(module_name_values)
schema_name_values_str = ','.join(schema_name_values)
test_category_values_str = ','.join(test_category_values)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Retrieve test cases details as per parameters

# COMMAND ----------

filtered_test_cases =(read_testcases_data.filter((col('table_name').isin(table_name_values_str.split(','))) & (col('module_name').isin(module_name_values_str.split(','))) & (col('schema_name').isin(schema_name_values_str.split(','))) & (col('test_category').isin(test_category_values_str.split(','))) ))
filtered_test_cases.show()

# COMMAND ----------

dbutils.widgets.text("notification_email","swati.gutgutia@hp.com")


# COMMAND ----------

#truncate test results table to be truncated at the start of every month 
#truncate_testresults_table_query =f""" TRUNCATE TABLE qa.test_results;"""
#submit_remote_query(configs,truncate_testresults_table_query )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Execute test cases and store test results

# COMMAND ----------

#insert test cases into test results table
data_collect = filtered_test_cases.collect()
with io.BytesIO() as buffer:
    with pd.ExcelWriter(buffer,engine='xlsxwriter') as writer:
        for row in data_collect:
            test_query=row["test_query"]   
            min_threshold=row["min_threshold"]
            max_threshold=row["max_threshold"]
            testcase_id=row["test_case_id"]
            testcase_module=row["module_name"]
            testcase_cat=row["test_category"]
            query_path=row["query_path"]
            element_name=row["element_name"]
            print(test_query)
            if('QA' in query_path):
                test_query=(get_file_content_from_s3(s3_bucket,query_path))
                print(test_query)
            test_result_detail_df = read_redshift_to_df(configs).option("query", test_query).load()
            print(test_result_detail_df.count())
            results=0
            if test_result_detail_df.count()<100:
                results = test_result_detail_df.toPandas().to_json(orient='records')
            print(results)
            #test_result_detail=test_result_detail_df.first()['count']
            test_result_detail=test_result_detail_df.count()
            #test_result=''
            s3_output_bucket = s3_bucket1+"QA Framework/test_results/"+str(testcase_module)+"/"+str(testcase_module)+"_"+str(testcase_id)
            print(s3_output_bucket) # already created on S3
            write_df_to_s3(test_result_detail_df, s3_output_bucket, "csv", "overwrite")
            insert_test_result= f""" INSERT INTO qa.test_results
            (test_case_id, version_id, test_rundate, test_run_by, test_result_detail, test_result,test_run_id,test_results_s3path)
            VALUES
            ('{testcase_id}','',getdate(),'{username}','{test_result_detail}',case when '{test_result_detail}'>='{min_threshold}' and '{test_result_detail}'<='{max_threshold}' then 'Pass' when '{test_result_detail}'='0' then 'Pass' else 'Fail' end,'{test_run_id}','{s3_output_bucket}');"""
            submit_remote_query(configs,insert_test_result ) # insert into test result table 
            if test_result_detail_df.count()>1 and test_result_detail_df.count()<5000:
                test_result_detail_df.toPandas().to_excel(writer, sheet_name=str(testcase_id), index= False)  
            if test_result_detail_df.count()>1 and test_result_detail_df.count()<500:              
                insert_test_result_detail= f""" INSERT INTO qa.test_results_detail
                (test_case_id,test_result_id,detail_value)
                VALUES
                ('{testcase_id}',(select max(test_result_id) from qa.test_results where test_case_id='{testcase_id}'),'{results}');"""
                submit_remote_query(configs,insert_test_result_detail ) # insert into test result table         
            if testcase_cat=="VOV Check":
                delete_from_test_results_vov=f""" delete from qa.test_results_detail_vov where module_name='{testcase_module}';"""
                submit_remote_query(configs,delete_from_test_results_vov ) # delete from vov table
                write_df_to_redshift(configs=configs, df=test_result_detail_df, destination="qa.test_results_detail_vov", mode="append")
    Testresultexcel=buffer.getvalue()    

# COMMAND ----------

critical_cases_df= read_redshift_to_df(configs).option("query", "select b.test_case_id,b.test_category ,b.test_case_name ,b.module_name ,b.table_name ,test_result ,test_result_detail ,test_rundate,a.test_results_s3path  from qa.test_results a inner join qa.test_cases b on a.test_case_id =b.test_case_id left join qa.test_results_detail c on a.test_result_id =c.test_result_id  and detail_value like '%s3a%' where severity='Critical' and test_run_id=(select max(test_run_id) from qa.test_results ) and test_result='Fail'").load()
medium_cases_df= read_redshift_to_df(configs).option("query", "select b.test_case_id,b.test_category ,b.test_case_name ,b.module_name ,b.table_name ,test_result ,test_result_detail ,test_rundate,a.test_results_s3path  from qa.test_results a inner join qa.test_cases b on a.test_case_id =b.test_case_id left join qa.test_results_detail c on a.test_result_id =c.test_result_id  and detail_value like '%s3a%' where severity='Medium' and test_run_id=(select max(test_run_id) from qa.test_results ) and test_result='Fail'").load()
low_cases_df= read_redshift_to_df(configs).option("query", "select b.test_case_id,b.test_category ,b.test_case_name ,b.module_name ,b.table_name ,test_result ,test_result_detail ,test_rundate,a.test_results_s3path  from qa.test_results a inner join qa.test_cases b on a.test_case_id =b.test_case_id left join qa.test_results_detail c on a.test_result_id =c.test_result_id  and detail_value like '%s3a%' where severity='Very Low' and test_run_id=(select max(test_run_id) from qa.test_results ) and test_result='Fail'").load()

# COMMAND ----------

def send_email(email_from, email_to, subject, message):
  
  msg = MIMEMultipart()
  if type(email_to) is str:
    email_to = [email_to]
  
  msg['Subject'] = subject
  msg['From'] = email_from
  msg['To'] =  ', '.join(email_to)
  
  #filedata = sc.textFile("/dbfs/df_testqa.csv", use_unicode=False)
  
  part = MIMEApplication(Testresultexcel)
  part.add_header('Content-Disposition','attachment',filename="testresulexcel.xlsx")
  msg.attach(MIMEText(message.encode('utf-8'), 'html', 'utf-8'))
  msg.attach(part)
  
  ses_service = boto3.client(service_name = 'ses', region_name = 'us-west-2')
    
  try:
    response = ses_service.send_raw_email(Source = email_from, Destinations = email_to, RawMessage = {'Data': msg.as_string()})
  
  
  except ClientError as e:
    raise Exception(str(e.response['Error']['Message']))

# COMMAND ----------

critical_cases_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Email notification of failed cases by severity

# COMMAND ----------

send_to_email=dbutils.widgets.get("notification_email")

# COMMAND ----------

if low_cases_df.count()>=1:
    subject='QA Framework - Low Severity cases Failed'
    message='Below is the details of failed test cases for ' +module_name_values_str+'\n'+ 'Please investigate the issues'+'\n'+low_cases_df.toPandas().to_html()
    send_email('phoenix_qa_team@hpdataos.com',send_to_email, subject, message)
if medium_cases_df.count()>=1:
    subject='QA Framework - Medium Severity cases Failed'
    message='Below is the details of failed test cases for ' +module_name_values_str+'\n'+ 'Please investigate the issues'+'\n'+medium_cases_df.toPandas().to_html()
    send_email('phoenix_qa_team@hpdataos.com',send_to_email, subject, message)

# COMMAND ----------

if critical_cases_df.count()>=1:
    subject='QA Framework - Critical Severity cases Failed'
    message='Below is the details of failed test cases for ' +module_name_values_str+'\n'+ 'The notebook will exit. Please investigate the issues'+'\n'+ critical_cases_df.toPandas().to_html()
    send_email('phoenix_qa_team@hpdataos.com',send_to_email, subject, message)
    dbutils.notebook.exit(json.dumps(subject))
