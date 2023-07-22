# Databricks notebook source
# MAGIC %pip install xlsxwriter

# COMMAND ----------

import io
from IPython.core.display import HTML
from IPython.display import HTML
from io import BytesIO
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

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

send_to_email='swati.gutgutia@hp.com'

# COMMAND ----------

ink_telemetry_df=spark.read.parquet(constants["S3_BASE_BUCKET"][stack]+"/cupsm_inputs/ink/")
ink_telemetry_df.show()
print(ink_telemetry_df.count())
ink_telemetry_df=ink_telemetry_df.filter(ink_telemetry_df['hp_share']>0.05)
ink_telemetry_df=ink_telemetry_df.filter(ink_telemetry_df['reporting_printers']>20)
ink_telemetry_df=ink_telemetry_df.select(['printer_platform_name','country_alpha2','printer_managed','date_month_dim_ky']).distinct()
ink_telemetry_df.show()
print(ink_telemetry_df.count())
toner_telemetry_df=spark.read.parquet(constants["S3_BASE_BUCKET"][stack]+"/cupsm_inputs/toner/")
print(toner_telemetry_df.count())
toner_telemetry_df.show()
toner_telemetry_df=toner_telemetry_df.filter(toner_telemetry_df['printer_count_fyqtr_page_share_flag_sum']>20)
toner_telemetry_df=toner_telemetry_df.filter(when(toner_telemetry_df.supply_pages_cmyk_share_ib_ext_sum==0,0).otherwise(toner_telemetry_df['supply_pages_cmyk_hp_ib_ext_sum']/toner_telemetry_df['supply_pages_cmyk_share_ib_ext_sum'])>0)
print(toner_telemetry_df.count())
toner_telemetry_df=toner_telemetry_df.select(['printer_platform_name','printer_country_iso_code','customer_engagement','date_month_dim_ky']).distinct()
toner_telemetry_df = toner_telemetry_df.withColumn("customer_engagement", when(toner_telemetry_df.customer_engagement == "STANDARD","STD").when(toner_telemetry_df.customer_engagement == "NON-YETI","TRAD").otherwise(toner_telemetry_df.customer_engagement))
toner_telemetry_df.show()
#toner_telemetry_df_ce=toner_telemetry_df.select(['customer_engagement']).distinct()
#toner_telemetry_df_ce.show()
read_usage_share_data = read_redshift_to_df(configs).option("query", "select platform_subset,geography,customer_engagement,calendar_yr_mo from prod.usage_share_country a inner join mdm.calendar c on a.cal_date =c.date where measure='HP_SHARE' and source='TELEMETRY'").load()
row_count_us=read_usage_share_data.count()
print(row_count_us)
df_result_ink=ink_telemetry_df.exceptAll(read_usage_share_data)
print(df_result_ink.count())
df_result_ink.show()
df_result_toner=toner_telemetry_df.exceptAll(read_usage_share_data)
print(df_result_toner.count())
df_result_toner.show()

# COMMAND ----------

with io.BytesIO() as buffer:
    with pd.ExcelWriter(buffer,engine='xlsxwriter') as writer:
        df_result_ink.toPandas().to_excel(writer, sheet_name=str('ink'), index= False)
        df_result_toner.toPandas().to_excel(writer, sheet_name=str('toner'), index= False)
    Testresultexcel=buffer.getvalue() 

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

subject='QA Framework - US Telemetry Override'
message='Attached is the drop in Ink and Toner telemetry'
send_email('phoenix_qa_team@hpdataos.com',send_to_email, subject, message)
