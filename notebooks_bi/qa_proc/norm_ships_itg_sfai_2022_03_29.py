# Databricks notebook source
# MAGIC %md
# MAGIC # Norm Ships - March 2022 Initial QC For New Redshift Process
# MAGIC 
# MAGIC Mark Middendorf, MS - Master Data Engineer
# MAGIC 
# MAGIC Phoenix | Normalized Shipments
# MAGIC 
# MAGIC **Notebook sections:**
# MAGIC  + review high-level v2v comparisons
# MAGIC  + review high-level technology comparisons (Laser, Ink/PWA)
# MAGIC  + deep dive into corner cases if needed

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Notebook setup

# COMMAND ----------

# Databricks notebook source
dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("stack", "")
dbutils.widgets.text("job_dbfs_path", "")

# COMMAND ----------

# MAGIC %run ../../notebooks/python/common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../../notebooks/python/common/database_utils

# COMMAND ----------

# MAGIC %run "../common/plot_helper"

# COMMAND ----------

import os
os.listdir("../../notebooks/") # returns list

# COMMAND ----------

constants = {
    "SFAI_URL": "jdbc:sqlserver://sfai.corp.hpicloud.net:1433;",
    "SFAI_DRIVER": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "S3_BASE_BUCKET" : {
        "dev" : "s3a://dataos-core-dev-team-phoenix/",
        "itg" : "s3a://dataos-core-itg-team-phoenix/",
        "prod" : "s3a://dataos-core-prod-team-phoenix/"
    },
    "REDSHIFT_URLS" : {
        "dev" : "dataos-core-dev-team-phoenix.dev.hpdataos.com",
        "itg" : "dataos-core-team-phoenix-itg.hpdataos.com",
        "prod" : "dataos-core-team-phoenix.hpdataos.com",
        "reporting" : "dataos-core-team-phoenix-reporting.hpdataos.com"
    },
    "REDSHIFT_PORTS" : {
        "dev" : "5439",
        "itg" : "5439",
        "prod" : "5439",
        "reporting" : "5439"
    },
    "REDSHIFT_DATABASE" : {
        "dev" : "dev",
        "itg" : "itg",
        "prod" : "prod",
        "reporting" : "prod"
    }
}

# COMMAND ----------

# retrieve secrets based on incoming/inputted secrets name - variables will be accessible across languages
redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")
sqlserver_secrets = secrets_get(dbutils.widgets.get("sqlserver_secrets_name"), "us-west-2")

# COMMAND ----------

# configs dict setup
configs = {}
configs["redshift_username"] = redshift_secrets["username"]
configs["redshift_password"] = redshift_secrets["password"]
configs["redshift_url"] = constants["REDSHIFT_URLS"][dbutils.widgets.get("stack")]
configs["redshift_port"] = constants["REDSHIFT_PORTS"][dbutils.widgets.get("stack")]
configs["redshift_dbname"] = constants["REDSHIFT_DATABASE"][dbutils.widgets.get("stack")]
# configs["aws_iam_role"] = dbutils.widgets.get("aws_iam_role")
# configs["redshift_temp_bucket"] =  "{}redshift_temp/".format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")])
configs["sfai_username"] = sqlserver_secrets["username"]
configs["sfai_password"] = sqlserver_secrets["password"]
configs["sfai_url"] = constants["SFAI_URL"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## SFAI review

# COMMAND ----------

# import libraries
import matplotlib.pyplot as plt
import pandas as pd
import warnings
import sys

# silence warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

ns_sfai_records = read_sql_server_to_df(configs) \
  .option("query", "select * from ie2_staging.dbt.norm_ships") \
  .load()

# COMMAND ----------

ns_sfai_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC testing pyspark df

# COMMAND ----------

# MAGIC %md
# MAGIC ## aggregating pyspark df

# COMMAND ----------

drop_list = ['region_5', 'record', 'country_alpha2', 'platform_subset', 'version']
ns_agg_prep = ns_sfai_records.toPandas()
ns_agg_1 = ns_agg_prep.drop(drop_list, axis=1)
ns_agg_2 = ns_agg_1.groupby(['cal_date'], as_index=False).sum().sort_values('cal_date')
ns_agg_2['variable'] = 'dbt.norm_ships'

# COMMAND ----------

display(ns_agg_2)

# COMMAND ----------

ns_agg_3 = ns_agg_2.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

display(ns_agg_3)

# COMMAND ----------

# only needed to be defined once for this notebook
# review library.plot_helper for detail

plot_dict = {
    'ticker': True,
    'axvspan_list': [
        '2021-12-01', '2022-10-01', 'STF-FCST'
    ],
}

# COMMAND ----------

ns_prep = ns_agg_3
ns_prep['cal_date'] = pd.to_datetime(ns_prep['cal_date'])
ns_df = ns_prep.set_index('cal_date')

# COMMAND ----------

# resize figsize for each graph
plt.rcParams['figure.figsize'] = (15, 10)

# setup format_dict
plot_dict_a = plot_dict
plot_dict_a['legend_list'] = [
    'dbt.norm_ships', 'stf-window'
]

plot_dict_a['title_list'] = [
    'SFAI - norm_shipments', 'cal_date', 'shipments'
]

# plot_dict_a['viz_path'] = r'./viz/ns_v2v_2021_07_01.png'

mpl_ts(df=ns_df, format_dict=plot_dict_a)

# COMMAND ----------

# MAGIC %md
# MAGIC TODO:
# MAGIC   + dbt tests applied to spark df
# MAGIC   + spark.df to pandas.df --> concat with sfai --> plot
