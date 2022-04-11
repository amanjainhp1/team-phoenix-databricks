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
# MAGIC  
# MAGIC **Intelligence:**
# MAGIC   + https://github.azc.ext.hp.com/supplies-bd/team-phoenix-databricks-jobs-config/blob/master/jobs/actuals-hw/itg.yml
# MAGIC   + https://spark.apache.org/docs/latest/api/python//reference/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Notebook setup - permissions and database utils

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

# MAGIC %run ../common/plot_helper

# COMMAND ----------

# MAGIC %run ./constants_test

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
configs["aws_iam_role"] = dbutils.widgets.get("aws_iam_role")
configs["redshift_temp_bucket"] =  "{}redshift_temp/".format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")])
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

ns_sfai_records.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## testing output - simulate dbt data tests using pyspark.sql.DataFrame
# MAGIC 
# MAGIC Below are simple data tests currently used with DBT to check normalized shipments output

# COMMAND ----------

# test 1 :: isna() returns a dataframe of boolean - same columns, same number of records
ns_sfai_records.where('units is null').collect()

# COMMAND ----------

# test 2 :: multiple CKs
ck = ['region_5', 'cal_date', 'country_alpha2', 'platform_subset']
ns_sfai_records.groupby(ck).count().filter("count > 1").show()

# COMMAND ----------

# test 3 :: find any generics
ns_sfai_records.filter(ns_sfai_records.platform_subset.like('%GENERIC%')).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shaping using pyspark.pandas.DataFrame for use with matplotlib

# COMMAND ----------

ns_agg_prep = ns_sfai_records.toPandas()

# drop unwanted columns
drop_list = ['region_5', 'record', 'country_alpha2', 'platform_subset', 'version']
ns_agg_1 = ns_agg_prep.drop(drop_list, axis=1)

# aggregate time series
ns_agg_2 = ns_agg_1.groupby(['cal_date'], as_index=False).sum().sort_values('cal_date')
ns_agg_2['variable'] = 'dbt.norm_ships'

# COMMAND ----------

display(ns_agg_2)

# COMMAND ----------

# re-order dataframe columns
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

display(ns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC TODO:
# MAGIC   + dbt tests applied to spark df
# MAGIC   + spark.df to pandas.df --> concat with sfai --> plot

# COMMAND ----------

# MAGIC %md
# MAGIC ## REDSHIFT review

# COMMAND ----------

ns_rs_records = read_redshift_to_df(configs) \
  .option("query", "select ns.* from itg.stage.norm_ships as ns \
          join itg.mdm.hardware_xref as hw \
          on hw.platform_subset = ns.platform_subset \
          where hw.technology in ('INK', 'LASER', 'PWA')") \
  .load()

# COMMAND ----------

ns_rs_records.show()

# COMMAND ----------

ns_rs_records.select('record').distinct().collect()

# COMMAND ----------

ns_rs_records.count()
# run 1 :: 2227912 (3/29/2022)
# run 2 :: 2227912 (3/30/2022)
# run 3 :: 2219185 (3/30/2022)

# COMMAND ----------

ns_rs_agg_prep = ns_rs_records.toPandas()

# drop unwanted columns
drop_list = ['region_5', 'record', 'country_alpha2', 'platform_subset', 'version']
ns_rs_agg_1 = ns_rs_agg_prep.drop(drop_list, axis=1)

# aggregate time series
ns_rs_agg_2 = ns_rs_agg_1.groupby(['cal_date'], as_index=False).sum().sort_values('cal_date')
ns_rs_agg_2['variable'] = 'itg.stage.norm_ships'

# COMMAND ----------

display(ns_rs_agg_2)

# COMMAND ----------

# re-order dataframe columns
ns_rs_agg_3 = ns_rs_agg_2.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

ns_rs_prep = ns_rs_agg_3
ns_rs_prep['cal_date'] = pd.to_datetime(ns_rs_prep['cal_date'])
ns_rs_df = ns_rs_prep.set_index('cal_date')

# COMMAND ----------

ns_rs_df.display()

# COMMAND ----------

final_df = ns_rs_df.append(ns_df, ignore_index=False)

# COMMAND ----------

# resize figsize for each graph
plt.rcParams['figure.figsize'] = (15, 10)

# setup format_dict
plot_dict_a = plot_dict
plot_dict_a['legend_list'] = [
    'dbt.norm_ships', 'rs.norm_ships', 'stf-window'
]

plot_dict_a['title_list'] = [
    'SFAI/RS - norm_shipments', 'cal_date', 'shipments'
]

# plot_dict_a['viz_path'] = r'./viz/ns_v2v_2021_07_01.png'

mpl_ts(df=final_df, format_dict=plot_dict_a)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plotly - RS data only

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go

# https://plotly.com/python-api-reference/generated/plotly.express.line
 
fig = px.line(ns_rs_agg_2, 
              x='cal_date', 
              y='units', 
              line_group='variable',
              title='RS - Norm Ships')

fig.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all")
        ])
    )
)

fig.update_layout(
    autosize=False,
    width=1400,
    height=500,
    margin=dict(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4
    ),
)

fig.show()
