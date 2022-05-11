# Databricks notebook source
# MAGIC %md
# MAGIC # IB - February 2022 Initial QA For New Redshift Process
# MAGIC 
# MAGIC Mark Middendorf, MS - Master Data Engineer
# MAGIC 
# MAGIC Candace Cox
# MAGIC 
# MAGIC Phoenix | Installed Base
# MAGIC 
# MAGIC **Notebook sections:**
# MAGIC  + review high-level v2v comparisons
# MAGIC  + review high-level technology comparisons (Laser, Ink/PWA)
# MAGIC  + deep dive into corner cases if needed

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook setup
# MAGIC ---

# COMMAND ----------

# import libraries
import matplotlib.pyplot as plt
import pandas as pd
import warnings
import sys

# silence warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

# MAGIC %run "../common/get_redshift_secrets"

# COMMAND ----------

# MAGIC %run "../common/plot_helper"

# COMMAND ----------

# Global Variables
username = spark.conf.get("username")
password = spark.conf.get("password")

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

# MAGIC %md
# MAGIC ### norm_ships_v2v.sql

# COMMAND ----------

ns_sql = """
SELECT 'dev.stage' AS variable
    , ns.cal_date
    , SUM(ns.units) AS units
FROM dev.stage.norm_ships AS ns
LEFT JOIN dev.mdm.hardware_xref AS hw
    ON hw.platform_subset = ns.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'LASER', 'PWA')
    AND ns.cal_date between '2019-03-01' and '2026-10-01'
GROUP BY ns.cal_date
UNION ALL
SELECT 'dev.prod' AS variable
    , ns.cal_date
    , SUM(ns.units) AS units
FROM dev.prod.norm_shipments AS ns
LEFT JOIN dev.mdm.hardware_xref AS hw
    ON hw.platform_subset = ns.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'LASER', 'PWA')
    AND ns.version = '2021.11.22.1'
    and ns.cal_date between '2019-03-01' and '2026-10-01'
GROUP BY ns.cal_date
"""

# COMMAND ----------

ns_spark_df = spark.read \
.format("com.databricks.spark.redshift") \
.option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
.option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
.option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
.option("user", username) \
.option("password", password) \
.option("query", ns_sql) \
.load()
        
ns_spark_df.show()

# COMMAND ----------

ns_prep = ns_spark_df.toPandas()
ns_prep['cal_date'] = pd.to_datetime(ns_prep['cal_date'])
ns_df = ns_prep.set_index('cal_date')

# COMMAND ----------

# resize figsize for each graph
plt.rcParams['figure.figsize'] = (15, 10)

# setup format_dict
plot_dict_a = plot_dict
plot_dict_a['legend_list'] = [
    'dev.prod', 'dev.stage'
]

plot_dict_a['title_list'] = [
    'norm_shipments v2v', 'cal_date', 'shipments'
]

# plot_dict_a['viz_path'] = r'./viz/ns_v2v_2021_07_01.png'

mpl_ts(df=ns_df, format_dict=plot_dict_a)

# COMMAND ----------

# MAGIC %run "./sql/qc_sql_test"

# COMMAND ----------

ns_spark_df2 = spark.read \
.format("com.databricks.spark.redshift") \
.option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
.option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
.option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
.option("user", username) \
.option("password", password) \
.option("query", norm_ships_sql) \
.load()
        
ns_spark_df2.show()
