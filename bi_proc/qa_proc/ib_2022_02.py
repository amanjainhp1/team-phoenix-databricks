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
# MAGIC + review high-level v2v comparisons
# MAGIC + review high-level technology comparisons (Laser, Ink/PWA)
# MAGIC + deep dive into corner cases if needed

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook setup
# MAGIC 
# MAGIC ---

# COMMAND ----------

# import libraries
import matplotlib.pyplot as plt
import pandas as pd
import warnings
import sys

# local libraries
sys.path.append('../library')
import library.plot_helper as ph

# silence warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

# MAGIC %run "../library/get_redshift_secrets"

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
FROM dev.stage.norm_shipments AS ns
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

ph.mpl_ts(df=ns_df, format_dict=plot_dict_a)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ib - multiple versions

# COMMAND ----------

ib_v2v_compare = """
with prod as
(
    SELECT ib.cal_date
        , ib.country
        , ib.platform_subset
        , ib.customer_engagement
        , ib.version
        , SUM(ib.units) AS units
    FROM "IE2_Prod"."dbo"."ib" AS ib
    WHERE ib.version IN ('2022.01.26.1')
        AND ib.measure = 'ib'
    GROUP BY ib.cal_date
        , ib.country
        , ib.platform_subset
        , ib.customer_engagement
        , ib.version
),
stage as
(
    SELECT ib.month_begin AS cal_date
        , ib.country_alpha2 AS country
        , ib.platform_subset
        , ib.split_name AS customer_engagement
        , 'ib_staging' AS version
        , SUM(ib.ib) AS ib
    FROM "IE2_Staging"."dbt"."ib_staging" AS ib
    GROUP BY ib.month_begin
        , ib.country_alpha2
        , ib.platform_subset
        , ib.split_name
)
select cal_date
    , prod.version as variable
    , sum(units) as ib
from prod
--join IE2_Prod.dbo.hardware_xref as hw
--    on hw.platform_subset = prod.platform_subset
where 1=1
    --and hw.technology = 'LASER'
    and cal_date BETWEEN '2013-11-01' AND '2025-10-01'
group by cal_date
    , prod.version

union all

select cal_date
    , stage.version as variable
    , sum(ib) as ib
from stage
--join IE2_Prod.dbo.hardware_xref as hw
--    on hw.platform_subset = stage.platform_subset
where 1=1
    --and hw.technology = 'LASER'
    and cal_date BETWEEN '2013-11-01' AND '2025-10-01'
group by cal_date
    , stage.version
order by 1,2
"""

# COMMAND ----------

v2v_spark_df = spark.read \
.format("com.databricks.spark.redshift") \
.option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
.option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
.option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
.option("user", username) \
.option("password", password) \
.option("query", ib_v2v_compare) \
.load()
        
v2v_spark_df.show()

# COMMAND ----------

v2v_prep['cal_date'] = pd.to_datetime(df_prep['cal_date'])
v2v_prep_go = df_prep.set_index('cal_date')

# COMMAND ----------

v2v_prep_go.head(n=10)

# COMMAND ----------

# resize figsize for each graph
plt.rcParams['figure.figsize'] = (12, 8)

# setup format_dict
plot_dict_a = plot_dict
plot_dict_a['legend_list'] = [
    '2022.01.19.1', 'ib_staging', 'stf-window',
]

plot_dict_a['title_list'] = [
    'IB\nV2V COMPARE', 'cal_date', 'value'
]

# plot_dict_a['viz_path'] = r'./viz/ib_v2v_2021_04_06.png'

ph.mpl_ts(df=df_prep_go, format_dict=plot_dict_a)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ib - laser

# COMMAND ----------

with open(r'./_sql/ib/ib_v2v_laser.sql', 'r') as f:
    stf_sql = f.read()

df_prep = pd.read_sql(stf_sql, conn)
df_prep['cal_date'] = pd.to_datetime(df_prep['cal_date'])
df_prep_go = df_prep.set_index('cal_date')

# COMMAND ----------

df_prep_go.head(n=10)

# COMMAND ----------

# resize figsize for each graph
plt.rcParams['figure.figsize'] = (12, 8)

# setup format_dict
plot_dict_a = plot_dict
plot_dict_a['legend_list'] = [
    '2022.01.19.1', 'ib_staging', 'stf-window',
]

plot_dict_a['title_list'] = [
    'IB - LASER\nV2V COMPARE', 'cal_date', 'value'
]

# plot_dict_a['viz_path'] = r'./viz/ib_laser_v2v_2021_07_01.png'

ph.mpl_ts(df=df_prep_go, format_dict=plot_dict_a)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ib - ink

# COMMAND ----------

with open(r'./_sql/ib/ib_v2v_ink.sql', 'r') as f:
    stf_sql = f.read()

df_prep = pd.read_sql(stf_sql, conn)
df_prep['cal_date'] = pd.to_datetime(df_prep['cal_date'])
df_prep_go = df_prep.set_index('cal_date')

# COMMAND ----------

df_prep_go.head(n=10)

# COMMAND ----------

# resize figsize for each graph
plt.rcParams['figure.figsize'] = (12, 8)

# update plot_dict
plot_dict_ink = {
    'ticker': True,
    'axvspan_list': [
        '2021-10-01', '2022-10-01', 'STF-FCST'
    ],
}

# setup format_dict
plot_dict_a = plot_dict_ink
plot_dict_a['legend_list'] = [
    '2022.01.19.1', 'ib_staging', 'stf-window',
]

plot_dict_a['title_list'] = [
    'IB - INK\nV2V COMPARE', 'cal_date', 'value'
]

# plot_dict_a['viz_path'] = r'./viz/ib_ink_v2v_2021_08_31_v2.png'

ph.mpl_ts(df=df_prep_go, format_dict=plot_dict_a)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ib - ink v2v by split

# COMMAND ----------

with open(r'./_sql/ib/ib_v2v_ink_split.sql', 'r') as f:
    stf_sql = f.read()

df_prep = pd.read_sql(stf_sql, conn)
df_prep['cal_date'] = pd.to_datetime(df_prep['cal_date'])
df_prep_go = df_prep.set_index('cal_date')

# COMMAND ----------

df_prep_go.head(n=10)

# COMMAND ----------

# resize figsize for each graph
plt.rcParams['figure.figsize'] = (12, 8)

# setup format_dict
plot_dict_a = plot_dict_ink
plot_dict_a['legend_list'] = [
    '2022.01.19.1 - I-INK', '2022.01.19.1 - TRAD', 'ib_staging - I-INK', 'ib_staging - TRAD', 'stf-window',
]

plot_dict_a['title_list'] = [
    'IB - INK\nV2V Split COMPARE', 'cal_date', 'value'
]

# plot_dict_a['viz_path'] = r'./viz/ib_ink_v2v_2021_07_26.png'

ph.mpl_ts(df=df_prep_go, format_dict=plot_dict_a)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ib - ink v2v by iink

# COMMAND ----------

with open(r'./_sql/ib/ib_v2v_iink.sql', 'r') as f:
    stf_sql = f.read()

df_prep = pd.read_sql(stf_sql, conn)
df_prep['cal_date'] = pd.to_datetime(df_prep['cal_date'])
df_prep_go = df_prep.set_index('cal_date')

# COMMAND ----------

df_prep_go.head(n=10)

# COMMAND ----------

# resize figsize for each graph
plt.rcParams['figure.figsize'] = (12, 8)

# setup format_dict
plot_dict_a = plot_dict_ink
plot_dict_a['legend_list'] = [
    '2022.01.19.1', 'ib_staging', 'stf-window',
]

plot_dict_a['title_list'] = [
    'IB - I-INK\nV2V Split COMPARE', 'cal_date', 'value'
]

# plot_dict_a['viz_path'] = r'./viz/ib_ink_v2v_2021_07_26.png'

ph.mpl_ts(df=df_prep_go, format_dict=plot_dict_a)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ib - ink v2v by trad

# COMMAND ----------

with open(r'./_sql/ib/ib_v2v_trad.sql', 'r') as f:
    stf_sql = f.read()

df_prep = pd.read_sql(stf_sql, conn)
df_prep['cal_date'] = pd.to_datetime(df_prep['cal_date'])
df_prep_go = df_prep.set_index('cal_date')

# COMMAND ----------

df_prep_go.head(n=10)

# COMMAND ----------

# resize figsize for each graph
plt.rcParams['figure.figsize'] = (12, 8)

# setup format_dict
plot_dict_a = plot_dict_ink
plot_dict_a['legend_list'] = [
    '2022.01.25.1', 'ib_staging', 'stf-window',
]

plot_dict_a['title_list'] = [
    'IB - TRAD\nV2V Split COMPARE', 'cal_date', 'value'
]

# plot_dict_a['viz_path'] = r'./viz/ib_ink_v2v_2021_07_26.png'

ph.mpl_ts(df=df_prep_go, format_dict=plot_dict_a)

# COMMAND ----------


