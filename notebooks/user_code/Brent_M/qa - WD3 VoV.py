# Databricks notebook source
# MAGIC %md
# MAGIC # HW WD3 review 04-10-2023

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook setup

# COMMAND ----------

# python libraries
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# COMMAND ----------

# MAGIC %run "../../python/common/configs"

# COMMAND ----------

# MAGIC %run ../../python/common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

prev_version_sql = """
select max(version) as version 
from prod.flash_wd3 where record = 'WD3' 
    and version <> (SELECT MAX(version) from prod.flash_wd3 WHERE record = 'WD3')
"""

prev_version = read_redshift_to_df(configs) \
  .option("query", prev_version_sql) \
  .load() \
  .select("version").head()[0]

# COMMAND ----------

# FLASH versions
# prev_version = '2023.03.09.1'

# COMMAND ----------

# MAGIC %md
# MAGIC ## hardware WD3 v2v compare
# MAGIC 
# MAGIC Synopsis: compare

# COMMAND ----------

# not a permanent solution as source SQL could change

wd3_sql = """
select wd3.*
from prod.flash_wd3 as wd3
left join mdm.rdma rdma on wd3.base_product_number = rdma.base_prod_number
left join mdm.hardware_xref AS hw
    on upper(hw.platform_subset) = upper(rdma.platform_subset)
where 1=1
    and hw.technology in ('INK', 'LASER', 'PWA')
    and wd3.version = (select max(version) from prod.flash_wd3 WHERE record = 'WD3')
"""

# COMMAND ----------

wd3_df = read_redshift_to_df(configs) \
  .option("query", wd3_sql) \
  .load()

# COMMAND ----------

wd3_df.show()

# COMMAND ----------

# prep for visualization
wd3_agg_prep = wd3_df.toPandas()

# drop unwanted columns
drop_list = ['record', 'source_name', 'country_alpha2', 'base_product_number', 'load_date','version']
wd3_agg_1 = wd3_agg_prep.drop(drop_list, axis=1)

# aggregate time series
wd3_agg_2 = wd3_agg_1.groupby(['cal_date'], as_index=False).sum().sort_values('cal_date')
wd3_agg_2['variable'] = 'current.wd3'

wd3_agg_3 = wd3_agg_2.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

wd3_agg_3

# COMMAND ----------

wd3_prod_sql = """
SELECT 'previous.wd3' AS variable
    , prev_wd3.cal_date
    , SUM(prev_wd3.units) AS units
FROM prod.flash_wd3 AS prev_wd3
left join mdm.rdma rdma on prev_wd3.base_product_number = rdma.base_prod_number
LEFT JOIN mdm.hardware_xref AS hw
    ON hw.platform_subset = rdma.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'LASER', 'PWA')
    AND prev_wd3.version = '{}'
    AND prev_wd3.record = 'WD3'
GROUP BY prev_wd3.cal_date
ORDER BY prev_wd3.cal_date
""".format(prev_version)

# COMMAND ----------

wd3_prod_df = read_redshift_to_df(configs) \
  .option("query", wd3_prod_sql) \
  .load()

# COMMAND ----------

wd3_prod_df.display()

# COMMAND ----------

wd3_agg_prod_prep = wd3_prod_df.toPandas()

# COMMAND ----------

wd3_agg_4 = wd3_agg_prod_prep.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

wd3_agg_4

# COMMAND ----------

wd3_agg_5 = pd.concat([wd3_agg_3, wd3_agg_4], sort=True)

# COMMAND ----------

# https://plotly.com/python-api-reference/generated/plotly.express.line

fig = px.line(data_frame=wd3_agg_5,
              x='cal_date',
              y='units',
              line_group='variable',
              color='variable',
              title='RS - WD3 v2v compare')

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests - look for generics

# COMMAND ----------

wd3_generics_sql = """

select distinct b.platform_subset
from prod.flash_wd3 a left join mdm.rdma b on a.base_product_number = b.base_prod_number
where 1=1
	and record = 'WD3'
    and version = (SELECT MAX(version) from prod.flash_wd3 WHERE record = 'WD3')
	and platform_subset like '%GENERIC%'
"""

# COMMAND ----------

wd3_generics_df = read_redshift_to_df(configs) \
  .option("query", wd3_generics_sql) \
  .load()

wd3_generics_df.show()
