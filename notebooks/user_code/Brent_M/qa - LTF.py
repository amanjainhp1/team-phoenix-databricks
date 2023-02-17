# Databricks notebook source
# MAGIC %md
# MAGIC # HW_LTF review 09-14-2022

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

# LTF versions
prev_version = '2023.02.13.1'

# COMMAND ----------

# MAGIC %md
# MAGIC ## hardware_LTF v2v compare
# MAGIC 
# MAGIC Synopsis: compare

# COMMAND ----------

# not a permanent solution as source SQL could change

LTF_sql = """
select LTF.*
from prod.hardware_ltf as LTF
left join mdm.rdma rdma on LTF.base_product_number = rdma.base_prod_number
left join mdm.hardware_xref AS hw
    on upper(hw.platform_subset) = upper(rdma.platform_subset)
where 1=1
    and hw.technology in ('INK', 'LASER', 'PWA')
    and LTF.record = 'HW_FCST'
    and LTF.official=1
"""

# COMMAND ----------

LTF_df = read_redshift_to_df(configs) \
  .option("query", LTF_sql) \
  .load()

# COMMAND ----------

LTF_df.show()

# COMMAND ----------

# prep for visualization
LTF_agg_prep = LTF_df.toPandas()

# drop unwanted columns
drop_list = ['hw_ltf_id', 'record', 'forecast_name', 'country_alpha2', 'platform_subset', 'base_product_number', 'official','load_date','version']
LTF_agg_1 = LTF_agg_prep.drop(drop_list, axis=1)

# aggregate time series
LTF_agg_2 = LTF_agg_1.groupby(['cal_date'], as_index=False).sum().sort_values('cal_date')
LTF_agg_2['variable'] = 'current.LTF'

LTF_agg_3 = LTF_agg_2.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

LTF_agg_3

# COMMAND ----------

LTF_prod_sql = """
SELECT 'previous.LTF' AS variable
    , LTF.cal_date
    , SUM(LTF.units) AS units
FROM prod.hardware_ltf AS LTF
LEFT JOIN mdm.hardware_xref AS hw
    ON hw.platform_subset = LTF.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'LASER', 'PWA')
    AND LTF.version = '{}'
    AND LTF.record = 'HW_FCST'
GROUP BY LTF.cal_date
ORDER BY LTF.cal_date
""".format(prev_version)

# COMMAND ----------

LTF_prod_df = read_redshift_to_df(configs) \
  .option("query", LTF_prod_sql) \
  .load()

# COMMAND ----------

LTF_agg_prod_prep = LTF_prod_df.toPandas()

# COMMAND ----------

LTF_agg_4 = LTF_agg_prod_prep.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

LTF_agg_4

# COMMAND ----------

LTF_agg_5 = pd.concat([LTF_agg_3, LTF_agg_4], sort=True)

# COMMAND ----------

# https://plotly.com/python-api-reference/generated/plotly.express.line

fig = px.line(data_frame=LTF_agg_5,
              x='cal_date',
              y='units',
              line_group='variable',
              color='variable',
              title='RS - LTF v2v compare')

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

LTF_generics_sql = """

select distinct platform_subset 
from prod.hardware_ltf
where 1=1
	and record = 'HW_FCST'
	and official =1
	and platform_subset like '%GENERIC%'
"""

# COMMAND ----------

LTF_generics_df = read_redshift_to_df(configs) \
  .option("query", LTF_generics_sql) \
  .load()

LTF_generics_df.show()
