# Databricks notebook source
# MAGIC %md
# MAGIC # Toner Working Forecast - COMPARE RS-ITG (working to base)
# MAGIC 
# MAGIC Reference: https://spark.apache.org/docs/latest/api/python//reference/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook setup - permissions and database utils

# COMMAND ----------

# MAGIC %run ../../../notebooks/python/common/database_utils

# COMMAND ----------

# MAGIC %run ../../../notebooks/python/common/configs

# COMMAND ----------

# python libraries
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pyspark.sql.functions import abs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Laser Fcst Compare - time series

# COMMAND ----------

rs_query_1 = """
select vtc.cal_date
    , 'rs-work' as variable
    , round(sum(vtc.adjusted_cartridges), 4) as units
from scen.toner_working_fcst as vtc
join mdm.hardware_xref as hw
    on hw.platform_subset = vtc.platform_subset
where 1=1
    and hw.technology = 'LASER'
    and vtc.cal_date between '2016-11-01' and '2025-10-01'
group by vtc.cal_date
"""

rs_query_2 = """
select vtc.cal_date
    , 'rs-base' as variable
    , round(sum(vtc.mvtc_adjusted_crgs), 4) as units
from stage.vtc as vtc
join mdm.hardware_xref as hw
    on upper(hw.platform_subset) = upper(vtc.platform_subset)
where 1=1
    and upper(hw.technology) = 'LASER'
    and vtc.cal_date between '2016-11-01' and '2025-10-01'
group by vtc.cal_date
"""

# COMMAND ----------

# rs dataframe 1
compare_rs_df_1 = read_redshift_to_df(configs) \
  .option("query", rs_query_1) \
  .load()

# COMMAND ----------

# rs dataframe 2
compare_rs_df_2 = read_redshift_to_df(configs) \
  .option("query", rs_query_2) \
  .load()

# COMMAND ----------

rs_prep_1 = compare_rs_df_1.toPandas()
rs_prep_2 = compare_rs_df_2.toPandas()

rs_prep_3 = rs_prep_1.reindex(['cal_date', 'variable', 'units'], axis=1)
rs_prep_4 = rs_prep_2.reindex(['cal_date', 'variable', 'units'], axis=1)

viz_df_1 = pd.concat([rs_prep_3, rs_prep_4], sort=True)
viz_df_2 = viz_df_1.sort_values(by=['cal_date', 'variable'])

# COMMAND ----------

viz_df_2.display()

# COMMAND ----------

# https://plotly.com/python-api-reference/generated/plotly.express.line

fig = px.line(data_frame=viz_df_2,
              x='cal_date',
              y='units',
              line_group='variable',
              color='variable',
              title='RS - Work to Base compare - LASER')

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
