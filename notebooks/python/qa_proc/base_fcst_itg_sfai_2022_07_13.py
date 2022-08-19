# Databricks notebook source
# MAGIC %md
# MAGIC # Base Forecast - COMPARE RS-ITG to SFAI
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
# MAGIC ## Laser Compare #1
# MAGIC 
# MAGIC Aggregate: cartridges by month, geography, platform_subset, customer_engagment  
# MAGIC Filter: LASER HW

# COMMAND ----------

sfai_query_1 = """
select vtc.cal_date
    , upper(vtc.geography) as geography
    , upper(vtc.platform_subset) as platform_subset
    , upper(vtc.customer_engagement) as customer_engagement
    , round(sum(vtc.mvtc_adjusted_crgs), 4) as adj_crgs_sfai
from ie2_staging.dbt.c2c_cartridges_w_vtc as vtc
join ie2_prod.dbo.hardware_xref as hw
    on hw.platform_subset = vtc.platform_subset
where 1=1
    and hw.technology = 'LASER'
    and vtc.cal_date between '2021-11-01' and '2025-10-01'
group by vtc.cal_date
    , upper(vtc.geography)
    , upper(vtc.platform_subset)
    , upper(vtc.customer_engagement)
"""

rs_query_1 = """
select vtc.cal_date
    , upper(vtc.geography) as geography
    , upper(vtc.platform_subset) as platform_subset
    , upper(vtc.customer_engagement) as customer_engagement
    , round(sum(vtc.mvtc_adjusted_crgs), 4) as adj_crgs_rs
from stage.vtc as vtc
join mdm.hardware_xref as hw
    on upper(hw.platform_subset) = upper(vtc.platform_subset)
where 1=1
    and upper(hw.technology) = 'LASER'
    and vtc.cal_date between '2021-11-01' and '2025-10-01'
group by vtc.cal_date
    , upper(vtc.geography)
    , upper(vtc.platform_subset)
    , upper(vtc.customer_engagement)
"""

# COMMAND ----------

# sfai dataframe
compare_sfai_df_1 = read_sql_server_to_df(configs) \
  .option("query", sfai_query_1) \
  .load()

# COMMAND ----------

# rs dataframe
compare_rs_df_1 = read_redshift_to_df(configs) \
  .option("query", rs_query_1) \
  .load()

# COMMAND ----------

compare_rs_sfai = compare_rs_df_1.join(compare_sfai_df_1, 
                                       ['cal_date', 'platform_subset', 'geography', 'customer_engagement'])

# COMMAND ----------

compare_rs_sfai.show()

# COMMAND ----------

compare_rs_sfai2 = compare_rs_sfai.withColumn('rate_change', (compare_rs_sfai.adj_crgs_rs - compare_rs_sfai.adj_crgs_sfai) / compare_rs_sfai.adj_crgs_sfai)

# COMMAND ----------

compare_rs_sfai2.show()

# COMMAND ----------

compare_rs_sfai3 = compare_rs_sfai2.withColumn('rate_change_abs', abs(compare_rs_sfai2.rate_change))                                      

# COMMAND ----------

compare_rs_sfai3.sort(compare_rs_sfai3.rate_change_abs.desc()).show()  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Laser Compare #2

# COMMAND ----------

sfai_query_2 = """
select vtc.cal_date
    , 'sfai' as variable
    , round(sum(vtc.mvtc_adjusted_crgs), 4) as units
from ie2_staging.dbt.c2c_cartridges_w_vtc as vtc
join ie2_prod.dbo.hardware_xref as hw
    on hw.platform_subset = vtc.platform_subset
where 1=1
    and hw.technology = 'LASER'
    and vtc.cal_date between '2021-11-01' and '2025-10-01'
group by vtc.cal_date
"""

rs_query_2 = """
select vtc.cal_date
    , 'rs' as variable
    , round(sum(vtc.mvtc_adjusted_crgs), 4) as units
from stage.vtc as vtc
join mdm.hardware_xref as hw
    on upper(hw.platform_subset) = upper(vtc.platform_subset)
where 1=1
    and upper(hw.technology) = 'LASER'
    and vtc.cal_date between '2021-11-01' and '2025-10-01'
group by vtc.cal_date
"""

# COMMAND ----------

# sfai dataframe
compare_sfai_df_2 = read_sql_server_to_df(configs) \
  .option("query", sfai_query_2) \
  .load()

# COMMAND ----------

# rs dataframe
compare_rs_df_2 = read_redshift_to_df(configs) \
  .option("query", rs_query_2) \
  .load()

# COMMAND ----------

sfai_prep_1 = compare_sfai_df_2.toPandas()
rs_prep_1 = compare_rs_df_2.toPandas()

sfai_prep_2 = sfai_prep_1.reindex(['cal_date', 'variable', 'units'], axis=1)
rs_prep_2 = rs_prep_1.reindex(['cal_date', 'variable', 'units'], axis=1)

viz_df_1 = pd.concat([sfai_prep_2, rs_prep_2], sort=True)
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
              title='RS - SFAI Base Fcst compare')

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
