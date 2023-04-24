# Databricks notebook source
# MAGIC %md
# MAGIC # HW FLASH review 02-21-2023

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

# FLASH versions
prev_version = '2023.03.17.1'

# COMMAND ----------

# MAGIC %md
# MAGIC ## hardware FLASH v2v compare
# MAGIC 
# MAGIC Synopsis: compare

# COMMAND ----------

# not a permanent solution as source SQL could change

flash_sql = """
select flash.*
from prod.flash_wd3 as flash
left join mdm.rdma rdma on flash.base_product_number = rdma.base_prod_number
left join mdm.hardware_xref AS hw
    on upper(hw.platform_subset) = upper(rdma.platform_subset)
where 1=1
    and hw.technology in ('INK', 'LASER', 'PWA')
    and flash.version = (select max(version) from prod.flash_wd3 WHERE record = 'FLASH')
"""

# COMMAND ----------

flash_df = read_redshift_to_df(configs) \
  .option("query", flash_sql) \
  .load()

# COMMAND ----------

flash_df.show()

# COMMAND ----------

# prep for visualization
flash_agg_prep = flash_df.toPandas()

# drop unwanted columns
drop_list = ['record', 'source_name', 'country_alpha2', 'base_product_number', 'load_date','version']
flash_agg_1 = flash_agg_prep.drop(drop_list, axis=1)

# aggregate time series
flash_agg_2 = flash_agg_1.groupby(['cal_date'], as_index=False).sum().sort_values('cal_date')
flash_agg_2['variable'] = 'current.flash'

flash_agg_3 = flash_agg_2.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

flash_agg_3

# COMMAND ----------

flash_prod_sql = """
SELECT 'previous.flash' AS variable
    , prev_flash.cal_date
    , SUM(prev_flash.units) AS units
FROM prod.flash_wd3 AS prev_flash
left join mdm.rdma rdma on prev_flash.base_product_number = rdma.base_prod_number
LEFT JOIN mdm.hardware_xref AS hw
    ON hw.platform_subset = rdma.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'LASER', 'PWA')
    AND prev_flash.version = '{}'
    AND prev_flash.record = 'FLASH'
GROUP BY prev_flash.cal_date
ORDER BY prev_flash.cal_date
""".format(prev_version)

# COMMAND ----------

flash_prod_df = read_redshift_to_df(configs) \
  .option("query", flash_prod_sql) \
  .load()

# COMMAND ----------

flash_agg_prod_prep = flash_prod_df.toPandas()

# COMMAND ----------

flash_agg_4 = flash_agg_prod_prep.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

flash_agg_4

# COMMAND ----------

flash_agg_5 = pd.concat([flash_agg_3, flash_agg_4], sort=True)

# COMMAND ----------

# https://plotly.com/python-api-reference/generated/plotly.express.line

fig = px.line(data_frame=flash_agg_5,
              x='cal_date',
              y='units',
              line_group='variable',
              color='variable',
              title='RS - FLASH v2v compare')

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

flash_generics_sql = """

select distinct b.platform_subset
from prod.flash_wd3 a left join mdm.rdma b on a.base_product_number = b.base_prod_number
where 1=1
	and record = 'FLASH'
    and version = (SELECT MAX(version) from prod.flash_wd3 WHERE record = 'FLASH')
	and platform_subset like '%GENERIC%'
"""

# COMMAND ----------

flash_generics_df = read_redshift_to_df(configs) \
  .option("query", flash_generics_sql) \
  .load()

flash_generics_df.show()
