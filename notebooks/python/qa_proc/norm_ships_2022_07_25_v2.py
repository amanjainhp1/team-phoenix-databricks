# Databricks notebook source
# MAGIC %md
# MAGIC # Norm shipments review 7-26-2022 -- Post promotion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook setup

# COMMAND ----------

# python libraries
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# COMMAND ----------

# MAGIC %run "../common/configs"

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# ns/ib versions
prev_version = '2022.08.23.1'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Norm shipments v2v compare
# MAGIC 
# MAGIC Synopsis: compare

# COMMAND ----------

# not a permanent solution as source SQL could change
# get the most recent version of norm_shipments_ce, that was just built and promoted
norm_shipments_ce_sql = """
select ns.*
from prod.norm_shipments_ce as ns
join mdm.hardware_xref AS hw
    on upper(hw.platform_subset) = upper(ns.platform_subset)
where 1=1
    and hw.technology in ('INK', 'LASER', 'PWA')
    and version = (select max(version) from prod.norm_shipments_ce)
    and ns.cal_date between '2018-11-01' and '2027-10-01'
"""

# COMMAND ----------

norm_ships_df = read_redshift_to_df(configs) \
  .option("query", norm_shipments_ce_sql) \
  .load()

# COMMAND ----------

norm_ships_df.show()

# COMMAND ----------

# prep for visualization
ns_agg_prep = norm_ships_df.toPandas()

# drop unwanted columns
drop_list = ['region_5', 'record', 'country_alpha2', 'platform_subset', 'version']
ns_agg_1 = ns_agg_prep.drop(drop_list, axis=1)

# aggregate time series
ns_agg_2 = ns_agg_1.groupby(['cal_date'], as_index=False).sum().sort_values('cal_date')
ns_agg_2['variable'] = 'test.norm_ships'

ns_agg_3 = ns_agg_2.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

ns_agg_3

# COMMAND ----------

# get the previous version (or whatever version you want to compare against, from the variable at the top of the page in Cmd 7)
ns_prod_sql = """
SELECT 'prod.norm_shipments_ce' AS variable
    , ns.cal_date
    , SUM(ns.units) AS units
FROM prod.norm_shipments_ce AS ns
LEFT JOIN mdm.hardware_xref AS hw
    ON hw.platform_subset = ns.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'LASER', 'PWA')
    AND ns.version = '{}'
    and ns.cal_date between '2018-11-01' and '2027-10-01'
GROUP BY ns.cal_date
ORDER BY ns.cal_date
""".format(prev_version)

# COMMAND ----------

norm_ships_prod_df = read_redshift_to_df(configs) \
  .option("query", ns_prod_sql) \
  .load()

# COMMAND ----------

ns_agg_prod_prep = norm_ships_prod_df.toPandas()

# COMMAND ----------

ns_agg_4 = ns_agg_prod_prep.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

ns_agg_4

# COMMAND ----------

ns_agg_5 = pd.concat([ns_agg_3, ns_agg_4], sort=True)

# COMMAND ----------

# https://plotly.com/python-api-reference/generated/plotly.express.line

fig = px.line(data_frame=ns_agg_5,
              x='cal_date',
              y='units',
              line_group='variable',
              color='variable',
              title='RS - NS v2v compare')

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

v2v_ink_trad = """
with prod as
(
select
	cal_date,
	version,
	sum(units) as units	
from prod.norm_shipments_ce a left join mdm.hardware_xref b on a.platform_subset =b.platform_subset 
where 1=1
	and version = (select max(version) from prod.norm_shipments_ce)
	and b.technology = 'INK'
	and a.customer_engagement = 'TRAD'
    and a.cal_date BETWEEN '2021-11-01' AND '2027-10-01'
group by
	cal_date,
	version
),

prev as 
( 
select
	cal_date,
	version,
	sum(units) as units	
from prod.norm_shipments_ce a left join mdm.hardware_xref b on a.platform_subset =b.platform_subset 
where 1=1
	and version = '{}'
	and b.technology = 'INK'
	and a.customer_engagement = 'TRAD'
    and a.cal_date BETWEEN '2021-11-01' AND '2027-10-01'
group by
	cal_date,
	version
)
select * 
from prod
union all
select *
from prev
order by cal_date, version
""".format(prev_version)

# COMMAND ----------

ns_v2v_ink_trad_df = read_redshift_to_df(configs) \
  .option("query", v2v_ink_trad) \
  .load()

# COMMAND ----------

ink_trad_prep = ns_v2v_ink_trad_df.toPandas()
ink_agg = ink_trad_prep.reindex(['cal_date', 'version', 'units'], axis=1)

# COMMAND ----------

fig = px.line(data_frame=ink_agg,
              x='cal_date',
              y='units',
              line_group='version',
              color='version',
              title='RS - NS ink TRAD v2v compare')

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

v2v_ink_iink = """
with prod as
(
select
	cal_date,
	version,
	sum(units) as units	
from prod.norm_shipments_ce a left join mdm.hardware_xref b on a.platform_subset =b.platform_subset 
where 1=1
	and version = (select max(version) from prod.norm_shipments_ce)
	and b.technology = 'INK'
	and a.customer_engagement = 'I-INK'
    and a.cal_date BETWEEN '2021-11-01' AND '2027-10-01'
group by
	cal_date,
	version
),

prev as 
( 
select
	cal_date,
	version,
	sum(units) as units	
from prod.norm_shipments_ce a left join mdm.hardware_xref b on a.platform_subset =b.platform_subset 
where 1=1
	and version = '{}'
	and b.technology = 'INK'
	and a.customer_engagement = 'I-INK'
    and a.cal_date BETWEEN '2021-11-01' AND '2027-10-01'
group by
	cal_date,
	version
)
select * 
from prod
union all
select *
from prev
order by cal_date, version
""".format(prev_version)

# COMMAND ----------

ns_v2v_ink_iink_df = read_redshift_to_df(configs) \
  .option("query", v2v_ink_iink) \
  .load()

# COMMAND ----------

ink_iink_prep = ns_v2v_ink_iink_df.toPandas()
iink_agg = ink_iink_prep.reindex(['cal_date', 'version', 'units'], axis=1)

# COMMAND ----------

fig = px.line(data_frame=iink_agg,
              x='cal_date',
              y='units',
              line_group='version',
              color='version',
              title='RS - NS ink I-INK v2v compare')

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


