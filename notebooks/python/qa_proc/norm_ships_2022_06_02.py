# Databricks notebook source
# MAGIC %md
# MAGIC # Norm shipments review 6-02-2022

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

# MAGIC %md
# MAGIC ## Norm shipments v2v compare
# MAGIC 
# MAGIC Synopsis: compare

# COMMAND ----------

# not a permanent solution as source SQL could change

norm_ships_sql = """
select ns.*
from stage.norm_ships as ns
join mdm.hardware_xref AS hw
    on upper(hw.platform_subset) = upper(ns.platform_subset)
where 1=1
    and hw.technology in ('INK', 'LASER', 'PWA')
"""

# COMMAND ----------

norm_ships_df = read_redshift_to_df(configs) \
  .option("query", norm_ships_sql) \
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

ns_prod_sql = """
SELECT 'prod.norm_ships' AS variable
    , ns.cal_date
    , SUM(ns.units) AS units
FROM prod.norm_shipments AS ns
LEFT JOIN mdm.hardware_xref AS hw
    ON hw.platform_subset = ns.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'LASER', 'PWA')
    AND ns.version = '2022.05.16.1'
    -- and ns.cal_date between '2019-03-01' and '2026-10-01'
GROUP BY ns.cal_date
ORDER BY ns.cal_date
"""

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

# MAGIC %md
# MAGIC ## Tests - Actuals to MDM
# MAGIC 
# MAGIC List:
# MAGIC + actuals to hardware_xref
# MAGIC + actuals to decay

# COMMAND ----------

acts_to_hw_sql = """
SELECT DISTINCT act.platform_subset AS actuals_platform_subset
    , act.source
    , xref.platform_subset AS hw_xref_platform_subset
    , xref.technology
    , xref.business_feature
    , xref.category_feature
    , xref.pl
FROM prod.actuals_hw AS act
LEFT JOIN mdm.hardware_xref xref
    ON act.platform_subset = xref.platform_subset
WHERE 1=1
    AND act.record = 'ACTUALS - HW'
    AND act.official = 1
    AND (xref.Platform_Subset IS NULL OR xref.technology ISNULL OR
         xref.business_feature IS NULL OR xref.category_feature ISNULL OR
         xref.pl ISNULL)
    AND xref.technology IN ('INK', 'LASER', 'PWA')
"""

# COMMAND ----------

acts_to_hw_df = read_redshift_to_df(configs) \
  .option("query", acts_to_hw_sql) \
  .load()

# COMMAND ----------

acts_to_hw_df.show()

# COMMAND ----------

acts_to_decay_sql = """
WITH actuals AS
(
    SELECT DISTINCT acts.platform_subset AS act_platform_subset
        , iso.region_5
    FROM prod.actuals_hw acts
        LEFT JOIN mdm.iso_country_code_xref iso
            ON acts.country_alpha2=iso.country_alpha2
    WHERE 1=1
        AND acts.record = 'ACTUALS - HW'
        AND acts.official = 1
)

SELECT 'ACTUALS' AS record
    , actuals.act_platform_subset
    , actuals.region_5
    , decay.platform_subset AS decay_platform_subset
    , decay.geography AS decay_region_5
    , hw.technology
FROM actuals
LEFT JOIN prod.decay decay
    ON actuals.act_platform_subset=decay.platform_subset
    AND actuals.region_5=decay.geography
    AND decay.official=1
LEFT JOIN mdm.hardware_xref AS hw
    ON actuals.act_platform_subset = hw.platform_subset
WHERE 1=1
    AND (decay.geography IS NULL OR decay.platform_subset IS NULL)
    AND hw.technology IN ('INK', 'LASER', 'PWA')
"""

# COMMAND ----------

acts_to_decay_df = read_redshift_to_df(configs) \
  .option("query", acts_to_decay_sql) \
  .load()

# COMMAND ----------

acts_to_decay_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests - Forecast to MDM
# MAGIC 
# MAGIC List:
# MAGIC + forecast to hardware_xref
# MAGIC + forecast to decay

# COMMAND ----------

fcst_to_hw_sql = """
SELECT DISTINCT ltf.record
    , ltf.platform_subset AS ltf_platform_subset
    , xref.platform_subset AS xref_platform_subset
    , xref.technology
    , xref.business_feature
    , xref.category_feature
    , xref.pl
FROM prod.hardware_ltf AS ltf
LEFT JOIN mdm.hardware_xref AS xref
    ON ltf.platform_subset = xref.platform_subset
WHERE 1=1
    AND ltf.record IN ('HW_STF_FCST')
	AND ltf.version = (SELECT MAX(VERSION) FROM prod.hardware_ltf WHERE record IN ('HW_STF_FCST') AND official = 1)
    AND (xref.Platform_Subset IS NULL OR xref.technology IS NULL OR
         xref.business_feature IS NULL OR xref.category_feature IS NULL OR
         xref.pl IS NULL)
    AND xref.technology IN ('INK', 'LASER', 'PWA')

UNION ALL

SELECT DISTINCT ltf.record
    , ltf.platform_subset AS ltf_platform_subset
    , xref.platform_subset AS xref_platform_subset
    , xref.technology
    , xref.business_feature
    , xref.category_feature
    , xref.pl
FROM prod.hardware_ltf AS ltf
LEFT JOIN mdm.hardware_xref AS xref
    ON ltf.platform_subset = xref.platform_subset
WHERE 1=1
    AND ltf.record IN ('HW_FCST')
	AND ltf.version = (SELECT MAX(VERSION) FROM prod.hardware_ltf WHERE record IN ('HW_FCST') AND official = 1)
    AND (xref.Platform_Subset IS NULL OR xref.technology IS NULL OR
         xref.business_feature IS NULL OR xref.category_feature IS NULL OR
         xref.pl IS NULL)
    AND xref.technology IN ('INK', 'LASER', 'PWA')
"""

# COMMAND ----------

fcst_to_hw_df = read_redshift_to_df(configs) \
  .option("query", fcst_to_hw_sql) \
  .load()

# COMMAND ----------

fcst_to_hw_df.show()

# COMMAND ----------

fcst_to_decay_sql = """
WITH fcst AS
(
    SELECT DISTINCT ltf.record
        , ltf.platform_subset AS ltf_platform_subset
        , iso.region_5
        , ltf.version
    FROM prod.hardware_ltf AS ltf
    LEFT JOIN mdm.iso_country_code_xref AS iso
        ON ltf.country_alpha2 = iso.country_alpha2
    WHERE 1=1
        AND ltf.record IN ('HW_STF_FCST')
        AND ltf.version = (SELECT MAX(VERSION) FROM prod.hardware_ltf WHERE record IN ('HW_STF_FCST') AND official = 1)
    
    UNION ALL
    
    SELECT DISTINCT ltf.record
        , ltf.platform_subset AS ltf_platform_subset
        , iso.region_5
        , ltf.version
    FROM prod.hardware_ltf AS ltf
    LEFT JOIN mdm.iso_country_code_xref AS iso
        ON ltf.country_alpha2=iso.country_alpha2
    WHERE 1=1
        AND ltf.record IN ('HW_FCST')
        AND ltf.version = (SELECT MAX(VERSION) FROM prod.hardware_ltf WHERE record IN ('HW_FCST') AND official = 1)
)

SELECT fcst.record
    , fcst.version
    , fcst.ltf_platform_subset
    , fcst.region_5
    , decay.platform_subset AS decay_platform_subset
    , decay.geography AS decay_geography
    , hw.technology
FROM fcst
LEFT JOIN prod.decay AS decay
    ON fcst.ltf_platform_subset = decay.platform_subset
    AND fcst.region_5 = decay.geography
    AND decay.official = 1
LEFT JOIN mdm.hardware_xref AS hw
    ON fcst.ltf_platform_subset = hw.platform_subset
WHERE 1=1
    AND (decay.geography IS NULL OR decay.platform_subset IS NULL)
    AND hw.technology IN ('INK', 'LASER', 'PWA')
"""

# COMMAND ----------

fcst_to_decay_df = read_redshift_to_df(configs) \
  .option("query", fcst_to_decay_sql) \
  .load()

# COMMAND ----------

fcst_to_decay_df.show()
