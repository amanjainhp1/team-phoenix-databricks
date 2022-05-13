# Databricks notebook source
# MAGIC %md
# MAGIC # Norm shipments review 5-13-2022

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
with nrm_01_filter_vars as
(
    SELECT record
        , forecast_name
        , MAX(version) AS max_version
    FROM prod.hardware_ltf
    WHERE record IN ('HW_FCST')
        AND official = 1
    GROUP BY record, forecast_name

    UNION ALL

    SELECT record
        , forecast_name
        , MAX(version) AS max_version
    FROM prod.hardware_ltf
    WHERE record IN ('HW_STF_FCST')
        AND official = 1
    GROUP BY record, forecast_name

    UNION ALL

    SELECT record
        , NULL as forecast_name
        , MAX(version) AS max_version
    FROM prod.actuals_hw
    WHERE record = 'ACTUALS - HW'
        AND official = 1
    GROUP BY record

    UNION ALL

    SELECT record
        , NULL as forecast_name
        , MAX(version) AS max_version
    FROM prod.actuals_hw
    WHERE record = 'ACTUALS_LF'
        AND official = 1
    GROUP BY record

    UNION ALL

    SELECT record
        , forecast_name
        , MAX(version) AS max_version
    FROM prod.hardware_ltf
    WHERE record IN ('HW_LTF_LF')
        AND official = 1
    GROUP BY record, forecast_name
)

, norm_ships_inputs as
(
    SELECT 'NORM_SHIPMENTS_STAGING' AS tbl_name
        , CASE WHEN record IN  ('HW_STF_FCST','HW_LF_FCST') THEN forecast_name ELSE record END AS record
        , max_version AS version
        , GETDATE() AS execute_time
    FROM nrm_01_filter_vars
)

, nrm_02_hw_acts as
(
    SELECT ref.region_5
        , act.record
        , act.cal_date
        , act.country_alpha2
        , act.platform_subset
        , SUM(act.base_quantity) AS units  -- base_prod_number to platform_subset
    FROM "prod"."actuals_hw" AS act
    JOIN "mdm"."iso_country_code_xref" AS ref
        ON act.country_alpha2 = ref.country_alpha2
    WHERE act.record IN ('ACTUALS - HW','ACTUALS_LF')
          AND act.official = 1
    GROUP BY ref.region_5
        , act.record
        , act.cal_date
        , act.country_alpha2
        , act.platform_subset
)

,  nrm_03_hw_stf_forecast as 
(
    SELECT ref.region_5
        , ltf.record
        , ltf.cal_date
        , ltf.country_alpha2
        , ltf.platform_subset
        , SUM(ltf.units) AS units
    FROM "prod"."hardware_ltf" AS ltf
    JOIN (SELECT DISTINCT record, max_version FROM nrm_01_filter_vars WHERE record = 'HW_STF_FCST') AS vars
        ON vars.max_version = ltf.version
        AND vars.record = ltf.record
    JOIN "mdm"."iso_country_code_xref" AS ref
        ON ltf.country_alpha2 = ref.country_alpha2
    GROUP BY ref.region_5
        , ltf.record
        , ltf.cal_date
        , ltf.country_alpha2
        , ltf.platform_subset
)
   
,  nrm_04_hw_ltf_forecast as 
(
    SELECT ref.region_5
        , ltf.record
        , ltf.cal_date
        , ltf.country_alpha2
        , ltf.platform_subset
        , SUM(ltf.units) AS units
    FROM "prod"."hardware_ltf" AS ltf
    JOIN (SELECT DISTINCT record, max_version FROM nrm_01_filter_vars WHERE record = 'HW_FCST') AS vars
        ON vars.max_version = ltf.version
        AND vars.record = ltf.record
    JOIN "mdm"."iso_country_code_xref" AS ref
        ON ltf.country_alpha2 = ref.country_alpha2
    GROUP BY ref.region_5
        , ltf.record
        , ltf.cal_date
        , ltf.country_alpha2
        , ltf.platform_subset
    
    UNION
    
    SELECT ref.region_5
        , ltf.record
        , ltf.cal_date
        , ltf.country_alpha2
        , ltf.platform_subset
        , SUM(ltf.units) AS units
    FROM "prod"."hardware_ltf" AS ltf
    JOIN (SELECT DISTINCT record, max_version FROM nrm_01_filter_vars WHERE record = 'HW_LTF_LF') AS vars
        ON vars.max_version = ltf.version
        AND vars.record = ltf.record
    JOIN "mdm"."iso_country_code_xref" AS ref
        ON ltf.country_alpha2 = ref.country_alpha2
    GROUP BY ref.region_5
        , ltf.record
        , ltf.cal_date
        , ltf.country_alpha2
        , ltf.platform_subset
)
   
,  nrm_05_combined_ships as 
(
    SELECT region_5
        , record
        , country_alpha2
        , platform_subset
        , cal_date
        , units
    FROM nrm_02_hw_acts
    
    UNION ALL
    
    SELECT region_5
        , record
        , country_alpha2
        , platform_subset
        , cal_date
        , units
    FROM nrm_03_hw_stf_forecast
    
    UNION ALL
    
    SELECT region_5
        , record
        , country_alpha2
        , platform_subset
        , cal_date
        , units
    FROM nrm_04_hw_ltf_forecast
)
   
,  nrm_08_combined_ships_acts as 
(
    SELECT sh.region_5
        , sh.record
        , sh.cal_date
        , sh.country_alpha2
        , sh.platform_subset
        , sh.units
    FROM nrm_05_combined_ships AS sh
    WHERE sh.record IN ('ACTUALS_LF','ACTUALS - HW')
)
   
,  nrm_06_printer_month_filters as 
(
    SELECT record
        , MIN(cal_date) AS min_cal_date
        , MAX(cal_date) AS max_cal_date
    FROM nrm_05_combined_ships
    GROUP BY record
)
   
,  nrm_07_printer_dates as 
(
    SELECT MAX(CASE WHEN record = 'ACTUALS - HW' THEN min_cal_date ELSE NULL END) AS act_min_cal_date
        , MAX(CASE WHEN record = 'ACTUALS - HW' THEN max_cal_date ELSE NULL END) AS act_max_cal_date
        , MAX(CASE WHEN record = 'HW_STF_FCST' THEN min_cal_date ELSE NULL END) AS stf_min_cal_date
        , MAX(CASE WHEN record = 'HW_STF_FCST' THEN max_cal_date ELSE NULL END) AS stf_max_cal_date
        , MAX(CASE WHEN record = 'HW_FCST' THEN min_cal_date ELSE NULL END) AS ltf_min_cal_date
        , MAX(CASE WHEN record = 'HW_FCST' THEN max_cal_date ELSE NULL END) AS ltf_max_cal_date
    FROM nrm_06_printer_month_filters
)
   
,  nrm_09_combined_ships_fcst as 
(
    SELECT stf.region_5
        , stf.record
        , stf.cal_date
        , stf.country_alpha2
        , stf.platform_subset
        , stf.units
    FROM nrm_05_combined_ships AS stf
    CROSS JOIN nrm_07_printer_dates AS pd
    WHERE 1=1
        AND stf.record = 'HW_STF_FCST'
        AND stf.cal_date > pd.act_max_cal_date
    
    UNION ALL
    
    SELECT ltf.region_5
        , ltf.record
        , ltf.cal_date
        , ltf.country_alpha2
        , ltf.platform_subset
        , ltf.units
    FROM nrm_05_combined_ships AS ltf
    CROSS JOIN nrm_07_printer_dates AS pd
    WHERE 1=1
        AND ltf.record IN ('HW_LTF_LF','HW_FCST')
        AND ltf.cal_date > pd.stf_max_cal_date
)

SELECT acts.region_5
    , acts.record
    , acts.cal_date
    , acts.country_alpha2
    , acts.platform_subset
    , acts.units
    , '1.1' AS version  -- used in ib process
FROM nrm_08_combined_ships_acts AS acts
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = acts.platform_subset
WHERE 1=1
    AND hw.technology IN ('LASER','INK','PWA','LF')
    AND NOT hw.pl IN ('GW', 'LX')

UNION ALL

SELECT fcst.region_5
    , fcst.record
    , fcst.cal_date
    , fcst.country_alpha2
    , fcst.platform_subset
    , fcst.units
    , '1.1' AS version  -- used in ib process
FROM nrm_09_combined_ships_fcst AS fcst
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = fcst.platform_subset
WHERE 1=1
    AND hw.technology IN ('LASER','INK','PWA','LF')
    AND NOT hw.pl IN ('GW', 'LX')
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
    AND ns.version = '2022.04.27.1'
    and ns.cal_date between '2019-03-01' and '2026-10-01'
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
         xref.business_feature IS NULL OR xref.category_feature IS NULL OR
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
