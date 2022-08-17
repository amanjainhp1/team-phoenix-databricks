# Databricks notebook source
# MAGIC %md
# MAGIC # IB - QC/QA
# MAGIC 
# MAGIC Mark Middendorf, Candace Cox, Brent Merrick
# MAGIC 
# MAGIC Phoenix | Installed Base
# MAGIC 
# MAGIC **Notebook sections:**
# MAGIC + Basic tests
# MAGIC   + stage.norm_ships
# MAGIC   + stage.ib_staging
# MAGIC + Advanced tests on IB
# MAGIC + Visualizations
# MAGIC   + review high-level v2v comparisons
# MAGIC   + review high-level technology comparisons (Laser, Ink/PWA)
# MAGIC   + deep dive into corner cases if needed

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook setup
# MAGIC 
# MAGIC ---

# COMMAND ----------

# import libraries
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# ns/ib versions - used to filter to the previous build's version
## retrieve all ib version in a list
ib_versions = read_redshift_to_df(configs) \
    .option('query', 'SELECT DISTINCT version FROM prod.ib ORDER BY version DESC') \
    .load() \
    .rdd.flatMap(lambda x: x) \
    .collect()

## create 'prev_version' dropdown widget defaulting to previous version
dbutils.widgets.dropdown("prev_version", ib_versions[1], ib_versions)
prev_version = dbutils.widgets.get('prev_version')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic tests
# MAGIC 
# MAGIC By dataset (norm shipments, ib):
# MAGIC + null units
# MAGIC + multiple CKs

# COMMAND ----------

# MAGIC %md
# MAGIC ### stage.norm_ships

# COMMAND ----------

ns_records = read_redshift_to_df(configs) \
  .option("query", "select * from stage.norm_ships") \
  .load()

# COMMAND ----------

ns_records.show()

# COMMAND ----------

ns_records.count()

# COMMAND ----------

# test 1 :: isna() returns a dataframe of boolean - same columns, same number of records
ns_records.where('units is null').collect()

# COMMAND ----------

# test 2 :: multiple CKs
# LF showing up; bring in record to the CK
ck = ['record', 'region_5', 'cal_date', 'country_alpha2', 'platform_subset']
ns_records.groupby(ck).count().filter("count > 1").show()

# COMMAND ----------

# test 3 :: find any generics
ns_records.filter(ns_records.platform_subset.like('%GENERIC%')).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### stage.ib_staging

# COMMAND ----------

ib_stage = """
select ib.*
from stage.ib_staging as ib
join mdm.hardware_xref as hw 
    on hw.platform_subset = ib.platform_subset
where hw.technology in ('INK','LASER','PWA')
"""

# COMMAND ----------

ib_records = read_redshift_to_df(configs) \
  .option("query", ib_stage) \
  .load()

# COMMAND ----------

ib_records.show()

# COMMAND ----------

ib_records.count()

# COMMAND ----------

# test 1 :: isna() returns a dataframe of boolean - same columns, same number of records
ib_records.where('ib is null').collect()

# COMMAND ----------

# test 2 :: multiple CKs
# LF showing up; bring in record to the CK
ck = ['record', 'month_begin', 'country_alpha2', 'split_name', 'platform_subset']
ib_records.groupby(ck).count().filter("count > 1").show()

# COMMAND ----------

# check for PAAS printers
paas_stage = """
select ib.*
from stage.ib_staging as ib
join mdm.hardware_xref as hw 
    on hw.platform_subset = ib.platform_subset
where hw.technology in ('INK','LASER','PWA')
    and ib.platform_subset like '%PAAS%'
"""

paas_records = read_redshift_to_df(configs) \
  .option("query", paas_stage) \
  .load()

paas_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced tests on IB
# MAGIC 
# MAGIC List:
# MAGIC + base_013
# MAGIC + base_014

# COMMAND ----------

base_013 = """
SELECT *
FROM
     (
        SELECT prod_cal_date
            , prod_country
            , prod_platform_subset
            , prod_ce_split
            , cal_date AS stage_cal_date
            , country AS stage_country
            , platform_subset AS stage_platform_subset
            , customer_engagement AS stage_ce_split
            , stage_ib.ib AS stage_sum_ib
            , prod_ib.prod_ib AS prod_sum_ib
            , (stage_ib.ib - prod_ib.prod_ib) * 1.0 / NULLIF(prod_ib.prod_ib, 0) AS rate_change
        FROM
            (
                SELECT ib.cal_date AS prod_cal_date
                    , ib.country_alpha2 AS prod_country
                    , ib.platform_subset AS prod_platform_subset
                    , ib.customer_engagement AS prod_ce_split
                    , SUM(ib.units) AS prod_ib
                FROM prod.ib AS ib
                CROSS JOIN (SELECT DATEADD(MONTH, -1, MAX(cal_date)) as acts_max_cal_date
                            FROM prod.actuals_hw
                            WHERE version = (SELECT MAX(version) FROM prod.actuals_hw)
                            ) AS hw
                WHERE ib.version = '{}'
                    AND ib.measure = 'IB'
                    AND ib.cal_date <= hw.acts_max_cal_date
                GROUP BY ib.cal_date
                    , ib.country_alpha2
                    , ib.platform_subset
                    , ib.customer_engagement
            ) AS prod_ib
        JOIN
            (
                SELECT ib.month_begin AS cal_date
                    , ib.country_alpha2 AS country
                    , ib.platform_subset
                    , ib.split_name AS customer_engagement
                    , SUM(ib.ib) AS ib
                FROM stage.ib_staging AS ib
                WHERE ib.ib <> 0  -- same as promotion logic
                GROUP BY ib.month_begin
                    , ib.country_alpha2
                    , ib.platform_subset
                    , ib.split_name
            ) AS stage_ib
                ON UPPER(prod_ib.prod_cal_date) = UPPER(stage_ib.cal_date)
                AND UPPER(prod_ib.prod_country) = UPPER(stage_ib.country)
                AND UPPER(prod_ib.prod_platform_subset) = UPPER(stage_ib.platform_subset)
                AND UPPER(prod_ib.prod_ce_split) = UPPER(stage_ib.customer_engagement)
     ) AS test
WHERE ROUND(ABS(test.rate_change), 4) > 0.25
""".format(prev_version)

# COMMAND ----------

print(base_013)

# COMMAND ----------

test_13_records = read_redshift_to_df(configs) \
  .option("query", base_013) \
  .load()

# COMMAND ----------

test_13_records.count()

# COMMAND ----------

test_13_records.show()

# COMMAND ----------

base_014 = """
SELECT *
FROM
     (
        SELECT prod_cal_date
            , prod_country
            , prod_platform_subset
            , prod_ce_split
            , prod_ib
            , cal_date
            , country
            , platform_subset
            , customer_engagement
        FROM
            (
                SELECT ib.cal_date AS prod_cal_date
                    , ib.country_alpha2 AS prod_country
                    , ib.platform_subset AS prod_platform_subset
                    , ib.customer_engagement AS prod_ce_split
                    , SUM(ib.units) AS prod_ib
                FROM prod.ib AS ib
                CROSS JOIN (SELECT DATEADD(MONTH, -1, MAX(cal_date)) as acts_max_cal_date
                            FROM prod.actuals_hw
                            WHERE version = (SELECT MAX(version) FROM prod.actuals_hw)
                            ) AS hw
                WHERE ib.version = '{}'
                    AND ib.measure = 'IB'
                    AND ib.cal_date <= hw.acts_max_cal_date
                GROUP BY ib.cal_date
                    , ib.country_alpha2
                    , ib.platform_subset
                    , ib.customer_engagement
            ) AS prod_ib
        LEFT JOIN
            (
                SELECT ib.month_begin AS cal_date
                    , ib.country_alpha2 AS country
                    , ib.platform_subset
                    , ib.split_name AS customer_engagement
                    , SUM(ib.ib) AS ib
                FROM stage.ib_staging  AS ib
                WHERE ib.ib <> 0  -- same as promotion logic
                GROUP BY ib.month_begin
                    , ib.country_alpha2
                    , ib.platform_subset
                    , ib.split_name
            ) AS stage_ib
                ON prod_ib.prod_cal_date = stage_ib.cal_date
                AND UPPER(prod_ib.prod_country) = UPPER(stage_ib.country)
                AND UPPER(prod_ib.prod_platform_subset) = UPPER(stage_ib.platform_subset)
                AND UPPER(prod_ib.prod_ce_split) = UPPER(stage_ib.customer_engagement)
     ) AS test
WHERE test.cal_date IS NULL
""".format(prev_version)

# COMMAND ----------

test_14_records = read_redshift_to_df(configs) \
  .option("query", base_014) \
  .load()

# COMMAND ----------

test_14_records.count()

# COMMAND ----------

test_14_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Norm ships - v2v

# COMMAND ----------

ns_sql = """
SELECT 'stage' AS variable
    , ns.cal_date
    , SUM(ns.units) AS units
FROM stage.norm_ships AS ns
LEFT JOIN mdm.hardware_xref AS hw
    ON hw.platform_subset = ns.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'LASER', 'PWA')
    AND ns.cal_date between '2019-03-01' and '2026-10-01'
GROUP BY ns.cal_date

UNION ALL

SELECT 'prod' AS variable
    , ns.cal_date
    , SUM(ns.units) AS units
FROM prod.norm_shipments AS ns
LEFT JOIN mdm.hardware_xref AS hw
    ON hw.platform_subset = ns.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'LASER', 'PWA')
    AND ns.version = '{}'
    and ns.cal_date between '2019-03-01' and '2026-10-01'
GROUP BY ns.cal_date
ORDER BY 2
""".format(prev_version)


# COMMAND ----------

norm_ships_prod_df = read_redshift_to_df(configs) \
  .option("query", ns_sql) \
  .load()

# COMMAND ----------

ns_agg_prod_prep = norm_ships_prod_df.toPandas()
ns_agg = ns_agg_prod_prep.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

# https://plotly.com/python-api-reference/generated/plotly.express.line

fig = px.line(data_frame=ns_agg,
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
# MAGIC ### IB - v2v

# COMMAND ----------

ib_v2v_compare = """
with prod as
(
    SELECT ib.cal_date
        , ib.country_alpha2 AS country
        , ib.platform_subset
        , ib.customer_engagement
        , ib.version
        , SUM(ib.units) AS units
    FROM prod.ib AS ib
    WHERE ib.version = '{}'
        AND ib.measure = 'IB'
    GROUP BY ib.cal_date
        , ib.country_alpha2
        , ib.platform_subset
        , ib.customer_engagement
        , ib.version
)

, stage as
(
    SELECT ib.month_begin AS cal_date
        , ib.country_alpha2 AS country
        , ib.platform_subset
        , ib.split_name AS customer_engagement
        , cast('ib_staging' as varchar) AS version
        , SUM(ib.ib) AS ib
    FROM stage.ib_staging AS ib
    GROUP BY ib.month_begin
        , ib.country_alpha2
        , ib.platform_subset
        , ib.split_name
)
select cal_date
    , version as variable
    , sum(units) as units
from prod
where 1=1
    and cal_date BETWEEN '2013-11-01' AND '2025-10-01'
group by cal_date
    , version

union all

select cal_date
    , version as variable
    , sum(ib) as units
from stage
where 1=1
    and cal_date BETWEEN '2013-11-01' AND '2025-10-01'
group by cal_date
    , version
order by 1,2
""".format(prev_version)

# COMMAND ----------

ib_v2v_df = read_redshift_to_df(configs) \
  .option("query", ib_v2v_compare) \
  .load()

# COMMAND ----------

ib_agg_prod_prep = ib_v2v_df.toPandas()
ib_agg = ib_agg_prod_prep.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

fig = px.line(data_frame=ib_agg,
              x='cal_date',
              y='units',
              line_group='variable',
              color='variable',
              title='RS - IB v2v compare')

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
# MAGIC ### IB - laser v2v

# COMMAND ----------

v2v_laser = """
with prod as
(
    SELECT ib.cal_date
        , ib.country_alpha2 as country
        , ib.platform_subset
        , ib.customer_engagement
        , ib.version
        , SUM(ib.units) AS units
    FROM prod.ib AS ib
    WHERE ib.version = '{}'
        AND ib.measure = 'IB'
    GROUP BY ib.cal_date
        , ib.country_alpha2
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
        , cast('ib_staging' as varchar) AS version
        , SUM(ib.ib) AS ib
    FROM stage.ib_staging AS ib
    GROUP BY ib.month_begin
        , ib.country_alpha2
        , ib.platform_subset
        , ib.split_name
)
select cal_date
    , prod.version as variable
    , sum(units) as units
from prod
join mdm.hardware_xref as hw
    on hw.platform_subset = prod.platform_subset
where 1=1
    and hw.technology = 'LASER'
    and cal_date between '2017-03-01' and '2025-10-01'
group by cal_date
    , prod.version

union all

select cal_date
    , stage.version as variable
    , sum(ib) as units
from stage
join mdm.hardware_xref as hw
    on hw.platform_subset = stage.platform_subset
where 1=1
    and hw.technology = 'LASER'
    and cal_date between '2017-03-01' and '2025-10-01'
group by cal_date
    , stage.version
order by 1,2
""".format(prev_version)

# COMMAND ----------

ib_v2v_laser_df = read_redshift_to_df(configs) \
  .option("query", v2v_laser) \
  .load()

# COMMAND ----------

laser_prod_prep = ib_v2v_laser_df.toPandas()
laser_agg = laser_prod_prep.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

fig = px.line(data_frame=laser_agg,
              x='cal_date',
              y='units',
              line_group='variable',
              color='variable',
              title='RS - IB laser v2v compare')

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
# MAGIC ### IB - ink v2v

# COMMAND ----------

v2v_ink = """
with prod as
(
    SELECT ib.cal_date
        , ib.version
        , SUM(ib.units) AS units
    FROM prod.ib AS ib
    join mdm.hardware_xref as hw
        on hw.platform_subset = ib.platform_subset
    WHERE ib.version = '{}'
        AND ib.measure = 'IB'
        AND ib.cal_date BETWEEN '2013-11-01' AND '2025-10-01'
        and hw.technology = 'INK'
    GROUP BY ib.cal_date
        , ib.version
),
stage as
(
    SELECT ib.month_begin AS cal_date
        , cast('ib_staging' as varchar) AS version
        , SUM(ib.ib) AS ib
    FROM stage.ib_staging AS ib
    join mdm.hardware_xref as hw
        on hw.platform_subset = ib.platform_subset
    WHERE 1=1
        AND ib.month_begin BETWEEN '2013-11-01' AND '2025-10-01'
        and hw.technology = 'INK'
    GROUP BY ib.month_begin
)
select prod.cal_date
    , prod.version as variable
    , prod.units as units
from prod

union all

select stage.cal_date
    , stage.version as variable
    , stage.ib as units
from stage
order by 1,2
""".format(prev_version)

# COMMAND ----------

ib_v2v_ink_df = read_redshift_to_df(configs) \
  .option("query", v2v_ink) \
  .load()

# COMMAND ----------

ink_prod_prep = ib_v2v_ink_df.toPandas()
ink_agg = ink_prod_prep.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

fig = px.line(data_frame=ink_agg,
              x='cal_date',
              y='units',
              line_group='variable',
              color='variable',
              title='RS - IB ink v2v compare')

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
# MAGIC ### IB - iink v2v

# COMMAND ----------

v2v_iink = """
with prod as
(
    SELECT ib.cal_date
        , ib.version
        , SUM(ib.units) AS units
    FROM prod.ib AS ib
    WHERE ib.version = '{}'
        AND ib.measure = 'IB'
        AND ib.customer_engagement = 'I-INK'
    GROUP BY ib.cal_date
        , ib.version
)

, stage as
(
    SELECT ib.month_begin AS cal_date
        , cast('ib_staging' as varchar) AS version
        , SUM(ib.ib) AS ib
    FROM stage.ib_staging AS ib
    WHERE 1=1
        AND ib.split_name = 'I-INK'
    GROUP BY ib.month_begin
        , ib.split_name
)

select cal_date
    , prod.version as variable
    , sum(units) as ib
from prod
where 1=1
    and cal_date BETWEEN '2013-11-01' AND '2025-10-01'
group by cal_date
    , prod.version

union all

select cal_date
    , stage.version as variable
    , sum(ib) as ib
from stage
where 1=1
    and cal_date BETWEEN '2013-11-01' AND '2025-10-01'
group by cal_date
    , stage.version
order by 1,2
""".format(prev_version)

# COMMAND ----------

ib_v2v_iink_df = read_redshift_to_df(configs) \
  .option("query", v2v_iink) \
  .load()

# COMMAND ----------

iink_prod_prep = ib_v2v_iink_df.toPandas()
iink_agg = ink_prod_prep.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

fig = px.line(data_frame=iink_agg,
              x='cal_date',
              y='units',
              line_group='variable',
              color='variable',
              title='RS - IB iink v2v compare')

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
# MAGIC ### IB - trad v2v

# COMMAND ----------

v2v_trad = """
with prod as
(
    SELECT ib.cal_date
        , ib.version
        , SUM(ib.units) AS units
    FROM prod.ib AS ib
    join mdm.hardware_xref as hw
      on hw.platform_subset = ib.platform_subset
    WHERE ib.version = '{}'
        AND ib.measure = 'IB'
        AND ib.customer_engagement = 'TRAD'
        AND ib.cal_date BETWEEN '2013-11-01' AND '2025-10-01'
        and hw.technology = 'INK'
    GROUP BY ib.cal_date
        , ib.version
)

, stage as
(
    SELECT ib.month_begin AS cal_date
        , cast('ib_staging' as varchar) AS version
        , SUM(ib.ib) AS units
    FROM stage.ib_staging AS ib
    join mdm.hardware_xref as hw
      on hw.platform_subset = ib.platform_subset
    WHERE 1=1
        AND ib.split_name = 'TRAD'
        AND ib.month_begin BETWEEN '2013-11-01' AND '2025-10-01'
        and hw.technology = 'INK'
    GROUP BY ib.month_begin
        , ib.split_name
)
select cal_date
    , prod.version as variable
    , prod.units
from prod

union all

select cal_date
    , stage.version as variable
    , stage.units
from stage
order by 1,2
""".format(prev_version)

# COMMAND ----------

ib_v2v_trad_df = read_redshift_to_df(configs) \
  .option("query", v2v_trad) \
  .load()

# COMMAND ----------

trad_prod_prep = ib_v2v_trad_df.toPandas()
trad_agg = trad_prod_prep.reindex(['cal_date', 'variable', 'units'], axis=1)

# COMMAND ----------

trad_agg

# COMMAND ----------

fig = px.line(data_frame=trad_agg,
              x='cal_date',
              y='units',
              line_group='variable',
              color='variable',
              title='RS - IB trad/ink v2v compare')

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
