# Databricks notebook source
# MAGIC %md
# MAGIC # IB - Mar 23, 2023 - QC/QA
# MAGIC 
# MAGIC Brent Merrick
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

# MAGIC %run "../common/configs"

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# ns/ib versions - used to filter to the previous build's versions
prev_version = '2023.03.03.1'

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

# test 3 :: find any generics in norm_ships dataset
# anything showing up here is a full-stop....go back to HW team and have them allocated GENERICS to speed licenses
ns_records.filter(ns_records.platform_subset.like('%GENERIC%')).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### stage.ib_staging

# COMMAND ----------

# pull in ib_staging dataset and filter to INK, LASER, and PWA

ib_stage = """
select ib.*
from stage.ib_staging as ib
join mdm.hardware_xref as hw 
    on hw.platform_subset = ib.platform_subset
left join mdm.iso_country_code_xref as xref
    on ib.country_alpha2 = xref.country_alpha2
where hw.technology in ('INK','LASER','PWA')
    --and xref.embargoed_sanctioned_flag = 'N'
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
ck = ['record', 'month_begin', 'country_alpha2', 'split_name', 'platform_subset']
ib_records.groupby(ck).count().filter("count > 1").show()

# COMMAND ----------

# check for PAAS printers
paas_stage = """
select distinct ib.platform_subset
from stage.ib_staging as ib
join mdm.hardware_xref as hw 
    on hw.platform_subset = ib.platform_subset
where hw.technology in ('INK','LASER','PWA')
    and ib.platform_subset like '%PAAS%'
"""

paas_records = read_redshift_to_df(configs) \
  .option("query", paas_stage) \
  .load()

paas_records.display()

# COMMAND ----------

# Check for platform_subset / country_alpha2 combinations that are in stage.norm_ships that are NOT IN stage.ib_staging

missing_combos = """
select distinct
    a.platform_subset NS_platform_subset,
    a.country_alpha2,
    b.platform_subset STAGE_IB_platform_subset,
    c.technology
from stage.norm_ships a
    left join stage.ib_staging b on a.platform_subset=b.platform_subset and a.country_alpha2=b.country_alpha2
    left join mdm.hardware_xref c on a.platform_subset=c.platform_subset
where 1=1
    and b.platform_subset is null
    and c.technology <> 'LF'
order by 1
"""

missing_combos_records = read_redshift_to_df(configs) \
  .option("query", missing_combos) \
  .load()

# COMMAND ----------

missing_combos_records.count()

# COMMAND ----------

# records showing up here were in Norm Ships, but did not show up in IB
missing_combos_records.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced tests on IB
# MAGIC 
# MAGIC List:
# MAGIC + base_013
# MAGIC + base_014

# COMMAND ----------

# This test compares current combinations to previous IB and identifies any combination where the IB has changed by 25% or more
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

# print(base_013)

# COMMAND ----------

test_13_records = read_redshift_to_df(configs) \
  .option("query", base_013) \
  .load()

# COMMAND ----------

test_13_records.count()

# COMMAND ----------

# Any records showing up here have changed the IB from Previous version to current by more than 25%
test_13_records.show()

# COMMAND ----------

# Check for date mismatches between NS and IB

date_mismatches_query = """
with stage_ib as
     (SELECT geography,
             split_name,
             platform_subset,
             MIN(month_begin) AS month_begin
      FROM stage.ib_staging
      GROUP BY geography,
               split_name,
               platform_subset),
stage_ns AS
    (SELECT b.market10 AS geography,
            a.customer_engagement as split_name,
            a.platform_subset,
            MIN(a.cal_date) AS month_begin
     FROM stage.norm_shipments_ce a
              LEFT JOIN mdm.iso_country_code_xref b ON a.country_alpha2 = b.country_alpha2
     GROUP BY b.market10,
              a.customer_engagement,
              a.platform_subset)
Select
    a.geography,
    a.split_name,
    a.platform_subset,
    a.month_begin as ib_begin_date,
    b.month_begin as ns_begin_date,
    datediff(month, b.month_begin, a.month_begin)
from stage_ib a
    left join stage_ns b on a.geography=b.geography
        and a.split_name=b.split_name
        and a.platform_subset = b.platform_subset
WHERE a.month_begin-b.month_begin <> 0
order by 6
"""

date_mismatches_records = read_redshift_to_df(configs) \
  .option("query", date_mismatches_query) \
  .load()

# COMMAND ----------

date_mismatches_records.show()

# COMMAND ----------

# Check for mismatches between Enrollees and IB

enrollee_mismatches_query = """
WITH ib_units as
     (SELECT 
        month_begin,
        SUM(ib) AS units
      FROM stage.ib_staging
      WHERE 1 = 1
        AND split_name = 'I-INK'
        AND month_begin BETWEEN '2022-11-01' AND '2028-10-01'
      GROUP BY month_begin),
enrollee_units as
    (SELECT 
       cal_date,
       SUM(value) AS ltf_enrollees
     FROM prod.instant_ink_enrollees_ltf
     WHERE 1 = 1
       AND official = 1
       AND metric = 'CUMULATIVE'
       AND cal_date BETWEEN '2022-11-01' AND '2028-10-01'
     GROUP BY cal_date)
SELECT
    a.month_begin as cal_date,
    a.units as ib_units,
    b.ltf_enrollees as enrollee_units,
    a.units - b.ltf_enrollees as diff_units
FROM ib_units a left join enrollee_units b on a.month_begin = b.cal_date
ORDER BY 4 desc
"""

enrollee_mismatches_records = read_redshift_to_df(configs) \
  .option("query", enrollee_mismatches_query) \
  .load()

# COMMAND ----------

#enrollee_mismatches_records.head().style.format({"ib_units": "{:,.0f}", "enrollee_units": "{:,.0f}"})


enrollee_mismatches_records.display()

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
ORDER BY 2,1
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
    left join mdm.iso_country_code_xref as xref
        on ib.country_alpha2 = xref.country_alpha2
    WHERE 1=1
        --and xref.embargoed_sanctioned_flag = 'N'
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
    left join mdm.iso_country_code_xref as xref
        on ib.country_alpha2 = xref.country_alpha2
    WHERE 1=1
        --and xref.embargoed_sanctioned_flag = 'N'
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
# MAGIC ### IB - total ink v2v

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
    left join mdm.iso_country_code_xref as xref
        on ib.country_alpha2 = xref.country_alpha2
    WHERE 1=1
        AND ib.month_begin BETWEEN '2013-11-01' AND '2025-10-01'
        and hw.technology = 'INK'
        --and xref.embargoed_sanctioned_flag = 'N'
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
# MAGIC ### IB - INK -- i-ink v2v

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
    left join mdm.iso_country_code_xref as xref
        on ib.country_alpha2 = xref.country_alpha2
    WHERE 1=1
        AND ib.split_name = 'I-INK'
        --and xref.embargoed_sanctioned_flag = 'N'
    GROUP BY ib.month_begin
        , ib.split_name
)

select cal_date
    , prod.version as variable
    , sum(units) as units
from prod
where 1=1
    and cal_date BETWEEN '2013-11-01' AND '2025-10-01'
group by cal_date
    , prod.version

union all

select cal_date
    , stage.version as variable
    , sum(ib) as units
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
iink_agg = iink_prod_prep.reindex(['cal_date', 'variable', 'units'], axis=1)
# iink_agg.display()

# COMMAND ----------

fig = px.line(data_frame=iink_agg,
              x='cal_date',
              y='units',
              line_group='variable',
              color='variable',
              title='RS - IB I-INK v2v compare')

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
# MAGIC ### IB - INK -- trad v2v

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
    left join mdm.iso_country_code_xref as xref
      on ib.country_alpha2 = xref.country_alpha2
    WHERE 1=1
        AND ib.split_name = 'TRAD'
        AND ib.month_begin BETWEEN '2013-11-01' AND '2025-10-01'
        and hw.technology = 'INK'
        --and xref.embargoed_sanctioned_flag = 'N'
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
              title='RS - IB INK TRAD v2v compare')

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


