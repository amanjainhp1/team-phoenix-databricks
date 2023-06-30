# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## NORM SHIPS

# COMMAND ----------

# create an allocated temp table for RTM historical actuals
# CREATE RTM RAW Norm_shipments dataset
rtm_ns_raw_temp_query = """
with rtm_actuals_denom as
(
    select
        b.platform_subset,
        a.geo,
        a.date,
        sum(a.units) as units
    from stage.rtm_historical_actuals a
        left join mdm.rdma b on a.base_prod_number=b.base_prod_number
        left join mdm.product_line_xref c on b.pl=c.pl
    where 1=1
        and a.rtm in ('DMPS','PMPS')
        and c.technology = 'LASER'
    GROUP BY b.platform_subset, a.geo, a.date
),

rtm_actuals_numerator as
(
    select
        b.platform_subset,
        a.geo,
        a.date,
        a.rtm,
        sum(a.units) as units
    from stage.rtm_historical_actuals a
        left join mdm.rdma b on a.base_prod_number=b.base_prod_number
        left join mdm.product_line_xref c on b.pl=c.pl
    where 1=1
        and a.rtm in ('DMPS','PMPS')
        and c.technology = 'LASER'
    GROUP BY b.platform_subset, a.geo, a.date, a.rtm
),

--used for when MPS > TRAD, later
rtm_split_pct as
(
    select
        a.platform_subset,
        a.geo,
        a.date,
        a.rtm,
        a.units,
        (a.units / sum(a.units) OVER (PARTITION BY a.platform_subset, a.geo, a.date)) AS pct
    from rtm_actuals_numerator a
        left join mdm.hardware_xref b on a.platform_subset=b.platform_subset
    where 1=1
        and a.rtm in ('DMPS','PMPS')
        and b.technology = 'LASER'
        and a.units > 0
    order by 1,2,3,4
),

phoenix_actuals as
(
    select
        a.platform_subset,
        a.country_alpha2,
        a.cal_date,
        sum(a.base_quantity) as units
    from prod.actuals_hw a left join mdm.hardware_xref b on a.platform_subset = b.platform_subset
    where 1=1
        and record = 'ACTUALS - HW'
        and b.technology = 'LASER'
        --and a.cal_date < '2023-02-01'
    GROUP BY a.platform_subset, a.country_alpha2, a.cal_date
    ORDER BY 1
),

mps_larger as
(
    --identify where MPS units > Phoenix total units
    select
        a.platform_subset,
        a.geo,
        a.date,
        a.units as rtm_units,
        b.units as phoenix_units,
        a.units - b.units as diff_units
    from rtm_actuals_denom a
        INNER JOIN phoenix_actuals b on a.platform_subset=b.platform_subset
            and a.geo=b.country_alpha2
            and a.date = b.cal_date
    where 1=1
        and a.units - b.units > 0
),

allocated_units as
(
    Select
        a.platform_subset,
        a.geo,
        a.date,
        b.rtm,
        a.phoenix_units,
        b.pct,
        (a.phoenix_units * b.pct) as allocated_units
    from mps_larger a
        left join rtm_split_pct b on a.platform_subset=b.platform_subset
            and a.geo = b.geo
            and a.date=b.date
    where 1=1
            and b.rtm IS NOT NULL
),
allocated_cleaned as
(
    --this is allocated and scaled mps units for products where the mps total > phoenix actuals
    select distinct
        platform_subset,
        geo,
        date,
        rtm,
        allocated_units
    from allocated_units
),

allocated_plus_trad as
(
    select
        platform_subset,
        geo,
        date,
        rtm,
        allocated_units
    from allocated_cleaned
    UNION ALL
    select
        platform_subset,
        geo,
        date,
        'TRAD' as rtm,
        1.0 as allocated_units
    from allocated_cleaned
),

mps_smaller as
(
    --identify where MPS units <= Phoenix total units (normal units)
    select
        a.platform_subset,
        a.geo,
        a.date,
        a.units as rtm_units,
        b.units as phoenix_units,
        a.units - b.units as diff_units,
        CASE WHEN b.units - a.units <= 1 THEN 1
            ELSE b.units - a.units
            END as trad_units
    from rtm_actuals_denom a
        INNER JOIN phoenix_actuals b on a.platform_subset=b.platform_subset
            and a.geo=b.country_alpha2
            and a.date = b.cal_date
    where 1=1
        and a.units - b.units < 0
),

trad_cleaned as
(
    select
        platform_subset,
        geo,
        date,
        'TRAD' as rtm,
        trad_units as units
    from mps_smaller
),

consolidated_mps as
(
    select
        platform_subset,
        geo as country_alpha2,
        date as cal_date,
        rtm,
        units
    from trad_cleaned  --these records have trad subtracted from MPS
    UNION ALL
    select
        a.platform_subset,
        a.geo as country_alpha2,
        a.date as cal_date,
        a.rtm,
        a.units
    from rtm_actuals_numerator a INNER JOIN trad_cleaned b  --these are mps actuals without the cleaned up trad records
        on a.platform_subset=b.platform_subset
        and a.geo=b.geo
        and a.date=b.date
    UNION ALL
    select
        platform_subset,
        geo as country_alpha2,
        date as cal_date,
        rtm,
        allocated_units as units
    from allocated_plus_trad -- these are records where the mps units have been scaled to align to phoenix actuals
)
--pull phoenix actuals that do not have a match on the MPS combinations
select
    'rtm_actuals' as record,
    a.cal_date,
    a.country_alpha2,
    a.platform_subset,
    c.technology,
    a.units,
    'TRAD' as rtm,
    getdate() as load_date
from phoenix_actuals a
    LEFT JOIN consolidated_mps b on a.platform_subset=b.platform_subset
        and a.country_alpha2 = b.country_alpha2
        and a.cal_date = b.cal_date
    LEFT JOIN mdm.hardware_xref c on a.platform_subset=c.platform_subset
WHERE b.platform_subset IS NULL
UNION ALL
--add back in the mps combinations
Select
    'rtm_actuals' as record,
    cal_date,
    country_alpha2,
    a.platform_subset,
    b.technology,
    units,
    rtm,
    getdate() as load_date
from consolidated_mps a LEFT JOIN mdm.hardware_xref b on a.platform_subset=b.platform_subset
"""


redshift_rtm_ns_raw_temp_records = read_redshift_to_df(configs) \
    .option("query", rtm_ns_raw_temp_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, redshift_rtm_ns_raw_temp_records, "stage.rtm_historical_actuals_temp", "overwrite")

# COMMAND ----------

# CREATE RTM RAW Norm_shipments dataset
rtm_ns_raw_query = """

--LTF Units
select
   record,
   calendar_month as cal_date,
   geo as country_alpha2,
   b.platform_subset,
   c.technology,
   sum(units) as units,
   rtm,
   getdate() as load_date
from stage.f_report_units a
    left join mdm.rdma b on a.base_prod_number=b.base_prod_number
    left join mdm.hardware_xref c on b.platform_subset=c.platform_subset
where 1=1
    and c.technology in ('LASER')
    and calendar_month > (select max(cal_date) from stage.rtm_historical_actuals_temp)
group by
    record,
    calendar_month,
    geo,
    b.platform_subset,
    c.technology,
    rtm

UNION ALL

--Actuals
select
   record,
   cal_date,
   country_alpha2,
   a.platform_subset,
   a.technology,
   sum(units) as units,
   rtm,
   getdate() as load_date
from stage.rtm_historical_actuals_temp a
LEFT JOIN mdm.hardware_xref b on a.platform_subset = b.platform_subset
where 1=1
    and a.technology in ('LASER')
    and b.pl NOT IN ('GW','LX')
group by
    record,
    cal_date,
    country_alpha2,
    a.platform_subset,
    a.technology,
    rtm
"""


redshift_rtm_ns_raw_records = read_redshift_to_df(configs) \
    .option("query", rtm_ns_raw_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, redshift_rtm_ns_raw_records, "stage.rtm_ns_raw_stage", "overwrite")

# COMMAND ----------

# CREATE RTM Norm_shipments with filtered and normalized data
rtm_ns_fixed_query = """

SELECT
    record,
    cal_date,
    country_alpha2,
    platform_subset,
    technology,
    sum(units) AS units,
    CASE
        WHEN rtm = 'CMPS_OTHER' THEN 'TRAD'
        WHEN rtm = 'CMPS' THEN 'TRAD'
        WHEN rtm = 'TRANSACTIONAL' THEN 'TRAD'
    ELSE rtm
    END AS rtm,
    load_date
FROM stage.rtm_ns_raw_stage
WHERE technology = 'LASER'
GROUP BY
    record,
    cal_date,
    country_alpha2,
    platform_subset,
    technology,
    CASE
        WHEN rtm = 'CMPS_OTHER' THEN 'TRAD'
        WHEN rtm = 'CMPS' THEN 'TRAD'
        WHEN rtm = 'TRANSACTIONAL' THEN 'TRAD'
    ELSE rtm
    END,
    load_date
"""

redshift_rtm_ns_fixed_records = read_redshift_to_df(configs) \
    .option("query", rtm_ns_fixed_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, redshift_rtm_ns_fixed_records, "stage.rtm_ns_stage", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAG

# COMMAND ----------


# CREATE RTM lagged NS
ib_01_hw_lagged_query = """

WITH iso AS
(
SELECT DISTINCT
    iso.country_alpha2,
    iso.region_5,
    iso.market10,
    CASE iso.developed_emerging
        WHEN 'DEVELOPED' THEN 'DM'
        ELSE 'EM'
    END as EM_DM,
    CONCAT(iso.market10, CONCAT('-', CASE iso.developed_emerging WHEN 'DEVELOPED' THEN 'DM' ELSE 'EM' END)) as market13
FROM mdm.iso_country_code_xref iso
WHERE CONCAT(iso.market10, CONCAT('-',CASE iso.developed_emerging WHEN 'DEVELOPED' THEN 'DM' ELSE 'EM' END)) NOT IN ('UK&I-EM','NORTHERN EUROPE-EM', 'SOUTHERN EUROPE-EM','WORLD WIDE-EM')
)

SELECT
    CAST(DATEPART(year, ucep.cal_date) AS INTEGER) + (CAST(DATEPART(month, ucep.cal_date) AS INTEGER) + pl.month_lag - 1) / 12 AS year
    , ((CAST(DATEPART(month, ucep.cal_date) AS INTEGER) - 1 + pl.month_lag) % 12) + 1 AS month
    , ucep.cal_date as month_begin
    , iso_xref.region_5
    , iso_xref.market10
    , ucep.country_alpha2
    , iso.market13
    , xref.business_feature as hps_ops
    , ucep.rtm as split_name
    , ucep.platform_subset
    , SUM(ucep.units * pl.value) AS printer_installs
FROM stage.rtm_ns_stage AS ucep
LEFT JOIN mdm.iso_country_code_xref iso_xref on ucep.country_alpha2=iso_xref.country_alpha2
LEFT JOIN mdm.hardware_xref xref on ucep.platform_subset=xref.platform_subset
JOIN "mdm"."printer_lag" AS pl  -- implicit cross join (or cartesian product)
    ON iso_xref.region_5 = pl.region_5
    AND xref.business_feature = pl.hps_ops
    AND pl.split_name = 'TRAD'
    AND pl.pre_post_flag = 'PRE'
JOIN iso iso on ucep.country_alpha2=iso.country_alpha2
WHERE 1=1
    --AND ucep.country_alpha2 = 'CA'
    --AND ucep.platform_subset = 'COMET'
GROUP BY CAST(DATEPART(year, ucep.cal_date) AS INTEGER) + (CAST(DATEPART(month, ucep.cal_date) AS INTEGER) + pl.month_lag - 1) / 12
    , ((CAST(DATEPART(month, ucep.cal_date) AS INTEGER) - 1 + pl.month_lag) % 12) + 1
    , ucep.cal_date
    , iso_xref.region_5
    , iso_xref.market10
    , ucep.country_alpha2
    , iso.market13
    , xref.business_feature
    , ucep.rtm
    , ucep.platform_subset

"""

ib_01_hw_lagged_records = read_redshift_to_df(configs) \
    .option("query", ib_01_hw_lagged_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, ib_01_hw_lagged_records, "stage.rtm_ib_hw_lagged", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DECAY

# COMMAND ----------

# CREATE RTM decayed dataset
rtm_ib_decayed_query = """
with ib_07_years as (
SELECT 1 AS year_num UNION ALL
SELECT 2 AS year_num UNION ALL
SELECT 3 AS year_num UNION ALL
SELECT 4 AS year_num UNION ALL
SELECT 5 AS year_num UNION ALL
SELECT 6 AS year_num UNION ALL
SELECT 7 AS year_num UNION ALL
SELECT 8 AS year_num UNION ALL
SELECT 9 AS year_num UNION ALL
SELECT 10 AS year_num UNION ALL
SELECT 11 AS year_num UNION ALL
SELECT 12 AS year_num UNION ALL
SELECT 13 AS year_num UNION ALL
SELECT 14 AS year_num UNION ALL
SELECT 15 AS year_num UNION ALL
SELECT 16 AS year_num UNION ALL
SELECT 17 AS year_num UNION ALL
SELECT 18 AS year_num UNION ALL
SELECT 19 AS year_num UNION ALL
SELECT 20 AS year_num UNION ALL
SELECT 21 AS year_num UNION ALL
SELECT 22 AS year_num UNION ALL
SELECT 23 AS year_num UNION ALL
SELECT 24 AS year_num UNION ALL
SELECT 25 AS year_num UNION ALL
SELECT 26 AS year_num UNION ALL
SELECT 27 AS year_num UNION ALL
SELECT 28 AS year_num UNION ALL
SELECT 29 AS year_num UNION ALL
SELECT 30 AS year_num
),

ib_06_months as (
SELECT 1 AS month_num UNION ALL
SELECT 2 AS month_num UNION ALL
SELECT 3 AS month_num UNION ALL
SELECT 4 AS month_num UNION ALL
SELECT 5 AS month_num UNION ALL
SELECT 6 AS month_num UNION ALL
SELECT 7 AS month_num UNION ALL
SELECT 8 AS month_num UNION ALL
SELECT 9 AS month_num UNION ALL
SELECT 10 AS month_num UNION ALL
SELECT 11 AS month_num UNION ALL
SELECT 12 AS month_num
),

ib_08_decay_months as (
SELECT d.geography_grain
    , d.geography
    , d.platform_subset
    , d.split_name
    , (y.year_num - 1) * 12 + m.month_num - 1 AS month_offset
    , d.value / 12 AS decayed_amt
FROM "prod"."decay_m13" AS d
JOIN ib_07_years AS y
    ON 'YEAR_' + CAST(y.year_num AS VARCHAR) = d.year
JOIN ib_06_months AS m
    ON 1=1
WHERE 1=1
    AND d.official = 1
    AND d.record IN ('HW_DECAY')
    AND d.split_name = 'TRAD'
),

ib_09_remaining_amt as (
SELECT geography_grain
    , geography
    , platform_subset
    , split_name
    , month_offset
    , CASE WHEN 1 - SUM(decayed_amt) OVER (PARTITION BY platform_subset, geography, split_name ORDER BY month_offset ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) > 0
           THEN 1 - SUM(decayed_amt) OVER (PARTITION BY platform_subset, geography, split_name ORDER BY month_offset ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
           ELSE 0 END AS remaining_amt
FROM ib_08_decay_months AS decay
WHERE NOT decayed_amt IS NULL  -- remove any nulls
),

ib_10_hw_lag_w_remaining_amt as (
SELECT hw_lag.year + CAST((hw_lag.month + amt.month_offset - 1) AS INTEGER) / 12 AS year
    , (hw_lag.month + amt.month_offset - 1) % 12 + 1 AS month
    , hw_lag.hps_ops
    , hw_lag.split_name
    , hw_lag.region_5
    , hw_lag.market10
    , hw_lag.market13
    , hw_lag.country_alpha2
    , hw_lag.platform_subset
    , SUM(amt.remaining_amt * hw_lag.printer_installs) AS ib
FROM "stage"."rtm_ib_hw_lagged" AS hw_lag
JOIN ib_09_remaining_amt AS amt
    ON hw_lag.platform_subset = amt.platform_subset
    AND hw_lag.market13 = amt.geography
    --AND hw_lag.split_name = amt.split_name
GROUP BY hw_lag.year + CAST((hw_lag.month + amt.month_offset - 1) AS INTEGER) / 12
    , (hw_lag.month + amt.month_offset - 1) % 12 + 1
    , hw_lag.hps_ops
    , hw_lag.split_name
    , hw_lag.region_5
    , hw_lag.market10
    , hw_lag.market13
    , hw_lag.country_alpha2
    , hw_lag.platform_subset
),

ib_11_prelim_output as (
SELECT to_date(cast(ic.month as varchar) + '/' + '01' + '/' + cast(ic.year as varchar), 'mm/dd/yyyy') as month_begin
    , ic.region_5
    , ic.country_alpha2
    , ic.hps_ops
    , ic.split_name
    , ic.platform_subset
    , SUM(CASE
        WHEN hw.printer_installs IS NULL THEN 0.0
        ELSE CAST(hw.printer_installs AS FLOAT)
        END) AS printer_installs
    , ic.ib
FROM ib_10_hw_lag_w_remaining_amt AS ic
LEFT JOIN "stage"."rtm_ib_hw_lagged" AS hw
    ON hw.year = ic.year
    AND hw.month = ic.month
    AND hw.country_alpha2 = ic.country_alpha2
    AND hw.hps_ops = ic.hps_ops
    AND (hw.split_name = ic.split_name OR ic.split_name IS NULL)
    AND hw.platform_subset = ic.platform_subset
    AND hw.printer_installs > 0
GROUP BY
    to_date(cast(ic.month as varchar) + '/' + '01' + '/' + cast(ic.year as varchar), 'mm/dd/yyyy')
    , ic.region_5
    , ic.country_alpha2
    , ic.hps_ops
    , ic.split_name
    , ic.platform_subset
    , ic.ib
)

SELECT
       month_begin,
       region_5,
       country_alpha2,
       hps_ops,
       split_name,
       platform_subset,
       printer_installs,
       ib
FROM ib_11_prelim_output
"""

rtm_ib_decayed_records = read_redshift_to_df(configs) \
    .option("query", rtm_ib_decayed_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, rtm_ib_decayed_records, "stage.rtm_ib_decayed", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## IB STAGING

# COMMAND ----------

# CREATE RTM ib_staging dataset
rtm_ib_staging_query = """
SELECT 'IB' AS record
    , 1 AS version  -- used for scenarios
    , GETDATE() as load_date
    , pre.month_begin
    , 'MARKET10' as geography_grain
    , iso.market10 as geography
    , pre.country_alpha2
    , pre.hps_ops
    , pre.split_name
    , pre.platform_subset
    , pre.printer_installs
    , CASE WHEN pre.ib < 0 THEN .01 
        ELSE pre.ib END AS ib
FROM stage.rtm_ib_decayed AS pre
LEFT JOIN mdm.iso_country_code_xref iso ON pre.country_alpha2=iso.country_alpha2
WHERE 1=1

"""

rtm_ib_staging_records = read_redshift_to_df(configs) \
    .option("query", rtm_ib_staging_query) \
    .load()

# COMMAND ----------

# create two copies of the ib_staging data for auditing purposes, this table is the original
write_df_to_redshift(configs, rtm_ib_staging_records, "stage.rtm_ib_pre_staging", "overwrite")

# COMMAND ----------

# create two copies of the ib_staging data for auditing purposes, this table will be updated from OZZY mps data with MAI uplift units
write_df_to_redshift(configs, rtm_ib_staging_records, "stage.rtm_ib_staging", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ACTUALS

# COMMAND ----------

# ACTUALS AREA

# update RTM historical actuals dataset with MAI from ozzy
rtm_mps_uplift_query = """
--pull IB units from Phoenix (via Ozzy)
WITH ozzy_ib_units as
(
    select
        'ozzy' as record,
        cal_date,
        platform_subset,
        a.country_alpha2,
        business_type_grp as split_name,
        sum(device_op_ib_total) as ib_units
    from prod.ib_mps a INNER JOIN mdm.iso_country_code_xref b on a.country_alpha2=b.country_alpha2
    where 1=1
        and business_type_grp in ('DMPS','PMPS')
        and is_this_a_printer = 'YES'
        and category_grp = 'LASER'
        and cal_date >= (SELECT MIN(month_begin) FROM stage.rtm_ib_staging WHERE split_name in ('DMPS','PMPS'))
    GROUP BY cal_date, platform_subset, a.country_alpha2, business_type_grp
),

--pull calculated RTM IB units from Phoenix
phoenix_rtm_units as
(
    Select
        'phoenix' as record,
        month_begin as cal_date,
        a.platform_subset,
        country_alpha2,
        split_name,
        sum(ib) as ib_units
    from stage.rtm_ib_staging a left join mdm.hardware_xref b on a.platform_subset=b.platform_subset
    where 1=1
        and split_name IN ('DMPS','PMPS')
        and b.technology = 'LASER'
    GROUP BY  month_begin, a.platform_subset, country_alpha2, split_name
),

extra_ozzy_units as
(
--Calculate how many extra units are in Ozzy IB compared to phoenix calculated RTM IB
    select
        a.cal_date,
        a.country_alpha2,
        a.platform_subset,
        a.split_name,
        coalesce(b.ib_units,a.ib_units)-a.ib_units as diff
    from phoenix_rtm_units a left join ozzy_ib_units b on
        a.cal_date=b.cal_date
        and a.country_alpha2=b.country_alpha2
        and a.split_name = b.split_name
        and a.platform_subset=b.platform_subset
    where 1=1
        and a.cal_date >= (SELECT MIN(cal_date) FROM ozzy_ib_units)
        and coalesce(b.ib_units,a.ib_units)-a.ib_units <> 0
),

-----==============================================================================================
--create the split percentages
rtm_splits AS
(
    --rtm_splits numerator
    select
        country_alpha2,
        month_begin,
        split_name,
        platform_subset,
        sum(ib) as numerator
    from stage.rtm_ib_staging
    where 1=1
        and split_name in ('DMPS','PMPS')
        and ib <> 0
    group by country_alpha2, month_begin, split_name, platform_subset
),

rtm_splits_totals as
(
    --rtm_splits denominator
    select
        country_alpha2,
        month_begin,
        platform_subset,
        sum(ib) as denominator
    from stage.rtm_ib_staging
    where 1=1
        and split_name in ('DMPS','PMPS')
    group by country_alpha2, month_begin, platform_subset
),

mix_pct as
(
    select
        a.country_alpha2,
        a.month_begin,
        a.split_name,
        a.platform_subset,
        a.numerator / b.denominator as split_pct
    from rtm_splits a INNER JOIN rtm_splits_totals b on
        a.country_alpha2=b.country_alpha2
        and a.month_begin=b.month_begin
        and a.platform_subset=b.platform_subset
    WHERE 1=1
)

--calculate the extra mps units to allocate to existing RTM IB
SELECT
    a.country_alpha2,
    month_begin,
    a.split_name,
    a.platform_subset,
    split_pct,
    split_pct*b.diff as new_mps_units
FROM mix_pct a INNER JOIN extra_ozzy_units b
    on a.month_begin = b.cal_date
    and a.split_name=b.split_name
    and a.country_alpha2 = b.country_alpha2
    and a.platform_subset = b.platform_subset
where 1=1

"""

rtm_mps_uplift_records = read_redshift_to_df(configs) \
    .option("query", rtm_mps_uplift_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, rtm_mps_uplift_records, "stage.rtm_ib_mps_uplift_staging", "overwrite")

# COMMAND ----------

# update rtm ib data with finalized ozzy mps ib data
rtm_mps_update_query=f"""
UPDATE stage.rtm_ib_staging a
SET ib = a.ib + b.new_mps_units
FROM stage.rtm_ib_mps_uplift_staging b
WHERE 1=1
    and a.platform_subset = b.platform_subset
    and a.country_alpha2 = b.country_alpha2
    and a.split_name = b.split_name
    and a.month_begin = b.month_begin
"""

submit_remote_query(configs, rtm_mps_update_query) 

# COMMAND ----------

# drop TRAD units
rtm_trad_uplift_query=f"""
with uplift_totals as
(
    select
        country_alpha2,
        month_begin,
        platform_subset,
        sum(new_mps_units) as new_mps_units
    from stage.rtm_ib_mps_uplift_staging
    GROUP BY country_alpha2, month_begin, platform_subset
)
select
    a.country_alpha2,
    a.month_begin,
    a.split_name,
    a.platform_subset,
    ib - b.new_mps_units as ib
from stage.rtm_ib_staging a inner join uplift_totals b
    on a.country_alpha2=b.country_alpha2
    and a.platform_subset = b.platform_subset
    and a.month_begin=b.month_begin
where 1=1
    and split_name = 'TRAD'
"""

rtm_trad_uplift_records = read_redshift_to_df(configs) \
    .option("query", rtm_trad_uplift_query) \
    .load()


# COMMAND ----------

write_df_to_redshift(configs, rtm_trad_uplift_records, "stage.rtm_ib_trad_uplift_staging", "overwrite")

# COMMAND ----------

# update rtm ib data
rtm_trad_update_query=f"""
UPDATE stage.rtm_ib_staging a
SET ib = b.ib
FROM stage.rtm_ib_trad_uplift_staging b
WHERE 1=1
    and a.platform_subset = b.platform_subset
    and a.country_alpha2 = b.country_alpha2
    and a.split_name = 'TRAD'
    and a.month_begin = b.month_begin
"""

submit_remote_query(configs, rtm_trad_update_query) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## FORECAST

# COMMAND ----------

# FORECAST AREA

# create % to uplift forecast
rtm_forecast_mps_uplift_pct_query=f"""
with vars as
(
    select distinct
        'date_var' as record,
        cal_date as max_date,
        trunc(add_months(cal_date,1)) as min_rtm_date,
        trunc(add_months(cal_date,-11)) as look_back_date
    from prod.ib_mps
    WHERE 1=1
        and cal_date = (SELECT MAX(cal_date) from prod.ib_mps WHERE business_type_grp in ('DMPS','PMPS') and is_this_a_printer = 'YES' and category_grp = 'LASER')
),

ozzy_totals as
(
    select
        b.market10,
        business_type_grp as split_name,
        sum(device_op_ib_total)/12 as ozzy_ib_units
    from prod.ib_mps a
        INNER JOIN mdm.iso_country_code_xref b on a.country_alpha2=b.country_alpha2
        INNER JOIN vars v on v.record='date_var'
    where 1=1
        and business_type_grp in ('DMPS','PMPS')
        and is_this_a_printer = 'YES'
        and category_grp = 'LASER'
        and cal_date between v.look_back_date and v.max_date
        and device_op_ib_total > 0
    GROUP BY b.market10, business_type_grp
),

rtm_ib_totals as
(
    select
        c.market10,
        a.split_name,
        sum(ib)/12 as rtm_ib_units
    from stage.rtm_ib_pre_staging a
        left join mdm.hardware_xref b on a.platform_subset=b.platform_subset
        left join mdm.iso_country_code_xref c on a.country_alpha2=c.country_alpha2
        INNER JOIN vars v on v.record = 'date_var'
    where 1=1
        and b.technology = 'LASER'
        and a.month_begin between v.look_back_date and v.max_date
        and a.split_name <> 'TRAD'
    GROUP BY c.market10, a.split_name
)

SELECT
    a.market10,
    a.split_name,
    rtm_ib_units,
    ozzy_ib_units,
    ozzy_ib_units - rtm_ib_units as unit_diff,
    rtm_ib_units / ozzy_ib_units as mix_pct
FROM rtm_ib_totals a INNER JOIN ozzy_totals b ON
    a.market10=b.market10
    and a.split_name = b.split_name
ORDER BY 1,2
"""

rtm_forecast_mps_uplift_pct_records = read_redshift_to_df(configs) \
    .option("query", rtm_forecast_mps_uplift_pct_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, rtm_forecast_mps_uplift_pct_records, "stage.rtm_forecast_mps_uplift_pct_staging", "overwrite")

# COMMAND ----------


# calculate units to uplift forecast and remove from TRAD
rtm_forecast_mps_uplift_query=f"""

--blow out Market10 uplift percent to country_alpha2
with market10_to_country_alpha2 as
(
    select DISTINCT
       a.market10,
       b.country_alpha2,
       a.split_name,
       a.mix_pct
    from stage.rtm_forecast_mps_uplift_pct_staging a inner join mdm.iso_country_code_xref b on a.market10=b.market10
)

select
    a.month_begin,
    a.country_alpha2,
    a.split_name,
    a.platform_subset,
    a.ib,
    b.mix_pct,
    a.ib/coalesce(b.mix_pct,1) as new_ib,
    a.ib/coalesce(b.mix_pct,1) - a.ib as units_remove_from_trad
from stage.rtm_ib_staging a left join market10_to_country_alpha2 b on
        a.country_alpha2=b.country_alpha2
        and a.split_name = b.split_name
where 1=1
    and a.split_name <> 'TRAD'
order by platform_subset, month_begin, split_name
"""

rtm_forecast_mps_uplift_records = read_redshift_to_df(configs) \
    .option("query", rtm_forecast_mps_uplift_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, rtm_forecast_mps_uplift_records, "stage.rtm_forecast_mps_uplift_staging", "overwrite")

# COMMAND ----------

#update mps forecast records
rtm_mps_forecast_update_query=f"""
UPDATE stage.rtm_ib_staging a
SET ib = b.new_ib
FROM stage.rtm_forecast_mps_uplift_staging b
WHERE 1=1
    and a.platform_subset = b.platform_subset
    and a.country_alpha2 = b.country_alpha2
    -- and a.split_name <> 'TRAD'
    and a.split_name = b.split_name
    and a.month_begin = b.month_begin
    and a.month_begin > (SELECT MAX(cal_date) from prod.ib_mps WHERE business_type_grp in ('DMPS','PMPS') and is_this_a_printer = 'YES' and category_grp = 'LASER')

"""

submit_remote_query(configs, rtm_mps_forecast_update_query) 

# COMMAND ----------

# update trad forecast records
rtm_trad_forecast_update_query=f"""
UPDATE stage.rtm_ib_staging a
SET ib = ib-b.units
FROM (  
    SELECT
        month_begin,
        country_alpha2,
        platform_subset,
        sum(units_remove_from_trad) as units
    FROM stage.rtm_forecast_mps_uplift_staging
    GROUP BY  month_begin, country_alpha2, platform_subset) b
WHERE 1=1
    and a.platform_subset = b.platform_subset
    and a.country_alpha2 = b.country_alpha2
    and a.split_name = 'TRAD'
    and a.month_begin = b.month_begin
    and a.month_begin > (SELECT MAX(cal_date) from prod.ib_mps WHERE business_type_grp in ('DMPS','PMPS') and is_this_a_printer = 'YES' and category_grp = 'LASER')
"""

submit_remote_query(configs, rtm_trad_forecast_update_query) 
