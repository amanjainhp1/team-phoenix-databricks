# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

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
    and c.technology in ('INK','LASER','PWA')
    and calendar_month > (select max(date) from stage.rtm_historical_actuals)
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
   'actuals_hw' as record,
   date as cal_date,
   geo as country_alpha2,
   b.platform_subset,
   c.technology,
   sum(units) as units,
   rtm,
   getdate() as load_date
from stage.rtm_historical_actuals a
    left join mdm.rdma b on a.base_prod_number=b.base_prod_number
    left join mdm.hardware_xref c on b.platform_subset=c.platform_subset
where 1=1
    and c.technology in ('INK','LASER','PWA')
    and a.date > '2017-10-01'
group by
    date,
    geo,
    b.platform_subset,
    c.technology,
    rtm
order by 2
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


# CREATE RTM lagged NS
ib_01_hw_decay_query = """

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

ib_01_hw_decay_records = read_redshift_to_df(configs) \
    .option("query", ib_01_hw_decay_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, ib_01_hw_decay_records, "stage.rtm_ib_01_hw_decay", "overwrite")

# COMMAND ----------

# CREATE RTM decayed dataset
rtm_ib_02_ce_splits_query = """
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
FROM "stage"."rtm_ib_01_hw_decay" AS hw_lag
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
    , CASE WHEN hw.printer_installs IS NULL THEN 0.0
           ELSE CAST(hw.printer_installs AS FLOAT) END AS printer_installs
    , ic.ib
FROM ib_10_hw_lag_w_remaining_amt AS ic
LEFT JOIN "stage"."rtm_ib_01_hw_decay" AS hw
    ON hw.year = ic.year
    AND hw.month = ic.month
    AND hw.country_alpha2 = ic.country_alpha2
    AND hw.hps_ops = ic.hps_ops
    AND (hw.split_name = ic.split_name OR ic.split_name IS NULL)
    AND hw.platform_subset = ic.platform_subset
    AND hw.printer_installs > 0
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

rtm_ib_02_ce_splits_records = read_redshift_to_df(configs) \
    .option("query", rtm_ib_02_ce_splits_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, rtm_ib_02_ce_splits_records, "stage.rtm_ib_02_ce_splits", "overwrite")

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
    , CASE WHEN pre.ib != 0 AND pre.ib < 1 THEN 1 
        ELSE pre.ib END AS ib
FROM stage.rtm_ib_02_ce_splits AS pre
LEFT JOIN mdm.iso_country_code_xref iso ON pre.country_alpha2=iso.country_alpha2
WHERE 1=1

"""

rtm_ib_staging_records = read_redshift_to_df(configs) \
    .option("query", rtm_ib_staging_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, rtm_ib_staging_records, "stage.rtm_ib_staging", "overwrite")
