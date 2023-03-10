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
    and a.date < (select min(calendar_month) from stage.f_report_units)
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
