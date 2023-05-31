# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

query = """
-- Pull IB TRAD 
with trad as (
SELECT record
    , ib.version::varchar
    , ib.load_date::date
    , month_begin::date
    , geography_grain
    , geography
    , country_alpha2
    , hps_ops
    , hw.technology
    , split_name
    , ib.platform_subset
    , printer_installs
    , ib
FROM "stage"."rtm_ib_staging" AS ib
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = ib.platform_subset
WHERE 1=1
    AND ib.split_name = 'TRAD'
)
-- Pull STF LTF data
, stf_ltf_union as (

    SELECT 'IB' AS record
    , cast(null as varchar(64)) AS version
    , cast(null as date) as load_date
    , ites.calendar_month_date::date as month_begin
    , 'MARKET10' AS geography_grain
    , ites.market10 AS geography
    , country as country_alpha2
    , CASE WHEN hw.business_feature IS NULL THEN 'OTHER' ELSE hw.business_feature END AS hps_ops
    , hw.technology
    , 'I_TONER' AS split_name
    , ites.platform_subset
    , 0 AS printer_installs
    , COALESCE(cumulative_enrollees,0) AS ib
FROM "stage"."instant_toner_enrollees_stf" AS ites
JOIN "mdm"."hardware_xref" AS hw
    ON ites.platform_subset = hw.platform_subset
WHERE 1=1
    AND hw.technology IN ('LASER')
UNION ALL
  SELECT 'IB' AS record
    , cast(null as varchar(64)) AS version
    , cast(null as date) as load_date
    , itel.calendar_month_date::date as month_begin
    , 'MARKET10' AS geography_grain
    , itel.market10 AS geography
    , country as country_alpha2
    , CASE WHEN hw.business_feature IS NULL THEN 'OTHER' ELSE hw.business_feature END AS hps_ops
    , hw.technology
    , 'I_TONER' AS split_name
    , itel.platform_subset
    , 0 AS printer_installs
    , COALESCE(cumulative_enrollees,0) AS ib
FROM "stage"."instant_toner_enrollees_ltf" AS itel
JOIN "mdm"."hardware_xref" AS hw
    ON itel.platform_subset = hw.platform_subset
WHERE itel.calendar_month_date::date > (SELECT MAX(calendar_month_date::date) FROM stage.instant_toner_enrollees_stf)
    AND hw.technology IN ('LASER')
)

, trad_ib as (
   SELECT trad.record
    , trad.version
    , trad.load_date
    , trad.month_begin
    , trad.geography_grain
    , trad.geography
    , trad.country_alpha2
    , trad.hps_ops
    , trad.technology
    , trad.split_name
    , trad.platform_subset
    , trad.printer_installs
    , trad.ib - COALESCE(itp.ib ,0) as ib
FROM trad trad
LEFT JOIN stf_ltf_union itp 
    ON trad.month_begin = itp.month_begin
    AND trad.country_alpha2 = itp.country_alpha2
    AND trad.platform_subset = itp.platform_subset
)
-- combine TRAD and I_TONER
, final as  (
select * from stf_ltf_union
union all 
select * from trad_ib
)
select * from final"""

# COMMAND ----------


redshift_rtm_ns_raw_temp_records = read_redshift_to_df(configs) \
    .option("query", query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs, redshift_rtm_ns_raw_temp_records, "stage.rtm_itp_ib_staging", "overwrite")
