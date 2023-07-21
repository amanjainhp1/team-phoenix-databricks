# Databricks notebook source
# MAGIC %md
# MAGIC # Normalized Shipments

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

norm_ships_inputs = """

with nrm_01_filter_vars as (


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
)SELECT 'NORM_SHIPMENTS_STAGING' AS tbl_name
    , CASE WHEN record IN  ('HW_LF_FCST') THEN forecast_name ELSE record END AS record
    , max_version AS version
    , GETDATE() AS execute_time
FROM nrm_01_filter_vars
"""

query_list.append(["stage.norm_ships_inputs_lf", norm_ships_inputs, "overwrite"])

# COMMAND ----------

norm_ships = """
--hardware actuals
with nrm_02_hw_acts as (
SELECT ref.region_5
    , act.record
    , act.cal_date
    , act.country_alpha2
    , act.platform_subset
    , SUM(act.base_quantity) AS units  -- base_prod_number to platform_subset
FROM "prod"."actuals_hw" AS act
JOIN "mdm"."iso_country_code_xref" AS ref
    ON act.country_alpha2 = ref.country_alpha2
JOIN "mdm".hardware_xref hw
    ON hw.platform_subset = act.platform_subset
WHERE act.record IN ('ACTUALS_LF')
      AND hw.technology = 'LF'
      AND act.official = 1
GROUP BY ref.region_5
    , act.record
    , act.cal_date
    , act.country_alpha2
    , act.platform_subset

UNION ALL

SELECT ref.region_5
    , act.record
    , act.cal_date
    , act.country_alpha2
    , act.platform_subset
    , SUM(act.base_quantity) AS units  -- base_prod_number to platform_subset
FROM "prod"."actuals_hw" AS act
JOIN "mdm"."iso_country_code_xref" AS ref
    ON act.country_alpha2 = ref.country_alpha2
JOIN "mdm".hardware_xref hw
    ON hw.platform_subset = act.platform_subset
WHERE act.record IN ('ACTUALS - HW')
      AND hw.technology in ('INK','PWA','LASER')
      AND act.official = 1
GROUP BY ref.region_5
    , act.record
    , act.cal_date
    , act.country_alpha2
    , act.platform_subset
),

--hardware STF
nrm_03_hw_stf_forecast as (
SELECT ref.region_5
    , ltf.record
    , ltf.cal_date
    , ltf.country_alpha2
    , ltf.platform_subset
    , SUM(ltf.units) AS units
FROM "prod"."hardware_ltf" AS ltf
JOIN stage.norm_ships_inputs_lf vars
    ON vars.version = ltf.version
    AND vars.record = ltf.record
JOIN "mdm"."iso_country_code_xref" AS ref
    ON ltf.country_alpha2 = ref.country_alpha2
WHERE vars.record = 'HW_STF_FCST'
GROUP BY ref.region_5
    , ltf.record
    , ltf.cal_date
    , ltf.country_alpha2
    , ltf.platform_subset
),

--hardware LTF Union LF LTF
nrm_04_hw_ltf_forecast as (
SELECT ref.region_5
    , ltf.record
    , ltf.cal_date
    , ltf.country_alpha2
    , ltf.platform_subset
    , SUM(ltf.units) AS units
FROM "prod"."hardware_ltf" AS ltf
JOIN stage.norm_ships_inputs_lf vars
    ON vars.version = ltf.version
    AND vars.record = ltf.record
JOIN "mdm"."iso_country_code_xref" AS ref
    ON ltf.country_alpha2 = ref.country_alpha2
WHERE vars.record = 'HW_FCST'
GROUP BY ref.region_5
    , ltf.record
    , ltf.cal_date
    , ltf.country_alpha2
    , ltf.platform_subset

UNION ALL

SELECT ref.region_5
    , ltf.record
    , ltf.cal_date
    , ltf.country_alpha2
    , ltf.platform_subset
    , SUM(ltf.units) AS units
FROM "prod"."hardware_ltf" AS ltf
JOIN stage.norm_ships_inputs_lf vars
    ON vars.version = ltf.version
    AND vars.record = ltf.record
JOIN "mdm"."iso_country_code_xref" AS ref
    ON ltf.country_alpha2 = ref.country_alpha2
WHERE vars.record = 'HW_LTF_LF'
GROUP BY ref.region_5
    , ltf.record
    , ltf.cal_date
    , ltf.country_alpha2
    , ltf.platform_subset
),

nrm_05_combined_ships as (
SELECT region_5
    , record
    , country_alpha2
    , platform_subset
    , cal_date
    , units
FROM nrm_02_hw_acts --hw actuals + lf actuals

UNION ALL

SELECT region_5
    , record
    , country_alpha2
    , platform_subset
    , cal_date
    , units
FROM nrm_03_hw_stf_forecast --STF

UNION ALL

SELECT region_5
    , record
    , country_alpha2
    , platform_subset
    , cal_date
    , units
FROM nrm_04_hw_ltf_forecast --LTF + LF LTF
),

--get min and max dates for each record (actuals, stf, ltf)
nrm_06_printer_month_filters as (
SELECT record
    , MIN(cal_date) AS min_cal_date
    , MAX(cal_date) AS max_cal_date
FROM nrm_05_combined_ships
GROUP BY record
),

--prepare to stitch the different record-sets together
nrm_07_printer_dates as (
SELECT MAX(CASE WHEN record = 'ACTUALS - HW' THEN min_cal_date ELSE NULL END) AS act_min_cal_date
    , MAX(CASE WHEN record = 'ACTUALS - HW' THEN max_cal_date ELSE NULL END) AS act_max_cal_date
    , MAX(CASE WHEN record = 'HW_STF_FCST' THEN min_cal_date ELSE NULL END) AS stf_min_cal_date
    , MAX(CASE WHEN record = 'HW_STF_FCST' THEN max_cal_date ELSE NULL END) AS stf_max_cal_date
    , MAX(CASE WHEN record = 'HW_FCST' THEN min_cal_date ELSE NULL END) AS ltf_min_cal_date
    , MAX(CASE WHEN record = 'HW_FCST' THEN max_cal_date ELSE NULL END) AS ltf_max_cal_date
    , MAX(CASE WHEN record = 'HW_LTF_LF' THEN min_cal_date ELSE NULL END) AS ltf_min_cal_date
    , MAX(CASE WHEN record = 'HW_LTF_LF' THEN max_cal_date ELSE NULL END) AS ltf_max_cal_date
    , MAX(CASE WHEN record = 'ACTUALS_LF' THEN min_cal_date ELSE NULL END) AS act_lf_min_cal_date
    , MAX(CASE WHEN record = 'ACTUALS_LF' THEN max_cal_date ELSE NULL END) AS act_lf_max_cal_date
FROM nrm_06_printer_month_filters
),

--stitch forecasts together based on dates in CTE above
nrm_09_combined_ships_fcst as (
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
    AND ltf.record IN ('HW_FCST')
    AND ltf.cal_date > pd.stf_max_cal_date

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
    AND ltf.record IN ('HW_LTF_LF')
    AND ltf.cal_date > pd.act_lf_max_cal_date
)

--actuals
SELECT acts.region_5
    , acts.record
    , acts.cal_date
    , acts.country_alpha2
    , acts.platform_subset
    , acts.units
    , '1.1' AS version  -- used in ib process
FROM nrm_02_hw_acts AS acts
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = acts.platform_subset
WHERE 1=1
    AND hw.technology IN ('LASER','INK','PWA','LF')
    AND NOT hw.pl IN ('GW', 'LX')

UNION ALL

--STF and LTF
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

query_list.append(["stage.norm_ships_lf", norm_ships, "overwrite"])

# COMMAND ----------

ce_splits_pre = """

with ib_01_filter_vars as (

SELECT record
	, MAX(version) AS version
FROM "prod"."ce_splits"
WHERE 1=1
    AND official = 1
GROUP BY record

UNION ALL

SELECT DISTINCT record
    , version
FROM "prod"."decay_m13"
WHERE 1=1
    AND official = 1
    -- AND record <> 'lfd_decay'

UNION ALL

SELECT DISTINCT record
    , version
FROM "mdm"."printer_lag"

UNION ALL

SELECT DISTINCT record
    , version
FROM "prod"."instant_ink_enrollees"
WHERE 1=1
    AND official = 1

UNION ALL

SELECT 'IINK_IB_LTF' AS record
    , MAX(version) AS version
FROM "prod"."instant_ink_enrollees_ltf"
WHERE 1=1

UNION ALL

SELECT 'HARDWARE_LTF_MAX_DATE' AS record
    , CAST(DATEADD(MONTH, 240, MAX(cal_date)) AS VARCHAR(25)) AS version
FROM "prod"."hardware_ltf"
WHERE 1=1
    AND record IN ('HW_FCST')
    AND version = (SELECT MAX(version) FROM "prod"."hardware_ltf" WHERE record = 'HW_FCST' AND official = 1)
    AND official = 1

UNION ALL

SELECT 'HARDWARE_LTF_LF_MAX_DATE' AS record
    , CAST(DATEADD(MONTH, 240, MAX(cal_date)) AS VARCHAR(25)) AS version
FROM "prod"."hardware_ltf"
WHERE 1=1
    AND record IN ('HW_LTF_LF')
    AND version = (SELECT MAX(version) FROM "prod"."hardware_ltf" WHERE record = 'HW_LTF_LF' AND official = 1)
    AND official = 1

UNION ALL

SELECT DISTINCT 'PROD_NORM_SHIPS' AS record
    , version
FROM "prod"."norm_shipments"
WHERE 1=1
    AND version = (SELECT MAX(version) FROM "prod"."norm_shipments" )

UNION ALL

SELECT 'BUILD_NORM_SHIPS' AS record
    , '1.1' AS version
),  ib_03_norm_shipments_agg as (

SELECT ns.region_5
    , cc.market10
    , ns.record
    , ns.cal_date AS month_begin
    , ns.country_alpha2
    , ns.platform_subset
    , case when hw.business_feature is null then 'other' else hw.business_feature end as hps_ops
    , ns.units
FROM "stage"."norm_ships_lf" AS ns
JOIN ib_01_filter_vars AS fv
    ON fv.record = 'BUILD_NORM_SHIPS'
    AND fv.version = CASE WHEN 'BUILD_NORM_SHIPS' = 'PROD_NORM_SHIPS' THEN ns.version ElSE '1.1' END
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = ns.platform_subset
JOIN "mdm"."iso_country_code_xref" AS cc
    ON cc.country_alpha2 = ns.country_alpha2
WHERE 1=1
    AND hw.technology IN ('LF')
    and ns.record in ('ACTUALS_LF','HW_LTF_LF')

UNION ALL

SELECT ns.region_5
    , cc.market10
    , ns.record
    , ns.cal_date AS month_begin
    , ns.country_alpha2
    , ns.platform_subset
    , case when hw.business_feature is null then 'other' else hw.business_feature end as hps_ops
    , ns.units
FROM "stage"."norm_ships_lf" AS ns
JOIN ib_01_filter_vars AS fv
    ON fv.record = 'BUILD_NORM_SHIPS'
    AND fv.version = CASE WHEN 'BUILD_NORM_SHIPS' = 'PROD_NORM_SHIPS' THEN ns.version ElSE '1.1' END
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = ns.platform_subset
JOIN "mdm"."iso_country_code_xref" AS cc
    ON cc.country_alpha2 = ns.country_alpha2
WHERE 1=1
    AND hw.technology IN ('INK','PWA','LASER')
    and ns.record in ('ACTUALS - HW','HW_STF_FCST' , 'HW_FCST')

), ib_02a_ce_splits as (

SELECT ce.record
    , ce.platform_subset
    , ce.region_5
    , ce.country_alpha2
    , ce.month_begin
    , ce.split_name
    , ce.pre_post_flag
    , ce.value
    , ce.load_date
FROM "prod"."ce_splits" AS ce
WHERE 1=1
    AND ce.official = 1
    AND ce.record IN ('CE_SPLITS_I-INK', 'CE_SPLITS_I-INK LF','CE_SPLITS')

),  ib_02b_ce_splits_filter as (

SELECT DISTINCT record
    , platform_subset
    , country_alpha2
    , split_name
    , load_date
    , ROW_NUMBER() OVER (PARTITION BY platform_subset, country_alpha2, split_name ORDER BY load_date DESC) AS load_select
FROM
(
    SELECT DISTINCT ce.record
        , ce.platform_subset
        , ce.country_alpha2
        , ce.split_name
        , ce.load_date
    FROM ib_02a_ce_splits AS ce
    WHERE 1=1
        AND pre_post_flag = 'PRE'
) AS sub
WHERE 1=1

),  ib_02c_ce_splits_final as (

SELECT ce.record
    , ce.platform_subset
    , ce.region_5
    , ce.country_alpha2
    , ce.month_begin
    , ce.split_name
    , ce.pre_post_flag
    , ce.value
    , ce.load_date
FROM ib_02a_ce_splits AS ce
JOIN ib_02b_ce_splits_filter AS f
    ON f.record = ce.record
    AND f.load_date = ce.load_date
    AND f.platform_subset = ce.platform_subset
    AND f.country_alpha2 = ce.country_alpha2
    AND f.split_name = ce.split_name
WHERE 1=1
    AND ce.pre_post_flag = 'PRE'
    AND f.load_select = 1
)SELECT ns.region_5
    , ns.market10
    , ns.hps_ops
    , ns.country_alpha2
    , ns.platform_subset
    , ns.month_begin
    , CASE WHEN ns.platform_subset LIKE '%PAAS%' THEN COALESCE(ce.split_name, 'I-INK') ELSE COALESCE(ce.split_name, 'TRAD') END AS split_name
    , COALESCE(ce.value, 1.0) AS split_value
    , ns.units * COALESCE(ce.value, 1.0) AS units
FROM ib_03_norm_shipments_agg AS ns
LEFT JOIN ib_02c_ce_splits_final AS ce
    ON ce.country_alpha2 = ns.country_alpha2
    AND ce.platform_subset = ns.platform_subset
    AND ce.month_begin = ns.month_begin
    AND ce.pre_post_flag = 'PRE'  -- filter to only PRE splits
    AND ce.value > 0  -- filter out rows where value = 0 (this is just adding rows and not information)
WHERE 1=1
"""

query_list.append(["stage.ib_04_units_ce_splits_pre_lf", ce_splits_pre, "overwrite"])

# COMMAND ----------

norm_ships_ce = """

SELECT
    'NORM_SHIPS_CE' AS record
    , ns.month_begin AS cal_date
    , ns.region_5 AS region_5
    , ns.country_alpha2
    , ns.platform_subset
    , UPPER(ns.split_name) AS customer_engagement
    , ns.split_value
    , ns.units AS units
    , getdate() as load_date
    , 'staging' as version
FROM stage.ib_04_units_ce_splits_pre_lf ns
WHERE 1=1

"""
query_list.append(["stage.norm_shipments_ce_lf", norm_ships_ce, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Redshift Tables

# COMMAND ----------

# MAGIC %run "../../../common/output_to_redshift" $query_list=query_list

# COMMAND ----------

# MAGIC %run "../../../common/configs"

# COMMAND ----------

# MAGIC %run ../../../common/database_utils

# COMMAND ----------

# copy from stage to scen
submit_remote_query(configs, f"DROP TABLE IF EXISTS scen.prelim_norm_ships; CREATE TABLE scen.prelim_norm_ships AS SELECT * FROM stage.norm_ships;")

# COMMAND ----------

# copy from stage to scen
submit_remote_query(configs, f"DROP TABLE IF EXISTS scen.prelim_norm_shipments_ce; CREATE TABLE scen.prelim_norm_shipments_ce AS SELECT * FROM stage.norm_shipments_ce;")
