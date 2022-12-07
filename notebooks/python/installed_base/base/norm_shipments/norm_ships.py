# Databricks notebook source
# MAGIC %md
# MAGIC # Normalized Shipments

# COMMAND ----------

# Global Variables
query_list = []
# ib_tech_filter = "('actuals - hw', 'actuals_lf')"

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

query_list.append(["stage.norm_ships_inputs", norm_ships_inputs, "overwrite"])

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
WHERE act.record IN ('ACTUALS - HW','ACTUALS_LF')
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
JOIN stage.norm_ships_inputs vars
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
JOIN stage.norm_ships_inputs vars
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

UNION

SELECT ref.region_5
    , ltf.record
    , ltf.cal_date
    , ltf.country_alpha2
    , ltf.platform_subset
    , SUM(ltf.units) AS units
FROM "prod"."hardware_ltf" AS ltf
JOIN stage.norm_ships_inputs vars
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
    AND ltf.record IN ('HW_LTF_LF','HW_FCST')
    AND ltf.cal_date > pd.stf_max_cal_date
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

query_list.append(["stage.norm_ships", norm_ships, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Redshift Tables

# COMMAND ----------

# MAGIC %run "../../../common/output_to_redshift" $query_list=query_list
