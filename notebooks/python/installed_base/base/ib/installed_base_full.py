# Databricks notebook source
# MAGIC %md
# MAGIC # Installed Base - Full Run

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

ib_staging_inputs = """

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
)SELECT 'ib_staging_temp' AS tbl_name
    , record
    , version AS version
    , GETDATE() AS execute_time
FROM ib_01_filter_vars
WHERE 1=1
    AND record NOT IN ('PROD_NORM_SHIPS', 'BUILD_NORM_SHIPS')

UNION ALL

-- just report the single norm ships input - either stage or prod
SELECT 'ib_staging_temp' AS tbl_name
    , record
    , version AS version
    , GETDATE() AS execute_time
FROM ib_01_filter_vars
WHERE 1=1
    AND record IN ('BUILD_NORM_SHIPS')
"""

query_list.append(["stage.ib_staging_inputs", ib_staging_inputs, "overwrite"])

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
FROM "stage"."norm_ships" AS ns
JOIN ib_01_filter_vars AS fv
    ON fv.record = 'BUILD_NORM_SHIPS'
    AND fv.version = CASE WHEN 'BUILD_NORM_SHIPS' = 'PROD_NORM_SHIPS' THEN ns.version ElSE '1.1' END
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = ns.platform_subset
JOIN "mdm"."iso_country_code_xref" AS cc
    ON cc.country_alpha2 = ns.country_alpha2
WHERE 1=1
    AND hw.technology IN ('LASER','INK','PWA','LF')

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
    AND ce.record IN ('CE_SPLITS_I-INK', 'CE_SPLITS_I-INK LF')

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
    , COALESCE(ce.split_name, 'TRAD') AS split_name
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

query_list.append(["stage.ib_04_units_ce_splits_pre", ce_splits_pre, "overwrite"])

# COMMAND ----------

hw_decay = """

WITH ib_01_filter_vars as (

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
), ib_03_norm_shipments_agg as (

SELECT ns.region_5
    , cc.market10
    , ns.record
    , ns.cal_date AS month_begin
    , ns.country_alpha2
    , ns.platform_subset
    , case when hw.business_feature is null then 'other' else hw.business_feature end as hps_ops
    , ns.units
FROM "stage"."norm_ships" AS ns
JOIN ib_01_filter_vars AS fv
    ON fv.record = 'BUILD_NORM_SHIPS'
    AND fv.version = CASE WHEN 'BUILD_NORM_SHIPS' = 'PROD_NORM_SHIPS' THEN ns.version ElSE '1.1' END
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = ns.platform_subset
JOIN "mdm"."iso_country_code_xref" AS cc
    ON cc.country_alpha2 = ns.country_alpha2
WHERE 1=1
    AND hw.technology IN ('LASER','INK','PWA','LF')

), iso as (

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
INNER JOIN ib_03_norm_shipments_agg norm ON norm.country_alpha2=iso.country_alpha2
WHERE CONCAT(iso.market10, CONCAT('-',CASE iso.developed_emerging WHEN 'DEVELOPED' THEN 'DM' ELSE 'EM' END)) NOT IN ('UK&I-EM','NORTHERN EUROPE-EM', 'SOUTHERN EUROPE-EM') 
)

SELECT CAST(DATEPART(year, ucep.month_begin) AS INTEGER) + (CAST(DATEPART(month, ucep.month_begin) AS INTEGER) + pl.month_lag - 1) / 12 AS year
    , ((CAST(DATEPART(month, ucep.month_begin) AS INTEGER) - 1 + pl.month_lag) % 12) + 1 AS month
    , ucep.region_5
    , ucep.market10
    , ucep.country_alpha2
    , iso.market13
    , ucep.hps_ops
    , ucep.split_name
    , ucep.platform_subset
    , SUM(ucep.units * pl.value) AS printer_installs
FROM "stage"."ib_04_units_ce_splits_pre" AS ucep
JOIN "mdm"."printer_lag" AS pl  -- implicit cross join (or cartesian product)
    ON ucep.region_5 = pl.region_5
    AND ucep.hps_ops = pl.hps_ops
    AND ucep.split_name = pl.split_name
    AND pl.pre_post_flag = 'PRE'
LEFT JOIN iso ON ucep.country_alpha2=iso.country_alpha2
GROUP BY CAST(DATEPART(year, ucep.month_begin) AS INTEGER) + (CAST(DATEPART(month, ucep.month_begin) AS INTEGER) + pl.month_lag - 1) / 12
    , ((CAST(DATEPART(month, ucep.month_begin) AS INTEGER) - 1 + pl.month_lag) % 12) + 1
    , ucep.region_5
    , ucep.market10
    , iso.market13
    , ucep.country_alpha2
    , ucep.hps_ops
    , ucep.split_name
    , ucep.platform_subset
"""

query_list.append(["stage.ib_01_hw_decay", hw_decay, "overwrite"])

# COMMAND ----------

ce_splits = """


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
),  ib_06_months as (


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
),  ib_08_decay_months as (


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
    AND d.record IN ('HW_DECAY','HW_DECAY_LF')
),  ib_09_remaining_amt as (


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
),  ib_10_hw_lag_w_remaining_amt as (


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
FROM "stage"."ib_01_hw_decay" AS hw_lag
JOIN ib_09_remaining_amt AS amt
    ON hw_lag.platform_subset = amt.platform_subset
    AND hw_lag.market13 = amt.geography
    AND hw_lag.split_name = amt.split_name
GROUP BY hw_lag.year + CAST((hw_lag.month + amt.month_offset - 1) AS INTEGER) / 12
    , (hw_lag.month + amt.month_offset - 1) % 12 + 1
    , hw_lag.hps_ops
    , hw_lag.split_name
    , hw_lag.region_5
    , hw_lag.market10
    , hw_lag.market13
    , hw_lag.country_alpha2
    , hw_lag.platform_subset
),  ib_11_prelim_output as (


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
LEFT JOIN "stage"."ib_01_hw_decay" AS hw
    ON hw.year = ic.year
    AND hw.month = ic.month
    AND hw.country_alpha2 = ic.country_alpha2
    AND hw.hps_ops = ic.hps_ops
    AND (hw.split_name = ic.split_name OR ic.split_name IS NULL)
    AND hw.platform_subset = ic.platform_subset
    AND hw.printer_installs > 0
),  ib_02a_ce_splits as (


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
    AND ce.record IN ('CE_SPLITS_I-INK', 'CE_SPLITS_I-INK LF')
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
),  ib_12_ce_splits_post as (


-- non LASER platform_subsets
SELECT ib.month_begin
    , ib.region_5
    , ib.country_alpha2
    , ib.hps_ops
    , COALESCE(ce.split_name, ib.split_name, 'TRAD') AS split_name
    , ib.platform_subset
    , ib.printer_installs * COALESCE(ce.value, 1.0) AS printer_installs
    , ib.ib * COALESCE(ce.value, 1.0) AS ib
FROM ib_11_prelim_output AS ib
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = ib.platform_subset
LEFT JOIN ib_02c_ce_splits_final AS ce
    ON ce.platform_subset = ib.platform_subset
    AND ce.country_alpha2 = ib.country_alpha2
    AND ce.month_begin = ib.month_begin
    AND ce.pre_post_flag = 'POST'
    AND ce.value > 0
WHERE 1=1
    AND hw.technology IN ('INK', 'PWA')

UNION ALL

-- LASER
SELECT ib.month_begin
    , ib.region_5
    , ib.country_alpha2
    , ib.hps_ops
    , CASE WHEN ib.platform_subset LIKE '%STND%' THEN 'STD'
           WHEN ib.platform_subset LIKE '%YET2%' THEN 'HP+'
           ELSE 'TRAD' END AS split_name
    , ib.platform_subset
    , ib.printer_installs AS printer_installs
    , ib.ib AS ib
FROM ib_11_prelim_output AS ib
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = ib.platform_subset
WHERE 1=1
    AND hw.technology IN ('LASER')

UNION ALL

SELECT ib.month_begin
    , ib.region_5
    , ib.country_alpha2
    , ib.hps_ops
    , 'TRAD' AS split_name
    , ib.platform_subset
    , ib.printer_installs  AS printer_installs
    , ib.ib  AS ib
FROM ib_11_prelim_output AS ib
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = ib.platform_subset
LEFT JOIN ib_02c_ce_splits_final AS ce
    ON ce.platform_subset = ib.platform_subset
    AND ce.country_alpha2 = ib.country_alpha2
    AND ce.month_begin = ib.month_begin
    AND ce.pre_post_flag = 'POST'
    AND ce.value > 0
WHERE 1=1
    AND hw.technology IN ('LF')
)SELECT month_begin
    , region_5
    , country_alpha2
    , hps_ops
    , split_name
    , platform_subset
    , printer_installs
    , ib
FROM ib_12_ce_splits_post
"""

query_list.append(["stage.ib_02_ce_splits", ce_splits, "overwrite"])

# COMMAND ----------

iink_complete = """


with ib_14_iink_act_stf as (


SELECT iiel.platform_subset
    , cast('I-INK' as char(50)) AS split_name
    , cast('IINK_ENROLLEES' as char(50)) AS type
    , c.Fiscal_Year_Qtr AS fiscal_year_qtr
    , iiel.year_fiscal
    , iiel.year_month AS month_begin
    , iiel.data_source
    , iiel.country AS country_alpha2
    , cc.region_5
    , cc.market10
    , SUM(iiel.p2_kitless_enrollments) AS p2_kitless_enroll_unmod
    , SUM(iiel.p2_kitless_enrollments) AS p2_kitless_enroll_mod
    , SUM(iiel.cum_enrollees_month) AS cum_enrollees_month
FROM "prod"."instant_ink_enrollees" AS iiel
JOIN "mdm"."calendar" AS c
        on c.Date = iiel.year_month
JOIN "mdm"."iso_country_code_xref" AS cc
    ON cc.country_alpha2 = iiel.country
WHERE 1=1
    AND iiel.official = 1
GROUP BY iiel.platform_subset
    , c.Fiscal_Year_Qtr
    , iiel.year_fiscal
    , iiel.year_month
    , iiel.data_source
    , iiel.country
    , cc.region_5
    , cc.market10
),  ib_15_iink_p2_cumulative as (


SELECT iink.platform_subset
    , iink.split_name
    , iink.fiscal_year_qtr
    , iink.year_fiscal
    , cast(iink.month_begin as date)
    , iink.data_source
    , iink.country_alpha2
    , iink.region_5
    , iink.market10
    , iink.cum_enrollees_month
    , iink.p2_kitless_enroll_unmod
    , iink.p2_kitless_enroll_mod
    , SUM(iink.p2_kitless_enroll_mod) OVER (PARTITION BY iink.platform_subset, iink.country_alpha2
                              ORDER BY iink.month_begin ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS p2_kitless_enroll_mod_cum
FROM  ib_14_iink_act_stf AS iink
),  ib_16_iink_stf_trend_prep as (


SELECT iink.platform_subset
    , iink.split_name
    , iink.market10
    , iink.country_alpha2
    , AVG(iink.p2_kitless_enroll_mod) AS p2_kitless_enroll_mod
    , AVG(iink.cum_enrollees_month) AS cum_enrollees_month
FROM ib_15_iink_p2_cumulative AS iink
JOIN
    (
        SELECT platform_subset
            , market10
            , country_alpha2
            , MAX(month_begin) AS max_cal_date
        FROM ib_15_iink_p2_cumulative
        GROUP BY platform_subset
            , market10
            , country_alpha2
    ) AS cal
    ON cal.platform_subset = iink.platform_subset
    AND cal.market10 = iink.market10
    AND cal.country_alpha2 = iink.country_alpha2
    AND iink.month_begin = cal.max_cal_date
    -- AND iink.month_begin BETWEEN DATEADD(MONTH, -11, cal.max_cal_date) AND cal.max_cal_date
WHERE 1=1
GROUP BY iink.platform_subset
    , iink.split_name
    , iink.market10
    , iink.country_alpha2
),  ib_17_iink_stf_trend as (


SELECT cal.[Date] AS month_begin
    , cast('MARKET10' as char(50)) AS geography_grain
    , stf.market10 AS geography
    , stf.country_alpha2
    , stf.platform_subset
    , stf.split_name
    , stf.p2_kitless_enroll_mod
    , stf.cum_enrollees_month
FROM ib_16_iink_stf_trend_prep AS stf
CROSS JOIN "mdm"."calendar" AS cal
WHERE 1=1
    AND cal.day_of_month = 1
    AND cal.Date BETWEEN
        (
            SELECT DATEADD(MONTH, 1, CAST(MAX(iink.month_begin) AS DATE))
            FROM ib_15_iink_p2_cumulative AS iink
        )
    AND
        (
            SELECT CAST(MAX(ltf.cal_date) AS DATE)
            FROM "stage"."norm_ships" AS ltf
        )
),  ib_18_iink_combined as (


SELECT stf.month_begin
    , cast('MARKET10' as char(50)) AS geography_grain
    , stf.market10 AS geography
    , stf.country_alpha2
    , stf.platform_subset
    , stf.split_name
    , stf.p2_kitless_enroll_mod
    , stf.cum_enrollees_month
FROM ib_15_iink_p2_cumulative AS stf

UNION ALL

SELECT stf.month_begin
    , stf.geography_grain
    , stf.geography
    , stf.country_alpha2
    , stf.platform_subset
    , stf.split_name
    , stf.p2_kitless_enroll_mod
    , stf.cum_enrollees_month
FROM ib_17_iink_stf_trend AS stf
),  ib_19_iink_sys as (


SELECT sys.month_begin
    , cast('MARKET10' as char(50)) AS geography_grain
    , iso.market10 AS geography
    , sys.country_alpha2
    , sys.hps_ops
    , sys.split_name
    , sys.platform_subset
    , sys.printer_installs
    , sys.ib
    , cast('IINK_SYS' as char(50)) AS type
FROM "stage"."ib_02_ce_splits" AS sys
JOIN "mdm"."iso_country_code_xref" AS iso
    ON iso.country_alpha2 = sys.country_alpha2
LEFT OUTER JOIN ib_18_iink_combined AS iink
    ON iink.month_begin = sys.month_begin
    AND iink.country_alpha2 = sys.country_alpha2
    AND iink.platform_subset = sys.platform_subset
WHERE 1=1
    AND sys.split_name = 'I-INK'
    AND sys.month_begin >= '2013-03-01'  -- first date of iink enrollees
    AND iink.month_begin IS NULL
    AND iink.country_alpha2 IS NULL
    AND iink.platform_subset IS NULL
),  ib_20_complete_iink_ib_prep as (


SELECT sys.month_begin
    , sys.geography_grain
    , sys.geography
    , sys.country_alpha2
    , sys.split_name
    , sys.platform_subset
    , 0.0 AS p2_kitless_enroll_mod
    , 0.0 AS p2_kitless_enroll_mod_cum
    , sys.ib
    , sys.type
FROM ib_19_iink_sys AS sys

UNION ALL 

SELECT iink.month_begin
    , iink.geography_grain
    , iink.geography
    , iink.country_alpha2
    , iink.split_name
    , iink.platform_subset
    , iink.p2_kitless_enroll_mod
    , SUM(iink.p2_kitless_enroll_mod) OVER (PARTITION BY iink.platform_subset, iink.country_alpha2
                                            ORDER BY iink.month_begin ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS p2_kitless_enroll_mod_cum
    , iink.cum_enrollees_month AS ib
    , 'IINK_TEAM_IB' AS type
FROM ib_18_iink_combined AS iink
)SELECT month_begin
    , geography_grain
    , geography
    , country_alpha2
    , split_name
    , platform_subset
    , SUM(p2_kitless_enroll_mod_cum) AS p2_attach
    , SUM(ib) AS ib
FROM ib_20_complete_iink_ib_prep
GROUP BY month_begin
    , geography_grain
    , geography
    , country_alpha2
    , split_name
    , platform_subset
"""

query_list.append(["stage.ib_03_iink_complete", iink_complete, "overwrite"])

# COMMAND ----------

ib_staging = """


with ib_22_iink_ltf_to_split as (


SELECT month_begin
    , geography_grain
    , geography
    , country_alpha2
    , split_name
    , platform_subset
    , p2_attach
    , ib
FROM "stage"."ib_03_iink_complete"
WHERE 1=1
    AND CAST(month_begin AS DATE) > (SELECT MAX(year_month) FROM prod.instant_ink_enrollees WHERE official = 1)
),  ib_14_iink_act_stf as (


SELECT iiel.platform_subset
    , cast('I-INK' as char(50)) AS split_name
    , cast('IINK_ENROLLEES' as char(50)) AS type
    , c.Fiscal_Year_Qtr AS fiscal_year_qtr
    , iiel.year_fiscal
    , iiel.year_month AS month_begin
    , iiel.data_source
    , iiel.country AS country_alpha2
    , cc.region_5
    , cc.market10
    , SUM(iiel.p2_kitless_enrollments) AS p2_kitless_enroll_unmod
    , SUM(iiel.p2_kitless_enrollments) AS p2_kitless_enroll_mod
    , SUM(iiel.cum_enrollees_month) AS cum_enrollees_month
FROM "prod"."instant_ink_enrollees" AS iiel
JOIN "mdm"."calendar" AS c
        on c.Date = iiel.year_month
JOIN "mdm"."iso_country_code_xref" AS cc
    ON cc.country_alpha2 = iiel.country
WHERE 1=1
    AND iiel.official = 1
GROUP BY iiel.platform_subset
    , c.Fiscal_Year_Qtr
    , iiel.year_fiscal
    , iiel.year_month
    , iiel.data_source
    , iiel.country
    , cc.region_5
    , cc.market10
),  ib_23_iink_ltf_prep as (


SELECT record, CASE WHEN ltf.region_5 IN ('AP', 'EU', 'NA') AND ltf.version = '2020.10.05.01' THEN 'region_5'
            WHEN ltf.region_5 IN ('APJ', 'EMEA', 'NA') AND ltf.version = '2020.12.07.1' THEN 'region_3'
            WHEN ltf.region_5 IN ('CENTRAL EUROPE','GREATER ASIA','GREATER CHINA','INDIA','ISE',
                                  'LATIN AMERICA','NORTH AMERICA','NORTHERN EUROPE','SOUTHER EUROPE','UK&I') THEN 'MARKET10'
            ELSE 'ERROR' END AS geography_grain
    , ltf.region_5 AS geography
    , c.Fiscal_Year_Qtr AS fiscal_year_qtr
    , ltf.cal_date AS month_begin
    , MAX(CASE WHEN ltf.metric = 'P2 ENROLLEES' THEN ltf.value END) AS p2_enrollees
    , MAX(CASE WHEN ltf.metric = 'P2 CUMULATIVE' THEN ltf.value END) AS p2_cumulative
    , MAX(CASE WHEN ltf.metric = 'CUMULATIVE' THEN ltf.value END) AS cumulative
FROM "prod"."instant_ink_enrollees_ltf" AS ltf
JOIN "stage"."ib_staging_inputs" AS fv
    ON fv.version = ltf.version
    AND fv.record = 'IINK_IB_LTF'
JOIN "mdm"."calendar" AS c
        on c.Date = ltf.cal_date
WHERE 1=1
    AND ltf.region_5 <> 'WW'
    AND ltf.metric IN ('CUMULATIVE', 'P2 ENROLLEES', 'P2 CUMULATIVE')
    AND ltf.value <> 0
    AND NOT ltf.value IS NULL
    AND ltf.cal_date > ( SELECT CAST(MAX(month_begin) AS DATE) FROM ib_14_iink_act_stf )
GROUP BY CASE WHEN ltf.region_5 IN ('AP', 'EU', 'NA') AND ltf.version = '2020.10.05.01' THEN 'region_5'
              WHEN ltf.region_5 IN ('APJ', 'EMEA', 'NA') AND ltf.version = '2020.12.07.1' THEN 'region_3'
              WHEN ltf.region_5 IN ('CENTRAL EUROPE','GREATER ASIA','GREATER CHINA','INDIA','ISE',
                                    'LATIN AMERICA','NORTH AMERICA','NORTHERN EUROPE','SOUTHER EUROPE','UK&I') THEN 'MARKET10'
              ELSE 'ERROR' END
    , ltf.region_5
    , c.Fiscal_Year_Qtr
    , ltf.cal_date
), 
hw_ships_paas as (
select cal_date,platform_subset,country, sum(units) over (partition by cal_date,platform_subset,country)/
sum(units) over (partition by cal_date,country) ps_mix
from prod.norm_ships
where version = (select max(version) from prod.norm_ships) and platform_subset like '%PAAS%'
)
ib_24_iink_ltf as (


SELECT sp.month_begin
    , sp.country_alpha2
    , sp.platform_subset
    , sp.split_name
    , ltf.p2_cumulative *
        SUM(sp.p2_attach) OVER (PARTITION BY sp.month_begin, sp.country_alpha2, sp.platform_subset) * 1.0 /
            NULLIF(SUM(sp.p2_attach) OVER (PARTITION BY sp.month_begin, sp.geography), 0) AS p2_cumulative
    , ltf.cumulative *
        SUM(sp.ib) OVER (PARTITION BY sp.month_begin, sp.country_alpha2, sp.platform_subset) * 1.0 /
            NULLIF(SUM(sp.ib) OVER (PARTITION BY sp.month_begin, sp.geography), 0) AS cum_enrollees_month
FROM ib_22_iink_ltf_to_split AS sp
JOIN ib_23_iink_ltf_prep AS ltf
    ON ltf.geography = sp.geography
    AND ltf.month_begin = sp.month_begin
WHERE 1=1 
    AND CAST(ltf.month_begin AS DATE) > (SELECT MAX(year_month) FROM prod.instant_ink_enrollees WHERE official = 1)
    and platform_subset not like '%PAAS%' and ltf.record = 'i_ink_core'
    
UNION 

SELECT sp.month_begin
    , sp.country_alpha2
    , sp.platform_subset
    , sp.split_name
    , 0  AS p2_cumulative
    , ltf.cumulative *
        ps_mix AS cum_enrollees_month
FROM ib_22_iink_ltf_to_split AS sp
JOIN  hw_ships_paas AS ltf
    ON ltf.country = sp.country
    AND ltf.cal_date = sp.month_begin
WHERE 1=1
    AND CAST(ltf.month_begin AS DATE) > (SELECT MAX(year_month) FROM prod.instant_ink_enrollees WHERE official = 1)
    and sp.platform_subset  like '%PAAS%' and ltf.record = 'i_ink_paas'
    
),  ib_25_sys_delta as (


SELECT comb.month_begin
    , comb.country_alpha2
    , comb.platform_subset
    , comb.p2_attach AS p2_cumulative
FROM "stage"."ib_03_iink_complete" AS comb
WHERE 1=1
    AND comb.p2_attach > 0
    AND NOT comb.p2_attach IS NULL
    AND CAST(comb.month_begin AS DATE) <= (SELECT MAX(year_month) FROM prod.instant_ink_enrollees WHERE official = 1)

UNION ALL

SELECT comb.month_begin
    , comb.country_alpha2
    , comb.platform_subset
    , comb.p2_cumulative
FROM ib_24_iink_ltf AS comb
WHERE 1=1
    AND comb.p2_cumulative > 0
    AND NOT comb.p2_cumulative IS NULL
),  ib_staging_temp_pre as (


-- TRAD
SELECT 'IB_TRAD' AS record
    , NULL AS version
    , NULL as load_date
    , ib.month_begin
    , 'MARKET10' AS geography_grain
    , cc.market10 AS geography
    , ib.country_alpha2
    , ib.hps_ops
    , hw.technology
    , UPPER(ib.split_name) AS split_name
    , ib.platform_subset
    , ib.printer_installs
    , ib.ib - COALESCE(sys.p2_cumulative, 0) AS ib
FROM "stage"."ib_02_ce_splits" AS ib
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = ib.platform_subset
JOIN "mdm"."iso_country_code_xref" AS cc
    ON cc.country_alpha2 = ib.country_alpha2
LEFT JOIN ib_25_sys_delta AS sys
    ON sys.month_begin = ib.month_begin
    AND sys.country_alpha2 = ib.country_alpha2
    AND sys.platform_subset = ib.platform_subset
WHERE 1=1
    AND NOT ib.split_name = 'I-INK'
    AND hw.technology IN ('LASER','INK','PWA','LF')

UNION ALL

-- I-INK; acts, stf
SELECT 'IB_IINK' AS record
    , NULL AS version
    , NULL as load_date
    , iink.month_begin
    , iink.geography_grain
    , iink.geography
    , iink.country_alpha2
    , CASE WHEN hw.business_feature IS NULL THEN 'other' ELSE hw.business_feature END AS hps_ops
    , hw.technology
    , UPPER(iink.split_name) AS split_name
    , iink.platform_subset
    , 0 AS printer_installs
    , iink.ib
FROM "stage"."ib_03_iink_complete" AS iink
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = iink.platform_subset
WHERE 1=1
    AND CAST(iink.month_begin AS DATE) <= (SELECT MAX(year_month) FROM prod.instant_ink_enrollees WHERE official = 1)
    AND hw.technology IN ('LASER','INK','PWA','LF')

UNION ALL

-- I-INK; ltf
SELECT 'IB_IINK' AS record
    , NULL AS version
    , NULL as load_date
    , iink.month_begin
    , 'MARKET10' AS geography_grain
    , cc.market10 AS geography
    , iink.country_alpha2
    , CASE WHEN hw.business_feature IS NULL THEN 'other' ELSE hw.business_feature END AS hps_ops
    , hw.technology
    , UPPER(iink.split_name) AS split_name
    , iink.platform_subset
    , 0 AS printer_installs
    , iink.cum_enrollees_month AS ib
FROM ib_24_iink_ltf AS iink
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = iink.platform_subset
JOIN "mdm"."iso_country_code_xref" AS cc
    ON cc.country_alpha2 = iink.country_alpha2
WHERE 1=1
    AND hw.technology IN ('LASER','INK','PWA','LF')
)SELECT 'IB' AS record
    , 1 AS version  -- used for scenarios
    , pre.load_date
    , pre.month_begin
    , pre.geography_grain
    , pre.geography
    , pre.country_alpha2
    , pre.hps_ops
    , pre.split_name
    , pre.platform_subset
    , pre.printer_installs
    , CASE WHEN pre.ib <= 0.0 THEN 1e-3 ELSE pre.ib END AS ib
FROM ib_staging_temp_pre AS pre
WHERE 1=1
    AND pre.record IN ('IB_TRAD', 'IB_IINK')
"""

query_list.append(["stage.ib_staging", ib_staging, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Tables in Redshift

# COMMAND ----------

# MAGIC %run "../../../common/output_to_redshift" $query_list=query_list
