# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share 03
# MAGIC 
# MAGIC - Mature Overrides

# COMMAND ----------

# for interactive sessions, define a version widget
dbutils.widgets.text("version", "")

# COMMAND ----------

# retrieve version from widget
version = dbutils.widgets.get("version")

# COMMAND ----------

query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Override Mature Landing

# COMMAND ----------

matures_landing = """
SELECT uso.record
    , uso.min_sys_dt
    , uso.month_num
    , uso.geography_grain
    , uso.geography
    , uso.platform_subset
    , uso.customer_engagement
    , uso.forecast_process_note
    , uso.post_processing_note
    , uso.forecast_created_date
    , uso.data_source
    , (SELECT MAX(version) FROM "prod"."version" WHERE record = 'USAGE_SHARE') as version
    , uso.measure
    , uso.units
    , uso.proxy_used
    , uso.ib_version
    , uso.load_date
FROM "prod"."usage_share_override_mature" uso
"""

query_list.append(["stage.usage_share_overrides_matures_landing", matures_landing, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Matures Normalized Landing

# COMMAND ----------

matures_norm_landing = f"""
with matures_02_month_num_combos as (


SELECT DISTINCT ml.platform_subset
    , ml.customer_engagement
    , ml.geography
FROM "stage"."usage_share_overrides_matures_landing" ml
WHERE geography IN ('AP','JP')
),  matures_03_month_num_ap_jp_combos as (


SELECT mc.platform_subset
    , mc.customer_engagement
    , COUNT(*) AS column_count
FROM matures_02_month_num_combos mc
GROUP BY mc.platform_subset
    , mc.customer_engagement
HAVING COUNT(*) > 1
),  matures_04_month_num_helper_1 as (


SELECT (platform_subset + UPPER(customer_engagement)) AS pl_key
FROM matures_03_month_num_ap_jp_combos
),  matures_08_month_num_usage as (


SELECT DISTINCT oml.record
    , oml.min_sys_dt
    , oml.month_num
    , oml.geography_grain
    , oml.geography
    , oml.platform_subset
    , oml.customer_engagement
    , oml.forecast_process_note
    , oml.post_processing_note
    , oml.forecast_created_date
    , oml.data_source
    , oml.version
    , oml.measure
    , oml.units
    , oml.proxy_used
    , oml.ib_version
    , oml.load_date
    , cc.market10
FROM "prod"."usage_share_override_mature" oml
LEFT JOIN "mdm"."iso_country_code_xref" cc
ON oml.geography=cc.region_5
WHERE 1=1
	AND cc.region_5 IS NOT NULL
	AND oml.geography IN ('EU','NA','LA','AP')
UNION ALL
SELECT DISTINCT oml.record
    , oml.min_sys_dt
    , oml.month_num
    , oml.geography_grain
    , oml.geography
    , oml.platform_subset
    , oml.customer_engagement
    , oml.forecast_process_note
    , oml.post_processing_note
    , oml.forecast_created_date
    , oml.data_source
    , oml.version
    , oml.measure
    , oml.units
    , oml.proxy_used
    , oml.ib_version
    , oml.load_date
	, cc.market10
FROM "prod"."usage_share_override_mature" oml
LEFT JOIN "mdm"."iso_country_code_xref" cc
ON oml.geography=cc.region_5
WHERE 1=1
	AND cc.region_5 IS NOT NULL
	AND oml.geography IN ('JP')
	AND (platform_subset + UPPER(customer_engagement)) NOT IN
		(SELECT * FROM matures_04_month_num_helper_1)
),  matures_06_month_num_ib_dates as (


SELECT DISTINCT ib.platform_subset
    , ib.cal_date
	, UPPER(ib.customer_engagement) AS customer_engagement
	, cc.market10
FROM "prod"."ib" ib
LEFT JOIN "mdm"."iso_country_code_xref" cc
ON ib.country=cc.country_alpha2
WHERE 1=1
	AND ib.version = '{version}'
	AND ib.measure = 'IB'
),  matures_05_month_num_min_max_dates as (


SELECT ib.platform_subset
    , cc.market10
    , ib.customer_engagement
	, CAST(min(cal_date) AS DATE) AS min_date
	, CAST(max(cal_date) AS DATE) AS max_date
FROM "prod"."ib" ib
LEFT JOIN "mdm"."iso_country_code_xref" cc
on ib.country=cc.country_alpha2
WHERE 1=1
	AND ib.version = '{version}'
	AND measure = 'IB'
GROUP BY ib.platform_subset
    , ib.customer_engagement
	, cc.market10
),  matures_07_month_num_date_diff as (


SELECT ibd.platform_subset
    , ibd.cal_date
    , UPPER(ibd.customer_engagement) AS customer_engagement
    , ibd.market10
    , mmd.min_date
    , CAST(datediff(month, mmd.min_date, ibd.cal_date) AS INT) AS diff
FROM matures_06_month_num_ib_dates ibd
INNER JOIN matures_05_month_num_min_max_dates mmd
ON ibd.platform_subset=mmd.platform_subset
	AND UPPER(ibd.customer_engagement)=UPPER(mmd.customer_engagement)
	AND ibd.market10=mmd.market10
),  matures_09_month_num_final as (


SELECT a.record
    , CAST(b.cal_date AS DATE) AS cal_date
    , a.geography
    , a.platform_subset
    , a.customer_engagement
    , a.forecast_process_note
    , a.post_processing_note
    , a.forecast_created_date
    , a.data_source
    , a.version
    , a.measure
    , a.units
    , a.proxy_used
    , a.ib_version
    , a.load_date
    , a.market10
FROM matures_08_month_num_usage a
INNER JOIN matures_07_month_num_date_diff b
	ON a.platform_subset=b.platform_subset
        AND UPPER(a.customer_engagement) = UPPER(b.customer_engagement)
        AND a.month_num=b.diff
        AND a.market10 = b.market10
)SELECT a.record
   , a.cal_date
   , a.geography
   , a.platform_subset
   , a.customer_engagement
   , a.forecast_process_note
   , a.post_processing_note
   , a.forecast_created_date
   , a.data_source
   , a.version
   , a.measure
   , a.units
   , a.proxy_used
   , a.ib_version
   , a.load_date
   , a.market10
FROM matures_09_month_num_final a
LEFT JOIN "mdm"."hardware_xref" b
    ON a.platform_subset=b.platform_subset
WHERE 1=1
   AND b.product_lifecycle_status IN ('M','C')
"""

query_list.append(["stage.usage_share_matures_normalized_landing", matures_norm_landing, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Matures Normalized Fill Landing

# COMMAND ----------

matures_norm_fill_landing = f"""
with matures_11_fill_missing_ib_data as (


SELECT ib.platform_subset
    , cc.market10
    , customer_engagement
    , CAST(min(cal_date) AS DATE) AS min_ib_date
    , CAST(max(cal_date) AS DATE) AS max_ib_date
FROM "prod"."ib" ib
LEFT JOIN "mdm"."iso_country_code_xref" cc
    on ib.country=cc.country_alpha2
LEFT JOIN "mdm"."hardware_xref" hw
    ON ib.platform_subset=hw.platform_subset
WHERE 1=1
	AND ib.version = '{version}'
	AND measure = 'IB'
	AND hw.product_lifecycle_status = 'M'
GROUP BY ib.platform_subset
    , customer_engagement
    , cc.market10
),  matures_12_fill_missing_us_data as (


SELECT mnl.platform_subset
    , mnl.market10
    , mnl.customer_engagement
    , CAST(min(mnl.cal_date) AS DATE) AS min_us_date
    , CAST(max(mnl.cal_date) AS DATE) AS max_us_date
FROM "stage"."usage_share_matures_normalized_landing" mnl
WHERE 1=1
GROUP BY platform_subset
    , customer_engagement
	, market10
),  matures_13_fill_missing_dates as (


SELECT a.platform_subset
    , a.market10
    , a.customer_engagement
    , a.min_ib_date
    , a.max_ib_date
    , b.min_us_date
    , b.max_us_date
    , DATEDIFF(MONTH, b.min_us_date, a.min_ib_date) AS min_diff
    , DATEDIFF(MONTH, b.max_us_date, a.max_ib_date) AS max_diff
FROM matures_11_fill_missing_ib_data a
LEFT JOIN matures_12_fill_missing_us_data b
	ON a.platform_subset=b.platform_subset
	AND a.market10=b.market10
	AND a.customer_engagement=b.customer_engagement
WHERE 1=1
	AND b.min_us_date IS NOT NULL
	AND DATEDIFF(MONTH, b.max_us_date, a.max_ib_date) > 0
),  matures_14_fill_missing_cal_dates as (


SELECT DISTINCT date
FROM "mdm"."calendar"
WHERE Day_of_Month = 1
),  matures_16_fill_missing_dates_to_expand as (


SELECT platform_subset
    , market10
    , UPPER(customer_engagement) AS customer_engagement
    , date AS cal_date
FROM matures_13_fill_missing_dates
CROSS JOIN matures_14_fill_missing_cal_dates
WHERE 1=1
AND [date] > max_us_date
AND [date] <= max_ib_date
),  matures_15_fill_missing_max_values as (


SELECT a.platform_subset
    , UPPER(a.customer_engagement) AS customer_engagement
    , a.market10
    , a.measure
    , a.units
FROM "stage"."usage_share_matures_normalized_landing" a
INNER JOIN matures_12_fill_missing_us_data b
    ON a.platform_subset = b.platform_subset
        AND a.customer_engagement = b.customer_engagement
        AND a.market10 = b.market10
        AND a.cal_date = b.max_us_date
),  matures_17_fill_missing_market10_countries as (


SELECT DISTINCT region_5
    , market10
FROM "mdm"."iso_country_code_xref"
WHERE 1=1
    AND region_5 IS NOT NULL
    AND region_5 <> 'JP'
    AND NOT region_5 LIKE ('X%')
)SELECT 'USAGE_SHARE_MATURES' As record
    , CAST(a.cal_date AS DATE) AS cal_date
    , c.region_5 AS geography
    , a.platform_subset
    , a.customer_engagement
    , 'MATURE OVERRIDE' AS forecast_process_note
    , 'NONE' AS post_processing_note
    , CAST(GETDATE() AS DATE) AS forecast_created_date
    , 'DATA_SOURCE' AS data_source
    , 'VERSION' AS version
    , b.measure
    , b.units
    , NULL AS proxy_used
    , 'IB_VERSION' AS ib_version
    , GETDATE() AS load_date
    , a.market10
FROM matures_16_fill_missing_dates_to_expand a
LEFT JOIN matures_15_fill_missing_max_values b
    ON a.platform_subset=b.platform_subset
        AND a.customer_engagement = b.customer_engagement
        AND a.market10 = b.market10
LEFT JOIN matures_17_fill_missing_market10_countries c
    ON a.market10 = c.market10
WHERE 1=1
"""

query_list.append(["stage.usage_share_matures_normalized_fill_landing", matures_norm_fill_landing, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Matures Normalized Final Landing

# COMMAND ----------

matures_norm_final_landing = """
SELECT nl.record
    , nl.cal_date
    , nl.geography
    , nl.platform_subset
    , nl.customer_engagement
    , nl.forecast_process_note
    , nl.post_processing_note
    , nl.forecast_created_date
    , nl.data_source
    , nl.version
    , nl.measure
    , nl.units
    , nl.proxy_used
    , nl.ib_version
    , nl.load_date
    , nl.market10
    , hw.product_lifecycle_status
FROM "stage"."usage_share_matures_normalized_landing" nl
INNER JOIN "mdm"."hardware_xref" hw
    ON nl.platform_subset=hw.platform_subset
WHERE 1=1
    AND hw.product_lifecycle_status = 'M'
UNION ALL
SELECT fl.record
    , fl.cal_date
    , fl.geography
    , fl.platform_subset
    , fl.customer_engagement
    , fl.forecast_process_note
    , fl.post_processing_note
    , fl.forecast_created_date
    , fl.data_source
    , fl.version
    , fl.measure
    , fl.units
    , cast(fl.proxy_used as varchar) as proxy_used
    , fl.ib_version
    , fl.load_date
    , fl.market10
    , hw.product_lifecycle_status
FROM "stage"."usage_share_matures_normalized_fill_landing" fl
INNER JOIN "mdm"."hardware_xref" hw
    ON fl.platform_subset=hw.platform_subset
WHERE 1=1
    AND hw.product_lifecycle_status = 'M'
"""

query_list.append(["stage.usage_share_matures_normalized_final_landing", matures_norm_final_landing, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Overrides Matures Landing Final

# COMMAND ----------

matures_out = """
SELECT ml.record
    , ml.min_sys_dt
    , ml.month_num
    , ml.geography_grain
    , ml.geography
    , ml.platform_subset
    , ml.customer_engagement
    , ml.forecast_process_note
    , ml.post_processing_note
    , ml.forecast_created_date
    , ml.data_source
    , (SELECT DISTINCT version FROM "prod"."version" WHERE record = 'USAGE_SHARE' AND official=1) as version
    , ml.measure
    , ml.units
    , ml.proxy_used
    , ml.ib_version
    , ml.load_date
FROM "stage"."usage_share_overrides_matures_landing" ml
"""

query_list.append(["stage.usage_share_overrides_matures_landing_final", matures_out, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Write Out Matures

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
