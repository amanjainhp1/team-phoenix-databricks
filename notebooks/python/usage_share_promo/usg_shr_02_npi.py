# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share 02
# MAGIC 
# MAGIC - NPI Overrides

# COMMAND ----------

query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Overrides Normalized Landing

# COMMAND ----------

overrides_norm_landing = """
with npi_06_month_num_ib_dates as (


SELECT DISTINCT ib.platform_subset
	, ib.cal_date
	, UPPER(ib.customer_engagement) AS customer_engagement
	, cc.market10
	, cc.region_5
FROM "prod"."ib" ib
LEFT JOIN "prod"."iso_country_code_xref" cc
ON ib.country=cc.country_alpha2
WHERE 1=1
	AND ib.version = '2022.04.25.1'
	AND ib.measure = 'IB'
),  npi_05_month_num_min_max_dates_r5 as (


SELECT ib.platform_subset
	, cc.region_5
	, customer_engagement
	, CAST(min(cal_date) AS DATE) AS min_date
	, CAST(max(cal_date) AS DATE) AS max_date
FROM "prod"."ib" ib
LEFT JOIN "prod"."iso_country_code_xref" cc
ON ib.country=cc.country_alpha2
WHERE 1=1
	AND ib.version = '2022.04.25.1'
	AND ib.measure = 'IB'
GROUP BY platform_subset
	, ib.customer_engagement
	, cc.region_5
),  npi_08_month_num_date_diff_r5 as (


SELECT ib.platform_subset
    , ib.cal_date
    , ib.customer_engagement
    , ib.market10
    , ib.region_5
	, r5.min_date
	, CAST(datediff(month, r5.min_date, ib.cal_date) AS INT) AS diff_r5
FROM npi_06_month_num_ib_dates  ib
INNER JOIN npi_05_month_num_min_max_dates_r5 r5
ON ib.platform_subset=r5.platform_subset
	AND UPPER(ib.customer_engagement)=UPPER(r5.customer_engagement)
	AND ib.region_5=r5.region_5
),  npi_09_month_num_usage_1 as (


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
FROM "stage"."usage_share_override_landing" a
INNER JOIN npi_08_month_num_date_diff_r5 b
ON a.platform_subset=b.platform_subset
	AND UPPER(a.customer_engagement) = UPPER(b.customer_engagement)
	AND a.month_num=b.diff_r5
	AND a.geography = b.region_5
),  npi_10_month_num_usage_1_2 as (


SELECT DISTINCT a.record
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
	, b.market10
FROM npi_09_month_num_usage_1 a
LEFT JOIN "prod"."iso_country_code_xref" b
ON a.geography=b.region_5
WHERE 1=1
	AND b.region_5 IS NOT NULL
	AND a.geography IN ('EU','NA','LA','AP')
UNION ALL
SELECT DISTINCT a.record
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
	, b.market10
FROM npi_09_month_num_usage_1 a
LEFT JOIN "prod"."iso_country_code_xref" b
ON a.geography=b.region_5
WHERE 1=1
	AND b.region_5 IS NOT NULL
	AND a.geography IN ('JP')
	AND (platform_subset + UPPER(customer_engagement)) NOT IN
		(SELECT * FROM ie2_staging.test.us_overrides_helper_1)
),  npi_04_month_num_min_max_date_m10 as (


SELECT ib.platform_subset
    , cc.market10
    , customer_engagement
    , CAST(min(cal_date) AS DATE) AS min_date
    , CAST(max(cal_date) AS DATE) AS max_date
FROM "prod"."ib" ib
LEFT JOIN "prod"."iso_country_code_xref" cc
on ib.country=cc.country_alpha2
WHERE 1=1
	AND ib.version = '2022.04.25.1'
	AND measure = 'ib'
GROUP BY ib.platform_subset
	, ib.customer_engagement
	, cc.market10
),  npi_11_month_num_usage_2 as (


SELECT a.record
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
    , b.min_date
FROM npi_10_month_num_usage_1_2 a
LEFT JOIN npi_04_month_num_min_max_date_m10 b
ON a.platform_subset= b.platform_subset
    and a.market10=b.market10
    and a.customer_engagement=b.customer_engagement
),  npi_12_month_num_usage_3 as (


SELECT nu.record
    , nu.cal_date
    , nu.geography
    , nu.platform_subset
    , nu.customer_engagement
    , nu.forecast_process_note
    , nu.post_processing_note
    , nu.forecast_created_date
    , nu.data_source
    , nu.version
    , nu.measure
    , nu.units
    , nu.proxy_used
    , nu.ib_version
    , nu.load_date
    , nu.market10
    , nu.min_date
FROM npi_11_month_num_usage_2 nu
WHERE cal_date >= min_date
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
    , a.min_date
FROM npi_12_month_num_usage_3 a
LEFT JOIN "mdm"."hardware_xref" b
    on a.platform_subset=b.platform_subset
WHERE 1=1
   AND b.product_lifecycle_status = 'N'
"""

query_list.append(["stage.usage_share_overrides_normalized_landing", overrides_norm_landing, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Overrides Normalized Fill Landing

# COMMAND ----------

norm_fill_landing = """
with npi_14_fill_missing_ib_data as (


SELECT a.platform_subset
    , b.market10
    , customer_engagement
	, CAST(min(cal_date) AS DATE) AS min_ib_date
	, CAST(max(cal_date) AS DATE) AS max_ib_date
FROM "prod"."ib" a
LEFT JOIN "prod"."iso_country_code_xref" b
ON a.country=country_alpha2
LEFT JOIN "prod"."hardware_xref" c
ON a.platform_subset=c.platform_subset
WHERE 1=1
	AND a.version = '2022.04.25.1'
	AND measure = 'IB'
	AND c.product_lifecycle_status = 'N'
GROUP BY a.platform_subset
    , customer_engagement
	, b.market10
),  npi_15_fill_missing_us_data as (


SELECT platform_subset
    , market10
    , customer_engagement
    , CAST(min(cal_date) AS DATE) AS min_us_date
    , CAST(max(cal_date) AS DATE) AS max_us_date
FROM "stage"."usage_share_overrides_normalized_landing"
WHERE 1=1
GROUP BY platform_subset
    , customer_engagement
    , market10
),  npi_16_fill_missing_dates as (


SELECT a.platform_subset
    , a.market10
    , a.customer_engagement
    , a.min_ib_date
    , a.max_ib_date
    , b.min_us_date
    , b.max_us_date
    , DATEDIFF(MONTH, b.min_us_date, a.min_ib_date) AS min_diff
    , DATEDIFF(MONTH, b.max_us_date, a.max_ib_date) AS max_diff
FROM npi_14_fill_missing_ib_data a
LEFT JOIN npi_15_fill_missing_us_data b
	ON a.platform_subset=b.platform_subset
	AND a.market10=b.market10
	AND a.customer_engagement=b.customer_engagement
WHERE 1=1
	AND b.min_us_date IS NOT NULL
	AND DATEDIFF(MONTH, b.max_us_date, a.max_ib_date) > 0
),  npi_17_fill_missing_calendar_dates as (


SELECT DISTINCT c.date
FROM "prod"."calendar" c
WHERE c.Day_of_Month = 1
),  npi_19_fill_missing_dates_to_expand as (


SELECT md.platform_subset
    , md.market10
    , UPPER(md.customer_engagement) AS customer_engagement
    , date AS cal_date
    , date
FROM npi_16_fill_missing_dates md
CROSS JOIN npi_17_fill_missing_calendar_dates
WHERE 1=1
    AND date > max_us_date
    AND date <= max_ib_date
),  npi_18_fill_missing_max_values as (


SELECT a.platform_subset
	, UPPER(a.customer_engagement) AS customer_engagement
	, a.market10
	, measure
	, units
FROM "stage"."usage_share_overrides_normalized_landing" a
INNER JOIN npi_15_fill_missing_us_data b
	ON a.platform_subset = b.platform_subset
	AND a.customer_engagement = b.customer_engagement
	AND a.market10 = b.market10
	AND a.cal_date = b.max_us_date
),  npi_20_fill_missing_market10_countries as (


SELECT DISTINCT x.region_5
    , x.market10
FROM "prod"."iso_country_code_xref" x
WHERE 1=1
    AND region_5 IS NOT NULL
    AND region_5 <> 'JP'
    AND NOT region_5 LIKE ('X%')
)SELECT 'USAGE_SHARE_NPI' As record
	, CAST(a.cal_date AS DATE) AS cal_date
	, c.region_5 AS geography
	, a.platform_subset
	, a.customer_engagement
    , 'NPI OVERRIDE' AS [forecast_process_note]
    , 'NONE' AS [post_processing_note]
    , CAST(GETDATE() AS DATE) AS [forecast_created_date]
    , 'DATA_SOURCE' AS [data_source]
    , 'VERSION' AS [version]
	, b.measure
	, b.units
    , NULL AS [proxy_used]
    , 'IB_VERSION' AS [ib_version]
    , GETDATE() AS [load_date]
	, a.market10
FROM npi_19_fill_missing_dates_to_expand a
LEFT JOIN npi_18_fill_missing_max_values b
	ON a.platform_subset=b.platform_subset
	AND a.customer_engagement = b.customer_engagement
	AND a.market10 = b.market10
LEFT JOIN npi_20_fill_missing_market10_countries c ON a.market10 = c.market10
WHERE 1=1
"""

query_list.append(["stage.usage_share_overrides_normalized_fill_landing", norm_fill_landing, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Overrides Normalized Final Landing

# COMMAND ----------

norm_final_landing = """
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
FROM "stage"."usage_share_overrides_normalized_landing" nl
UNION ALL
SELECT nfl.record
    , nfl.cal_date
    , nfl.geography
    , nfl.platform_subset
    , nfl.customer_engagement
    , nfl.forecast_process_note
    , nfl.post_processing_note
    , nfl.forecast_created_date
    , nfl.data_source
    , nfl.version
    , nfl.measure
    , nfl.units
    , cast(nfl.proxy_used as varchar) as proxy_used
    , nfl.ib_version
    , nfl.load_date
    , nfl.market10
FROM "stage"."usage_share_overrides_normalized_fill_landing" nfl
"""

query_list.append(["stage.usage_share_overrides_normalized_final_landing", norm_final_landing, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Override NPI Out

# COMMAND ----------

npi_out = """


with npi_23_uss_telemetry as (


SELECT ls.record
    , ls.cal_date
    , ls.geography_grain
    , ls.geography
    , ls.platform_subset
    , ls.customer_engagement
    , ls.measure
    , ls.units
    , ls.ib_version
    , ls.source
    , CAST(NULL AS VARCHAR) AS version
    , CAST(NULL AS VARCHAR) AS load_date
FROM "stage"."uss_03_land_spin" ls
LEFT JOIN "mdm"."hardware_xref" hw
    ON ls.platform_subset = hw.platform_subset
WHERE source = 'Telemetry'
    AND hw.product_lifecycle_status = 'N'
),  npi_24_month_num_override_temp_pre as (


SELECT fl.record
	, fl.cal_date
	, 'market10' AS geography_grain
	, fl.market10 AS geography
	, fl.platform_subset
	, UPPER(fl.customer_engagement) AS customer_engagement
	, fl.measure
	, fl.units
	, CAST(NULL AS VARCHAR) as ib_version
	, 'NPI' as source
    , CAST(NULL AS VARCHAR) AS version
    , CAST(NULL AS VARCHAR) AS load_date
FROM "stage"."usage_share_overrides_normalized_final_landing" as fl
WHERE (fl.platform_subset + customer_engagement + market10 + CONVERT(VARCHAR(20),cal_date,20) + measure) NOT IN
(
    SELECT DISTINCT t1.platform_subset + t1.customer_engagement + t1.geography +
        CONVERT(VARCHAR(10),t1.cal_date,20) + t1.measure
    FROM npi_23_uss_telemetry t1
)
),  npi_25_month_num_override_temp as (


SELECT ust.record
    , ust.cal_date
    , ust.geography_grain
    , ust.geography
    , ust.platform_subset
    , ust.customer_engagement
    , ust.measure
    , ust.units
    , ust.ib_version
    , ust.source
    , ust.version
    , ust.load_date
FROM npi_23_uss_telemetry ust
UNION ALL
SELECT usp.record
    , usp.cal_date
    , usp.geography_grain
    , usp.geography
    , usp.platform_subset
    , usp.customer_engagement
    , usp.measure
    , usp.units
    , usp.ib_version
    , usp.source
    , usp.version
    , usp.load_date
FROM npi_24_month_num_override_temp_pre usp
),  npi_26_hp_share_1 as (


select ust.record
        , ust.cal_date
        , ust.geography_grain
        , ust.geography
        , ust.platform_subset
        , ust.customer_engagement
        , ust.measure
        , 1 as units
        , ust.ib_version
        , ust.source
        , ust.version
        , ust.load_date
    from npi_25_month_num_override_temp as ust
    join ie2_prod.dbo.hardware_xref as hw
        on hw.platform_subset = ust.platform_subset
    where 1=1
        and ust.customer_engagement = 'I-INK'
        and hw.product_lifecycle_status = 'N'
        and ust.measure = 'HP_SHARE'
),  npi_27_hp_share_2 as (


select distinct 'USAGE_SHARE_NPI' as record
    , ust.cal_date
    , ust.geography_grain
    , ust.geography
    , ust.platform_subset
    , ust.customer_engagement
    , max(case when ust.measure = 'HP_SHARE' then 1 else 0 end) as hp_share
    , null as ib_version
    , 'NPI' AS source
    , null as version
    , null as load_date
from npi_25_month_num_override_temp as ust
join "mdm"."hardware_xref" as hw
    on hw.platform_subset = ust.platform_subset
where 1=1
    and ust.customer_engagement = 'I-INK'
    and ust.record in ('USAGE_SHARE_NPI', 'USAGE_SHARE')
    and hw.product_lifecycle_status = 'N'
group by ust.cal_date
    , ust.geography_grain
    , ust.geography
    , ust.platform_subset
    , ust.customer_engagement
),  npi_28_hp_share_3 as (


select ust.record
    , ust.cal_date
    , ust.geography_grain
    , ust.geography
    , ust.platform_subset
    , ust.customer_engagement
    , 'HP_SHARE' as measure
    , 1 as units
    , ust.ib_version
    , ust.source
    , ust.version
    , ust.load_date
from npi_27_hp_share_2 as ust
where 1=1
    and hp_share = 0
),  npi_29_hp_share_4 as (


select ust.record
    , ust.cal_date
    , ust.geography_grain
    , ust.geography
    , ust.platform_subset
    , ust.customer_engagement
    , ust.measure
    , ust.units
    , ust.ib_version
    , ust.source
    , ust.version
    , ust.load_date
from npi_25_month_num_override_temp as ust
left outer join npi_26_hp_share_1 as m26
    on m26.platform_subset = ust.platform_subset
    and m26.cal_date = ust.cal_date
    and m26.geography = ust.geography
    and m26.customer_engagement = ust.customer_engagement
    and m26.measure = ust.measure
where 1=1
    and m26.platform_subset is null
    and m26.cal_date is null
    and m26.geography is null
    and m26.customer_engagement is null
    and m26.measure is null

union all

-- hp_share fix to existing iink/npis
select ust.record
    , ust.cal_date
    , ust.geography_grain
    , ust.geography
    , ust.platform_subset
    , ust.customer_engagement
    , ust.measure
    , ust.units
    , ust.ib_version
    , ust.source
    , ust.version
    , ust.load_date
from npi_26_hp_share_1 as ust

union all

-- additional hp_share records
select ust.record
    , ust.cal_date
    , ust.geography_grain
    , ust.geography
    , ust.platform_subset
    , ust.customer_engagement
    , ust.measure
    , ust.units
    , ust.ib_version
    , ust.source
    , ust.version
    , ust.load_date
from npi_28_hp_share_3 as ust
)select ust.record
    , ust.cal_date
    , ust.geography_grain
    , ust.geography
    , ust.platform_subset
    , ust.customer_engagement
    , ust.measure
    , ust.units
    , ust.ib_version
    , ust.source
    , ust.version
    , ust.load_date
from npi_29_hp_share_4 as ust
"""

query_list.append(["stage.usage_share_override_npi_out", npi_out, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Write Out NPIs

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
