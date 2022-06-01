# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share 01 
# MAGIC - Landing
# MAGIC - Spin Landing
# MAGIC - PRE Override

# COMMAND ----------

# for interactive sessions, define a version widget
dbutils.widgets.text("version", "2022.05.13.1")

# COMMAND ----------

# retrieve version from widget
version = dbutils.widgets.get("version")

# COMMAND ----------

query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Landing

# COMMAND ----------

usg_shr_landing = """
SELECT lnd.record
    , lnd.cal_date
    , lnd.geography_grain
    , lnd.geography
    , lnd.platform_subset
    , lnd.customer_engagement
    , lnd.forecast_process_note
    , lnd.forecast_created_date
    , lnd.data_source
    , (SELECT MAX(version) FROM "prod"."version" WHERE record = 'USAGE_SHARE') as version
    , lnd.measure
    , lnd.units
    , lnd.proxy_used
    , lnd.ib_version
    -- , '2022.05.12.1' as ib_version -- UPDATE 03-30-2022 because toner and ink are different versions in the version table
    , (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'USAGE_SHARE') as load_date
FROM "phoenix_spectrum_itg"."cupsm" lnd
"""

query_list.append(["stage.usage_share_landing", usg_shr_landing, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Spin Landing

# COMMAND ----------

usg_shr_spin_landing = """
SELECT spin.record
    , spin.cal_date
    , spin.geography_grain
    , spin.geography
    , spin.platform_subset
    , spin.customer_engagement
    , spin.forecast_process_note
    , spin.forecast_created_date
    , spin.data_source
    , (SELECT MAX(version) FROM "prod"."version" WHERE record = 'USAGE_SHARE') as version
    , spin.measure
    , spin.units
    , spin.proxy_used
    , spin.ib_version
    , (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'USAGE_SHARE') as load_date
FROM "prod"."usage_share_country_spin" spin
"""

query_list.append(["stage.usage_share_spin_landing", usg_shr_spin_landing, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Staging Landing

# COMMAND ----------

land_out = f"""
with land_03_helper_1 as (


SELECT us.record
	, us.cal_date
	, us.geography
	, us.platform_subset
	, us.customer_engagement
	, MIN(CASE WHEN us.measure='USAGE' THEN
		CASE WHEN us.data_source='DASHBOARD' THEN 'TELEMETRY'
		WHEN us.data_source LIKE 'UPM%' THEN 'MODELLED' END
		ELSE NULL END) AS data_source_u
	, MIN(CASE WHEN us.measure='USAGE' THEN
		CASE WHEN us.data_source='Dashboard' THEN 'TELEMETRY'
		WHEN us.data_source LIKE 'UPM%' THEN 'MODELLED' END
		ELSE NULL END) AS data_source_c
	, MIN(CASE WHEN us.measure='USAGE' THEN
		CASE WHEN us.data_source='DASHBOARD' THEN 'TELEMETRY'
		WHEN us.data_source LIKE 'UPM%' THEN 'MODELLED' END
		ELSE NULL END) AS data_source_k
	, MIN(CASE WHEN us.measure='HP_SHARE' THEN
		CASE WHEN us.data_source='HAVE DATA' THEN 'TELEMETRY'
		WHEN us.data_source LIKE 'MODEL%' THEN 'MODELLED'
		WHEN us.data_source LIKE 'PROXIED%' THEN 'MODELLED' END
		ELSE NULL END) AS data_source_s
	, SUM(CASE WHEN us.measure='USAGE' THEN us.units ELSE 0 END) AS Usage
    , SUM(CASE WHEN us.measure='HP_SHARE' THEN us.units ELSE 0 END) AS Page_Share
    , SUM(CASE WHEN us.measure='COLOR_USAGE' THEN us.units ELSE 0 END) AS Usage_c
	, SUM(CASE WHEN us.measure='K_USAGE' THEN us.units ELSE 0 END) AS Usage_k
	, us.ib_version
FROM "stage"."usage_share_landing" us   -- usage_share_steady_state_landing_temp     possible global variable
GROUP BY us.record
	, us.cal_date
	, us.geography
	, us.platform_subset
	, us.customer_engagement
	, us.ib_version
),  land_04_helper_2 as (


SELECT us.record
	, us.cal_date
	, us.geography
	, cc.market10
	, us.platform_subset
	, us.customer_engagement
	, us.data_source_u
	, us.data_source_s
	, us.data_source_c
	, us.data_source_k
	, us.Usage
	, us.Page_Share
	, us.Usage_c
	, us.Usage_k
	, ib.units as ib
	, us.ib_version
FROM land_03_helper_1 us
LEFT JOIN "prod"."ib" ib
    ON us.cal_date=ib.cal_date
    AND us.geography=ib.country_alpha2
    AND us.platform_subset=ib.platform_subset
    AND ib.version='{version}'
    AND ib.customer_engagement = us.customer_engagement
LEFT JOIN "mdm"."iso_country_code_xref" cc
    ON us.geography=cc.country_alpha2
),  land_05_helper_3 as (


SELECT h2.record
	, h2.cal_date
	, h2.geography
	, h2.market10
	, h2.data_source_u
	, h2.data_source_s
	, h2.data_source_c
	, h2.data_source_k
	, h2.platform_subset
	, h2.customer_engagement
	, Usage*COALESCE(ib,0) AS Usage_Num
	, h2.Page_Share*Usage*COALESCE(ib,0) AS PS_Num
	, h2.Usage_c*COALESCE(ib,0) AS UsageC_Num
	, h2.Usage_k*COALESCE(ib,0) AS UsageK_Num
	, COALESCE(ib,0) as ib
	, h2.ib_version
FROM land_04_helper_2 h2
),  land_06_helper_4 as (


SELECT h3.record
	, h3.cal_date
	, h3.market10
	, MIN(data_source_u) AS data_source_u
	, MIN(data_source_s) AS data_source_s
	, MIN(data_source_c) AS data_source_c
	, MIN(data_source_k) AS data_source_k
	, h3.platform_subset
	, h3.customer_engagement
	, SUM(Usage_Num) AS Usage_Num
	, SUM(PS_Num) AS PS_Num
	, SUM(UsageC_Num) AS UsageC_Num
	, SUM(UsageK_Num) AS UsageK_Num
	, SUM(ib) AS ib
	, h3.ib_version
FROM land_05_helper_3 h3
GROUP BY h3.record
		, h3.cal_date
		, h3.market10
		, h3.platform_subset
		, h3.customer_engagement
		, h3.ib_version
),  land_07_helper_5 as (


SELECT h4.record
	, h4.cal_date
	, h4.market10
	, h4.data_source_u
	, h4.data_source_s
	, h4.data_source_c
	, h4.data_source_k
	, h4.platform_subset
	, h4.customer_engagement
	, h4.Usage_Num/nullif(ib,0) AS Usage
	, h4.PS_Num/nullif(Usage_Num,0) AS Page_Share
	, h4.UsageC_Num/nullif(ib,0) AS Usage_C
	, h4.UsageK_Num/nullif(ib,0) AS Usage_K
	, h4.ib_version
FROM land_06_helper_4 h4
),  land_08_helper_6 as (


SELECT record
	, cal_date
	, market10
	, platform_subset
	, customer_engagement
	, ib_version
	, 'USAGE' as measure
	, Usage as units
	, data_source_u as source
FROM land_07_helper_5
WHERE Usage IS NOT NULL
    AND Usage > 0
UNION ALL
SELECT record
	, cal_date
	, market10
	, platform_subset
	, customer_engagement
	, ib_version
	, 'HP_SHARE' as measure
	, Page_Share as units
	, data_source_s as source
FROM land_07_helper_5
WHERE Page_Share IS NOT NULL
    AND Page_Share > 0
UNION ALL
SELECT record
	, cal_date
	, market10
	, platform_subset
	, customer_engagement
	, ib_version
	, 'COLOR_USAGE' as measure
	, Usage_C as units
	, data_source_c as source
FROM land_07_helper_5
WHERE Usage_C IS NOT NULL
    AND Usage_C > 0
UNION ALL
SELECT record
	, cal_date
	, market10
	, platform_subset
	, customer_engagement
	, ib_version
	, 'K_USAGE' as measure
	, Usage_K as units
	, data_source_k as source
FROM land_07_helper_5
WHERE Usage_K IS NOT NULL
    AND Usage_K > 0
)SELECT h6.record
    , h6.cal_date
    , 'MARKET10' AS geography_grain
    , h6.market10
    , h6.platform_subset
    , h6.customer_engagement
    , h6.measure
    , h6.units
    , h6.ib_version
    , h6.source
FROM land_08_helper_6 h6
"""

query_list.append(["stage.usage_share_staging_landing", land_out, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Staging Spin

# COMMAND ----------

spin_land_out = f"""
with spin_01_helper_1 as (


SELECT us.record
	, us.cal_date
	, us.geography
	, us.platform_subset
	, us.customer_engagement
	, MIN(CASE WHEN us.measure='USAGE' THEN
		CASE WHEN us.data_source='DASHBOARD' THEN 'TELEMETRY'
		WHEN us.data_source = 'UPM' THEN 'MODELLED' END
		ELSE NULL END) AS data_source_u
	, MIN(CASE WHEN us.measure='USAGE' THEN
		CASE WHEN us.data_source='DASHBOARD' THEN 'TELEMETRY'
		WHEN us.data_source = 'UPM' THEN 'MODELLED' END
		ELSE NULL END) AS data_source_c
	, MIN(CASE WHEN us.measure='USAGE' THEN
		CASE WHEN us.data_source='Dashboard' THEN 'TELEMETRY'
		WHEN us.data_source = 'UPM' THEN 'MODELLED' END
		ELSE NULL END) AS data_source_k
	, MIN(CASE WHEN us.measure='HP_SHARE' THEN
		CASE WHEN us.data_source='HAVE DATA' THEN 'TELEMETRY'
		WHEN us.data_source='MODELED BY PROXY' THEN 'MODELLED'
		WHEN us.data_source='MODELED' THEN 'MODELLED' END
		ELSE NULL END) AS data_source_s
	, SUM(CASE WHEN us.measure='USAGE' THEN us.units ELSE 0 END) AS Usage
    , SUM(CASE WHEN us.measure='HP_SHARE' THEN us.units ELSE 0 END) AS Page_Share
    , SUM(CASE WHEN us.measure='COLOR_USAGE' THEN us.units ELSE 0 END) AS Usage_c
	, SUM(CASE WHEN us.measure='K_USAGE' THEN us.units ELSE 0 END) AS Usage_k
	, us.ib_version
FROM "stage"."usage_share_spin_landing" us
GROUP BY us.record
	, us.cal_date
	, us.geography
	, us.platform_subset
	, us.customer_engagement
	, us.ib_version
),  spin_02_helper_2 as (


SELECT us.record
    , us.cal_date
    , us.geography
    , cc.market10
    , us.platform_subset
    , us.customer_engagement
    , us.data_source_u
    , us.data_source_s
    , us.data_source_c
    , us.data_source_k
    , us.Usage
    , us.Page_Share
    , us.Usage_c
    , us.Usage_k
    , ib.units as ib
    , us.ib_version
FROM spin_01_helper_1 us
LEFT JOIN "prod"."ib" ib
    ON us.cal_date=ib.cal_date
    AND us.geography=ib.country_alpha2
    AND us.platform_subset=ib.platform_subset
    AND ib.version='{version}'
LEFT JOIN "mdm"."iso_country_code_xref" cc
    ON us.geography=cc.country_alpha2
),  spin_03_helper_3 as (


SELECT h2.record
	, h2.cal_date
	, h2.geography
	, h2.market10
	, h2.data_source_u
	, h2.data_source_s
	, h2.data_source_c
	, h2.data_source_k
	, h2.platform_subset
	, h2.customer_engagement
	, Usage*COALESCE(ib,0) AS Usage_Num
	, h2.Page_Share*Usage*COALESCE(ib,0) AS PS_Num
	, h2.Usage_c*COALESCE(ib,0) AS UsageC_Num
	, h2.Usage_k*COALESCE(ib,0) AS UsageK_Num
	, COALESCE(ib,0) as ib
	, h2.ib_version
FROM spin_02_helper_2 h2
),  spin_04_helper_4 as (


SELECT h3.record
	, h3.cal_date
	, h3.market10
	, MIN(data_source_u) AS data_source_u
	, MIN(data_source_s) AS data_source_s
	, MIN(data_source_c) AS data_source_c
	, MIN(data_source_k) AS data_source_k
	, h3.platform_subset
	, h3.customer_engagement
	, SUM(Usage_Num) AS Usage_Num
	, SUM(PS_Num) AS PS_Num
	, SUM(UsageC_Num) AS UsageC_Num
	, SUM(UsageK_Num) AS UsageK_Num
	, SUM(ib) AS ib
	, h3.ib_version
FROM spin_03_helper_3 h3
GROUP BY h3.record
    , h3.cal_date
    , h3.market10
    , h3.platform_subset
    , h3.customer_engagement
    , h3.ib_version
),  spin_05_helper_5 as (


SELECT h4.record
	, h4.cal_date
	, h4.market10
	, h4.data_source_u
	, h4.data_source_s
	, h4.data_source_c
	, h4.data_source_k
	, h4.platform_subset
	, h4.customer_engagement
	, h4.Usage_Num/nullif(ib,0) AS Usage
	, h4.PS_Num/nullif(Usage_Num,0) AS Page_Share
	, h4.UsageC_Num/nullif(ib,0) AS Usage_C
	, h4.UsageK_Num/nullif(ib,0) AS Usage_K
	, h4.ib_version
FROM spin_04_helper_4 h4
),  spin_06_helper_6 as (


SELECT record
	, cal_date
	, market10
	, platform_subset
	, customer_engagement
	, ib_version
	, 'USAGE' as measure
	, Usage as units
	, data_source_u as source
FROM spin_05_helper_5
WHERE Usage IS NOT NULL
    AND Usage > 0
UNION ALL
SELECT record
	, cal_date
	, market10
	, platform_subset
	, customer_engagement
	, ib_version
	, 'HP_SHARE' as measure
	, Page_Share as units
	, data_source_s as source
FROM spin_05_helper_5
WHERE Page_Share IS NOT NULL
    AND Page_Share > 0
UNION ALL
SELECT record
	, cal_date
	, market10
	, platform_subset
	, customer_engagement
	, ib_version
	, 'COLOR_USAGE' as measure
	, Usage_C as units
	, data_source_c as source
FROM spin_05_helper_5
WHERE Usage_C IS NOT NULL
    AND Usage_C > 0
UNION ALL
SELECT record
	, cal_date
	, market10
	, platform_subset
	, customer_engagement
	, ib_version
	, 'K_USAGE' as measure
	, Usage_K as units
	, data_source_k as source
FROM spin_05_helper_5
WHERE Usage_K IS NOT NULL
    AND Usage_K > 0
)SELECT h6.record
    , h6.cal_date
    , 'MARKET10' AS geography_grain
    , h6.market10
    , h6.platform_subset
    , h6.customer_engagement
    , h6.measure
    , h6.units
    , h6.ib_version
    , h6.source
FROM spin_06_helper_6 h6
"""

query_list.append(["stage.usage_share_staging_spin", spin_land_out, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Usage Share Staging 01 Land Spin

# COMMAND ----------

uss_01 = """
SELECT usl.record
    , usl.cal_date
    , usl.geography_grain
    , usl.market10 as geography
    , usl.platform_subset
    , usl.customer_engagement
    , usl.measure
    , usl.units
    , usl.ib_version
    , usl.source
FROM "stage"."usage_share_staging_landing" usl
UNION ALL
SELECT uss.record
    , uss.cal_date
    , uss.geography_grain
    , uss.market10 as geography
    , uss.platform_subset
    , uss.customer_engagement
    , uss.measure
    , uss.units
    , uss.ib_version
    , uss.source
FROM "stage"."usage_share_staging_spin" uss
"""

query_list.append(["stage.uss_01_land_spin", uss_01, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Staging PRE Override

# COMMAND ----------

pre_override = """
SELECT us.record
    , us.cal_date
    , geography_grain
    , us.geography
    , us.platform_subset
    , us.customer_engagement
    , us.measure
    , us.units
    , us.ib_version
    , us.source
FROM "stage"."uss_01_land_spin" us
"""

query_list.append(["stage.usage_share_staging_pre_override", pre_override, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Share Write Out Landing

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list

# COMMAND ----------


