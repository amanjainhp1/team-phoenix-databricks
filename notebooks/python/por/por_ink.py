# Databricks notebook source
# MAGIC %md
# MAGIC # POR INK

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

por_actuals_hw = """
select
	acts.record,
	acts.cal_date,
	acts.country_alpha2,
	acts.base_product_number,
	acts.platform_subset,
	acts.base_quantity,
	acts.load_date,
	acts.official,
	acts.version,
	acts.source,
	current_date as por_date,
	'INK' technology 
from prod.actuals_hw as acts
join mdm.hardware_xref as hw
    on hw.platform_subset = acts.platform_subset
where 1=1
    and acts.official = 1
    and hw.technology in ('INK', 'PWA')
"""

query_list.append(["por.por_actuals_hw", por_actuals_hw, "append"])

# COMMAND ----------

por_actuals_supplies = """
select
	ac.record,
	ac.cal_date,
	country_alpha2,
	MARKET10,
	ac.platform_subset,
	base_product_number,
	customer_engagement,
	base_quantity,
	equivalent_units,
	yield_x_units,
	yield_x_units_black_only,
	ac.official,
	ac.load_date,
	ac.version,
	current_date as por_date,
	'INK'
from prod.actuals_supplies ac
join mdm.hardware_xref as hw
    on hw.platform_subset = ac.platform_subset
where 1=1
    and ac.official = 1
    and hw.technology in ('INK', 'PWA')
"""

query_list.append(["por.por_actuals_supplies", por_actuals_supplies, "append"])

# COMMAND ----------

por_ce_splits = """
select
	ce.record,
	ce.platform_subset,
	ce.region_5,
	ce.country_alpha2,
	ce.em_dm,
	ce.business_model,
	ce.month_begin,
	ce.split_name,
	ce.pre_post_flag,
	ce.value,
	ce.load_date,
	ce.version,
	ce.official,
	current_date as por_date,
    'INK'
from prod.ce_splits as ce
join mdm.hardware_xref as hw
    on hw.platform_subset = ce.platform_subset
where 1=1
    and ce.official = 1
    and hw.technology in ('INK', 'PWA')
"""

query_list.append(["por.por_ce_splits", por_ce_splits, "append"])

# COMMAND ----------

por_decay = """
select
	d.record,
	d.platform_subset,
	d.geography,
	d.year,
	d.split_name,
	d.value,
	d.avg_printer_life,
	d.load_date,
	d.version,
	d.official,
	d.geography_grain,
	current_date as por_date,
	'INK'
from prod.decay as d
join mdm.hardware_xref as hw
    on hw.platform_subset = d.platform_subset
where 1=1
    and d.official = 1
    and hw.technology in ('INK', 'PWA')
"""
query_list.append(["por.por_decay", por_decay, "append"])

# COMMAND ----------

por_hardware_xref = """
select
	platform_subset,
	pl,
	technology,
	mono_ppm,
	color_ppm,
	predecessor,
	predecessor_proxy,
	successor,
	vc_category,
	format,
	sf_mf,
	mono_color,
	managed_nonmanaged,
	oem_vendor,
	business_feature,
	business_segment,
	category_feature,
	category_plus,
	product_structure,
	por_ampv,
	intro_date,
	hw_product_family,
	supplies_mkt_cat,
	epa_family,
	brand,
	product_lifecycle_status,
	product_lifecycle_status_usage,
	product_lifecycle_status_share,
	intro_price,
	last_modified_date,
	load_date,
	current_date as por_date
from mdm.hardware_xref
where 1=1
    and technology in ('INK', 'PWA')
"""
query_list.append(["por.por_hardware_xref", por_hardware_xref, "append"])

# COMMAND ----------

por_supplies_hw_mapping = """
select
	shm.record,
	shm.geography_grain,
	shm.geography,
	shm.base_product_number,
	shm.customer_engagement,
	shm.platform_subset,
	shm.load_date,
	shm.official,
	shm.eol,
	shm.eol_date,
	shm.host_multiplier,
    current_date as por_date,
    'INK'
from mdm.supplies_hw_mapping as shm
join mdm.hardware_xref as hw
    on hw.platform_subset = shm.platform_subset
where 1=1
    and hw.technology in ('INK', 'PWA')
"""
query_list.append(["por.por_supplies_hw_mapping", por_supplies_hw_mapping, "append"])

# COMMAND ----------

por_supplies_xref = """
select
	record,
	base_product_number,
	pl,
	cartridge_alias,
	regionalization,
	toner_category,
	"type",
	single_multi,
	crg_chrome,
	k_color,
	crg_intro_dt,
	"size",
	technology,
	supplies_family,
	supplies_group,
	supplies_technology,
	equivalents_multiplier,
	last_modified_date,
	load_date,
	current_date as por_date
from mdm.supplies_xref
where 1=1
and technology in ('INK', 'PWA')
"""
query_list.append(["por.por_supplies_xref", por_supplies_xref, "append"])

# COMMAND ----------

por_yield = """
select
	record,
	geography,
	base_product_number,
	value,
	effective_date,
	load_date,
	version,
	official,
	geography_grain,
	current_date as por_date,
    'INK'
from mdm.yield
where 1=1
    and official = 1
"""
query_list.append(["por.por_yield", por_yield, "append"])

# COMMAND ----------

por_usage_share = """
SELECT usi.geography_grain
    , usi.geography
    , usi.cal_date
    , usi.customer_engagement
    , usi.platform_subset
    , usi.measure
    , usi.units
    , usi.version version
    , usi.ib_version,
	, current_date as por_date
    , 'INK'
FROM scen.ink_03_usage_share AS usi
"""
query_list.append(["por.por_usage_share", por_usage_share, "append"])

# COMMAND ----------

por_pages_ccs_mix_base = """
select
	"type",
	cal_date,
	geography_grain,
	geography,
	p.platform_subset,
	base_product_number,
	customer_engagement,
	mix_rate ,
	composite_key,
		current_date as por_date,
    'INK'
from stage.page_cc_mix p
join mdm.hardware_xref as hw
    on hw.platform_subset = p.platform_subset
where 1=1
    and hw.technology in ('INK', 'PWA')
"""
query_list.append(["por.por_pages_ccs_mix_base", por_pages_ccs_mix_base, "append"])

# COMMAND ----------

por_pages_ccs_mix_working = """
select
    cal_date,
	geography_grain,
	geography,
	platform_subset,
	base_product_number,
	customer_engagement,
	mix_rate,
	"type",
	single_multi,
	current_date as por_date,
    'INK'
from scen.ink_06_mix_rate_final
"""
query_list.append(["por.por_pages_ccs_mix_working", por_pages_ccs_mix_working, "append"])

# COMMAND ----------

por_channel_fill = """
with __dbt__CTE__s_ink_01_filter_vars as (
    SELECT DISTINCT 'SCENARIO_CHANNEL_FILL' AS record
        , cf.user_name AS user_name
        , cf.load_date AS load_date
        , CAST(cf.load_date AS VARCHAR(50)) AS version
    FROM scen.working_forecast_channel_fill AS cf
    WHERE 1=1
        AND cf.upload_type = 'WORKING-FORECAST'
        AND CAST(cf.load_date AS DATE) > CAST('2021-05-01' AS DATE)
        AND cf.user_name IN ('ANAA','ANA.ANAYA@HP.COM', 'SAIMANK', 'SAIMAN.KUSIN@HP.COM', 'SONSEEAHRAY', 'SONSEEAHRAY.RUCKER@HP.COM', 'ZACP', 'ZACHARY.PEAKE@HP.COM')
)

,  __dbt__CTE__s_ink_11_channel_fill_prep as (
    SELECT fv.user_name
        , MAX(fv.load_date) AS max_load_date
    FROM __dbt__CTE__s_ink_01_filter_vars AS fv
    WHERE 1=1
        AND fv.user_name <> 'SYSTEM'
        AND fv.record = 'SCENARIO_CHANNEL_FILL'
    GROUP BY fv.user_name
)

,  __dbt__CTE__shm_mapping as (
    SELECT DISTINCT shm.platform_subset
        , shm.base_product_number
        , shm.customer_engagement
    FROM mdm.supplies_hw_mapping AS shm
    WHERE 1=1
        AND shm.official = 1
)

,  __dbt__CTE__c2c_02_geography_mapping as (
    SELECT 'CENTRAL EUROPE' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'GREATER ASIA' AS market_10, 'AP' AS region_5 UNION ALL
    SELECT 'INDIA SL & BLL' AS market_10, 'AP' AS region_5 UNION ALL
    SELECT 'ISE' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'LATIN AMERICA' AS market_10, 'LA' AS region_5 UNION ALL
    SELECT 'NORTH AMERICA' AS market_10, 'NA' AS region_5 UNION ALL
    SELECT 'Northern Europe' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'SOUTHERN EUROPE' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'UK&I' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'GREATER CHINA' AS market_10, 'AP' AS region_5
)

,  __dbt__CTE__s_ink_12_channel_fill_subset as (
    SELECT DISTINCT DATEADD(month, cf.month_num, cf.min_sys_date) AS cal_date
        , cf.geography
        , cf.platform_subset
        , cf.base_product_number
        , cf.customer_engagement
        , cf.value AS channel_fill
        , cf.user_name
        , cf.load_date
    FROM scen.working_forecast_channel_fill AS cf
    JOIN __dbt__CTE__s_ink_11_channel_fill_prep AS cfp
        ON cfp.user_name = cf.user_name
        AND cfp.max_load_date = cf.load_date
    JOIN __dbt__CTE__shm_mapping AS shm
        ON shm.base_product_number = cf.base_product_number
        AND shm.platform_subset = cf.platform_subset
        AND shm.customer_engagement = cf.customer_engagement
    WHERE 1=1
        AND cf.upload_type = 'WORKING-FORECAST'
        AND cf.geography_grain = 'MARKET10'

    UNION ALL

    SELECT DISTINCT DATEADD(month, cf.month_num, cf.min_sys_date) AS cal_date
        , geo.market_10 AS geography
        , cf.platform_subset
        , cf.base_product_number
        , cf.customer_engagement
        , cf.value AS channel_fill
        , cf.user_name
        , cf.load_date
    FROM scen.working_forecast_channel_fill AS cf
    JOIN __dbt__CTE__s_ink_11_channel_fill_prep AS cfp
        ON cfp.user_name = cf.user_name
        AND cfp.max_load_date = cf.load_date
    JOIN __dbt__CTE__shm_mapping AS shm
        ON shm.base_product_number = cf.base_product_number
        AND shm.platform_subset = cf.platform_subset
        AND shm.customer_engagement = cf.customer_engagement
    JOIN __dbt__CTE__c2c_02_geography_mapping AS geo
        ON geo.region_5 = cf.geography
    WHERE 1=1
        AND cf.upload_type = 'WORKING-FORECAST'
        AND cf.geography_grain = 'REGION_5'
)


SELECT DISTINCT cfs.cal_date
    , cfs.geography
    , cfs.platform_subset
    , cfs.base_product_number
    , cfs.customer_engagement
    , cfs.channel_fill
    , cfs.user_name
    , cfs.load_date
	, current_date as por_date,
    , 'INK'
FROM __dbt__CTE__s_ink_12_channel_fill_subset AS cfs
WHERE 1=1
"""
query_list.append(["por.por_channel_fill", por_channel_fill, "append"])

# COMMAND ----------

por_vtc = """
with __dbt__CTE__s_ink_01_filter_vars as (
    SELECT DISTINCT 'SCENARIO_VTC_OVERRIDE' AS record
        , v.user_name AS user_name
        , v.load_date AS load_date
        , CAST(v.load_date AS VARCHAR(50)) AS version
    FROM scen.working_forecast_vtc_override AS v
    WHERE 1=1
        AND v.upload_type = 'WORKING-FORECAST'
        AND CAST(v.load_date AS DATE) > CAST('2021-05-01' AS DATE)
        AND v.user_name IN ('ANAA','ANA.ANAYA@HP.COM', 'SAIMANK', 'SAIMAN.KUSIN@HP.COM', 'SONSEEAHRAY', 'SONSEEAHRAY.RUCKER@HP.COM', 'ZACP', 'ZACHARY.PEAKE@HP.COM')
)

,  __dbt__CTE__s_ink_17_vtc_prep as (
    SELECT fv.user_name
        , MAX(fv.load_date) AS max_load_date
    FROM __dbt__CTE__s_ink_01_filter_vars AS fv
    WHERE 1=1
        AND fv.user_name <> 'SYSTEM'
        AND fv.record = 'SCENARIO_VTC_OVERRIDE'
    GROUP BY fv.user_name
)

,  __dbt__CTE__c2c_02_geography_mapping as (
    SELECT 'CENTRAL EUROPE' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'GREATER ASIA' AS market_10, 'AP' AS region_5 UNION ALL
    SELECT 'INDIA SL & BLL' AS market_10, 'AP' AS region_5 UNION ALL
    SELECT 'ISE' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'LATIN AMERICA' AS market_10, 'LA' AS region_5 UNION ALL
    SELECT 'NORTH AMERICA' AS market_10, 'NA' AS region_5 UNION ALL
    SELECT 'Northern Europe' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'SOUTHERN EUROPE' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'UK&I' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'GREATER CHINA' AS market_10, 'AP' AS region_5
)

,  __dbt__CTE__s_ink_18_vtc_subset as (
    SELECT v.geography
        , v.base_product_number
        , v.min_sys_date
        , v.month_num
        , v.value
        , v.user_name
        , v.load_date
    FROM scen.working_forecast_vtc_override AS v
    JOIN __dbt__CTE__s_ink_17_vtc_prep AS vp
        ON vp.user_name = v.user_name
        AND vp.max_load_date = v.load_date
    WHERE 1=1
        AND v.upload_type = 'WORKING-FORECAST'
        AND v.geography_grain = 'MARKET10'

    UNION ALL

    SELECT geo.market_10 AS geography
        , v.base_product_number
        , v.min_sys_date
        , v.month_num
        , v.value
        , v.user_name
        , v.load_date
    FROM scen.working_forecast_vtc_override AS v
    JOIN __dbt__CTE__s_ink_17_vtc_prep AS vp
        ON vp.user_name = v.user_name
        AND vp.max_load_date = v.load_date
    JOIN __dbt__CTE__c2c_02_geography_mapping AS geo
        ON geo.region_5 = v.geography
    WHERE 1=1
        AND v.upload_type = 'WORKING-FORECAST'
        AND v.geography_grain = 'REGION_5'
)


SELECT DISTINCT DATEADD(month, v.month_num, v.min_sys_date) AS cal_date
    , v.geography
    , v.base_product_number
    , v.value as mvtc
    , CAST(DATEADD(month, v.month_num, v.min_sys_date) AS VARCHAR) + ' ' + v.geography + ' ' + v.base_product_number as composite_key
    , v.user_name
    , v.load_date
	, current_date as por_date
    , 'INK'
FROM __dbt__CTE__s_ink_18_vtc_subset AS v
WHERE 1=1
"""
query_list.append(["por.por_vtc", por_vtc, "append"])

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

# COMMAND ----------


