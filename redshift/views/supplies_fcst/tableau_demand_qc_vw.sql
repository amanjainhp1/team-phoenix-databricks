-- supplies_fcst.tableau_demand_qc_vw source

CREATE OR REPLACE VIEW supplies_fcst.tableau_demand_qc_vw AS

WITH __dbt__CTE__tableau_01_ib_cust as (

SELECT
    ib.platform_subset as ib_platform,
    ccx.region_5 as region,
    ib.customer_engagement as ce,
    min(ib.cal_date) as ib_min,
    max(ib.version) as ib_vrsn
FROM prod.ib as ib
JOIN mdm.iso_country_code_xref AS ccx
    ON ib.country = ccx.country_alpha2
WHERE ib.measure = 'IB'
GROUP BY
    ib.platform_subset,
    ccx.region_5,
    ib.customer_engagement
),  __dbt__CTE__tableau_03_hw_cust as (


SELECT
    hw.platform_subset as hw_platform,
    hw.pl as pl,
    hw.technology as tech_raw,
    hw.product_lifecycle_status as pls,
    case
    when hw.pl = 'E0' then 'Y'
    when hw.pl = 'E4' then 'Y'
    when hw.pl = 'ED' then 'Y'
    when hw.pl = 'GW' then 'Y'
    else 'N'
    end as hppk,
	hw.hw_product_family
FROM mdm.hardware_xref AS hw
WHERE hw.technology = 'LASER'
    OR hw.technology = 'INK'
    OR hw.technology = 'PWA'
),  __dbt__CTE__tableau_02_ib_version_cust as (


SELECT max(version) as ib_max_version
FROM prod.ib
),  __dbt__CTE__tableau_04_demand_cust as (


select
    d.platform_subset as dmd_platform,
    ccx.region_5,
    d.customer_engagement as ce,
    min(d.cal_date) as dmd_min,
    max(d.version) as dmd_vrsn
FROM prod.demand as d
JOIN mdm.iso_country_code_xref AS ccx
    ON d.geography = ccx.market10
GROUP BY
    d.platform_subset,
    ccx.region_5,
    d.customer_engagement
),  __dbt__CTE__tableau_05_demand_version_cust as (


SELECT
    d.dmd_platform,
    d.dmd_min,
    d.dmd_vrsn,
    dmd.dmd_max_version
FROM __dbt__CTE__tableau_04_demand_cust as d
JOIN (SELECT max(version) as dmd_max_version
FROM prod.demand) as dmd
ON d.dmd_vrsn=dmd.dmd_max_version
),  __dbt__CTE__tableau_06_us_cust as (


select
    us.platform_subset as us_platform,
    us.geography as geography,
    min(us.cal_date) as us_min,
    max(us.version) as us_vrsn
FROM prod.usage_share as us
GROUP BY
    us.platform_subset,
    us.geography
),  __dbt__CTE__tableau_07_us_version_cust as (


SELECT
    us.us_platform,
    us.us_min,
    us.geography,
    us.us_vrsn,
    us_ver.us_max_version
FROM __dbt__CTE__tableau_06_us_cust as us
JOIN (SELECT max(version) as us_max_version
FROM prod.usage_share) as us_ver
    ON us.us_vrsn=us_ver.us_max_version
)select
    d.dmd_platform,
    d.dmd_min,
    d.dmd_vrsn,
    d.dmd_max_version,
    hw.hw_platform,
    hw.pl,
    hw.tech_raw,
    hw.pls,
    hw.hppk,
	hw.hw_product_family,
    ib.ib_platform,
    ib.region,
    ib.ce,
    ib.ib_min,
    ib.ib_vrsn,
    ib_vrsn.ib_max_version,
    us.us_platform,
    us.geography,
    us.us_min,
    us.us_vrsn,
    us.us_max_version
FROM __dbt__CTE__tableau_01_ib_cust as ib 
JOIN __dbt__CTE__tableau_03_hw_cust as hw
    ON ib.ib_platform = hw.hw_platform
JOIN __dbt__CTE__tableau_02_ib_version_cust as ib_vrsn
    ON ib_vrsn.ib_max_version = ib.ib_vrsn
LEFT JOIN __dbt__CTE__tableau_05_demand_version_cust as d
    ON ib.ib_platform = d.dmd_platform
LEFT JOIN __dbt__CTE__tableau_07_us_version_cust as us
    ON ib.ib_platform = us.us_platform;