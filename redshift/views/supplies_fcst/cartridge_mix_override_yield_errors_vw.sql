-- supplies_fcst.cartridge_mix_override_yield_errors_vw source
CREATE OR REPLACE VIEW supplies_fcst.cartridge_mix_override_yield_errors_vw AS with __dbt__CTE__c2c_02_geography_mapping as (
        SELECT DISTINCT market10,
            region_5
        FROM mdm.iso_country_code_xref
        where 1 = 1
            and market10 is not null
            and market10 not in ('WORLD WIDE')
            and region_5 not like 'X%'
            and region_5 not in ('JP')
    ),
    __dbt__CTE__or_01_prod_crg_mix_region5 as (
        SELECT cmo.cal_date,
            map.market10 AS market10,
            cmo.platform_subset,
            cmo.crg_base_prod_number,
            cmo.customer_engagement,
            cmo.mix_pct,
            CASE
                WHEN NOT sup.k_color IS NULL THEN sup.k_color
                WHEN sup.k_color IS NULL
                AND sup.crg_chrome IN ('K', 'BLK') THEN 'BLACK'
                WHEN sup.k_color IS NULL
                AND sup.crg_chrome IN ('C', 'CYN', 'M', 'MAG', 'Y', 'YEL') THEN 'COLOR'
                ELSE NULL
            END AS k_color,
            CASE
                WHEN sup.crg_chrome = 'C' THEN 'CYN'
                WHEN sup.crg_chrome = 'M' THEN 'MAG'
                WHEN sup.crg_chrome = 'Y' THEN 'YEL'
                WHEN sup.crg_chrome = 'K' THEN 'BLK'
                ELSE sup.crg_chrome
            END AS crg_chrome,
            CASE
                WHEN sup.crg_chrome IN ('DRUM') THEN 'DRUM'
                ELSE 'CARTRIDGE'
            END AS consumable_type,
            cmo.load_date
        FROM prod.cartridge_mix_override AS cmo
            JOIN mdm.supplies_xref AS sup ON cmo.crg_base_prod_number = sup.base_product_number
            JOIN mdm.hardware_xref AS hw ON hw.platform_subset = cmo.platform_subset
            JOIN __dbt__CTE__c2c_02_geography_mapping AS map ON map.region_5 = cmo.geography
        WHERE 1 = 1
            AND cmo.official = 1
            AND cmo.geography_grain = 'REGION_5'
            AND NOT sup.crg_chrome IN ('HEAD', 'UNK')
            AND hw.product_lifecycle_status = 'N'
            AND hw.technology IN ('LASER', 'INK', 'PWA')
    ),
    __dbt__CTE__or_02_prod_crg_mix_market10 as (
        SELECT cmo.cal_date,
            cmo.geography AS market10,
            cmo.platform_subset,
            cmo.Crg_Base_Prod_Number,
            cmo.customer_engagement,
            cmo.mix_pct,
            CASE
                WHEN NOT sup.k_color IS NULL THEN sup.k_color
                WHEN sup.k_color IS NULL
                AND sup.crg_chrome IN ('K', 'BLK') THEN 'BLACK'
                WHEN sup.k_color IS NULL
                AND sup.crg_chrome IN ('C', 'CYN', 'M', 'MAG', 'Y', 'YEL') THEN 'COLOR'
                ELSE NULL
            END AS k_color,
            CASE
                WHEN sup.crg_chrome = 'C' THEN 'CYN'
                WHEN sup.crg_chrome = 'M' THEN 'MAG'
                WHEN sup.crg_chrome = 'Y' THEN 'YEL'
                WHEN sup.crg_chrome = 'K' THEN 'BLK'
                ELSE sup.crg_chrome
            END AS crg_chrome,
            CASE
                WHEN sup.crg_chrome IN ('DRUM') THEN 'DRUM'
                ELSE 'CARTRIDGE'
            END AS consumable_type,
            cmo.load_date
        FROM prod.cartridge_mix_override AS cmo
            JOIN mdm.supplies_xref AS sup ON cmo.Crg_base_prod_number = sup.base_product_number
            JOIN mdm.hardware_xref AS hw ON hw.platform_subset = cmo.platform_subset
        WHERE 1 = 1
            AND cmo.official = 1
            AND cmo.geography_grain = 'MARKET10'
            AND NOT sup.crg_chrome IN ('HEAD', 'UNK')
            AND hw.product_lifecycle_status = 'N'
            AND hw.technology IN ('LASER', 'INK', 'PWA')
    ),
    __dbt__CTE__or_03_prod_crg_mix as (
        SELECT cmo.cal_date,
            cmo.market10,
            cmo.platform_subset,
            cmo.Crg_Base_Prod_Number,
            cmo.customer_engagement,
            cmo.mix_pct,
            cmo.k_color,
            cmo.crg_chrome,
            cmo.consumable_type,
            cmo.load_date
        FROM __dbt__CTE__or_01_prod_crg_mix_region5 AS cmo
        UNION ALL
        SELECT cmo.cal_date,
            cmo.market10,
            cmo.platform_subset,
            cmo.Crg_Base_Prod_Number,
            cmo.customer_engagement,
            cmo.mix_pct,
            cmo.k_color,
            cmo.crg_chrome,
            cmo.consumable_type,
            cmo.load_date
        FROM __dbt__CTE__or_02_prod_crg_mix_market10 AS cmo
    ),
    __dbt__CTE__or_04_prod_crg_mix_filter as (
        SELECT cmo.cal_date,
            cmo.market10,
            cmo.platform_subset,
            cmo.customer_engagement,
            cmo.k_color,
            MAX(cmo.load_date) AS filter
        FROM __dbt__CTE__or_03_prod_crg_mix AS cmo
        GROUP BY cmo.cal_date,
            cmo.market10,
            cmo.platform_subset,
            cmo.customer_engagement,
            cmo.k_color
    ),
    __dbt__CTE__c2c_03_yield as (
        SELECT y.base_product_number,
            map.market10 -- NOTE: assumes effective_date is in YYYYMM format. Multiplying by 100 and adding 1 to get to YYYYMMDD
,
            y.effective_date,
            COALESCE(
                LEAD(effective_date) OVER (
                    PARTITION BY y.base_product_number,
                    map.market10
                    ORDER BY y.effective_date
                ),
                CAST('2119-08-30' AS DATE)
            ) AS next_effective_date,
            y.value AS yield
        FROM mdm.yield AS y
            JOIN __dbt__CTE__c2c_02_geography_mapping AS map ON map.region_5 = y.geography
        WHERE 1 = 1
            AND y.official = 1
            AND y.geography_grain = 'REGION_5'
    ),
    __dbt__CTE__c2c_01_crg_months as (
        SELECT date_key,
            date AS cal_date
        FROM mdm.calendar
        WHERE 1 = 1
            AND day_of_month = 1
    ),
    __dbt__CTE__c2c_04_pen_fills as (
        SELECT y.base_product_number,
            m.cal_date,
            y.market10,
            y.yield
        FROM __dbt__CTE__c2c_03_yield AS y
            JOIN __dbt__CTE__c2c_01_crg_months AS m ON y.effective_date <= m.cal_date
            AND y.next_effective_date > m.cal_date
    )
SELECT cmo.base_product_number,
    cmo.market10,
    cmo.min_cal_date as cmo_min_cal_date,
    pf.min_cal_date as yd_min_cal_date
FROM (
        SELECT cmo.market10,
            cmo.Crg_Base_Prod_Number AS base_product_number,
            MIN(cmo.cal_date) AS min_cal_date
        FROM __dbt__CTE__or_03_prod_crg_mix AS cmo
            JOIN __dbt__CTE__or_04_prod_crg_mix_filter AS filter ON filter.cal_date = cmo.cal_date
            AND filter.market10 = cmo.market10
            AND filter.platform_subset = cmo.platform_subset
            AND filter.customer_engagement = cmo.customer_engagement
            AND filter.k_color = cmo.k_color
            AND filter.filter = cmo.load_date
        GROUP BY cmo.market10,
            cmo.crg_Base_Prod_Number
    ) AS cmo
    JOIN (
        SELECT pf.base_product_number,
            pf.market10,
            MIN(pf.cal_date) AS min_cal_date
        FROM __dbt__CTE__c2c_04_pen_fills AS pf
        GROUP BY pf.base_product_number,
            pf.market10
    ) AS pf -- only base_product_number with a match; we have other tests to catch dropout
    ON cmo.base_product_number = pf.base_product_number
    AND cmo.market10 = pf.market10
    AND cmo.min_cal_date < pf.min_cal_date -- test condition
WHERE 1 = 1;