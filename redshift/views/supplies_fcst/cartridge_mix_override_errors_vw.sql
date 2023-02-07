-- supplies_fcst.cartridge_mix_override_errors_vw source

CREATE OR REPLACE VIEW supplies_fcst.cartridge_mix_override_errors_vw AS

WITH geo_map AS
(
    SELECT DISTINCT market10
        , region_5
    FROM mdm.iso_country_code_xref
    where 1=1
        and market10 IS NOT NULL
        and market10 NOT IN ('WORLD WIDE')
        and region_5 NOT LIKE 'X%'
        and region_5 NOT IN ('JP')
)

, src AS
(
    SELECT DISTINCT cmo.cal_date
        , cmo.platform_subset
        , cmo.crg_base_prod_number
        , COALESCE(g.market10, cmo.geography) AS geography
        , CASE WHEN NOT sup.k_color IS NULL THEN sup.k_color
               WHEN sup.k_color IS NULL AND sup.crg_chrome IN ('K', 'BLK') THEN 'BLACK'
               WHEN sup.k_color IS NULL AND sup.crg_chrome IN ('C', 'CYN', 'M', 'MAG', 'Y', 'YEL') THEN 'COLOR'
               ELSE NULL END AS k_color
        , CASE WHEN sup.crg_chrome IN ('DRUM') THEN 'DRUM'
               ELSE 'CARTRIDGE' END AS consumable_type
        , cmo.customer_engagement
        , cmo.load_date
        , cmo.mix_pct
        , hw.technology
    FROM prod.cartridge_mix_override AS cmo
    JOIN mdm.supplies_xref AS sup
        ON sup.base_product_number = cmo.Crg_Base_Prod_Number
    JOIN mdm.hardware_xref AS hw
        ON hw.platform_subset = cmo.platform_subset
    LEFT JOIN geo_map AS g  -- blow out from region_5 to market10
        ON g.region_5 = cmo.geography
        AND cmo.geography_grain = 'REGION_5'
    WHERE 1=1
        AND cmo.official = 1
        AND hw.technology IN ('INK', 'PWA', 'LASER')
)

-- select * from src where 1=1 and cal_date = '2021-03-01' and platform_subset = 'EYRIE STND CNIN'

, src_max as
(
    SELECT cal_date
        , platform_subset
        , crg_base_prod_number
        , geography
        , customer_engagement
        , MAX(load_date) OVER (PARTITION BY cal_date, platform_subset, Crg_Base_Prod_Number, customer_engagement, geography ORDER BY cal_date ROWS UNBOUNDED PRECEDING) AS max_load_date
    FROM src
)

-- select * from src_max where 1=1 and cal_date = '2021-03-01' and platform_subset = 'EYRIE STND CNIN'

, src_filter as
-- filter to just the most recent upload by CK (see src_max CTE)
(
    SELECT DISTINCT src.cal_date
        , src.platform_subset
        , src.crg_base_prod_number
        , src.geography
        , src.k_color
        , src.consumable_type
        , src.customer_engagement
        , src.mix_pct
        , src.technology
        , src.load_date
        , sm.max_load_date
        , SUM(src.mix_pct) OVER (PARTITION BY src.cal_date, src.platform_subset, src.geography, src.k_color, src.consumable_type, src.customer_engagement
            ORDER BY src.cal_date ROWS UNBOUNDED PRECEDING) AS sum_mix_pct
    FROM src
    JOIN src_max AS sm
        ON sm.cal_date = src.cal_date
        AND sm.platform_subset = src.platform_subset
        AND sm.crg_base_prod_number = src.crg_base_prod_number
        AND sm.customer_engagement = src.customer_engagement
        AND sm.geography = src.geography
    WHERE 1=1
        AND src.load_date = sm.max_load_date
)

, src_agg AS
(
    SELECT cal_date
        , platform_subset
        , crg_base_prod_number
        , geography
        , k_color
        , consumable_type
        , customer_engagement
        , mix_pct
        , technology
        , load_date
        , SUM(mix_pct) OVER (PARTITION BY cal_date, platform_subset, geography, k_color, consumable_type, customer_engagement
            ORDER BY cal_date ROWS UNBOUNDED PRECEDING) AS sum_mix_pct
    FROM src_filter
)

-- select * from src_agg where 1=1 and cal_date = '2021-03-01' and platform_subset = 'EYRIE STND CNIN' order by geography

, test1 AS
-- check to see if all NPIs in hardware_xref have at least one record in cartridge_mix_override
(
    SELECT 'TEST1 - HW NPIS WO CMO' AS type
        , NULL AS cal_date
        , hw.platform_subset
        , NULL AS base_product_number
        , NULL AS geography
        , NULL AS k_color
        , NULL AS consumable_type
        , NULL AS customer_engagement
        , hw.technology
        , NULL AS mix_pct
        , NULL AS sum_mix_pct
    FROM mdm.hardware_xref AS hw
    LEFT JOIN (SELECT DISTINCT platform_subset FROM src) AS src
        ON hw.platform_subset = src.platform_subset
    WHERE 1=1
        AND hw.technology IN ('INK', 'PWA', 'LASER')
        AND hw.product_lifecycle_status = 'N'
        AND src.platform_subset IS NUll
)

, test2 AS
-- return records where sum_mix_pct <> 1
(
    SELECT 'TEST2 - SUM_MIX_PCT <> 1' AS type
        , cal_date
        , platform_subset
        , crg_base_prod_number AS base_product_number
        , geography
        , k_color
        , consumable_type
        , customer_engagement
        , technology
        , mix_pct
        , sum_mix_pct
    FROM src_agg
    WHERE 1=1
        AND ROUND(sum_mix_pct, 0) <> 1.0
)

SELECT * FROM test1

UNION ALL

SELECT *
FROM test2
WHERE sum_mix_pct <> 0;