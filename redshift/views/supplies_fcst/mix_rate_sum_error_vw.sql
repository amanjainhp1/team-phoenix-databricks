-- supplies_fcst.mix_rate_sum_error_vw source

CREATE OR REPLACE VIEW supplies_fcst.mix_rate_sum_error_vw AS

WITH __dbt__CTE__c2c_02_geography_mapping AS
(
    SELECT 'CENTRAL EUROPE' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'GREATER ASIA' AS market_10, 'AP' AS region_5 UNION ALL
    SELECT 'INDIA SL & BL' AS market_10, 'AP' AS region_5 UNION ALL
    SELECT 'ISE' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'LATIN AMERICA' AS market_10, 'LA' AS region_5 UNION ALL
    SELECT 'NORTH AMERICA' AS market_10, 'NA' AS region_5 UNION ALL
    SELECT 'NORTHERN EUROPE' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'SOUTHERN EUROPE' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'UK&I' AS market_10, 'EU' AS region_5 UNION ALL
    SELECT 'GREATER CHINA' AS market_10, 'AP' AS region_5
)

,  __dbt__CTE__or_01_prod_crg_mix_region5 AS
(
    SELECT cmo.cal_date
        , map.market_10 AS market10
        , cmo.platform_subset
        , cmo.crg_base_prod_number
        , cmo.customer_engagement
        , cmo.mix_pct
        , CASE WHEN sup.single_multi = 'TRI-PACK' THEN 'MULTI'
               ELSE 'SINGLE' END AS single_multi
        , CASE WHEN hw.technology = 'LASER' AND CAST(cmo.load_date AS DATE) > '2021-11-15' THEN 'PAGE_MIX'
               WHEN hw.technology <> 'LASER' THEN 'PAGE_MIX'
               ELSE 'CRG_MIX' END AS upload_type  -- HARD-CODED cut-line from cartridge mix to page/ccs mix; PAGE_MIX is page/ccs mix
        , CASE WHEN NOT sup.k_color IS NULL THEN sup.k_color
               WHEN sup.k_color IS NULL AND sup.crg_chrome IN ('K', 'BLK') THEN 'BLACK'
               WHEN sup.k_color IS NULL AND sup.crg_chrome IN ('C', 'CYN', 'M', 'MAG', 'Y', 'YEL') THEN 'COLOR'
               ELSE NULL END AS k_color
        , CASE WHEN sup.crg_chrome = 'C' THEN 'CYN'
               WHEN sup.crg_chrome = 'M' THEN 'MAG'
               WHEN sup.crg_chrome = 'Y' THEN 'YEL'
               WHEN sup.crg_chrome = 'K' THEN 'BLK'
               ELSE sup.crg_chrome END AS crg_chrome
        , CASE WHEN sup.crg_chrome IN ('DRUM') THEN 'DRUM'
               ELSE 'CARTRIDGE' END AS consumable_type
        , cmo.load_date
    FROM prod.cartridge_mix_override AS cmo
    JOIN mdm.supplies_xref AS sup
        ON cmo.crg_base_prod_number = sup.base_product_number
    JOIN mdm.hardware_xref AS hw
        ON hw.platform_subset = cmo.platform_subset
    JOIN __dbt__CTE__c2c_02_geography_mapping AS map
        ON map.region_5 = cmo.geography
    WHERE 1=1
        AND cmo.official = 1
        AND cmo.geography_grain = 'REGION_5'
        AND NOT sup.crg_chrome IN ('HEAD', 'UNK')
        AND hw.product_lifecycle_status = 'N'
        AND hw.technology IN ('LASER','INK','PWA')
)

,  __dbt__CTE__or_02_prod_crg_mix_market10 AS (
    SELECT cmo.cal_date
        , cmo.geography AS market10
        , cmo.platform_subset
        , cmo.crg_base_prod_number
        , cmo.customer_engagement
        , cmo.mix_pct
        , CASE WHEN sup.single_multi = 'TRI-PACK' THEN 'Multi'
               ELSE 'SINGLE' END AS single_multi
        , CASE WHEN hw.technology = 'LASER' AND CAST(cmo.load_date AS DATE) > '2021-11-15' THEN 'PAGE_MIX'
               WHEN hw.technology <> 'LASER' THEN 'PAGE_MIX'
               ELSE 'CRG_MIX' END AS upload_type  -- HARD-CODED cut-line from cartridge mix to page/ccs mix; PAGE_MIX is page/ccs mix
        , CASE WHEN NOT sup.k_color IS NULL THEN sup.k_color
               WHEN sup.k_color IS NULL AND sup.crg_chrome IN ('K', 'BLK') THEN 'BLACK'
               WHEN sup.k_color IS NULL AND sup.crg_chrome IN ('C', 'CYN', 'M', 'MAG', 'Y', 'YEL') THEN 'COLOR'
               ELSE NULL END AS k_color
        , CASE WHEN sup.crg_chrome = 'C' THEN 'CYN'
               WHEN sup.crg_chrome = 'M' THEN 'MAG'
               WHEN sup.crg_chrome = 'Y' THEN 'YEL'
               WHEN sup.crg_chrome = 'K' THEN 'BLK'
               ELSE sup.crg_chrome END AS crg_chrome
        , CASE WHEN sup.crg_chrome IN ('DRUM') THEN 'DRUM'
               ELSE 'CARTRIDGE' END AS consumable_type
        , cmo.load_date
    FROM prod.cartridge_mix_override AS cmo
    JOIN mdm.supplies_xref AS sup
        ON cmo.crg_base_prod_number = sup.base_product_number
    JOIN mdm.hardware_xref AS hw
        ON hw.platform_subset = cmo.platform_subset
    WHERE 1=1
        AND cmo.official = 1
        AND cmo.geography_grain = 'MARKET10'
        AND NOT sup.crg_chrome IN ('HEAD', 'UNK')
        AND hw.product_lifecycle_status = 'N'
        AND hw.technology IN ('LASER','INK','PWA')
)

,  __dbt__CTE__or_03_prod_crg_mix AS
(
    SELECT cmo.cal_date
        , cmo.market10
        , cmo.platform_subset
        , cmo.crg_base_prod_number
        , cmo.customer_engagement
        , cmo.single_multi
        , cmo.mix_pct
        , cmo.upload_type
        , cmo.k_color
        , cmo.crg_chrome
        , cmo.consumable_type
        , cmo.load_date
    FROM __dbt__CTE__or_01_prod_crg_mix_region5 AS cmo

    UNION ALL

    SELECT cmo.cal_date
        , cmo.market10
        , cmo.platform_subset
        , cmo.crg_base_prod_number
        , cmo.customer_engagement
        , cmo.single_multi
        , cmo.mix_pct
        , cmo.upload_type
        , cmo.k_color
        , cmo.crg_chrome
        , cmo.consumable_type
        , cmo.load_date
    FROM __dbt__CTE__or_02_prod_crg_mix_market10 AS cmo
)

,  __dbt__CTE__or_04_prod_crg_mix_filter AS (
    SELECT cmo.cal_date
        , cmo.market10
        , cmo.platform_subset
        , cmo.customer_engagement
        , cmo.k_color
        , MAX(cmo.load_date) AS filter
    FROM __dbt__CTE__or_03_prod_crg_mix AS cmo
    GROUP BY cmo.cal_date
        , cmo.market10
        , cmo.platform_subset
        , cmo.customer_engagement
        , cmo.k_color
)

,  __dbt__CTE__or_05_add_type_and_yield_MOD AS (
    SELECT cmo.cal_date
        , cmo.market10
        , cmo.k_color
        , cmo.crg_chrome
        , cmo.consumable_type
        , cmo.platform_subset
        , cmo.crg_base_prod_number AS base_product_number
        , cmo.customer_engagement
        , cmo.single_multi
        , cmo.mix_pct
        , cmo.upload_type
    FROM __dbt__CTE__or_03_prod_crg_mix AS cmo
    JOIN __dbt__CTE__or_04_prod_crg_mix_filter AS filter
        ON filter.cal_date = cmo.cal_date
        AND filter.market10 = cmo.market10
        AND filter.platform_subset = cmo.platform_subset
        AND filter.customer_engagement = cmo.customer_engagement
        AND filter.k_color = cmo.k_color  -- key condition for crg mix logic (does it impact future uploads of PAGE_MIX?)
        AND filter.filter = cmo.load_date  -- filter to most recent upload by JOIN conditions; if not used then pens can drop/add throwing off mix_rate
    WHERE 1=1
        AND cmo.upload_type = 'PAGE_MIX'
)

, step_1_k AS
(
    SELECT cal_date
        , market10
        , k_color
        , crg_chrome
        , platform_subset
        , customer_engagement
        , sum(mix_pct) AS mix_rate
    FROM __dbt__CTE__or_05_add_type_and_yield_MOD
    WHERE 1=1
        AND k_color = 'BLACK'
    GROUP BY cal_date
        , market10
        , k_color
        , crg_chrome
        , platform_subset
        , customer_engagement
)

, step_2_color_single AS
(
        SELECT cal_date
        , market10
        , crg_chrome
        , platform_subset
        , customer_engagement
        , SUM(mix_pct) AS mix_rate
    FROM __dbt__CTE__or_05_add_type_and_yield_MOD
    WHERE 1=1
        AND k_color = 'COLOR'
        AND single_multi = 'SINGLE'
    GROUP BY cal_date
        , market10
        , crg_chrome
        , platform_subset
        , customer_engagement
)

, step_3_color_multi AS
(
    SELECT cal_date
        , market10
        , crg_chrome
        , platform_subset
       , customer_engagement
        , SUM(mix_pct) AS mix_rate
    FROM __dbt__CTE__or_05_add_type_and_yield_MOD
    WHERE 1=1
        AND k_color = 'COLOR'
        AND single_multi <> 'SINGLE'
    GROUP BY cal_date
        , market10
        , crg_chrome
        , platform_subset
        , customer_engagement
)

, step_4_color_final AS
(
    SELECT m1.cal_date
        , m1.market10
        , m1.crg_chrome
        , m1.platform_subset
        , m1.customer_engagement
        , m1.mix_rate + COALESCE(m2.mix_rate, 0) AS mix_rate
    FROM step_2_color_single AS m1
    LEFT JOIN step_3_color_multi AS m2
        ON m2.cal_date = m1.cal_date
        AND m2.market10 = m1.market10
        AND m2.platform_subset = m1.platform_subset
        AND m2.customer_engagement = m1.customer_engagement
)

, exceptions AS
(
    SELECT 'LOCHSA STND DM1' AS platform_subset UNION ALL
    SELECT 'LOCHSA STND DM2' AS platform_subset UNION ALL
    SELECT 'LOCHSA YET2 DM1' AS platform_subset UNION ALL
    SELECT 'LOCHSA YET2 DM2' AS platform_subset UNION ALL
    SELECT 'YOSHINO STND DM1' AS platform_subset UNION ALL
    SELECT 'YOSHINO STND DM1 DORN' AS platform_subset UNION ALL
    SELECT 'YOSHINO STND DM2' AS platform_subset UNION ALL
    SELECT 'YOSHINO STND DM2 DORN' AS platform_subset UNION ALL
    SELECT 'YOSHINO YET2 DM1' AS platform_subset UNION ALL
    SELECT 'YOSHINO YET2 DM1 DORN' AS platform_subset UNION ALL
    SELECT 'YOSHINO YET2 DM2' AS platform_subset UNION ALL
    SELECT 'YOSHINO YET2 DM2 DORN' AS platform_subset UNION ALL
    SELECT 'SELENE STND DM1' AS platform_subset UNION ALL
    SELECT 'SELENE STND DM2' AS platform_subset UNION ALL
    SELECT 'SELENE YET2 DM1' AS platform_subset UNION ALL
    SELECT 'SELENE YET2 DM2' AS platform_subset UNION ALL
    SELECT 'ULYSSES STND DM1' AS platform_subset UNION ALL
    SELECT 'ULYSSES STND DM2' AS platform_subset UNION ALL
    SELECT 'ULYSSES YET2 DM1' AS platform_subset UNION ALL
    SELECT 'ULYSSES YET2 DM2' AS platform_subset UNION ALL
    SELECT 'CHERRY STND DM1' AS platform_subset UNION ALL
    SELECT 'CHERRY STND DM1 DORN' AS platform_subset UNION ALL
    SELECT 'CHERRY STND DM2' AS platform_subset UNION ALL
    SELECT 'CHERRY STND DM2 DORN' AS platform_subset UNION ALL
    SELECT 'CHERRY YET2 DM1' AS platform_subset UNION ALL
    SELECT 'CHERRY YET2 DM1 DORN' AS platform_subset UNION ALL
    SELECT 'CHERRY YET2 DM2' AS platform_subset UNION ALL
    SELECT 'CHERRY YET2 DM2 DORN' AS platform_subset UNION ALL
    SELECT 'LOTUS STND DM1' AS platform_subset UNION ALL
    SELECT 'LOTUS STND DM2' AS platform_subset UNION ALL
    SELECT 'LOTUS YET2 DM1' AS platform_subset UNION ALL
    SELECT 'LOTUS YET2 DM2' AS platform_subset UNION ALL
    SELECT 'EYRIE YET2 DM' AS platform_subset UNION ALL
    SELECT 'SKYREACH YET2 DE' AS platform_subset UNION ALL
    SELECT 'SKYREACH YET2 DM' AS platform_subset UNION ALL
    SELECT 'EYRIE STND DM' AS platform_subset UNION ALL
    SELECT 'SKYREACH STND DE' AS platform_subset UNION ALL
    SELECT 'SKYREACH STND DM' AS platform_subset UNION ALL
    SELECT 'EYRIE YET2 DM MLK' AS platform_subset UNION ALL
    SELECT 'SKYREACH YET2 DM MLK' AS platform_subset UNION ALL
    SELECT 'EYRIE STND DM MLK' AS platform_subset UNION ALL
    SELECT 'SKYREACH STND DM MLK' AS platform_subset UNION ALL
    SELECT 'KAY YET2 DM' AS platform_subset UNION ALL
    SELECT 'GAHERIS YET2 DM' AS platform_subset UNION ALL
    SELECT 'GAHERIS YET2 DE' AS platform_subset UNION ALL
    SELECT 'KAY STND DM' AS platform_subset UNION ALL
    SELECT 'GAHERIS STND DM' AS platform_subset UNION ALL
    SELECT 'GAHERIS STND DE' AS platform_subset UNION ALL
    SELECT 'STORM YET2 DM1' AS platform_subset UNION ALL
    SELECT 'HULK YET2 DM1' AS platform_subset UNION ALL
    SELECT 'STORM STND DM1' AS platform_subset UNION ALL
    SELECT 'HULK STND DM1' AS platform_subset UNION ALL
    SELECT 'STORM YET2 DM2' AS platform_subset UNION ALL
    SELECT 'HULK YET2 DM2' AS platform_subset UNION ALL
    SELECT 'STORM STND DM2' AS platform_subset UNION ALL
    SELECT 'HULK STND DM2' AS platform_subset UNION ALL
    SELECT 'ZELUS YET2 DM1' AS platform_subset UNION ALL
    SELECT 'ZELUS YET2 DM1' AS platform_subset UNION ALL
    SELECT 'EUTHENIA YET2 DM1' AS platform_subset UNION ALL
    SELECT 'EUTHENIA YET2 DM1' AS platform_subset UNION ALL
    SELECT 'ZELUS YET2 DM2' AS platform_subset UNION ALL
    SELECT 'ZELUS YET2 DM2' AS platform_subset UNION ALL
    SELECT 'EUTHENIA YET2 DM2' AS platform_subset UNION ALL
    SELECT 'EUTHENIA YET2 DM2' AS platform_subset UNION ALL
    SELECT 'ZELUS STND DM1' AS platform_subset UNION ALL
    SELECT 'EUTHENIA STND DM1' AS platform_subset UNION ALL
    SELECT 'ZELUS STND DM2' AS platform_subset UNION ALL
    SELECT 'EUTHENIA STND DM2' AS platform_subset
)

, combined AS
(
    SELECT cal_date
        , market10
        , 'BLACK' AS k_color
        , crg_chrome
        , s.platform_subset
        , customer_engagement
        , mix_rate
    FROM step_1_k AS s
    LEFT JOIN exceptions AS e
        on e.platform_subset = s.platform_subset
    WHERE 1=1
        AND NOT(ROUND(mix_rate, 0) = 1.0 OR ROUND(mix_rate, 0) = 0)
        AND e.platform_subset IS NULL
        AND crg_chrome <> 'DRUM'

    UNION ALL

    SELECT cal_date
        , market10
        , 'COLOR' AS k_color
        , crg_chrome
        , s.platform_subset
        , customer_engagement
        , mix_rate
    FROM step_4_color_final AS s
    LEFT JOIN exceptions AS e
        on e.platform_subset = s.platform_subset
    WHERE 1=1
        AND NOT(ROUND(mix_rate, 0) = 1.0 OR ROUND(mix_rate, 0) = 0)
        AND e.platform_subset IS NULL
        AND crg_chrome <> 'DRUM'

)

, review AS
(
      SELECT c.cal_date
        , c.market10
        , c.k_color
        , c.crg_chrome
        , c.platform_subset
        , c.customer_engagement
        , c.mix_rate
        , hw.hw_product_family
        , hw.pl
        , coalesce(f.forecaster, p.forecaster) AS forecaster
    FROM combined AS c
    JOIN mdm.hardware_xref AS hw
        on hw.platform_subset = c.platform_subset
    LEFT JOIN mdm.hw_product_family_ink_forecaster_mapping AS f
        on f.hw_product_family = hw.hw_product_family
    LEFT JOIN mdm.pl_toner_forecaster_mapping AS p
        on p.pl = hw.pl
    WHERE 1=1
)

SELECT *
FROM review;