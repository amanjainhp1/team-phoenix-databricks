CREATE VIEW ib.hw_plan_vw
AS

WITH calendar_fy AS
(
    SELECT fiscal_yr
    FROM mdm.calendar
    WHERE date = CAST(getdate() AS DATE)
),

ltf_name AS
(
    SELECT DISTINCT 
    	forecast_name,
    	version
    FROM  prod.hardware_ltf hw
    WHERE hw.record = 'HW_FCST'
    ORDER BY version DESC
    LIMIT 2
),

mon_abb AS
(
    SELECT DISTINCT
        month,
        month_abbrv
    FROM mdm.calendar
),

tbl_hw_ltf_final AS
/* Long Term Forecast
*/
(
    SELECT 
    	CONCAT('LTF - 20', CONCAT(SUBSTRING(ltf.forecast_name,5,2), CONCAT('-', LPAD(cal.month,2,'0')))) AS report_name
        ,a.platform_subset
        ,d.pl
        ,e.l6_description AS pl_name
        ,d.hw_product_family
        ,c.date
        ,b.region_3
        ,b.region_5
        ,b.market10
        ,plx.pl_level_1
        ,plx.pl_level_2
        ,plx.pl_level_3
        ,SUM(a.units) AS units
    FROM  prod.hardware_ltf AS a
    LEFT JOIN ltf_name ltf
        ON ltf.forecast_name = a.forecast_name
    LEFT JOIN mdm.iso_country_code_xref b
        ON a.country_alpha2 = b.country_alpha2
    INNER JOIN mdm.calendar AS c
        ON a.cal_date = c.date
    INNER JOIN mdm.hardware_xref AS d
        ON a.platform_subset = d.platform_subset
    LEFT JOIN mdm.product_line_xref e
        ON d.pl=e.pl
    LEFT JOIN mdm.product_line_scenarios_xref plx
        ON d.pl=plx.pl
    LEFT JOIN mon_abb cal
        ON cal.month_abbrv = LEFT(ltf.forecast_name, 3)
    WHERE 1=1 
        AND a.version IN (
            SELECT DISTINCT version FROM prod.hardware_ltf
            WHERE record = 'HW_FCST'
            ORDER BY version DESC
            LIMIT 2)
        AND a.forecast_name LIKE '%LTF FINAL%' 
        AND a.forecast_name NOT LIKE '%OBSOLETE%'
        AND a.forecast_name NOT LIKE '%FLASH%'
        AND a.forecast_name NOT LIKE '%ADJUST%'
        AND plx.pl_scenario='DRIVERS'
    GROUP BY
        a.forecast_name
        ,ltf.forecast_name
        ,a.platform_subset
        ,d.pl
        ,e.l6_description
        ,d.hw_product_family
        ,c.date
        ,cal.month
        ,b.region_3
        ,b.region_5
        ,b.market10
        ,plx.pl_level_1
        ,plx.pl_level_2
        ,plx.pl_level_3
),

tbl_hw_actuals AS
/*  Hardware Actuals (most recent 2 fiscal years)
*/
(
    SELECT 
        CONCAT('ACT FY',SUBSTRING(c.fiscal_yr,3,2)) AS report_name
        ,a.platform_subset
        ,b.pl
        ,e.l6_description AS pl_name
        ,b.hw_product_family
        ,c.date
        ,d.region_3
        ,d.region_5
        ,d.market10
        ,plx.pl_level_1
        ,plx.pl_level_2
        ,plx.pl_level_3
        ,SUM(a.base_quantity) AS units
    FROM prod.actuals_hw AS a
    INNER JOIN mdm.hardware_xref AS b
        ON a.platform_subset = b.platform_subset
    INNER JOIN mdm.calendar AS c
        ON a.cal_date = c.date
    INNER JOIN mdm.iso_country_code_xref AS d
        ON a.country_alpha2 = d.country_alpha2
    INNER JOIN mdm.product_line_xref AS e
        ON b.pl = e.pl
    INNER JOIN calendar_fy
        ON 1=1
    LEFT JOIN mdm.product_line_scenarios_xref plx
        ON b.pl=plx.pl 
    WHERE 1=1
        AND a.cal_date BETWEEN DATEADD(YEAR,-3,GETDATE()) AND GETDATE()
        AND c.fiscal_yr > calendar_fy.fiscal_yr-3 ---can change 2 to how many years you want
        AND plx.pl_scenario='DRIVERS'
    GROUP BY 
        a.platform_subset
        ,b.pl
        ,e.l6_description 
        ,b.hw_product_family
        ,d.region_3
        ,d.region_5
        ,d.market10
        ,plx.pl_level_1
        ,plx.pl_level_2
        ,plx.pl_level_3
        ,c.fiscal_yr
        ,c.date
),

tbl_act_for_flash AS
(
    SELECT a.cal_date
        ,a.country_alpha2
        ,a.base_quantity
        ,a.base_product_number
    FROM prod.actuals_hw a
    INNER JOIN mdm.calendar AS c
    ON a.cal_date = c.date
    WHERE c.fiscal_yr=(
        SELECT fiscal_yr
        FROM calendar_fy
    )
),

tbl_flash_wo_act1 AS
(
    SELECT DISTINCT
        a.source_name
        , c.date AS cal_date
        , a.load_date
    FROM mdm.calendar AS c
    LEFT JOIN prod.flash_wd3 AS a
        ON 1=1
    WHERE 1=1
        AND a.record = 'FLASH'
        AND c.fiscal_yr = (
            SELECT fiscal_yr FROM calendar_fy
        )
        AND source_name in (
            SELECT DISTINCT
                source_name
            FROM prod.flash_wd3
            WHERE record = 'FLASH'
            ORDER BY source_name DESC
            LIMIT 3
        )
        AND c.day_of_month = 1
    UNION 
    SELECT DISTINCT
        a.source_name
        , c.date AS cal_date
        , a.load_date
    FROM mdm.calendar AS c
    LEFT JOIN prod.flash_wd3 AS a
        ON 1=1
    WHERE 1=1
        AND a.record = 'FLASH'
        AND c.fiscal_yr = (
            SELECT fiscal_yr FROM calendar_fy
        )
        AND source_name = CONCAT('FLASH',CONCAT(' - ',CONCAT(TO_CHAR(GETDATE(), 'YYYY'),CONCAT('-','04'))))
        AND c.day_of_month = 1
),

tbl_flash_wo_act AS
(
    SELECT DISTINCT
        a.source_name
        , a.cal_date
        , b.country_alpha2
        , b.base_product_number
        , b.units
        , b.load_date
        FROM tbl_flash_wo_act1 a
        LEFT JOIN prod.flash_wd3 b
            ON 1=1
            AND b.record = 'FLASH'
            AND a.source_name=b.source_name
            AND a.cal_date=b.cal_date
),

tbl_flash_w_act AS
(
    SELECT DISTINCT
        a.cal_date
        , a.source_name
        , COALESCE(a.country_alpha2, b.country_alpha2) country_alpha2
        , COALESCE(a.base_product_number, b.base_product_number) base_product_number
        , COALESCE(a.units, b.base_quantity) units
    FROM tbl_flash_wo_act a
    LEFT JOIN tbl_act_for_flash b
        ON a.cal_date=b.cal_date 
),

tbl_flash_final AS
/* Flash
*/
(
SELECT a.source_name AS report_name
    ,b.platform_subset
    ,f.region_3
    ,f.region_5
    ,f.market10
    ,c.pl
    ,d.l6_description AS pl_name
    ,c.hw_product_family
    ,e.date
    ,plx.pl_level_1
    ,plx.pl_level_2
    ,plx.pl_level_3
    ,SUM(a.units) AS units
FROM tbl_flash_w_act AS a
INNER JOIN mdm.rdma AS b
    ON a.base_product_number = b.base_prod_number
INNER JOIN mdm.hardware_xref AS c
    ON b.platform_subset = c.platform_subset
INNER JOIN mdm.product_line_xref AS d
    ON c.pl = d.pl
INNER JOIN mdm.calendar AS e
    ON a.cal_date = e.date
LEFT JOIN mdm.product_line_scenarios_xref plx ON c.pl=plx.pl
INNER JOIN mdm.iso_country_code_xref AS f
    ON a.country_alpha2 = f.country_alpha2
WHERE 1=1
    AND e.fiscal_yr = (SELECT fiscal_yr FROM calendar_fy)
    AND plx.pl_scenario='DRIVERS'
GROUP BY
    a.source_name
    ,b.platform_subset
    ,f.region_3
    ,f.region_5
    ,f.market10
    ,c.pl
    ,d.l6_description
    ,c.hw_product_family
    ,e.date
    ,plx.pl_level_1
    ,plx.pl_level_2
    ,plx.pl_level_3
),

tbl_budget AS
/* Budget
*/
(
SELECT
    CONCAT(a.record,CONCAT(' ',e.fiscal_yr)) AS report_name
    ,b.platform_subset
    ,c.pl
    ,d.l6_description AS pl_name
    ,c.hw_product_family
    ,iccx.region_3
    ,iccx.region_5
    ,iccx.market10
    ,e.date
    ,plx.pl_level_1
    ,plx.pl_level_2
    ,plx.pl_level_3
    ,SUM(a.units) AS units
FROM prod.budget AS a
INNER JOIN mdm.rdma AS b
    ON a.base_product_number = b.base_prod_number 
INNER JOIN mdm.hardware_xref AS c
    ON b.platform_subset = c.platform_subset
INNER JOIN mdm.product_line_xref AS d
    ON c.pl = d.pl
LEFT JOIN mdm.product_line_scenarios_xref plx ON c.pl=plx.pl 
INNER JOIN mdm.calendar AS e
    ON a.cal_date = e.date
INNER JOIN mdm.iso_country_code_xref AS iccx
    ON a.country_alpha2 = iccx.country_alpha2  
WHERE 1=1
    AND a.version = (SELECT MAX(version) FROM prod.budget)
    AND plx.pl_scenario='DRIVERS'
GROUP BY
    CONCAT(a.record, CONCAT(' ' ,e.fiscal_yr))
    ,b.platform_subset
    ,c.pl
    ,d.l6_description
    ,c.hw_product_family
    ,iccx.region_3
    ,iccx.region_5
    ,iccx.market10
    ,plx.pl_level_1
    ,plx.pl_level_2
    ,plx.pl_level_3
    ,e.date
),

tbl_final_union AS
/* Combine AND reformat geography
*/
(
SELECT report_name, platform_subset, pl, pl_name, hw_product_family, region_3, region_5, market10, pl_level_1, pl_level_2, pl_level_3, date, units FROM tbl_hw_actuals
UNION ALL
SELECT report_name, platform_subset, pl, pl_name, hw_product_family, region_3, region_5, market10, pl_level_1, pl_level_2, pl_level_3, date, units FROM tbl_hw_ltf_final
UNION ALL
SELECT report_name, platform_subset, pl, pl_name, hw_product_family, region_3, region_5, market10, pl_level_1, pl_level_2, pl_level_3, date, units FROM tbl_flash_final
UNION ALL
SELECT report_name, platform_subset, pl, pl_name, hw_product_family, region_3, region_5, market10, pl_level_1, pl_level_2, pl_level_3, date, units FROM tbl_budget
)

SELECT 
    report_name,
    platform_subset,
    hw_product_family,
    pl,
    pl_name,
    region_3,
    region_5,
    market10,
    pl_level_1,
    pl_level_2,
    pl_level_3,
    date,
    units
FROM tbl_final_union;
