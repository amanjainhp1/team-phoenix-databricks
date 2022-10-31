CREATE OR REPLACE VIEW supplies_fcst.fill_rates_vw
AS

WITH yield AS
(
    SELECT 
        y.base_product_number
        ,y.geography  -- region_5
        ,y.effective_date
        ,COALESCE(LEAD(effective_date) OVER (PARTITION BY y.base_product_number, y.geography ORDER BY y.effective_date),
            CAST('2025-10-01' AS DATE)) AS next_effective_date
        ,y.value
    FROM mdm.yield AS y
    INNER JOIN mdm.supplies_xref supx ON supx.base_product_number = y.base_product_number 
    WHERE 1=1
        AND supx.technology IN ('INK','PWA','LASER')
        AND y.official = 1
),

fiscal_cte AS
(
    SELECT
        date
        , fiscal_year_qtr
        , fiscal_yr
        , calendar_yr_qtr
        , calendar_yr
    FROM mdm.calendar
    WHERE 1=1
        AND day_of_month = 1 
        AND fiscal_yr >= DATE_PART_YEAR(CAST(GETDATE() AS DATE))-6
        AND fiscal_yr <= DATE_PART_YEAR(CAST(GETDATE() AS DATE))+4
),

max_yield AS (
    SELECT
        geography AS region_5
        , f.date AS date
        , y1.base_product_number AS base_product_number
        , r.pl
        , pl.technology
        , CASE
            WHEN pl.technology = 'LASER' THEN 'PAGES'
            ELSE 'CC'
        END AS pages_cc
        , y1.value AS value
        , supx.size    
    FROM yield y1 
    INNER JOIN fiscal_cte f 
        ON 1=1
    LEFT JOIN mdm.supplies_xref supx 
        ON supx.base_product_number = y1.base_product_number
    LEFT JOIN mdm.rdma r
        ON r.base_prod_number = y1.base_product_number
    LEFT JOIN mdm.product_line_xref pl
        ON pl.pl = r.pl
    WHERE 1=1
        AND f.date < y1.next_effective_date
)

SELECT
    'YIELD' AS record,
    region_5,
    MAX(date) AS date,
    base_product_number,
    pl,
    technology,
    pages_cc,
    value,
    size
FROM max_yield
GROUP BY
    record,
    region_5,
    base_product_number,
    pl,
    technology,
    pages_cc,
    value,
    size;