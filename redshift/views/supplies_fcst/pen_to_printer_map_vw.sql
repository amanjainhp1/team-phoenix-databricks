CREATE VIEW supplies_fcst.pen_to_printer_map_vw
AS

WITH hw_cte AS (
    SELECT
        hwx.platform_subset 
        , hwx.pl 
        , hwx.technology
        , predecessor 
        , format 
        , sf_mf 
        , mono_color 
        , CASE WHEN product_lifecycle_status = 'C' THEN 'CURRENT'
            WHEN product_lifecycle_status = 'N' THEN 'NPI'
            ELSE 'MATURE/EOL'
        END AS product_life_cycle_status
        , business_feature 
        , product_structure
        , hw_product_family 
        , supplies_mkt_cat 
        , epa_family 
        , plref.l6_Description AS crg_pl_name
        , plref.l5_Description AS crg_category
        , plref.business_division AS crg_business
    FROM mdm.hardware_xref AS hwx
    LEFT JOIN mdm.product_line_xref AS plref
        ON hwx.pl = plref.PL
    LEFT JOIN mdm.rdma ON rdma.platform_subset=hwx.platform_subset
    WHERE 1=1
        AND hwx.technology IN ('LASER','INK','PWA')
),

hw_base_prod_map AS 
(
    SELECT DISTINCT base_product_number, platform_subset
    FROM prod.actuals_hw
    WHERE official = 1
)

SELECT DISTINCT 
    sh1.record
    , supx.technology
    , hwx.pl AS hw_pl -- added HW PL
    , sh1.platform_subset
    , hwx.product_life_cycle_status AS hw_life_cycle_status
    , sh1.customer_engagement
    , sh1.geography
    , sh1.geography_grain
    , rdma.pl AS supplies_pl -- added supplies_pl
    , hwx.hw_product_family AS supplies_family --added supplies_family
    , sh1.base_product_number AS supplies_base_prod_number
    , hw.base_product_number AS hw_base_prod_number
    , rdma.base_prod_name
    , supx.cartridge_alias
    , supx.type
    , supx.size
    , supx.single_multi
    , supx.crg_chrome
    , CASE WHEN sh1.eol = 1 THEN 'YES' ELSE '' END AS supplies_end_of_Life
    , CASE WHEN sh1.eol = 1 THEN eol_date ELSE NULL END AS supplies_eol_date
    , sh1.host_multiplier
FROM mdm.supplies_hw_mapping AS sh1
INNER JOIN mdm.supplies_xref AS supx 
    ON supx.base_product_number = sh1.base_product_number
LEFT JOIN hw_base_prod_map hw ON hw.platform_subset = sh1.platform_subset
LEFT JOIN hw_cte AS hwx 
    ON hwx.platform_subset = sh1.platform_subset
LEFT JOIN mdm.rdma on rdma.base_prod_number = sh1.base_product_number
WHERE 1=1
    AND sh1.official = 1
    AND supx.Technology IN ('LASER','INK','PWA');