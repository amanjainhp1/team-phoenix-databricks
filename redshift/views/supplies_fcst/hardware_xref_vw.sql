CREATE OR REPLACE VIEW supplies_fcst.hardware_xref_vw
AS

WITH norm as 
(
    SELECT distinct platform_subset
    FROM prod.norm_shipments
    WHERE version = (SELECT MAX(version) from prod.norm_shipments)
)


SELECT 
    'HARDWARE_XREF' AS record
    , technology
    , hw.pl
    , hw.platform_subset
    , hw_product_family
    , mono_color
    , mono_ppm
    , color_ppm
    , format
    , sf_mf
    , brand
    , business_feature
    , vc_category
    , category_feature
    , plx.pl_level_2 as category
    , plx.pl_level_3 as segment
    , CASE product_lifecycle_status 
        WHEN 'C' THEN 'CURRENT' 
        WHEN 'M' THEN 'MATURE'
        WHEN 'N' THEN 'NPI'
        WHEN 'E' Then 'EOL/NOT LAUNCHED'
    END AS product_lifecycle_status
    , predecessor
    , intro_date
FROM mdm.hardware_xref hw
INNER JOIN norm 
    ON norm.platform_subset = hw.platform_subset
LEFT JOIN mdm.product_line_scenarios_xref plx 
    ON plx.pl=hw.pl
    AND plx.pl_scenario = 'DRIVERS'
WHERE 1=1
    AND technology in ('INK', 'LASER', 'PWA')
;