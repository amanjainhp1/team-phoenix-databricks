CREATE OR REPLACE VIEW ib.ib_dash_vw
AS
SELECT
    ib.record AS record,
    ib.cal_date AS cal_date,
    ib.region_5 AS region_5,
    ib.iso_alpha2 AS iso_alpha2,
    ib.country_name AS country_name,
    ib.market10 AS market10,
    CASE iso.developed_emerging WHEN 'EMERGING' THEN 'EM' WHEN 'DEVELOPED' THEN 'DM' END AS EM_DM,
    ib.platform_subset AS platform_subset,
    ib.pl AS pl,
    ib.product_family AS product_family,
    ib.business_category AS business_category,
    ib.customer_engagement AS customer_engagement,
    ib.hw_hps_ops AS hw_hps_ops,
    ib.technology AS technology,
    ib.tech_split AS tech_split,
    ib.brand AS brand,
    hw.mono_color,
    hw.product_structure AS vol_val,
    CASE WHEN ib.ib IS NULL THEN 0 ELSE ib.ib END AS ib,
    ships.units AS printer_installs,
    ib.official,
    ib.version AS version,
    ib.load_date AS load_date
FROM prod.ib_datamart_source_vw ib
LEFT JOIN prod.norm_shipments ships ON ib.cal_date=ships.cal_date
    AND ib.iso_alpha2=ships.country_alpha2
    AND ib.platform_subset=ships.platform_subset
    AND ships.version = ib.version
LEFT JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2=ib.iso_alpha2
LEFT JOIN mdm.hardware_xref hw ON hw.platform_subset = ib.platform_subset
WHERE ib.version IN (SELECT DISTINCT version from prod.ib_datamart_source_vw);
