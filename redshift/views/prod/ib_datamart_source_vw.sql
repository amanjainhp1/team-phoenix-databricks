CREATE OR REPLACE VIEW prod.ib_datamart_source_vw
AS

SELECT
    'IB' AS record,
    ib.cal_date,
    iso.region_5,
    ib.country_alpha2 AS iso_alpha2,
    iso.country AS country_name,
    cc.country_level_2 AS market10,
    ib.platform_subset,
    COALESCE(rdmapl.pl, hw.pl) AS pl,
    rdmapl.product_family,
    COALESCE(rdmapl.pl_level_1, pls.pl_level_1) AS business_category,
    ib.customer_engagement,
    hw.business_feature AS hw_hps_ops,
    hw.technology,
    hw.hw_product_family AS tech_split,
    hw.brand,
    SUM(
        CASE
            WHEN ib.measure::text = 'IB'::text THEN ib.units
            ELSE NULL::double precision
        END) AS ib,
    NULL::"UNKNOWN" AS printer_installs,
    ib.official,
    ib.version,
    ib.load_date::date AS load_date,
    ib.cal_date::character varying(25)::text + '-'::text + ib.platform_subset::text + '-'::text + ib.country_alpha2::text + '-'::text + ib.customer_engagement::text AS composite_key
FROM prod.ib ib
LEFT JOIN (
    SELECT DISTINCT rdma.platform_subset, rdma.pl, rdma.product_family, plx.pl_level_1
    FROM mdm.rdma rdma
    LEFT JOIN (
        SELECT DISTINCT rdma.platform_subset, rdma.pl, plx.technology, pls.pl_level_1
        FROM mdm.rdma rdma
        JOIN mdm.product_line_xref plx ON plx.pl::text = rdma.pl::text
        LEFT JOIN mdm.product_line_scenarios_xref pls ON pls.pl::text = rdma.pl::text
        WHERE 1 = 1 AND pls.pl_scenario::text = 'IB-DASHBOARD'::text
    ) plx ON plx.pl::text = rdma.pl::text
WHERE 1 = 1
    AND rdma.platform_subset::text <> 'CRICKET'::text
    AND rdma.platform_subset::text <> 'EVERETT'::text
    AND rdma.platform_subset::text <> 'FORRESTER'::text
    AND rdma.platform_subset::text <> 'MAYBACH'::text
UNION ALL 
SELECT DISTINCT
    rdma.platform_subset,
    rdma.pl,
    rdma.product_family,
    plx.pl_level_1
FROM mdm.rdma rdma
LEFT JOIN ( 
    SELECT DISTINCT rdma.platform_subset, rdma.pl, plx.technology, pls.pl_level_1
    FROM mdm.rdma rdma
    JOIN mdm.product_line_xref plx ON plx.pl::text = rdma.pl::text
    LEFT JOIN mdm.product_line_scenarios_xref pls ON pls.pl::text = rdma.pl::text
    WHERE 1 = 1 AND pls.pl_scenario::text = 'IB-DASHBOARD'::text
) plx ON plx.pl::text = rdma.pl::text
WHERE 1 = 1
    AND (
        rdma.platform_subset::text = 'CRICKET'::text
        AND rdma.pl::text = '2Q'::text OR rdma.platform_subset::text = 'EVERETT'::text
        AND rdma.pl::text = '3Y'::text OR rdma.platform_subset::text = 'FORRESTER'::text
        AND rdma.pl::text = '3Y'::text OR rdma.platform_subset::text = 'MAYBACH'::text
        AND rdma.pl::text = '3Y'::text)) rdmapl ON rdmapl.platform_subset::text = ib.platform_subset::text
LEFT JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2::text = ib.country_alpha2::text
LEFT JOIN mdm.iso_cc_rollup_xref cc ON cc.country_alpha2::text = ib.country_alpha2::text
LEFT JOIN mdm.hardware_xref hw ON hw.platform_subset::text = ib.platform_subset::text
LEFT JOIN mdm.product_line_scenarios_xref pls ON pls.pl::text = COALESCE(rdmapl.pl, hw.pl)::text AND pls.pl_scenario::text = 'IB-DASHBOARD'::text
LEFT JOIN (
    SELECT "max"(hardware_ltf.cal_date) AS max_date
    FROM prod.hardware_ltf
    WHERE 1 = 1
    AND hardware_ltf.record::text = 'HW_FCST'::text
    AND hardware_ltf.official = 1::boolean
) max_f ON 1 = 1
WHERE 1 = 1
    AND ib.version::text IN (( SELECT "max"(ib.version::text) AS "max" FROM prod.ib GROUP BY official))
    AND cc.country_scenario::text = 'MARKET10'::text AND ib.cal_date <= max_f.max_date
    AND (hw.technology::text = 'LASER'::text OR hw.technology::text = 'INK'::text OR hw.technology::text = 'PWA'::text)
GROUP BY ib.cal_date, iso.region_5, ib.country_alpha2, iso.country, cc.country_level_2, ib.platform_subset, rdmapl.pl, hw.pl, rdmapl.product_family, rdmapl.pl_level_1, pls.pl_level_1, ib.customer_engagement, hw.business_feature, hw.technology, hw.hw_product_family, hw.brand, ib.official, ib.version, ib.load_date;