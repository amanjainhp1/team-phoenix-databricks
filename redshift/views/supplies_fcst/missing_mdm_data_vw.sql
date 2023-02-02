CREATE OR REPLACE VIEW supplies_fcst.missing_mdm_data AS
SELECT DISTINCT missing.ps,
    missing.region_5,
    missing.market10,
    missing.pl,
    missing.technology,
    SUM(miss_hw) AS hw_miss,
    SUM(miss_decay) AS decay_miss,
    SUM(miss_supphw) AS supplies_hw_map,
    SUM(miss_rdma) AS rdma_miss,
    missing.product_lifecycle_status
FROM (
        -- Missing From Hardware Xref
        SELECT DISTINCT act_fcst.platform_subset AS ps,
            hw.platform_subset,
            act_fcst.region_5,
            '' AS market10,
            hw.pl,
            hw.technology,
            1 AS miss_hw,
            CAST(NULL AS INTEGER) AS miss_decay,
            CAST(NULL AS INTEGER) miss_supphw,
            CAST(NULL AS INTEGER) miss_rdma,
            hw.product_lifecycle_status
        FROM (
                SELECT act.record,
                    iso.region_5,
                    iso.market10,
                    platform_subset
                FROM prod.norm_shipments act
                    INNER JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2 = act.country_alpha2
                WHERE act.version =(
                        SELECT MAX(version)
                        from prod.norm_shipments
                        WHERE official = 1
                    )
                GROUP BY act.record,
                    iso.region_5,
                    market10,
                    platform_subset
            ) AS act_fcst
            LEFT JOIN mdm.hardware_xref hw ON hw.platform_subset = act_fcst.platform_subset
        WHERE hw.platform_subset IS NULL
        GROUP BY act_fcst.platform_subset,
            hw.platform_subset,
            hw.pl,
            hw.technology,
            act_fcst.region_5,
            hw.product_lifecycle_status
        UNION ALL
        -- Missing From Decay Xref
        SELECT DISTINCT act_fcst.platform_subset AS ps,
            decay.platform_subset,
            act_fcst.region_5,
            '' AS market10,
            hw.pl,
            hw.technology,
            CAST(NULL AS INTEGER) miss_hw,
            1 AS miss_decay,
            CAST(NULL AS INTEGER) miss_supphw,
            CAST(NULL AS INTEGER) miss_rdma,
            hw.product_lifecycle_status
        FROM (
                SELECT act.record,
                    iso.region_5,
                    iso.market10,
                    platform_subset
                FROM prod.norm_shipments act
                    INNER JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2 = act.country_alpha2
                WHERE act.version =(
                        SELECT MAX(version)
                        from prod.norm_shipments
                        WHERE official = 1
                    )
                GROUP BY act.record,
                    iso.region_5,
                    iso.market10,
                    platform_subset
            ) AS act_fcst
            LEFT JOIN prod.decay ON decay.platform_subset = act_fcst.platform_subset
            and decay.geography = act_fcst.region_5
            LEFT JOIN mdm.hardware_xref hw ON act_fcst.platform_subset = hw.platform_subset
        WHERE decay.platform_subset is null
        GROUP BY act_fcst.platform_subset,
            decay.platform_subset,
            act_fcst.region_5,
            hw.pl,
            hw.technology,
            hw.product_lifecycle_status
        UNION ALL
        -- Missing From Supplies HW Mapping
        SELECT DISTINCT act_fcst.platform_subset AS ps,
            supphw.platform_subset,
            act_fcst.region_5,
            '' AS market10,
            hw.pl,
            hw.technology,
            CAST(NULL AS INTEGER) miss_hw,
            CAST(NULL AS INTEGER) miss_decay,
            1 AS miss_supphw,
            CAST(NULL AS INTEGER) miss_rdma,
            hw.product_lifecycle_status
        FROM (
                SELECT act.record,
                    iso.region_5,
                    iso.market10,
                    platform_subset
                FROM prod.norm_shipments act
                    INNER JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2 = act.country_alpha2
                WHERE act.version =(
                        SELECT MAX(version)
                        from prod.norm_shipments
                        WHERE official = 1
                    )
                GROUP BY act.record,
                    iso.region_5,
                    iso.market10,
                    platform_subset
            ) AS act_fcst
            LEFT JOIN mdm.supplies_hw_mapping supphw ON supphw.platform_subset = act_fcst.platform_subset
            LEFT JOIN mdm.hardware_xref hw ON act_fcst.platform_subset = hw.platform_subset
        WHERE supphw.platform_subset is null
        GROUP BY act_fcst.platform_subset,
            supphw.platform_subset,
            act_fcst.region_5,
            hw.pl,
            hw.technology,
            hw.product_lifecycle_status
        UNION ALL
        -- Missing From rdma
        SELECT DISTINCT act_fcst.platform_subset AS ps,
            rdma.platform_subset,
            act_fcst.region_5,
            '' AS market10,
            hw.pl,
            hw.technology,
            CAST(NULL AS INTEGER) miss_hw,
            CAST(NULL AS INTEGER) miss_decay,
            CAST(NULL AS INTEGER) miss_supphw,
            1 AS miss_rdma,
            hw.product_lifecycle_status
        FROM (
                SELECT act.record,
                    iso.region_5,
                    iso.market10,
                    platform_subset
                FROM prod.norm_shipments act
                    INNER JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2 = act.country_alpha2
                WHERE act.version =(
                        SELECT MAX(version)
                        from prod.norm_shipments
                        WHERE official = 1
                    )
                GROUP BY act.record,
                    iso.region_5,
                    iso.market10,
                    platform_subset
            ) AS act_fcst
            LEFT JOIN mdm.rdma ON rdma.platform_subset = act_fcst.platform_subset
            LEFT JOIN mdm.hardware_xref hw ON act_fcst.platform_subset = hw.platform_subset
        WHERE rdma.platform_subset is null
        GROUP BY act_fcst.platform_subset,
            rdma.platform_subset,
            hw.pl,
            hw.technology,
            act_fcst.region_5,
            hw.product_lifecycle_status
    ) AS missing
GROUP BY ps,
    region_5,
    market10,
    pl,
    technology,
    product_lifecycle_status;