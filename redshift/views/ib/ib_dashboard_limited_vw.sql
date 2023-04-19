create view ib.ib_dashboard_limited_vw
            (record, cal_date, region_5, iso_alpha2, country_name, market10, platform_subset, pl, product_family,
             business_category, customer_engagement, hw_hps_ops, technology, tech_split, brand, ib, printer_installs,
             version, load_date)
as
SELECT 'IB'                                        AS record,
       ib.cal_date,
       iso.region_5,
       ib.country_alpha2                           AS iso_alpha2,
       iso.country                                 AS country_name,
       iso.market10,
       ib.platform_subset,
       COALESCE(rdmapl.pl, hw.pl)                  AS pl,
       rdmapl.product_family,
       COALESCE(rdmapl.pl_level_1, pls.pl_level_1) AS business_category,
       ib.customer_engagement,
       hw.business_feature                         AS hw_hps_ops,
       hw.technology,
       hw.hw_product_family                        AS tech_split,
       hw.brand,
       SUM(
               CASE
                   WHEN ib.measure::text = 'IB'::character varying::text THEN ib.units
                   ELSE NULL::double precision
                   END)                            AS ib,
       NULL::"unknown"                             AS printer_installs,
       ib.version,
       ib.load_date::date                          AS load_date
FROM prod.ib ib
         LEFT JOIN (SELECT DISTINCT
                           rdma.platform_subset,
                           rdma.pl,
                           rdma.product_family,
                           plx.pl_level_1
                    FROM mdm.rdma rdma
                             LEFT JOIN (SELECT DISTINCT
                                               rdma.platform_subset,
                                               rdma.pl,
                                               plx.technology,
                                               pls.pl_level_1
                                        FROM mdm.rdma rdma
                                                 JOIN mdm.product_line_xref plx ON plx.pl::text = rdma.pl::text
                                                 LEFT JOIN mdm.product_line_scenarios_xref pls ON pls.pl::text = rdma.pl::text
                                        WHERE 1 = 1
                                          AND pls.pl_scenario::text = 'IB-DASHBOARD'::character varying::text) plx
                                       ON plx.pl::text = rdma.pl::text
                    WHERE 1 = 1
                      AND rdma.platform_subset::text <> 'CRICKET'::character varying::text
                      AND rdma.platform_subset::text <> 'EVERETT'::character varying::text
                      AND rdma.platform_subset::text <> 'FORRESTER'::character varying::text
                      AND rdma.platform_subset::text <> 'MAYBACH'::character varying::text
                    UNION ALL
                    SELECT DISTINCT
                           rdma.platform_subset,
                           rdma.pl,
                           rdma.product_family,
                           plx.pl_level_1
                    FROM mdm.rdma rdma
                             LEFT JOIN (SELECT DISTINCT
                                               rdma.platform_subset,
                                               rdma.pl,
                                               plx.technology,
                                               pls.pl_level_1
                                        FROM mdm.rdma rdma
                                                 JOIN mdm.product_line_xref plx ON plx.pl::text = rdma.pl::text
                                                 LEFT JOIN mdm.product_line_scenarios_xref pls ON pls.pl::text = rdma.pl::text
                                        WHERE 1 = 1
                                          AND pls.pl_scenario::text = 'IB-DASHBOARD'::character varying::text) plx
                                       ON plx.pl::text = rdma.pl::text
                    WHERE 1 = 1
                      AND (rdma.platform_subset::text = 'CRICKET'::character varying::text AND
                           rdma.pl::text = '2Q'::character varying::text OR
                           rdma.platform_subset::text = 'EVERETT'::character varying::text AND
                           rdma.pl::text = '3Y'::character varying::text OR
                           rdma.platform_subset::text = 'FORRESTER'::character varying::text AND
                           rdma.pl::text = '3Y'::character varying::text OR
                           rdma.platform_subset::text = 'MAYBACH'::character varying::text AND
                           rdma.pl::text = '3Y'::character varying::text)) rdmapl
                   ON rdmapl.platform_subset::text = ib.platform_subset::text
         LEFT JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2::text = ib.country_alpha2::text
         LEFT JOIN mdm.hardware_xref hw ON hw.platform_subset::text = ib.platform_subset::text
         LEFT JOIN mdm.product_line_scenarios_xref pls ON pls.pl::text = COALESCE(rdmapl.pl, hw.pl)::text AND
                                                          pls.pl_scenario::text = 'IB-DASHBOARD'::character varying::text
         LEFT JOIN (SELECT "max"(hardware_ltf.cal_date) AS max_date
                    FROM prod.hardware_ltf
                    WHERE 1 = 1
                      AND hardware_ltf.record::text = 'HW_FCST'::character varying::text
                      AND hardware_ltf.official = 1::boolean) max_f ON 1 = 1
WHERE 1 = 1
  AND (ib.version::text IN (SELECT "max"(ib.version::text) AS "max"
                            FROM prod.ib
                            GROUP BY ib.official))
  AND ib.cal_date <= max_f.max_date
  AND (hw.technology::text = 'LASER'::character varying::text OR hw.technology::text = 'INK'::character varying::text OR
       hw.technology::text = 'PWA'::character varying::text)
GROUP BY ib.cal_date, iso.region_5, ib.country_alpha2, iso.country, iso.market10, ib.platform_subset, rdmapl.pl, hw.pl,
         rdmapl.product_family, rdmapl.pl_level_1, pls.pl_level_1, ib.customer_engagement, hw.business_feature,
         hw.technology, hw.hw_product_family, hw.brand, ib.version, ib.load_date
LIMIT 10;

alter table ib.ib_dashboard_limited_vw
    owner to auto_glue;

grant select on ib.ib_dashboard_limited_vw to auto_team_phoenix_analyst;

grant delete, insert, references, select, trigger, truncate, update on ib.ib_dashboard_limited_vw to group phoenix_dev;

grant select on ib.ib_dashboard_limited_vw to group int_analyst;

