create view ib.ib_norm_ships_vw as
SELECT insc.version,
       insc.cal_date,
       insc.country_alpha2,
       insc.region_5,
       insc.market10,
       insc.platform_subset,
       insc.measure,
       insc.units,
       insc.customer_engagement,
       insc.official,
       hx.technology,
       hx.business_feature,
       hx.hw_product_family,
       hx.brand
FROM (SELECT ib.version,
             ib.cal_date,
             ib.country_alpha2,
             c.region_5,
             c.market10,
             ib.platform_subset,
             ib.measure,
             SUM(ib.units) AS units,
             ib.customer_engagement,
             ib.official
      FROM prod.ib ib
               LEFT JOIN mdm.iso_country_code_xref c ON c.country_alpha2::text = ib.country_alpha2::text
      WHERE (ib.version IN (SELECT DISTINCT
                                   ib.version
                            FROM prod.ib
                            ORDER BY ib.version DESC
                            LIMIT 6))
        AND ib.cal_date >= '2016-11-01'::date
        AND ib.cal_date <= '2027-10-31'::date
      GROUP BY ib.country_alpha2, ib.platform_subset, ib.version, ib.cal_date, ib.official, ib.customer_engagement,
               ib.measure, c.region_5, c.market10
      UNION ALL
      SELECT ship.version,
             ship.cal_date,
             ship.country_alpha2                 AS country,
             c.region_5,
             c.market10,
             ship.platform_subset,
             'Norm Shipments'::character varying AS measure,
             SUM(ship.units)                     AS units,
             ship.customer_engagement,
             NULL::boolean                       AS official
      FROM prod.norm_shipments_ce ship
               LEFT JOIN mdm.iso_country_code_xref c ON c.country_alpha2::text = ship.country_alpha2::text
      WHERE (ship.version IN (SELECT DISTINCT
                                     norm_shipments_ce.version
                              FROM prod.norm_shipments_ce
                              ORDER BY norm_shipments_ce.version DESC
                              LIMIT 6))
        AND ship.cal_date >= '2016-11-01'::date
        AND ship.cal_date <= '2027-10-31'::date
      GROUP BY ship.country_alpha2, ship.platform_subset, ship.version, ship.customer_engagement, ship.cal_date,
               c.region_5, c.market10) insc
         LEFT JOIN mdm.hardware_xref hx ON insc.platform_subset::text = hx.platform_subset::text;

alter table ib.ib_norm_ships_vw
    owner to auto_glue;

grant select on ib.ib_norm_ships_vw to ???;

grant delete, insert, references, select, trigger, truncate, update on ib.ib_norm_ships_vw to group phoenix_dev;