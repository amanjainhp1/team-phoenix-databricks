create view financials.v_stf_dollarization as
SELECT DISTINCT
       stf.geography,
       stf.base_product_number,
       rdma.pl,
       supplies_xref.technology,
       stf.cal_date,
       stf.units,
       COALESCE(stf.units * supplies_xref.equivalents_multiplier::double precision, stf.units) AS equivalent_units,
       fin.baseprod_gru * stf.units                                                            AS gross_revenue,
       fin.baseprod_contra_per_unit * stf.units                                                AS contra,
       fin.baseprod_revenue_currency_hedge_unit * stf.units                                    AS revenue_currency_hedge,
       fin.baseprod_aru * stf.units                                                            AS net_revenue,
       fin.baseprod_variable_cost_per_unit * stf.units                                         AS variable_cost,
       fin.baseprod_contribution_margin_unit * stf.units                                       AS contribution_margin,
       fin.baseprod_fixed_cost_per_unit * stf.units                                            AS fixed_cost,
       fin.baseprod_gross_margin_unit * stf.units                                              AS gross_margin,
       c2c.cartridges                                                                          AS insights_units,
       stf.username
FROM stage.supplies_stf_landing stf
         LEFT JOIN fin_prod.forecast_supplies_baseprod_region_stf fin
                   ON fin.base_product_number::text = stf.base_product_number::text AND
                      fin.region_5::text = stf.geography::text AND fin.cal_date = stf.cal_date AND
                      fin.version::text = ((SELECT "max"(forecast_supplies_baseprod_region_stf.version::text) AS "max"
                                            FROM fin_prod.forecast_supplies_baseprod_region_stf))
         LEFT JOIN mdm.rdma rdma ON rdma.base_prod_number::text = stf.base_product_number::text
         JOIN mdm.iso_country_code_xref ctry ON ctry.region_5::text = stf.geography::text
         LEFT JOIN (SELECT c2c.base_product_number,
                           ctry.region_5,
                           c2c.cal_date,
                           SUM(c2c.cartridges) AS cartridges
                    FROM prod.working_forecast_country c2c
                             JOIN mdm.iso_country_code_xref ctry ON ctry.country_alpha2::text = c2c.country::text
                    WHERE c2c.version::text = ((SELECT "max"(working_forecast_country.version::text) AS "max"
                                                FROM prod.working_forecast_country))
                    GROUP BY c2c.base_product_number, ctry.region_5, c2c.cal_date) c2c
                   ON c2c.base_product_number::text = stf.base_product_number::text AND
                      c2c.region_5::text = stf.geography::text AND c2c.cal_date = stf.cal_date
         LEFT JOIN mdm.supplies_xref supplies_xref
                   ON supplies_xref.base_product_number::text = stf.base_product_number::text;

alter table financials.v_stf_dollarization
    owner to auto_glue;

grant delete, insert, references, select, trigger, update on financials.v_stf_dollarization to group phoenix_dev;