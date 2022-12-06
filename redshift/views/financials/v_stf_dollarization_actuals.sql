create view financials.v_stf_dollarization_actuals as
SELECT actuals_plus_stf.record,
       actuals_plus_stf.geography,
       actuals_plus_stf.base_product_number,
       actuals_plus_stf.pl,
       actuals_plus_stf.technology,
       actuals_plus_stf.cal_date,
       actuals_plus_stf.units,
       actuals_plus_stf.equivalent_units,
       actuals_plus_stf.gross_revenue,
       actuals_plus_stf.contractual_discounts,
       actuals_plus_stf.discretionary_discounts,
       actuals_plus_stf.contra,
       actuals_plus_stf.revenue_currency_hedge,
       actuals_plus_stf.net_revenue,
       actuals_plus_stf.variable_cost,
       actuals_plus_stf.contribution_margin,
       actuals_plus_stf.fixed_cost,
       actuals_plus_stf.total_cos,
       actuals_plus_stf.gross_margin,
       actuals_plus_stf.insights_units,
       actuals_plus_stf.username
FROM (SELECT 'actuals'::character varying AS record,
             actuals.geography,
             actuals.base_product_number,
             actuals.pl,
             actuals.technology,
             actuals.cal_date,
             actuals.units,
             actuals.equivalent_units,
             actuals.gross_revenue,
             actuals.contractual_discounts,
             actuals.discretionary_discounts,
             actuals.contra,
             actuals.revenue_currency_hedge,
             actuals.net_revenue,
             actuals.variable_cost,
             actuals.contribution_margin,
             actuals.fixed_cost,
             actuals.total_cos,
             actuals.gross_margin,
             actuals.insights_units,
             actuals.username
      FROM (SELECT iso.region_5                                                    AS geography,
                   bp.base_product_number,
                   bp.pl,
                   plx.technology,
                   bp.cal_date,
                   SUM(bp.revenue_units)                                           AS units,
                   SUM(bp.equivalent_units)                                        AS equivalent_units,
                   SUM(bp.gross_revenue)                                           AS gross_revenue,
                   SUM(bp.contractual_discounts)                                   AS contractual_discounts,
                   SUM(bp.discretionary_discounts)                                 AS discretionary_discounts,
                   SUM(bp.contractual_discounts) + SUM(bp.discretionary_discounts) AS contra,
                   SUM(bp.net_currency)                                            AS revenue_currency_hedge,
                   SUM(bp.net_revenue)                                             AS net_revenue,
                   NULL::double precision                                          AS variable_cost,
                   NULL::double precision                                          AS contribution_margin,
                   NULL::double precision                                          AS fixed_cost,
                   SUM(bp.total_cos)                                               AS total_cos,
                   SUM(bp.gross_profit)                                            AS gross_margin,
                   SUM(bp.revenue_units)                                           AS insights_units,
                   NULL::character varying                                         AS username
            FROM fin_prod.actuals_supplies_baseprod bp
                     LEFT JOIN mdm.iso_country_code_xref iso ON bp.country_alpha2::text = iso.country_alpha2::text
                     LEFT JOIN mdm.product_line_xref plx ON bp.pl::text = plx.pl::text
                     LEFT JOIN mdm.calendar cal ON cal.date = bp.cal_date
            WHERE cal.day_of_month = 1::double precision
              AND cal.fiscal_yr::numeric(18, 0) > ((SELECT actuals_fiscal_yr.start_fiscal_yr
                                                    FROM (SELECT calendar.fiscal_yr::numeric(18, 0) - 2::numeric::numeric(18, 0) AS start_fiscal_yr
                                                          FROM mdm.calendar
                                                          WHERE calendar.date = 'now'::character varying::date) actuals_fiscal_yr))
              AND bp.pl::text <> '1N'::character varying::text
              AND bp.pl::text <> 'GD'::character varying::text
              AND bp.pl::text <> 'GM'::character varying::text
              AND bp.pl::text <> 'LU'::character varying::text
              AND bp.pl::text <> 'K6'::character varying::text
              AND bp.pl::text <> 'UD'::character varying::text
              AND bp.pl::text <> 'HF'::character varying::text
              AND bp.pl::text <> '65'::character varying::text
              AND bp.customer_engagement::text <> 'est_direct_fulfillment'::character varying::text
            GROUP BY iso.region_5, bp.base_product_number, bp.pl, bp.cal_date, plx.technology) actuals
      UNION ALL
      SELECT 'stf'::character varying                           AS record,
             "forecast".geography,
             "forecast".base_product_number,
             "forecast".pl,
             "forecast".technology,
             "forecast".cal_date,
             "forecast".units,
             "forecast".equivalent_units,
             "forecast".gross_revenue,
             "forecast".contractual_discounts::numeric(18, 0)   AS contractual_discounts,
             "forecast".discretionary_discounts::numeric(18, 0) AS discretionary_discounts,
             "forecast".contra,
             "forecast".revenue_currency_hedge,
             "forecast".net_revenue,
             "forecast".variable_cost,
             "forecast".contribution_margin,
             "forecast".fixed_cost,
             "forecast".total_cos,
             "forecast".gross_margin,
             "forecast".insights_units,
             "forecast".username
      FROM (SELECT DISTINCT
                   stf.geography,
                   stf.base_product_number,
                   rdma.pl,
                   supplies_xref.technology,
                   stf.cal_date,
                   stf.units,
                   COALESCE(stf.units * supplies_xref.equivalents_multiplier::double precision,
                            stf.units)                                                                            AS equivalent_units,
                   fin.baseprod_gru * stf.units                                                                   AS gross_revenue,
                   NULL::character varying                                                                        AS contractual_discounts,
                   NULL::character varying                                                                        AS discretionary_discounts,
                   fin.baseprod_contra_per_unit * stf.units                                                       AS contra,
                   fin.baseprod_revenue_currency_hedge_unit * stf.units                                           AS revenue_currency_hedge,
                   fin.baseprod_aru * stf.units                                                                   AS net_revenue,
                   fin.baseprod_variable_cost_per_unit * stf.units                                                AS variable_cost,
                   fin.baseprod_contribution_margin_unit * stf.units                                              AS contribution_margin,
                   fin.baseprod_fixed_cost_per_unit * stf.units                                                   AS fixed_cost,
                   fin.baseprod_variable_cost_per_unit * stf.units + fin.baseprod_fixed_cost_per_unit *
                                                                     stf.units                                    AS total_cos,
                   fin.baseprod_gross_margin_unit * stf.units                                                     AS gross_margin,
                   c2c.cartridges                                                                                 AS insights_units,
                   stf.username
            FROM stage.supplies_stf_landing stf
                     LEFT JOIN fin_prod.forecast_supplies_baseprod_region_stf fin
                               ON fin.base_product_number::text = stf.base_product_number::text AND
                                  fin.region_5::text = stf.geography::text AND fin.cal_date = stf.cal_date AND
                                  fin.version::text =
                                  ((SELECT "max"(forecast_supplies_baseprod_region_stf.version::text) AS "max"
                                    FROM fin_prod.forecast_supplies_baseprod_region_stf))
                     LEFT JOIN mdm.rdma rdma ON rdma.base_prod_number::text = stf.base_product_number::text
                     JOIN mdm.iso_country_code_xref ctry ON ctry.region_5::text = stf.geography::text
                     LEFT JOIN (SELECT c2c.base_product_number,
                                       ctry.region_5,
                                       c2c.cal_date,
                                       SUM(c2c.cartridges) AS cartridges
                                FROM prod.working_forecast_country c2c
                                         JOIN mdm.iso_country_code_xref ctry ON ctry.country_alpha2::text = c2c.country::text
                                WHERE c2c.version::text =
                                      ((SELECT "max"(working_forecast_country.version::text) AS "max"
                                        FROM prod.working_forecast_country))
                                GROUP BY c2c.base_product_number, ctry.region_5, c2c.cal_date) c2c
                               ON c2c.base_product_number::text = stf.base_product_number::text AND
                                  c2c.region_5::text = stf.geography::text AND c2c.cal_date = stf.cal_date
                     LEFT JOIN mdm.supplies_xref supplies_xref ON supplies_xref.base_product_number::text =
                                                                  stf.base_product_number::text) "forecast") actuals_plus_stf;

alter table financials.v_stf_dollarization_actuals
    owner to auto_glue;

grant delete, insert, references, select, trigger, update on financials.v_stf_dollarization_actuals to group phoenix_dev;