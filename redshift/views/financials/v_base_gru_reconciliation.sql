create or replace view financials.v_base_gru_reconciliation as
SELECT DISTINCT
       actuals_acct.record_type,
       actuals_acct.cal_date,
       actuals_acct.technology,
       actuals_acct.base_product_number,
       actuals_acct.cartridge_alias AS fiji_alias,
       actuals_acct.base_product_line_code,
       actuals_acct.region_5,
       actuals_acct.country_alpha2,
       actuals_acct.insights_base_units,
       actuals_acct.base_gru,
       actuals_acct.gross_revenue,
       actuals_acct.currency,
       actuals_acct.accountingrate,
       ((SELECT "max"(forecast_supplies_baseprod.version::text) AS "max"
         FROM fin_prod.forecast_supplies_baseprod))::character varying AS version
FROM (SELECT DISTINCT
             'actuals'::character varying AS record_type,
             actuals.cal_date,
             supplies_xref.technology,
             actuals.base_product_number,
             supplies_xref.cartridge_alias,
             actuals.pl                   AS base_product_line_code,
             actuals.region_5,
             actuals.country_alpha2,
             actuals.revenue_units        AS insights_base_units,
             actuals.base_gru,
             actuals.gross_revenue,
             actuals.currency,
             acct_rates.accountingrate
      FROM (SELECT DISTINCT
                   actuals_supplies_baseprod.cal_date,
                   iso_country_code_xref.region_5,
                   actuals_supplies_baseprod.country_alpha2,
                   CASE
                       WHEN list_price_eu_countrylist.currency::text = 'Euro'::character varying::text
                           THEN 'EUR'::character varying
                       WHEN list_price_eu_countrylist.currency::text = 'Dollar'::character varying::text
                           THEN 'USD'::character varying
                       WHEN actuals_supplies_baseprod.country_alpha2::text = 'CL'::character varying::text
                           THEN 'USD'::character varying
                       WHEN actuals_supplies_baseprod.country_alpha2::text = 'PR'::character varying::text
                           THEN 'USD'::character varying
                       ELSE country_currency_map_landing.currency_iso_code
                       END                                        AS currency,
                   actuals_supplies_baseprod.base_product_number,
                   actuals_supplies_baseprod.pl,
                   SUM(actuals_supplies_baseprod.gross_revenue) /
                   CASE
                       WHEN SUM(actuals_supplies_baseprod.revenue_units) = 0::double precision
                           THEN NULL::double precision
                       ELSE SUM(actuals_supplies_baseprod.revenue_units)
                       END                                        AS base_gru,
                   SUM(actuals_supplies_baseprod.revenue_units)   AS revenue_units,
                   SUM(actuals_supplies_baseprod.gross_revenue)   AS gross_revenue,
                   actuals_supplies_baseprod.base_product_number::text + ' '::character varying::text +
                   actuals_supplies_baseprod.pl::text + ' '::character varying::text +
                   actuals_supplies_baseprod.country_alpha2::text AS product_key
            FROM fin_prod.actuals_supplies_baseprod actuals_supplies_baseprod
                     LEFT JOIN mdm.iso_country_code_xref iso_country_code_xref
                               ON actuals_supplies_baseprod.country_alpha2::text = iso_country_code_xref.country_alpha2::text
                     LEFT JOIN mdm.list_price_eu_countrylist list_price_eu_countrylist
                               ON list_price_eu_countrylist.country_alpha2::text =
                                  actuals_supplies_baseprod.country_alpha2::text
                     LEFT JOIN mdm.country_currency_map country_currency_map_landing
                               ON country_currency_map_landing.country_alpha2::text =
                                  actuals_supplies_baseprod.country_alpha2::text
            WHERE actuals_supplies_baseprod.cal_date > ((SELECT date_add('month'::character varying::text, - 6::bigint,
                                                                         "max"(actuals_supplies_baseprod.cal_date)::timestamp WITHOUT TIME ZONE) AS date_add
                                                         FROM fin_prod.actuals_supplies_baseprod))
              AND actuals_supplies_baseprod.revenue_units > 0::double precision
            GROUP BY actuals_supplies_baseprod.cal_date, iso_country_code_xref.region_5,
                     actuals_supplies_baseprod.country_alpha2, list_price_eu_countrylist.currency,
                     country_currency_map_landing.currency_iso_code, actuals_supplies_baseprod.base_product_number,
                     actuals_supplies_baseprod.pl) actuals
               LEFT JOIN prod.acct_rates acct_rates ON acct_rates.isocurrcd::text = actuals.currency::text AND
                                                       acct_rates.effectivedate::text =
                                                       actuals.cal_date::character varying::text
               LEFT JOIN mdm.supplies_xref supplies_xref
                         ON supplies_xref.base_product_number::text = actuals.base_product_number::text
      WHERE 1 = 1
        AND (actuals.product_key IN (SELECT DISTINCT
                                            composite_key.composite_key
                                     FROM (SELECT forecast_list.base_product_number,
                                                  forecast_list.pl,
                                                  forecast_list.country_alpha2,
                                                  forecast_list.base_product_number::text +
                                                  ' '::character varying::text + forecast_list.pl::text +
                                                  ' '::character varying::text +
                                                  forecast_list.country_alpha2::text AS composite_key
                                           FROM (SELECT DISTINCT
                                                        forecast_supplies_baseprod.base_product_number,
                                                        forecast_supplies_baseprod.base_product_line_code AS pl,
                                                        forecast_supplies_baseprod.country_alpha2
                                                 FROM fin_prod.forecast_supplies_baseprod forecast_supplies_baseprod
                                                 WHERE forecast_supplies_baseprod.version::text =
                                                       ((SELECT "max"(forecast_supplies_baseprod.version::text) AS "max"
                                                         FROM fin_prod.forecast_supplies_baseprod))
                                                   AND forecast_supplies_baseprod.insights_base_units >= 1::double precision) forecast_list) composite_key))) actuals_acct
UNION
SELECT DISTINCT
       forecast_acct.record_type,
       forecast_acct.cal_date,
       forecast_acct.technology,
       forecast_acct.base_product_number,
       forecast_acct.cartridge_alias                                   AS fiji_alias,
       forecast_acct.base_product_line_code,
       forecast_acct.region_5,
       forecast_acct.country_alpha2,
       forecast_acct.insights_base_units,
       forecast_acct.base_gru,
       forecast_acct.gross_revenue,
       forecast_acct.currency,
       forecast_acct.accountingrate,
       ((SELECT "max"(forecast_supplies_baseprod.version::text) AS "max"
         FROM fin_prod.forecast_supplies_baseprod))::character varying AS version
FROM (SELECT DISTINCT
             'forecast'::character varying AS record_type,
             "forecast".cal_date,
             supplies_xref.technology,
             "forecast".base_product_number,
             supplies_xref.cartridge_alias,
             "forecast".base_product_line_code,
             "forecast".region_5,
             "forecast".country_alpha2,
             "forecast".insights_base_units,
             "forecast".base_gru,
             "forecast".gross_revenue,
             "forecast".currency,
             acct_rates.accountingrate
      FROM (SELECT DISTINCT
                   forecast_supplies_baseprod.base_product_number,
                   forecast_supplies_baseprod.base_product_line_code,
                   forecast_supplies_baseprod.region_5,
                   forecast_supplies_baseprod.country_alpha2,
                   forecast_supplies_baseprod.insights_base_units,
                   forecast_supplies_baseprod.baseprod_gru                                                  AS base_gru,
                   forecast_supplies_baseprod.baseprod_gru *
                   forecast_supplies_baseprod.insights_base_units                                           AS gross_revenue,
                   forecast_supplies_baseprod.cal_date,
                   CASE
                       WHEN list_price_eu_countrylist.currency::text = 'Euro'::character varying::text
                           THEN 'EUR'::character varying
                       WHEN list_price_eu_countrylist.currency::text = 'Dollar'::character varying::text
                           THEN 'USD'::character varying
                       WHEN forecast_supplies_baseprod.country_alpha2::text = 'CL'::character varying::text
                           THEN 'USD'::character varying
                       WHEN forecast_supplies_baseprod.country_alpha2::text = 'PR'::character varying::text
                           THEN 'USD'::character varying
                       ELSE country_currency_map_landing.currency_iso_code
                       END                                                                                  AS currency
            FROM fin_prod.forecast_supplies_baseprod forecast_supplies_baseprod
                     LEFT JOIN mdm.list_price_eu_countrylist list_price_eu_countrylist
                               ON list_price_eu_countrylist.country_alpha2::text =
                                  forecast_supplies_baseprod.country_alpha2::text
                     LEFT JOIN mdm.country_currency_map country_currency_map_landing
                               ON country_currency_map_landing.country_alpha2::text =
                                  forecast_supplies_baseprod.country_alpha2::text
            WHERE forecast_supplies_baseprod.version::text =
                  ((SELECT "max"(forecast_supplies_baseprod.version::text) AS "max"
                    FROM fin_prod.forecast_supplies_baseprod))
              AND forecast_supplies_baseprod.insights_base_units >= 1::double precision
              AND forecast_supplies_baseprod.cal_date = ((SELECT MIN(forecast_supplies_baseprod.cal_date) AS min
                                                          FROM fin_prod.forecast_supplies_baseprod
                                                          WHERE forecast_supplies_baseprod.version::text =
                                                                ((SELECT "max"(forecast_supplies_baseprod.version::text) AS "max"
                                                                  FROM fin_prod.forecast_supplies_baseprod))))) "forecast"
               LEFT JOIN prod.acct_rates acct_rates ON acct_rates.isocurrcd::text = "forecast".currency::text
               LEFT JOIN mdm.supplies_xref supplies_xref
                         ON supplies_xref.base_product_number::text = "forecast".base_product_number::text
      WHERE acct_rates.effectivedate::text = ((SELECT "max"(acct_rates.effectivedate::text) AS "max"
                                               FROM prod.acct_rates acct_rates))) forecast_acct;

alter table financials.v_base_gru_reconciliation
    owner to auto_glue;

grant delete, insert, references, select, trigger, update on financials.v_base_gru_reconciliation to group phoenix_dev;