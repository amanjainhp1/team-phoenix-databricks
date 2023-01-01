create view financials.v_finance_forecast_pl as
SELECT base_pl.record_type,
       base_pl.product_number,
       base_pl.product_line_code,
       base_pl.technology,
       base_pl.is_canon,
       mix.platform_subset,
       hw.hw_product_family,
       base_pl.supplies_family,
       base_pl.region_5,
       base_pl.country_alpha2,
       base_pl.market10,
       base_pl.cal_date,
       base_pl.fiscal_year_qtr,
       base_pl.fiscal_year_half,
       base_pl.fiscal_yr,
       COALESCE(base_pl.units, 0::double precision) * COALESCE(mix.cartridge_printer_mix, 0::double precision)              AS units,
       COALESCE(base_pl.gross_revenue, 0::double precision) * COALESCE(mix.cartridge_printer_mix, 0::double precision)      AS gross_revenue,
       COALESCE(base_pl.contra, 0::double precision) * COALESCE(mix.cartridge_printer_mix, 0::double precision)             AS contra,
       COALESCE(base_pl.revenue_currency, 0::double precision) * COALESCE(mix.cartridge_printer_mix, 0::double precision)   AS revenue_currency,
       COALESCE(base_pl.net_revenue, 0::double precision) * COALESCE(mix.cartridge_printer_mix, 0::double precision)        AS net_revenue,
       COALESCE(base_pl.variable_cost, 0::double precision) * COALESCE(mix.cartridge_printer_mix, 0::double precision)      AS variable_cost,
       COALESCE(base_pl.contribution_margin, 0::double precision) * COALESCE(mix.cartridge_printer_mix, 0::double precision) AS contribution_margin,
       COALESCE(base_pl.fixed_cost, 0::double precision) * COALESCE(mix.cartridge_printer_mix, 0::double precision)         AS fixed_cost,
       COALESCE(base_pl.gross_profit, 0::double precision) * COALESCE(mix.cartridge_printer_mix, 0::double precision)       AS gross_profit,
       base_pl.contra_version,
       base_pl.variable_cost_version,
       base_pl.acct_rates_version,
       base_pl.lp_gpsy_version
FROM (SELECT 'base_product'::character varying                                             AS record_type,
             forecast_supplies_baseprod.base_product_number                                AS product_number,
             forecast_supplies_baseprod.base_product_line_code                             AS product_line_code,
             supplies_xref.technology,
             supplies_xref.supplies_family,
             CASE
                 WHEN forecast_supplies_baseprod.base_product_line_code::text = '5T'::character varying::text OR
                      forecast_supplies_baseprod.base_product_line_code::text = 'GJ'::character varying::text OR
                      forecast_supplies_baseprod.base_product_line_code::text = 'GK'::character varying::text OR
                      forecast_supplies_baseprod.base_product_line_code::text = 'GP'::character varying::text OR
                      forecast_supplies_baseprod.base_product_line_code::text = 'IU'::character varying::text OR
                      forecast_supplies_baseprod.base_product_line_code::text = 'LS'::character varying::text THEN 1
                 ELSE 0
                 END                                                                       AS is_canon,
             iso_country_code_xref.region_5,
             iso_country_code_xref.country_alpha2,
             iso_country_code_xref.market10,
             forecast_supplies_baseprod.cal_date,
             calendar.fiscal_year_qtr,
             calendar.fiscal_year_half,
             calendar.fiscal_yr,
             forecast_supplies_baseprod.insights_base_units                                AS units,
             COALESCE(forecast_supplies_baseprod.baseprod_gru, 0::double precision) *
             COALESCE(forecast_supplies_baseprod.insights_base_units, 0::double precision) AS gross_revenue,
             COALESCE(forecast_supplies_baseprod.baseprod_contra_per_unit, 0::double precision) *
             COALESCE(forecast_supplies_baseprod.insights_base_units, 0::double precision) AS contra,
             COALESCE(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0::double precision) *
             COALESCE(forecast_supplies_baseprod.insights_base_units, 0::double precision) AS revenue_currency,
             (COALESCE(forecast_supplies_baseprod.baseprod_gru, 0::double precision) -
              COALESCE(forecast_supplies_baseprod.baseprod_contra_per_unit, 0::double precision) +
              COALESCE(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0::double precision)) *
             COALESCE(forecast_supplies_baseprod.insights_base_units, 0::double precision) AS net_revenue,
             COALESCE(forecast_supplies_baseprod.baseprod_variable_cost_per_unit, 0::double precision) *
             COALESCE(forecast_supplies_baseprod.insights_base_units, 0::double precision) AS variable_cost,
             (COALESCE(forecast_supplies_baseprod.baseprod_gru, 0::double precision) -
              COALESCE(forecast_supplies_baseprod.baseprod_contra_per_unit, 0::double precision) +
              COALESCE(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0::double precision) -
              COALESCE(forecast_supplies_baseprod.baseprod_variable_cost_per_unit, 0::double precision)) *
             COALESCE(forecast_supplies_baseprod.insights_base_units, 0::double precision) AS contribution_margin,
             COALESCE(forecast_supplies_baseprod.baseprod_fixed_cost_per_unit, 0::double precision) *
             COALESCE(forecast_supplies_baseprod.insights_base_units, 0::double precision) AS fixed_cost,
             (COALESCE(forecast_supplies_baseprod.baseprod_gru, 0::double precision) -
              COALESCE(forecast_supplies_baseprod.baseprod_contra_per_unit, 0::double precision) +
              COALESCE(forecast_supplies_baseprod.baseprod_revenue_currency_hedge_unit, 0::double precision) -
              COALESCE(forecast_supplies_baseprod.baseprod_variable_cost_per_unit, 0::double precision) -
              COALESCE(forecast_supplies_baseprod.baseprod_fixed_cost_per_unit, 0::double precision)) *
             COALESCE(forecast_supplies_baseprod.insights_base_units, 0::double precision) AS gross_profit,
             forecast_supplies_baseprod.contra_version,
             forecast_supplies_baseprod.variable_cost_version,
             lpv.acct_rates_version,
             lpv.lp_gpsy_version
      FROM fin_prod.forecast_supplies_baseprod forecast_supplies_baseprod
               JOIN mdm.iso_country_code_xref iso_country_code_xref
                    ON forecast_supplies_baseprod.country_alpha2::text = iso_country_code_xref.country_alpha2::text AND
                       forecast_supplies_baseprod.version::text =
                       ((SELECT "max"(forecast_supplies_baseprod.version::text) AS "max"
                         FROM fin_prod.forecast_supplies_baseprod))
               JOIN mdm.calendar calendar ON calendar.date = forecast_supplies_baseprod.cal_date
               JOIN mdm.supplies_xref supplies_xref
                    ON supplies_xref.base_product_number::text = forecast_supplies_baseprod.base_product_number::text
               JOIN fin_prod.list_price_version lpv
                    ON lpv.version::text = forecast_supplies_baseprod.sales_gru_version::text) base_pl
         JOIN (SELECT sup.cal_date,
                      sup.geography AS market10,
                      sup.platform_subset,
                      sup.base_product_number,
                      CASE
                          WHEN (SUM(sup.adjusted_cartridges)
                                OVER (
                                    PARTITION BY sup.cal_date, sup.geography, sup.base_product_number
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 0::double precision
                              THEN NULL::double precision
                          ELSE sup.adjusted_cartridges / (SUM(sup.adjusted_cartridges)
                                                          OVER (
                                                              PARTITION BY sup.cal_date, sup.geography, sup.base_product_number
                                                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
                          END       AS cartridge_printer_mix
               FROM prod.working_forecast sup
               WHERE sup.version::text = ((SELECT "max"(working_forecast.version::text) AS "max"
                                           FROM prod.working_forecast))
                 AND sup.cal_date >= ((SELECT MIN(forecast_supplies_baseprod.cal_date) AS min
                                       FROM fin_prod.forecast_supplies_baseprod))
                 AND sup.adjusted_cartridges <> 0::double precision
                 AND sup.geography_grain::text = 'market10'::character varying::text
               GROUP BY sup.cal_date, sup.geography, sup.platform_subset, sup.base_product_number,
                        sup.adjusted_cartridges) mix
              ON base_pl.cal_date = mix.cal_date AND base_pl.market10::text = mix.market10::text AND
                 base_pl.product_number::text = mix.base_product_number::text
         JOIN mdm.hardware_xref hw ON mix.platform_subset::text = hw.platform_subset::text;

alter table financials.v_finance_forecast_pl
    owner to auto_glue;

grant delete, insert, references, select, trigger, update on financials.v_finance_forecast_pl to group phoenix_dev;