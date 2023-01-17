CREATE OR REPLACE VIEW financials.v_adjusted_revenue_main_dashboard
AS SELECT f.record_description, f.cal_date, f.fiscal_year_qtr, f.fiscal_yr, f.geography, f.geography_grain, f.region_3, f.region_5, f.hq_flag, f.pl, f.l5_description, plx.l6_description, f.technology, f.accounting_rate, sum(f.reported_revenue) AS reported_revenue, sum(f.currency) AS currency_at_current_rate, sum(f.revenue_in_cc) AS rev_in_cc_at_current_rate, sum(f.inventory_change) AS ci_change, sum(f.ci_currency_impact) AS ci_valuation_impact, sum(f.total_inventory_change) AS total_ci_change, sum(f.adjusted_revenue) AS adjusted_revenue, 
        CASE
            WHEN f.version::text = '2021.05.18.1'::text THEN 'May Flash'::text
            WHEN f.version::text = '2021.08.12.1'::text THEN 'Q321 Actuals @ July Rates'::text
            WHEN f.version::text = '2021.08.12.2'::text THEN 'Aug Flash @ Aug Rates'::text
            WHEN f.version::text = '2021.11.10.1'::text THEN 'Q421 Actuals @ Oct Rates'::text
            WHEN f.version::text = '2022.02.17.1'::text THEN 'Q122 Actuals @ Jan Rates'::text
            WHEN f.version::text = '2022.05.03.1'::text THEN 'April Flash @ May Rates'::text
            WHEN f.version::text = '2022.05.12.1'::text THEN 'Q222 Actuals @ April Rates'::text
            WHEN f.version::text = '2022.08.15.1'::text THEN 'Q322 Actuals @ July Rates'::text
            WHEN f.version::text = '2022.11.09.1'::text THEN 'Q422 Actuals @ Oct Rates'::text
            ELSE concat(to_char(f.load_date, 'Month'::text), ' Flash'::text)
        END AS flash, f.version
   FROM fin_prod.adjusted_revenue_flash f
   LEFT JOIN mdm.calendar cal ON f.cal_date = cal.date
   LEFT JOIN mdm.product_line_xref plx ON f.pl::text = plx.pl::text
  WHERE 1 = 1 AND cal.day_of_month = 1::double precision AND (f.version::text = '2022.02.17.1'::text OR f.version::text = '2022.05.12.1'::text OR f.version::text = '2022.08.15.1'::text OR f.version::text = '2022.10.13.1'::text OR f.version::text = '2022.11.09.1'::text OR f.version::text = '2022.11.11.1'::text)
  GROUP BY f.record_description, f.cal_date, f.fiscal_year_qtr, f.geography, f.geography_grain, f.region_3, f.region_5, f.hq_flag, f.pl, f.l5_description, plx.l6_description, f.technology, f.fiscal_yr, f.accounting_rate, f.version, cal.month_name, f.load_date;
 
 -- Permissions

grant delete, insert, references, select, trigger, update on financials.v_adjusted_revenue_main_dashboard to group phoenix_dev;
