create view financials.v_sales_product_monthly as
SELECT act.record,
       act.cal_date,
       cal.edw_fiscal_yr_mo,
       iso.region_5,
       ''                               AS platform_subset,
       act.sales_product_number         AS supplies_sales_product_number,
       CASE
           WHEN rdma.sales_prod_nm IS NULL THEN act.sales_product_number
           ELSE rdma.sales_prod_nm
           END                          AS supplies_sales_product_name,
       act.pl,
       act.l5_description,
       CASE
           WHEN act.pl::text = '1N'::character varying::text THEN 'INK'::character varying
           WHEN act.pl::text = 'GD'::character varying::text THEN 'INK'::character varying
           WHEN act.pl::text = 'GM'::character varying::text THEN 'PWA'::character varying
           WHEN act.pl::text = 'K6'::character varying::text THEN 'PWA'::character varying
           WHEN act.pl::text = 'LU'::character varying::text THEN 'INK'::character varying
           WHEN act.pl::text = 'LZ'::character varying::text THEN 'INK'::character varying
           ELSE NULL::character varying
           END                          AS technology,
       ''                               AS tech_split,
       act.customer_engagement,
       CASE
           WHEN SUM(act.gross_revenue) < SUM(act.contractual_discounts + act.discretionary_discounts - act.net_currency)
               THEN SUM(act.contractual_discounts + act.discretionary_discounts - act.net_currency)
           ELSE SUM(act.gross_revenue)
           END                          AS gross_rev,
       SUM(act.contractual_discounts)   AS contractual_discounts,
       SUM(act.discretionary_discounts) AS discretionary_discounts,
       SUM(act.net_revenue)             AS net_revenue,
       SUM(act.total_cos)               AS total_cos,
       SUM(act.gross_profit)            AS gross_profit,
       SUM(act.revenue_units)           AS revenue_units
FROM fin_prod.actuals_supplies_salesprod act
         LEFT JOIN mdm.calendar cal ON act.cal_date = cal.date
         LEFT JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2::text = act.country_alpha2::text
         LEFT JOIN mdm.rdma_sales_product rdma ON rdma.sales_prod_nr::text = act.sales_product_number::text
WHERE (cal.fiscal_yr::text = (((SELECT calendar.fiscal_yr
                                FROM mdm.calendar
                                WHERE calendar.date = 'now'::character varying::date))::text) OR
       cal.fiscal_yr::numeric(18, 0) = ((SELECT calendar.fiscal_yr::numeric(18, 0) - 1::numeric::numeric(18, 0)
                                         FROM mdm.calendar
                                         WHERE calendar.date = 'now'::character varying::date)))
  AND cal.day_of_month = 1::double precision
  AND (act.pl::text = '1N'::character varying::text OR act.pl::text = 'GD'::character varying::text OR
       act.pl::text = 'GM'::character varying::text OR act.pl::text = 'K6'::character varying::text OR
       act.pl::text = 'LU'::character varying::text OR act.pl::text = 'LZ'::character varying::text)
  AND act.gross_revenue >= 0::double precision
GROUP BY act.record, act.cal_date, iso.region_5, act.sales_product_number, act.pl, act.l5_description,
         act.customer_engagement, rdma.sales_prod_nm, cal.edw_fiscal_yr_mo;

alter table financials.v_sales_product_monthly
    owner to auto_glue;

grant delete, insert, references, select, trigger, update on financials.v_sales_product_monthly to group phoenix_dev;