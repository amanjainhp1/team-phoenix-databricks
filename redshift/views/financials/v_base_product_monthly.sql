create or replace view financials.v_base_product_monthly as
SELECT act.record,
       act.cal_date,
       cal.edw_fiscal_yr_mo,
       iso.region_5,
       act.platform_subset,
       act.base_product_number          AS supplies_base_product_number,
       rdma.base_prod_name              AS supplies_product_name,
       rdma.base_prod_desc              AS supplies_product_desc,
       act.pl,
       plx.l5_description,
       plx.technology,
       hw.hw_product_family             AS tech_split,
       act.customer_engagement,
       SUM(act.gross_revenue)           AS gross_rev,
       SUM(act.contractual_discounts)   AS contractual_discounts,
       SUM(act.discretionary_discounts) AS discretionary_discounts,
       SUM(act.net_revenue)             AS net_revenue,
       SUM(act.total_cos)               AS total_cos,
       SUM(act.gross_profit)            AS gross_profit,
       SUM(act.revenue_units)           AS revenue_units,
       SUM(act.equivalent_units)        AS equivalent_units
FROM fin_prod.actuals_supplies_baseprod act
         LEFT JOIN mdm.hardware_xref hw ON act.platform_subset::text = hw.platform_subset::text
         LEFT JOIN mdm.calendar cal ON act.cal_date = cal.date
         LEFT JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2::text = act.country_alpha2::text
         LEFT JOIN mdm.rdma ON act.base_product_number::text = rdma.base_prod_number::text
         LEFT JOIN mdm.product_line_xref plx ON act.pl::text = plx.pl::text
WHERE cal.fiscal_yr::text = (((SELECT calendar.fiscal_yr
                               FROM mdm.calendar
                               WHERE calendar.date = 'now'::character varying::date))::text)
   OR cal.fiscal_yr::numeric(18, 0) = ((SELECT calendar.fiscal_yr::numeric(18, 0) - 1::numeric::numeric(18, 0)
                                        FROM mdm.calendar
                                        WHERE calendar.date = 'now'::character varying::date))
   OR cal.fiscal_yr::numeric(18, 0) = ((SELECT calendar.fiscal_yr::numeric(18, 0) - 2::numeric::numeric(18, 0)
                                        FROM mdm.calendar
                                        WHERE calendar.date = 'now'::character varying::date))
   OR cal.fiscal_yr::numeric(18, 0) = ((SELECT calendar.fiscal_yr::numeric(18, 0) - 3::numeric::numeric(18, 0)
                                        FROM mdm.calendar
                                        WHERE calendar.date = 'now'::character varying::date)) AND
      cal.day_of_month = 1::double precision AND
      (act.pl::text = '1N'::character varying::text OR act.pl::text = 'GD'::character varying::text OR
       act.pl::text = 'GM'::character varying::text OR act.pl::text = 'K6'::character varying::text OR
       act.pl::text = 'LU'::character varying::text OR act.pl::text = 'LZ'::character varying::text)
GROUP BY act.record, act.cal_date, iso.region_5, act.platform_subset, act.base_product_number, rdma.base_prod_name,
         rdma.base_prod_desc, act.pl, plx.l5_description, act.customer_engagement, plx.technology, hw.hw_product_family,
         cal.edw_fiscal_yr_mo;

alter table financials.v_base_product_monthly
    owner to auto_glue;

grant delete, insert, references, select, trigger, update on financials.v_base_product_monthly to group phoenix_dev;