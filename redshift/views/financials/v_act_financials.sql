create or replace view financials.v_act_financials as
SELECT act.record,
       act.cal_date,
       act.country_alpha2,
       iso.country,
       act.market10,
       cc.country_level_1 AS market8,
       iso.region_5,
       act.platform_subset,
       act.base_product_number,
       act.pl,
       act.l5_description,
       act.customer_engagement,
       UPPER(sup.single_multi::text) AS pack_size,
       sup.equivalents_multiplier,
       act.gross_revenue,
       act.net_currency,
       act.contractual_discounts,
       act.discretionary_discounts,
       act.net_revenue,
       act.total_cos,
       act.gross_profit,
       act.revenue_units,
       act.equivalent_units,
       act.yield_x_units,
       act.yield_x_units_black_only,
       act.official,
       act.load_date,
       act.version
FROM fin_prod.actuals_supplies_baseprod act
         LEFT JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2::text = act.country_alpha2::text
         LEFT JOIN mdm.supplies_xref sup ON sup.base_product_number::text = act.base_product_number::text
         LEFT JOIN mdm.iso_cc_rollup_xref cc ON cc.country_alpha2::text = act.country_alpha2::text
WHERE cc.country_scenario::text = 'MARKET8'::character varying::text;

alter table financials.v_act_financials
    owner to auto_glue;

grant delete, insert, references, select, trigger, update on financials.v_act_financials to group phoenix_dev;

