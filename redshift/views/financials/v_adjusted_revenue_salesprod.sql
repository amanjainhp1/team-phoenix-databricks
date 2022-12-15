CREATE OR REPLACE VIEW financials.v_adjusted_revenue_salesprod
AS 

SELECT 
record,
cal_date,
country_alpha2,
country,
geography,
geography_grain,
region_5,y
sales_product_number,
pl,
l5_description,
customer_engagement,
currency,
net_revenue_before_hedge,
net_hedge_benefit,
net_revenue,
original_rate_usd,
current_rate_usd,
currency_rate_adjustment,
currency_impact,
cc_net_revenue,
inventory_usd,
prev_inv_usd,
inventory_qty,
prev_inv_qty,
monthly_unit_change,
reported_inventory_valuation_rate,
inventory_change_impact,
currency_impact_ch_inventory,
cc_inventory_impact, adjusted_revenue,
official,
load_date,
version,
accounting_rate,
monthly_inv_usd_change,
previous_inventory_valuation_rate,
implied_period_price_change,
implied_price_change_x_prev_inv_qty,
monthly_unit_change_dollar_impact,
proxy_change,
monthly_mix_change,
alt_inventory_change_calc,
inv_chg_calc_variance
FROM fin_prod.adjusted_revenue_salesprod
where version = (select max(version) from fin_prod.adjusted_revenue_salesprod)
and official = 1;

-- Permissions

GRANT ALL ON TABLE financials.v_act_financials TO auto_glue;
GRANT UPDATE, TRIGGER, REFERENCES, INSERT, SELECT, DELETE ON TABLE financials.v_act_financials TO group phoenix_dev;
GRANT SELECT ON TABLE financials.v_adjusted_revenue_salesprod TO "118(financials)";
