-- financials.currency_hedge_vw source

CREATE OR REPLACE VIEW financials.currency_hedge_vw
AS SELECT a.profit_center, a.currency, a."month", a.revenue_currency_hedge, b.technology, c.region_5, a.load_date, a.version
   FROM fin_prod.currency_hedge a
   LEFT JOIN mdm.product_line_xref b ON a.profit_center::text = b.profit_center_code::text
   LEFT JOIN ( SELECT DISTINCT d.region_5, c.currency_iso_code
      FROM mdm.country_currency_map c
   LEFT JOIN mdm.iso_country_code_xref d ON c.country_alpha2::text = d.country_alpha2::text
  WHERE (c.currency_iso_code IN ( SELECT DISTINCT currency_hedge.currency
         FROM fin_prod.currency_hedge))) c ON a.currency::text = c.currency_iso_code::text;
