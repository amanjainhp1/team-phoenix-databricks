create view financials.v_stf_gru as
SELECT forecast_supplies_baseprod_region_stf.base_product_number,
       forecast_supplies_baseprod_region_stf.base_product_line_code,
       forecast_supplies_baseprod_region_stf.region_5,
       forecast_supplies_baseprod_region_stf.cal_date,
       forecast_supplies_baseprod_region_stf.baseprod_gru,
       'WORKING' AS version
FROM fin_prod.forecast_supplies_baseprod_region_stf
WHERE 1 = 1
UNION
SELECT stf_dollarization.base_product_number,
       stf_dollarization.pl        AS base_product_line_code,
       stf_dollarization.geography AS region_5,
       stf_dollarization.cal_date,
       stf_dollarization.gross_revenue /
       CASE
           WHEN stf_dollarization.units = 0::double precision THEN NULL::double precision
           ELSE stf_dollarization.units
           END                     AS baseprod_gru,
       stf_dollarization.version
FROM fin_prod.stf_dollarization;

alter table financials.v_stf_gru
    owner to auto_glue;