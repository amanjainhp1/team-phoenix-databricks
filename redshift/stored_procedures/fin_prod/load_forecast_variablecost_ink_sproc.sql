CREATE OR REPLACE PROCEDURE fin_prod.load_forecast_variablecost_ink_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

--Add version to version table
CALL prod.addversion_sproc('FORECAST_VARIABLECOST_INK','FINANCE');

UPDATE fin_prod.forecast_variable_cost_ink
SET official = 0
WHERE EXISTS (
    SELECT 1
    FROM fin_stage.forecast_variablecost_ink_landing landing
    WHERE forecast_variablecost_ink.base_product_number = landing.base_product_number
        AND forecast_variablecost_ink.region_5 = landing.region_5
        AND forecast_variablecost_ink.country_code = landing.country_code
        AND forecast_variablecost_ink.fiscal_yr_qtr = landing.fiscal_yr_qtr
);

--copy max version from landing to financials
INSERT INTO fin_prod.forecast_variable_cost_ink
    (record
    ,product_family
    ,region_5
    ,country_code
    ,fiscal_yr_qtr
    ,variable_cost
    ,official
    ,version
    ,load_date)
SELECT 'FORECAST_VARIABLECOST_INK' as record
    ,product_family
    ,region_5
    ,country_code
    ,fiscal_yr_qtr
    ,variable_cost
    ,1 as official
    ,(SELECT version from prod.version WHERE record = 'FORECAST_VARIABLECOST_INK' 
            AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'FORECAST_VARIABLECOST_INK')) as version
    ,(SELECT load_date from prod.version WHERE record = 'FORECAST_VARIABLECOST_INK' 
            AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'FORECAST_VARIABLECOST_INK')) as load_date
FROM fin_stage.forecast_variablecost_ink_landing;

END;

$$
;
