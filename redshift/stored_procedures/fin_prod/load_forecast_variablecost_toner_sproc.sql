CREATE OR REPLACE PROCEDURE fin_prod.load_forecast_variablecost_toner_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

--Add version to version table
CALL prod.addversion_sproc('FORECAST_VARIABLECOST_TONER','FINANCE');

UPDATE fin_prod.forecast_variablecost_toner
SET official = 0
WHERE EXISTS (
    SELECT 1
    FROM fin_stage.forecast_variablecost_toner_landing landing
    WHERE forecast_variablecost_toner.base_product_number = landing.base_product_number
        AND forecast_variablecost_toner.region_5 = landing.region_5
        AND forecast_variablecost_toner.fiscal_yr_qtr = landing.fiscal_yr_qtr
);

INSERT INTO fin_prod.forecast_variablecost_toner
    (record
    ,base_product_number
    ,region_5
    ,country_code
    ,fiscal_yr_qtr
    ,variable_cost
    ,official
    ,version
    ,load_date)
SELECT 'FORECAST_VARIABLECOST_TONER' as record
    ,base_product_number
    ,region_5
    ,country_code
    ,fiscal_yr_qtr
    ,variable_cost
    ,1 as official
    ,(SELECT version from prod.version WHERE record = 'FORECAST_VARIABLECOST_TONER' 
            AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'FORECAST_VARIABLECOST_TONER')) as version
    ,(SELECT load_date from prod.version WHERE record = 'FORECAST_VARIABLECOST_TONER' 
            AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'FORECAST_VARIABLECOST_TONER')) as load_date
FROM fin_stage.forecast_variablecost_toner_landing;

END;

$$
;