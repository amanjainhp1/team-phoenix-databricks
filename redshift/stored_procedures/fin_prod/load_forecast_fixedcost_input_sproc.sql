CREATE OR REPLACE PROCEDURE fin_prod.load_forecast_fixedcost_input_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

--Add version to version table
CALL prod.addversion_sproc('FORECAST_FIXED_COST','FINANCE');

UPDATE fin_prod.forecast_fixedcost_input
SET official = 0
WHERE EXISTS (
    SELECT 1
    FROM fin_stage.forecast_fixedcost_input_landing landing
    WHERE forecast_fixedcost_input.pl = landing.pl
        AND forecast_fixedcost_input.region_3 = landing.region_3
        AND forecast_fixedcost_input.country_code = landing.country_code
        AND forecast_fixedcost_input.fiscal_yr_qtr = landing.fiscal_yr_qtr
);

--copy max version from landing to financials
INSERT INTO fin_prod.forecast_fixedcost_input
    (record
    ,fixedcost_desc
    ,pl
    ,region_3
    ,country_code
    ,fiscal_yr_qtr
    ,fixedcost_k_qtr
    ,official
    ,version
    ,load_date)
SELECT 'FORECAST_FIXED_COST' as record
    ,fixedcost_desc
    ,pl
    ,region_3
    ,country_code
    ,fiscal_yr_qtr
    ,fixedcost_k_qtr
    ,1 as official
    ,(SELECT version from prod.version WHERE record = 'FORECAST_FIXED_COST' 
            AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'FORECAST_FIXED_COST')) as version
    ,(SELECT load_date from prod.version WHERE record = 'FORECAST_FIXED_COST' 
            AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'FORECAST_FIXED_COST')) as load_date
FROM fin_stage.forecast_variablecost_ink_landing;

END;

$$
;