CREATE OR REPLACE PROCEDURE fin_prod.load_forecast_contra_input_lt_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

--Add version to version table
CALL prod.addversion_sproc('FORECAST_CONTRA_INPUT_LT', 'FINANCE');

DELETE FROM fin_prod.forecast_contra_input
USING fin_stage.forecast_contra_input_lt_staging
WHERE fin_prod.forecast_contra_input.pl = fin_stage.forecast_contra_input_lt_staging.pl
    AND fin_prod.forecast_contra_input.region_5 = fin_stage.forecast_contra_input_lt_staging.region_5
    AND fin_prod.forecast_contra_input.fiscal_yr_qtr = fin_stage.forecast_contra_input_lt_staging.fiscal_yr_qtr
;

UPDATE fin_prod.forecast_contra_input
SET official = 0
WHERE EXISTS (
    SELECT *
    FROM fin_stage.forecast_contra_input_lt_staging contra_LT
    WHERE forecast_contra_input.pl = contra_LT.pl
    AND forecast_contra_input.region_5 = contra_LT.region_5
    AND forecast_contra_input.country = contra_LT.country
    AND forecast_contra_input.fiscal_yr_qtr = contra_LT.fiscal_yr_qtr
);

INSERT INTO fin_prod.forecast_contra_input
           (record
           ,pl
           ,region_5
           ,country
		   ,fiscal_yr_qtr
           ,contra_per_qtr
           ,official
           ,version
           ,load_date
		   ,base_product_number)
SELECT 'FORECAST_CONTRA_INPUT' as record
	  ,pl
      ,region_5
      ,country_code as country
	  ,fiscal_yr_qtr
      ,contra_per_qtr
      ,1 as official
	  ,(SELECT version FROM prod.version WHERE record = 'FORECAST_CONTRA_INPUT_LT' 
				AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'FORECAST_CONTRA_INPUT_LT')) AS version
      ,(SELECT load_date FROM prod.version WHERE record = 'FORECAST_CONTRA_INPUT_LT' 
				AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'FORECAST_CONTRA_INPUT_LT')) AS load_date
	  ,NULL as base_product_number
FROM fin_stage.forecast_contra_input_lt_staging;

END;

$$
;