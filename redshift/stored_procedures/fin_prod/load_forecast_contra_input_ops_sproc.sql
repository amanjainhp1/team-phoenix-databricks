CREATE OR REPLACE PROCEDURE fin_prod.load_forecast_contra_input_ops_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

--Add version to version table
CALL prod.addversion_sproc('FORECAST_CONTRA_INPUT','FINANCE');

UPDATE fin_prod.forecast_contra_input
SET official = 0
WHERE EXISTS (
    SELECT *
    FROM fin_stage.forecast_contra_input_ops_staging contra_ops
    WHERE forecast_contra_input.pl = contra_ops.pl
        AND forecast_contra_input.region_5 = contra_ops.region_5
        AND forecast_contra_input.country = contra_ops.country
        AND forecast_contra_input.fiscal_yr_qtr = contra_ops.fiscal_yr_qtr
        AND contra_ops.base_product_number IS NULL
        AND forecast_contra_input.base_product_number IS NULL
);

UPDATE fin_prod.forecast_contra_input
set official = 0
WHERE EXISTS (
    SELECT *
    FROM fin_stage.forecast_contra_input_ops_staging contra_ops
    WHERE contra_ops.pl is null
        AND contra_ops.pl is null
        AND forecast_contra_input.region_5 = contra_ops.region_5
        AND forecast_contra_input.country = contra_ops.country
        AND forecast_contra_input.fiscal_yr_qtr = contra_ops.fiscal_yr_qtr
        AND forecast_contra_input.base_product_number = contra_ops.base_product_number
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
      ,country
	  ,fiscal_yr_qtr
      ,contra_per_qtr
      ,1 AS official
	  ,(SELECT version FROM prod.version WHERE record = 'FORECAST_CONTRA_INPUT' 
				AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'FORECAST_CONTRA_INPUT')) AS version
      ,(SELECT load_date from prod.version WHERE record = 'FORECAST_CONTRA_INPUT' 
				AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'FORECAST_CONTRA_INPUT')) AS load_date
	  ,base_product_number
FROM fin_stage.forecast_contra_input_ops_staging;

END;

$$
;