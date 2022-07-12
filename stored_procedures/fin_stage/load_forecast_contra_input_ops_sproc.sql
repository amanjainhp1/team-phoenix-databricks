CREATE OR REPLACE PROCEDURE fin_prod.load_forecast_contra_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

CREATE TABLE IF NOT EXISTS fin_stage.forecast_contra_input_ops_staging(
    pl VARCHAR(255)
    ,region_5 VARCHAR(255)
    ,country_code VARCHAR(255)
    ,fiscal_yr_qtr VARCHAR(255)
    ,contra_per_qtr DOUBLE PRECISION
    ,base_product_number VARCHAR(50)
);
GRANT ALL ON TABLE fin_stage.forecast_contra_input_ops_staging TO auto_glue;

---------------------TRUNCATE FORECAST CONTRA TABLE IN fin_prod----------------------------------

TRUNCATE TABLE fin_stage.forecast_contra_input_ops_staging;

-------------------------------------POPULATE FORECAST CONTRA TABLE IN fin_prod FROM fin_stage------------------------------------
INSERT INTO fin_stage.forecast_contra_input_ops_staging
(
 pl
,region_5
,country
,fiscal_yr_qtr
,contra_per_qtr
,base_product_number
)
SELECT 
 pl
,region_5
,country
,fiscal_yr_qtr
,contra_per_qtr
,base_product_number
FROM fin_stage.forecast_contra_input_ops_landing;

---------------------------------------------------------MOVE DATA TO OFFICIAL FINANCIAL TABLE-------------------------------------

CALL fin_prod.load_forecast_contra_input_ops_sproc();

--------------------------------------------DROP TABLE------------------------------------------------------

DROP TABLE fin_stage.forecast_contra_input_ops_landing;

END;

$$
;