CREATE OR REPLACE PROCEDURE fin_stage.load_forecast_contra_input_sproc(group_param varchar)
    LANGUAGE plpgsql
AS $$

BEGIN

CREATE TABLE IF NOT EXISTS fin_stage.forecast_contra_input_lt_staging(
    pl VARCHAR(255)
    ,region_5 VARCHAR(255)
    ,country_code VARCHAR(255)
    ,fiscal_yr_qtr VARCHAR(255)
    ,contra_per_qtr DOUBLE PRECISION
);
GRANT ALL ON TABLE fin_stage.forecast_contra_input_lt_staging TO auto_glue;
GRANT ALL ON TABLE fin_stage.forecast_contra_input_lt_staging TO GROUP group_param;

---------TRUNCATE FINANCIAL TABLE--------------------------------

TRUNCATE TABLE fin_stage.forecast_contra_input_lt_staging;

-------------------------------LOAD DATA TO FINANCIAL TABLES-----------------------------

INSERT INTO fin_stage.forecast_contra_input_lt_staging(
 pl
,region_5
,country
,fiscal_yr_qtr
,contra_per_qtr
)
SELECT 
 pl
,region_5
,country
,fiscal_yr_qtr
,contra_per_qtr
FROM fin_stage.forecast_contra_input_lt_landing;

------------------------------------------RUN PROC--------------------------------------

CALL fin_prod.load_forecast_contra_input_lt_sproc();

--------------------------------DROP LANDING TABLE-------------------------------------
DROP TABLE fin_stage.forecast_contra_input_lt_landing;

END;

$$
;