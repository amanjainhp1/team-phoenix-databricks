CREATE OR REPLACE PROCEDURE fin_stage.load_forecast_variablecost_ink_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

---------------------------------TRUNCATE FINANCIAL TABLES---------------------------------------

CREATE TABLE IF NOT EXISTS fin_stage.forecast_variablecost_ink_landing(
    product_family VARCHAR(255)
    ,region_5 VARCHAR(255)
    ,country_code VARCHAR(255)
    ,fiscal_yr_qtr VARCHAR(255)
    ,variable_cost DOUBLE PRECISION
);
GRANT ALL ON TABLE fin_stage.forecast_variablecost_ink_landing TO auto_glue;

--------------------------------------------LOAD DATA TO FINANCIAL TABLES----------------------------------------------------------------

INSERT INTO fin_stage.forecast_variablecost_ink_landing
(
    product_family
    ,region_5
    ,country_code
    ,fiscal_yr_qtr
    ,variable_cost
)
SELECT 
    product_family
    ,region_5
    ,country_code
    ,fiscal_yr_qtr
    ,variable_cost
FROM fin_stage.forecast_variablecost_ink_temp_landing;

-------------------------------------RUN FINANCIAL PROCS---------------------------------------------------------

CALL fin_prod.load_forecast_variablecost_ink_sproc();

-----------------------------------------TRUNCATE LANDING TABLES-------------------------------------------------

DROP TABLE fin_stage.forecast_variablecost_ink_temp_landing;

-------------------------------------------------------------------------------------------------------------------

END;

$$
;