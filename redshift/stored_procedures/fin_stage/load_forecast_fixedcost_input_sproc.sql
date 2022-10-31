CREATE OR REPLACE PROCEDURE fin_stage.load_forecast_fixedcost_input_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

---------------------------------TRUNCATE FINANCIAL TABLES---------------------------------------

CREATE TABLE IF NOT EXISTS fin_stage.forecast_fixedcost_input_landing(
    fixedcost_desc VARCHAR(255)
    ,pl VARCHAR(255)
    ,region_3 VARCHAR(255)
    ,country_code VARCHAR(255)
    ,fiscal_yr_qtr VARCHAR(255)
    ,fixedcost_k_qtr DOUBLE PRECISION
);
GRANT ALL ON TABLE fin_stage.forecast_fixedcost_input_landing TO auto_glue;

--------------------------------------------LOAD DATA TO FINANCIAL TABLES----------------------------------------------------------------

INSERT INTO fin_stage.forecast_fixedcost_input_landing
(
    fixedcost_desc
    ,pl
    ,region_3
    ,country_code
    ,fiscal_yr_qtr
    ,fixedcost_k_qtr
)
SELECT 
    fixedcost_desc
    ,pl
    ,region_3
    ,country_code
    ,fiscal_yr_qtr
    ,fixedcost_k_qtr
FROM fin_stage.forecast_fixedcost_input_temp_landing;

-------------------------------------RUN FINANCIAL PROCS---------------------------------------------------------

CALL fin_prod.load_forecast_fixedcost_input_sproc();

-----------------------------------------TRUNCATE LANDING TABLES-------------------------------------------------

DROP TABLE fin_stage.forecast_fixedcost_input_temp_landing;

-------------------------------------------------------------------------------------------------------------------

END;

$$
;