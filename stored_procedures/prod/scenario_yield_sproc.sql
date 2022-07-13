CREATE OR REPLACE PROCEDURE prod.scenario_yield_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

--Description: LOAD 'SCENARIO' data FROM TEMP_LANDING TO PROD
--Created Date: 02/12/2021
--Created By: Daman Singh
--Modified : --

-------------------------------RUN SPROC TO UPDATE VERSION AND LOAD DATE IN PROD TABLE-------------------------------------------

CALL prod.addversion_sproc('SCENARIO_YIELD', 'FORECASTER INPUT');

---------------------------------LOAD DATA IN LANDING TABLE------------------------------------

CREATE TABLE IF NOT EXISTS stage.scenario_yield_landing(
    user_name VARCHAR(256) NOT NULL
    ,load_date TIMESTAMP WITH TIME ZONE NOT NULL
    ,upload_type VARCHAR(512) NOT NULL
    ,scenario_name VARCHAR(512)
    ,geography_grain VARCHAR(256) NOT NULL
    ,geography VARCHAR(256) NOT NULL
    ,platform_subset VARCHAR(256) NOT NULL
    ,customer_engagement VARCHAR(20) NOT NULL
    ,base_product_number VARCHAR(50) NOT NULL
    ,measure VARCHAR(128) NOT NULL
    ,min_sys_date DATE
    ,month_num INTEGER NOT NULL
    ,value DOUBLE PRECISION NOT NULL
);
GRANT ALL ON TABLE stage.scenario_yield_landing TO auto_glue;

INSERT INTO stage.scenario_yield_landing
(
       user_name
      ,load_date
      ,upload_type
      ,scenario_name
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,base_product_number
      ,measure
      ,min_sys_date
      ,month_num
      ,value
)
SELECT user_name
      ,load_date
      ,TO_TIMESTAMP(load_date, 'YYYY-MM-DD HH24:MI:SS')
      ,upload_type
      ,scenario_name
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,base_product_number
      ,measure
      ,TO_DATE(min_sys_date, 'YYYY-MM-DD')
      ,month_num
      ,value
  FROM stage.scenario_yield_temp_landing;

----------------------------TRUNCATE STAGING---------------------------------

DROP TABLE IF EXISTS stage.scenario_yield_staging;

CREATE TABLE stage.scenario_yield_staging(
    user_name VARCHAR(256) NOT NULL
    ,load_date TIMESTAMP WITH TIME ZONE NOT NULL
    ,upload_type VARCHAR(512) NOT NULL
    ,scenario_name VARCHAR(512)
    ,geography_grain VARCHAR(256) NOT NULL
    ,geography VARCHAR(256) NOT NULL
    ,platform_subset VARCHAR(256) NOT NULL
    ,customer_engagement VARCHAR(20) NOT NULL
    ,base_product_number VARCHAR(50) NOT NULL
    ,measure VARCHAR(128) NOT NULL
    ,min_sys_date DATE
    ,month_num INTEGER NOT NULL
    ,value DOUBLE PRECISION NOT NULL
);
GRANT ALL ON TABLE stage.scenario_yield_staging TO auto_glue;

---------LOAD STAGING TABLE----------------------------------------------------------

INSERT INTO stage.scenario_yield_staging
(
       user_name
      ,load_date
      ,upload_type
      ,scenario_name
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,base_product_number
      ,measure
      ,min_sys_date
      ,month_num
      ,value
)
SELECT user_name
      ,load_date
      ,upload_type
      ,scenario_name
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,base_product_number
      ,measure
      ,min_sys_date
      ,month_num
      ,value
  FROM stage.scenario_yield_landing
  WHERE load_date = (SELECT MAX(load_date) FROM stage.scenario_yield_landing);

------------------------------LOAD SCENARIO DATA TO PROD------------------------------------------

INSERT INTO prod.scenario_yield
(
       user_name
      ,load_date
      ,upload_type
      ,scenario_name
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,base_product_number
      ,measure
      ,min_sys_date
      ,month_num
      ,value
)
SELECT user_name
      ,load_date
      ,upload_type
      ,scenario_name
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,base_product_number
      ,measure
      ,min_sys_date
      ,month_num
      ,value
  FROM stage.scenario_yield_staging
  WHERE upload_type = 'FORECAST-SCENARIO';

  ------------------------------------LOAD WORKING FORECAST TO PROD-------------------

  INSERT INTO prod.working_forecast_yield
(
       user_name
      ,load_date
      ,upload_type
      ,scenario_name
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,base_product_number
      ,measure
      ,min_sys_date
      ,month_num
      ,value
)
SELECT user_name
      ,load_date
      ,upload_type
      ,scenario_name
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,base_product_number
      ,measure
      ,min_sys_date
      ,month_num
      ,value
  FROM stage.scenario_yield_staging
  WHERE upload_type = 'WORKING-FORECAST';


  ------------------------------------LOAD EPA DRIVERS TO PROD--------------------------------

  INSERT INTO prod.epa_drivers_yield
(
       user_name
      ,load_date
      ,upload_type
      ,scenario_name
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,base_product_number
      ,measure
      ,min_sys_date
      ,month_num
      ,value
)
SELECT user_name
      ,load_date
      ,upload_type
      ,scenario_name
      ,geography_grain
      ,geography
      ,platform_subset
      ,customer_engagement
      ,base_product_number
      ,measure
      ,min_sys_date
      ,month_num
      ,value
  FROM stage.scenario_yield_staging
  WHERE upload_type = 'EPA-DRIVERS';

------------------DROP TEMP LANDING--------------------------------

DROP TABLE IF EXISTS stage.scenario_yield_temp_landing;

----------------------------------------------xxxx-------------------------------

END;

$$
;