
CREATE OR REPLACE PROCEDURE prod.load_cartridge_mix_override_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

--Description: LOAD 'cartridge_mix_override' data FROM TEMP_LANDING TO PROD
--Created Date: 11/15/2020
--Created By: Daman Singh
--Modified : --

CREATE TABLE IF NOT EXISTS stage.cartridge_mix_override_landing(
    record VARCHAR(25) NOT NULL
    ,platform_subset VARCHAR(100) NOT NULL
    ,crg_base_prod_number VARCHAR(25) NOT NULL
    ,geography VARCHAR(256) NOT NULL
    ,geography_grain VARCHAR(256) NOT NULL
    ,cal_date DATE NOT NULL
    ,mix_pct DOUBLE PRECISION
    ,product_lifecycle_status VARCHAR(5)
    ,customer_engagement VARCHAR(25) NOT NULL
    ,load_date TIMESTAMP WITH TIME ZONE NOT NULL
    ,official INTEGER NOT NULL
);
GRANT ALL ON TABLE stage.cartridge_mix_override_landing TO auto_glue;

---------------------------------SET EXISTING RECORDS TO ZERO------------------------------------

UPDATE TGT
SET TGT.official = 0
FROM stage.cartridge_mix_override_landing TGT
INNER JOIN stage.cartridge_mix_override_temp_landing SRC
ON  SRC.platform_subset = TGT.platform_subset
    AND SRC.crg_base_prod_number = TGT.crg_base_prod_number
    AND SRC.geography =TGT.geography 
    AND SRC.geography_grain = TGT.geography_grain
    AND SRC.product_lifecycle_status = TGT.product_lifecycle_status
    AND SRC.customer_engagement = TGT.customer_engagement;


---------------INSERT LATEST RECORDS FROM TEMP AND OFFICIAL = 1------------------------------

INSERT INTO stage.cartridge_mix_override_landing(
 record
,platform_subset
,crg_base_prod_number
,geography
,geography_grain
,cal_date
,mix_pct
,product_lifecycle_status
,customer_engagement
,load_date
,official
)
SELECT 
 record
,platform_subset
,crg_base_prod_number
,geography
,geography_grain
,cal_date
,mix_pct
,product_lifecycle_status
,customer_engagement   
,GETDATE() AS load_date
,1 AS official
FROM stage.cartridge_mix_override_temp_landing;

----------------------------DROP STAGING---------------------------------

DROP TABLE IF EXISTS stage.cartridge_mix_override_staging;

CREATE TABLE stage.cartridge_mix_override_staging(
    record VARCHAR(25) NOT NULL
    ,platform_subset VARCHAR(100) NOT NULL
    ,crg_base_prod_number VARCHAR(25) NOT NULL
    ,geography VARCHAR(256) NOT NULL
    ,geography_grain VARCHAR(256) NOT NULL
    ,cal_date DATE NOT NULL
    ,mix_pct DOUBLE PRECISION
    ,product_lifecycle_status VARCHAR(5)
    ,customer_engagement VARCHAR(25) NOT NULL
    ,load_date TIMESTAMP WITH TIME ZONE NOT NULL
    ,official INTEGER NOT NULL
);
GRANT ALL ON TABLE stage.cartridge_mix_override_staging TO auto_glue;

------------------------------LOAD DATA TO STAGING-------------------------

INSERT INTO stage.cartridge_mix_override_staging
(
 record
,platform_subset
,crg_base_prod_number
,geography
,geography_grain
,cal_date
,mix_pct
,product_lifecycle_status
,customer_engagement
,load_date
,official
)
SELECT
 record
,platform_subset
,crg_base_prod_number
,geography
,geography_grain
,cal_date
,mix_pct
,product_lifecycle_status
,customer_engagement
,load_date
,official
FROM  stage.cartridge_mix_override_landing
WHERE load_date = (SELECT MAX(load_date) FROM stage.cartridge_mix_override_landing);

------------------------------LOAD DATA TO PROD------------------------------------------

UPDATE TGT
SET TGT.official = 0
FROM prod.cartridge_mix_override TGT
INNER JOIN  stage.cartridge_mix_override_staging SRC
ON  SRC.platform_subset = TGT.platform_subset
    AND SRC.crg_Base_Prod_Number =  TGT.crg_base_prod_number
    AND SRC.geography = TGT.geography 
    AND SRC.geography_grain = TGT.geography_grain
    AND SRC.product_lifecycle_status = TGT.product_lifecycle_status
    AND SRC.customer_engagement = TGT.customer_engagement;

INSERT INTO prod.cartridge_mix_override(
 record
,platform_subset
,crg_base_prod_number
,geography
,geography_grain
,cal_date
,mix_pct
,product_lifecycle_status
,customer_engagement
,load_date
,official
)
SELECT 
 record
,platform_subset
,crg_base_prod_number
,geography
,geography_grain
,cal_date
,mix_pct
,product_lifecycle_status
,customer_engagement   
,load_date
,1
FROM stage.cartridge_mix_override_staging;

------------------DROP TEMP LANDING--------------------------------

DROP TABLE stage.cartridge_mix_override_temp_landing;

----------------------------------------------xxxx-------------------------------

END;

$$
;