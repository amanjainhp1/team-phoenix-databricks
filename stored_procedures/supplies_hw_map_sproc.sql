CREATE OR REPLACE PROCEDURE prod.supplies_hw_mapping_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

-----------------------------------SET offical FLAG = 1, VERSION = NULL, LoadDate= GETDATE() FOR TEMP_LANDING-------------------------

UPDATE stage.supplies_hw_mapping_temp_landing
SET official = 1, version = NULL, load_date = GETDATE()
WHERE is_delete != 1;

------------------DELETE RECORDS FROM SWM MDM TABLES THAT ARE IN SWM TEMP TABLE -------------------------------

DELETE FROM mdm.supplies_hw_mapping 
USING stage.supplies_hw_mapping_temp_landing stm
WHERE supplies_hw_mapping.base_product_number = stm.base_product_number 
    AND supplies_hw_mapping.geography = stm.geography
    AND supplies_hw_mapping.platform_subset = stm.platform_subset 
    AND supplies_hw_mapping.customer_engagement = stm.customer_engagement

---------------------------------RUN SPROC TO UPDATE VERSION AND LOAD DATE IN PROD TABLE-------------------------------------------

CALL prod.addversion_sproc('SUPPLIES_HW_MAP', 'FORECASTER INPUT');

-------------------------------UPDATE TEMP_LANDING WITH PROD VERSION AND LOAD DATE----------------------------

UPDATE  stage.supplies_hw_mapping_temp_landing
SET 
    load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'SUPPLIES_HW_MAP'),
    version = (SELECT MAX(version) FROM prod.version WHERE record = 'SUPPLIES_HW_MAP')
WHERE version is NULL
    AND is_delete != 1; 

-----------------------------------INSERT RECORDS FROM TEMP TABLE-------------------------------------------------------------------

INSERT INTO mdm.supplies_hw_mapping 
(
    record
    , geography_grain
    , geography
    , base_product_number
    , customer_engagement
    , platform_subset
    , eol
    , eol_date
    , host_multiplier
    , official
    , last_modified_date
    , load_date
    , version
)
SELECT 'SUPPLIES_HW_MAP' AS record
    , geography_grain
    , geography
    , base_product_number
    , sm.customer_engagement
    , sm.platform_subset
    , sm.eol
    , sm.eol_date
    , sm.host_multiplier
    , 1 AS official
    , sm.load_date AS last_modified_date
    , sm.load_date
    , sm.version
FROM stage.supplies_hw_mapping_temp_landing sm
WHERE is_delete != 1; 

---------------------TRUNCATE TEMP TABLE-----------------------------------

TRUNCATE TABLE stage.supplies_hw_mapping_temp_landing;

END;

$$
;