CREATE OR REPLACE PROCEDURE mdm.supplies_hw_mapping_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

-----------------------------------SET official FLAG = 1, LoadDate= GETDATE() FOR TEMP_LANDING-------------------------

UPDATE stage.supplies_hw_mapping_temp_landing
SET official = 1, load_date = GETDATE()
WHERE is_delete != 1;

------------------DELETE RECORDS FROM SWM MDM TABLES THAT ARE IN SWM TEMP TABLE -------------------------------

DELETE FROM mdm.supplies_hw_mapping 
USING stage.supplies_hw_mapping_temp_landing stm
WHERE supplies_hw_mapping.base_product_number = stm.base_product_number 
    AND supplies_hw_mapping.geography = stm.geography
    AND supplies_hw_mapping.platform_subset = stm.platform_subset 
    AND supplies_hw_mapping.customer_engagement = stm.customer_engagement;

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
)
SELECT
    'SUPPLIES_HW_MAP' AS record
    , geography_grain
    , geography
    , base_product_number
    , sm.customer_engagement
    , sm.platform_subset
    , CAST(CAST(sm.eol AS INTEGER) AS BOOLEAN)
    , TO_DATE(sm.eol_date, 'YYYY-MM-DD')
    , sm.host_multiplier
    , 1 AS official
    , TO_TIMESTAMP(sm.load_date, 'YYYY-MM-DD HH24:MI:SS') AS last_modified_date
    , TO_TIMESTAMP(sm.load_date, 'YYYY-MM-DD HH24:MI:SS')
FROM stage.supplies_hw_mapping_temp_landing sm
WHERE is_delete != 1; 

DROP TABLE IF EXISTS stage.supplies_hw_mapping_temp_landing;

END;

$$
;
