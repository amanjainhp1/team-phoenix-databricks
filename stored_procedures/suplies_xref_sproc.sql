CREATE OR REPLACE PROCEDURE prod.supplies_xref_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

DECLARE
current_date TIMESTAMP;

SELECT GETDATE() INTO current_date;
---------------------------DELETE RECORDS FROM PROD THAT EXIST IN TEMP TABLE---------------------------------------------

DELETE FROM mdm.supplies_xref 
USING  stage.supplies_xref_temp_landing sxref
WHERE supplies_xref.base_product_number = sxref.base_product_number;

-----------------------------------INSERT RECORDS FROM TEMP TABLE-------------------------------------------------------------------

INSERT INTO mdm.supplies_xref
(
    record
    , base_product_number
    , pl
    , cartridge_alias
    , regionalization
    , toner_category
    , "type"
    , single_multi
    , crg_chrome
    , k_color
    , crg_intro_dt
    , "size"
    , technology
    , supplies_family
    , supplies_group
    , supplies_technology
    , equivalents_multiplier
    , official
    , last_modified_date
    , load_date
    , version
)
SELECT record
    , base_product_number
    , pl
    , cartridge_alias
    , regionalization
    , toner_category
    , "type"
    , single_multi
    , crg_chrome
    , k_color
    , crg_intro_dt
    , "size"
    , technology
    , supplies_family
    , supplies_group
    , supplies_technology
    , equivalents_multiplier
    , 1 AS offical
    , current_date AS last_modified_date
    , current_date AS load_date
    , 'NEW DATA' AS version
FROM stage.supplies_xref_temp_landing;

---------------------------------RUN SPROC TO UPDATE VERSION AND LOAD DATE IN PROD TABLE-------------------------------------------

CALL prod.addversion_sproc('SUPPLIES_XREF', 'FORECASTER INPUT');

----------------UPDATE PROD VERSION------------------------------------------

UPDATE mdm.supplies_xref
SET 
    version = (SELECT MAX(version) FROM prod.version v WHERE record  = 'SUPPLIES_XREF'),
    load_date = (SELECT MAX(load_date) from prod.version v  WHERE record  = 'SUPPLIES_XREF')
WHERE version = 'NEW DATA';

---------------------TRUNCATE TEMP TABLE-----------------------------------

TRUNCATE TABLE stage.supplies_xref_temp_landing;

END;

$$
;
