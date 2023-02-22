CREATE OR REPLACE PROCEDURE mdm.supplies_xref_sproc()
    LANGUAGE plpgsql
AS $$

DECLARE
current_date TIMESTAMP;

BEGIN

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
)
SELECT record
    , base_product_number
    , pl
    , cartridge_alias
    , CAST(null as varchar(8)) as regionalization
    , toner_category
    , "type"
    , single_multi
    , crg_chrome
    , k_color
    , TO_TIMESTAMP(crg_intro_dt, 'YYYY-MM-DD HH24:MI:SS')
    , "size"
    , technology
    , supplies_family
    , supplies_group
    , supplies_technology
    , equivalents_multiplier
    , 1 AS official
    , current_date AS last_modified_date
    , current_date AS load_date
FROM stage.supplies_xref_temp_landing;

DROP TABLE IF EXISTS stage.supplies_xref_temp_landing;

END;

$$
;
