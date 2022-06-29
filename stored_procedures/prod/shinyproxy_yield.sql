CREATE OR REPLACE PROCEDURE prod.shinyproxy_yield()
    LANGUAGE plpgsql
AS $$

BEGIN

--------------------------RUN SPROC TO UPDATE VERSION AND LOAD DATE IN PROD TABLE--------------------------------------

CALL prod.addversion_sproc('YIELD', 'FORECASTER INPUT');

-----------------------------DELETE RECORDS FROM PROD THAT EXIST IN TEMP TABLE-----------------------------------------

DELETE FROM mdm.yield 
USING stage.yield_temp_landing ytm
WHERE mdm.yield.geography = ytm.geography 
    AND mdm.yield.base_product_number = ytm.base_product_number
    AND mdm.yield.effective_date = ytm.effective_date 
    AND mdm.yield.record = ytm.record
    AND mdm.yield.geography_grain = ytm.geography_grain;

----------------------------------------INSERT RECORDS FROM TEMP TABLE---------------------------------------------------

INSERT INTO mdm.yield
SELECT
    'YIELD' as record,
    ytm.geography_grain,
    ytm.geography,
    ytm.base_product_number,
    ytm.value,
    TO_DATE(ytm.effective_date, 'YYYY-MM-DD'),
    NULL AS active_at,
    NULL AS inactive_at,
    1 AS official,
    NULL AS last_modified_date,
    NULL AS load_date,
    'NEW DATA' AS version
FROM stage.yield_temp_landing ytm;

----------------------------------------UPDATE LANDING WITH PROD VERSION AND LOAD DATE---------------------------------

UPDATE  mdm.yield
SET
    last_modified_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'YIELD'),
    load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'YIELD'),
    version = (SELECT MAX(version) FROM prod.version WHERE record = 'YIELD')
WHERE version = 'NEW DATA'; 

DROP TABLE stage.yield_temp_landing;

END;

$$
;