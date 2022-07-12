CREATE OR REPLACE PROCEDURE fin_prod.load_gru_override_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

--Add version to version table
CALL prod.addversion_sproc('GRU_OVERRIDE','FINANCE');

UPDATE fin_prod.forecast_gru_override
SET official = 0
WHERE EXISTS (
    SELECT 1
    FROM fin_stage.forecast_gru_override_ops_temp_landing gru_ops
    WHERE forecast_gru_override.base_product_number = gru_ops.base_product_number
        AND forecast_gru_override.region_5 = gru_ops.region_5
        AND forecast_gru_override.country_code = gru_ops.country_code
);

--copy max version from landing to financials
INSERT INTO fin_prod.forecast_gru_override
           (record
           ,pl
           ,base_product_number
           ,region_5
           ,country_code
           ,gru
           ,official
           ,version
           ,load_date)
SELECT 'GRU_OVERRIDE' as record
	  ,pl
      ,base_product_number
      ,region_5
      ,country_code
      ,gru
      ,1 as official
	  ,(SELECT version from prod.version WHERE record = 'GRU_OVERRIDE' 
				AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'GRU_OVERRIDE')) as version
      ,(SELECT load_date from prod.version WHERE record = 'GRU_OVERRIDE' 
				AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'GRU_OVERRIDE')) as load_date
FROM fin_stage.forecast_gru_override_ops_temp_landing

END;

$$
;