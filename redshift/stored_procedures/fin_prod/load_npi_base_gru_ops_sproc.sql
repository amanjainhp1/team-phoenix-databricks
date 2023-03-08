CREATE OR REPLACE PROCEDURE fin_prod.load_npi_base_gru_ops_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

--Add version to version table
CALL prod.addversion_sproc('NPI_BASE_GRU','FINANCE');

UPDATE fin_prod.npi_base_gru
SET official = 0
WHERE EXISTS (
    SELECT 1
    FROM fin_stage.npi_base_gru_ops_staging gru_ops
    WHERE npi_base_gru.base_product_number = gru_ops.base_product_number
    AND npi_base_gru.region_5 = gru_ops.region_5
    AND npi_base_gru.country_alpha2 = gru_ops.country_alpha2
);

--copy max version from landing to financials
INSERT INTO fin_prod.npi_base_gru
           (record
           ,pl
           ,base_product_number
           ,region_5
           ,country_alpha2
           ,gru
           ,official
           ,load_date
           ,version
           )
SELECT 'NPI_BASE_GRU' as record
	  ,pl
      ,base_product_number
      ,region_5
      ,country_alpha2
      ,gru
      ,1 as official
      ,(SELECT load_date from prod.version WHERE record = 'NPI_BASE_GRU' 
				AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'NPI_BASE_GRU')) as load_date
	  ,(SELECT version from prod.version WHERE record = 'NPI_BASE_GRU' 
				AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'NPI_BASE_GRU')) as version
FROM fin_stage.npi_base_gru_ops_staging;

END;

$$
;
