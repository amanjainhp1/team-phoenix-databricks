CREATE OR REPLACE PROCEDURE fin_stage.load_npi_base_gru_ops_sproc(group_param varchar)
    LANGUAGE plpgsql
AS $$

BEGIN

CREATE TABLE IF NOT EXISTS fin_stage.npi_base_gru_ops_staging(
    pl VARCHAR(255)
    ,base_product_number VARCHAR(255)
    ,region_5 VARCHAR(255)
    ,country_alpha2 VARCHAR(255)
    ,gru DOUBLE PRECISION
);
GRANT ALL ON TABLE fin_stage.npi_base_gru_ops_staging TO auto_glue;
EXECUTE 'GRANT ALL ON TABLE fin_stage.forecast_contra_input_lt_staging TO GROUP '||group_param||';';

---------------------TRUNCATE NPI BASE TABLE IN fin_stage----------------------------------

DELETE FROM fin_stage.npi_base_gru_ops_staging;

-------------------------------------POPULATE NPI BASE TABLE IN fin_stage FROM LANDING------------------------------------

INSERT INTO fin_stage.npi_base_gru_ops_staging
(
 pl
,base_product_number
,region_5
,country_alpha2
,gru
)
SELECT 
 pl
,base_product_number
,region_5
,country_alpha2
,gru
FROM fin_stage.npi_base_gru_ops_landing;

---------------------------------------------------------MOVE DATA TO OFFICIAL FINANCIAL TABLE-------------------------------------

CALL fin_prod.load_npi_base_gru_sproc();

--------------------------------------------DROP TABLE  IN LANDING-----------------------------------------------------------

DROP TABLE fin_stage.npi_base_gru_ops_landing;

END;

$$
;