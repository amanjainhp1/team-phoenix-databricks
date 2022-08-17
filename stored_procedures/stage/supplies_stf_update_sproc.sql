CREATE OR REPLACE PROCEDURE stage.supplies_stf_update_sproc()
    LANGUAGE plpgsql
AS $$

BEGIN

--remove any records that overlap from the temp table
DELETE FROM stage.supplies_stf_temp_landing
WHERE units = 0;

CREATE TABLE IF NOT EXISTS stage.supplies_stf_landing(
    record VARCHAR(15) NOT NULL
    ,geography_grain VARCHAR(20) NOT NULL
    ,geography VARCHAR(20) NOT NULL
    ,base_product_number VARCHAR(15) NOT NULL
    ,cal_date DATE NOT NULL
    ,units DOUBLE PRECISION NOT NULL
    ,load_date TIMESTAMP WITH TIME ZONE
    ,version VARCHAR(20)
    ,official INTEGER
    ,username VARCHAR(256)
);
GRANT ALL ON TABLE stage.supplies_stf_landing TO auto_glue;

DELETE
FROM stage.supplies_stf_landing
USING stage.supplies_stf_temp_landing
WHERE stage.supplies_stf_landing.geography_grain = stage.supplies_stf_temp_landing.geography_grain
    AND stage.supplies_stf_landing.geography = stage.supplies_stf_temp_landing.geography
    AND REPLACE(stage.supplies_stf_landing.base_product_number, CHR(10),'') = REPLACE(stage.supplies_stf_temp_landing.base_product_number, CHR(10),'')
    AND stage.supplies_stf_landing.cal_date = stage.supplies_stf_temp_landing.cal_date;

--insert records from the temp table
INSERT INTO stage.supplies_stf_landing
           (record
           ,geography_grain
		   ,geography
           ,base_product_number
           ,cal_date
           ,units
           ,load_date
           ,version
           ,official
		   ,username)
SELECT 'SUPPLIES_STF' AS record
	  ,UPPER(geography_grain) AS geography_grain
	  ,UPPER(geography) AS geography
      ,REPLACE(base_product_number, CHR(10),'') AS base_product_number
      ,cal_date
      ,units
	  ,getdate() as load_date
	  ,'WORKING VERSION' as version
	  ,null as official
	  ,username
  FROM stage.supplies_stf_temp_landing
  WHERE units <> 0;

--load data to historical table
CREATE TABLE IF NOT EXISTS stage.supplies_stf_historical_landing(
    geography_grain VARCHAR(20) NOT NULL
    ,geography VARCHAR(20) NOT NULL
    ,base_product_number VARCHAR(15) NOT NULL
    ,cal_date DATE NOT NULL
    ,units DOUBLE PRECISION NOT NULL
    ,load_date TIMESTAMP WITH TIME ZONE
    ,username VARCHAR(256)
);
GRANT ALL ON TABLE stage.supplies_stf_landing TO auto_glue;

INSERT INTO stage.supplies_stf_historical_landing
           (geography_grain
		   ,geography
           ,base_product_number
           ,cal_date
           ,units
           ,load_date
		   ,username)
SELECT UPPER(geography_grain)
	  ,UPPER(geography)
      ,REPLACE(base_product_number, CHR(10),'') AS base_product_number
      ,cal_date
      ,units
	  ,getdate() as load_date
	  ,username
  FROM stage.supplies_stf_temp_landing
  WHERE units <> 0;

--clear out the temp table
DROP TABLE stage.supplies_stf_temp_landing;

END;

$$
;