CREATE OR REPLACE PROCEDURE prod.usage_share_override_sproc()
    LANGUAGE plpgsql
AS $$

DECLARE
current_date TIMESTAMP;

BEGIN

SELECT GETDATE() INTO current_date;

CREATE TABLE IF NOT EXISTS prod.usage_share_override_npi(
    record VARCHAR(25) NOT NULL
    ,min_sys_dt DATE NOT NULL
    ,month_num INTEGER NOT NULL
    ,geography_grain VARCHAR(25) NOT NULL
    ,geography VARCHAR(25) NOT NULL
    ,platform_subset VARCHAR(150) NOT NULL
    ,customer_engagement VARCHAR(10) NOT NULL
    ,forecast_process_note VARCHAR(512)
    ,post_processing_note VARCHAR(512)
    ,forecast_created_date DATE
    ,data_source VARCHAR(255)
    ,version VARCHAR(25) NOT NULL
    ,measure VARCHAR(25) NOT NULL
    ,units DOUBLE PRECISION NOT NULL
    ,proxy_used VARCHAR(255)
    ,ib_version VARCHAR(20)
    ,load_date TIMESTAMP WITH TIME ZONE NOT NULL
);

--remove any records that overlap from the temp table
DELETE FROM prod.usage_share_override_npi
USING stage.usage_share_override_landing_temp b
WHERE prod.usage_share_override_npi.geography = b.geography
    AND prod.usage_share_override_npi.platform_subset = b.platform_subset
    AND prod.usage_share_override_npi.customer_engagement = b.customer_engagement
    AND prod.usage_share_override_npi.measure = b.measure;
--might need to add month_num = month_num here to avoid partial loads like Cherry STND DM1 on 5/18/21

--insert records from the temp table
INSERT INTO prod.usage_share_override_npi
(
    record
    ,min_sys_dt
    ,month_num
    ,geography_grain
    ,geography
    ,platform_subset
    ,customer_engagement
    ,forecast_process_note
    ,post_processing_note
    ,forecast_created_date
    ,data_source
    ,version
    ,measure
    ,units
    ,proxy_used
    ,ib_version
    ,load_date
)
SELECT
    record
    ,TO_DATE(min_sys_dt, 'YYYY-MM-DD')
    ,month_num
    ,geography_grain
    ,geography
    ,platform_subset
    ,customer_engagement
    ,forecast_process_note
    ,post_processing_note
    ,TO_DATE(forecast_created_date, 'YYYY-MM-DD')
    ,data_source
    ,version
    ,measure
    ,units
    ,proxy_used
    ,ib_version
    ,TO_TIMESTAMP(load_date, 'YYYY-MM-DD HH24:MI:SS')
 FROM stage.usage_share_override_landing_temp;

--remove rows with a negative statistical_forecast_value value
DELETE FROM prod.usage_share_override_npi WHERE units < 0;

--insert records into the historical table
CREATE TABLE IF NOT EXISTS stage.usage_share_override_historical2_landing(
    record VARCHAR(25) NOT NULL
    ,min_sys_dt DATE NOT NULL
    ,month_num INTEGER NOT NULL
    ,geography_grain VARCHAR(25) NOT NULL
    ,geography VARCHAR(25) NOT NULL
    ,platform_subset VARCHAR(150) NOT NULL
    ,customer_engagement VARCHAR(10) NOT NULL
    ,forecast_process_note VARCHAR(512)
    ,post_processing_note VARCHAR(512)
    ,forecast_created_date DATE
    ,data_source VARCHAR(255)
    ,version VARCHAR(25) NOT NULL
    ,measure VARCHAR(25) NOT NULL
    ,units DOUBLE PRECISION NOT NULL
    ,proxy_used VARCHAR(255)
    ,ib_version VARCHAR(20)
    ,load_date TIMESTAMP WITH TIME ZONE NOT NULL
);

INSERT INTO stage.usage_share_override_historical2_landing
(
    record
    ,min_sys_dt
    ,month_num
    ,geography_grain
    ,geography
    ,platform_subset
    ,customer_engagement
    ,forecast_process_note
    ,post_processing_note
    ,forecast_created_date
    ,data_source
    ,version
    ,measure
    ,units
    ,proxy_used
    ,ib_version
    ,load_date
)
SELECT
    record
    ,TO_DATE(min_sys_dt, 'YYYY-MM-DD')
    ,month_num
    ,geography_grain
    ,geography
    ,platform_subset
    ,customer_engagement
    ,forecast_process_note
    ,post_processing_note
    ,TO_DATE(forecast_created_date, 'YYYY-MM-DD')
    ,data_source
    ,version
    ,measure
    ,units
    ,proxy_used
    ,ib_version
    ,current_date AS load_date
FROM stage.usage_share_override_landing_temp;

--drop the temp table
DROP TABLE IF EXISTS stage.usage_share_override_landing_temp;

END;


$$
;
