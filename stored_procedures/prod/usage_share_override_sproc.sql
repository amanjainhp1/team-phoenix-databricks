CREATE OR REPLACE PROCEDURE prod.usage_share_override_sproc()
    LANGUAGE plpgsql
AS $$

DECLARE
current_date TIMESTAMP;

BEGIN

SELECT GETDATE() INTO current_date;

CREATE TABLE IF NOT EXISTS stage.usage_share_override_landing(
    record VARCHAR(25) NOT NULL
    ,min_sys_date DATE NOT NULL
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
GRANT ALL ON TABLE stage.usage_share_override_landing TO auto_glue;

--remove any records that overlap from the temp table
DELETE a
FROM stage.usage_share_override_landing a
JOIN stage.usage_share_override_landing_temp b
ON a.geography = b.geography
    AND a.platform_subset = b.platform_subset
    AND a.customer_engagement = b.customer_engagement
    AND a.measure = b.measure;
--might need to add month_num = month_num here to avoid partial loads like Cherry STND DM1 on 5/18/21

--insert records from the temp table
INSERT INTO stage.usage_share_override_landing
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
SELECT * FROM stage.usage_share_override_landing_temp;

--remove rows with a negative statistical_forecast_value value
DELETE FROM stage.usage_share_override_landing WHERE units < 0;

--insert records into the historical table
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
    ,current_date AS load_date
FROM stage.usage_share_override_landing_temp;

--drop the temp table
DROP TABLE IF EXISTS stage.usage_share_override_landing_temp;

END;


$$
;
