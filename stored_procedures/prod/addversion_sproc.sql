CREATE OR REPLACE PROCEDURE prod.addversion_sproc(v1_record varchar, v2_source_name varchar)
	LANGUAGE plpgsql
AS $$

DECLARE
record_count INTEGER;
max_version TEXT;
current_date TIMESTAMP;
current_date_string TEXT;

BEGIN

/* Built 1/1/2022 - Brent Merrick
* This function adds records to the prod.version table for
* different datasets
*/

SELECT GETDATE() INTO current_date;

SELECT REPLACE(TRUNC(GETDATE()), '-', '.') INTO current_date_string;

SELECT COUNT(1) INTO record_count
FROM prod.version
WHERE 1=1
	AND record = v1_record
	AND version LIKE (current_date_string + '.%');

SELECT MAX(version) INTO max_version
FROM prod.version
WHERE record = v1_record;

UPDATE prod.version
SET official = 0
WHERE 1=1
	AND record = v1_record
	AND record <> 'IB';

IF record_count > 0 THEN
    INSERT INTO prod.version
    (
        record
        , version
        , source_name
        , official
        , load_date
    )
    VALUES
    (
        v1_record
        , current_date_string + '.' + CAST(CAST(RIGHT(max_version, 1) AS INT)+1 AS TEXT)
        , v2_source_name
        , CASE v1_record
            WHEN 'IB' THEN 0
            ELSE 1
            END
    , current_date
    );
ELSE
    INSERT INTO prod.version
    (
        record
        , version
        , source_name
        , official
        , load_date
    )
    VALUES
    (
        v1_record
        , current_date_string + '.1'
        , v2_source_name
        , CASE v1_record
            WHEN 'IB' then 0
            ELSE 1
            END
        , current_date
    );
END IF;

END;

$$
;
