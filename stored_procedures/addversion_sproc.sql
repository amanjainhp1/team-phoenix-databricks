CREATE OR REPLACE PROCEDURE prod.addversion_sproc(v1_record varchar, v2_source_name varchar)
	LANGUAGE plpgsql
AS $$

declare
record_count integer;
max_version text;

begin

/* Built 1/1/2022 - Brent Merrick
* This function adds records to the prod.version table for
* different datasets
*/

select COUNT(1) into record_count
from prod.version
where 1=1
	and record = v1_record
	and version like (convert(char(4), date_part(y,getdate()))
			+ '.' + to_char(date_part(month,getdate()), 'fm00')
			+ '.' + to_char(date_part(d,getdate()), 'fm00')
			+ '.%');

select max(version) into max_version
from prod.version
where record = v1_record;

IF record_count > 0 then
		update prod.version
		set official = 0
		where 1=1
			and record = v1_record
			and record <> 'IB';

		INSERT INTO prod.version
		(record, version, source_name, official, load_date)
		VALUES
		(
		v1_record
		,convert(char(4), date_part(y,getdate()))
			+ '.' + to_char(date_part(month,getdate()), 'fm00')
			+ '.' + to_char(date_part(d,getdate()), 'fm00')
			+ '.' + cast(cast(right(max_version,1) as int)+1 as text)
		,v2_source_name
		,case v1_record
				when 'IB' then 0
				else 1
			end
		,getdate()
		);
else
		INSERT INTO prod.version
		(record, version, source_name, official, load_date)
		VALUES
		(
		v1_record
		,convert(char(4), date_part(y,getdate()))
			+ '.' + to_char(date_part(month,getdate()), 'fm00')
			+ '.' + to_char(date_part(d,getdate()), 'fm00')
			+ '.1'
		,v2_source_name
		,case v1_record
						when 'IB' then 0
						else 1
					end
		,getdate()
		);
end if;
end;

$$
;