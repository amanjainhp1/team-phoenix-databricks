CREATE OR REPLACE PROCEDURE prod.p_supplies_hw_mapping()
	LANGUAGE plpgsql
AS $$
	
	
	
begin
	
-----------------------------------SET offical and active FLAG = 1, VERSION = NULL, LoadDate= GETDATE() FOR TEMP_LANDING-------------------------

update stage.supplies_hw_mapping_temp_landing
set official = 1, active = 1, version = null, load_date = getdate()
where is_delete != 1;

------------------DELETE RECORDS FROM SWM MDM TABLES THAT ARE IN SWM TEMP TABLE -------------------------------

delete from mdm.supplies_hw_mapping 
using  stage.supplies_hw_mapping_temp_landing stm where supplies_hw_mapping.base_product_number = stm.base_product_number 
																		 and supplies_hw_mapping.geography = stm.geography and supplies_hw_mapping.platform_subset = stm.platform_subset 
																		 and supplies_hw_mapping.customer_engagement = stm.customer_engagement


																		 
---------------------------------RUN SPROC TO UPDATE VERSION AND LOAD DATE IN PROD TABLE-------------------------------------------


call prod.addversion_sproc('SUPPLIES_HW_MAP', 'FORECASTER INPUT');
																		 

-------------------------------UPDATE TEMP_LANDING WITH PROD VERSION AND LOAD DATE----------------------------


update  stage.supplies_hw_mapping_temp_landing
set load_date = (select max(load_date) from prod.version where record = 'SUPPLIES_HW_MAP'),
version = (select max(version) from prod.version where record = 'SUPPLIES_HW_MAP')
where version is null and is_delete != 1; 


-----------------------------------INSERT RECORDS FROM TEMP TABLE-------------------------------------------------------------------

insert into mdm.supplies_hw_mapping(record, geography_grain, geography, base_product_number, customer_engagement, platform_subset, eol, eol_date, host_multiplier, official, last_modified_date, load_date)
select 'supplies_hw_map'  record, geography_grain, geography, base_product_number, sm.customer_engagement ,sm.platform_subset,sm.eol,sm.eol_date ,sm.host_multiplier,1,sm.load_date,sm.load_date
from stage.supplies_hw_mapping_temp_landing sm
where is_delete  != 1; 

---------------------TRUNCATE TEMP TABLE-----------------------------------

truncate table stage.supplies_hw_mapping_temp_landing;

end;



$$
;
