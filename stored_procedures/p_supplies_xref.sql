CREATE OR REPLACE PROCEDURE prod.p_supplies_xref()
	LANGUAGE plpgsql
AS $$
	
	
begin
	


---------------------------DELETE RECORDS FROM PROD THAT EXIST IN TEMP TABLE---------------------------------------------
delete from mdm.supplies_xref 
using  stage.supplies_xref_temp_landing sxref where supplies_xref.base_product_number = sxref.base_product_number;


-----------------------------------INSERT RECORDS FROM TEMP TABLE-------------------------------------------------------------------

insert into mdm.supplies_xref
(record, base_product_number, pl, cartridge_alias, regionalization, toner_category, "type", single_multi, crg_chrome, k_color, crg_intro_dt, "size", technology, supplies_family, supplies_group, supplies_technology, equivalents_multiplier, active, last_modified_date,load_date)
select record, base_product_number, pl, cartridge_alias, regionalization, toner_category, "type", single_multi, crg_chrome, k_color, crg_intro_dt, "size", technology, supplies_family, supplies_group, supplies_technology, equivalents_multiplier,1,getdate(),getdate()
from stage.supplies_xref_temp_landing;

---------------------------------RUN SPROC TO UPDATE VERSION AND LOAD DATE IN PROD TABLE-------------------------------------------


call prod.addversion_sproc('SUPPLIES_XREF', 'FORECASTER INPUT');
				

----------------UPDATE PROD VERSION------------------------------------------

update mdm.supplies_xref
set version = (select max(version) from prod.version v  where record  = 'SUPPLIES_XREF'),
    load_date = (select max(load_date) from prod.version v  where record  = 'SUPPLIES_XREF')
where version is null;

---------------------TRUNCATE TEMP TABLE-----------------------------------

truncate table stage.supplies_xref_temp_landing;

end;


$$
;
