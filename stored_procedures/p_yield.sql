CREATE OR REPLACE PROCEDURE prod.p_yield()
	LANGUAGE plpgsql
AS $$
	
	
	
begin
	
--------------------------RUN SPROC TO UPDATE VERSION AND LOAD DATE IN PROD TABLE--------------------------------------
	
	call addversion_sproc('YIELD' , 'FORECASTER INPUT');
	
-----------------------------DELETE RECORDS FROM PROD THAT EXIST IN TEMP TABLE-----------------------------------------
	
delete from mdm.yield 
using  stage.yield_temp_landing ytm where mdm.yield.geography = ytm.geography 
																		 and mdm.yield.base_product_number = ytm.base_product_number and mdm.yield.effective_date = ytm.effective_date 
																		 and mdm.yield.record = ytm.record and mdm.yield.geography_grain = ytm_geography_grain;

----------------------------------------INSERT RECORDS FROM TEMP TABLE---------------------------------------------------
																		 
insert into mdm.yield 
select ytm.record ,ytm.geography ,ytm.base_product_number ,ytm.value ,ytm.effective_date ,1 ,null , null , getdate() , "NEW DATA" ,geography_grain 
from stage.yield_temp_landing ytm;	

----------------------------------------UPDATE LANDING WITH PROD VERSION AND LOAD DATE---------------------------------

UPDATE  mdm.yield
SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'YIELD'),
version = (SELECT MAX(version) FROM prod.version WHERE record = 'YIELD')
WHERE version = 'NEW DATA'; 


----------------------------------------TRUNCATE TEMP LANDING-------------------------------------------------------------

truncate table stage.yield_temp_landing;
end;


$$
;
