# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import col, current_date, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType, DecimalType

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

query = """
CREATE OR REPLACE VIEW financials.v_stf_dollarization
AS 


with c2c as
(
	select
		base_product_number
		, region_5
		, cal_date
		, sum(cartridges) cartridges
	from
		prod.working_forecast_country c2c
		inner join
		mdm.iso_country_code_xref ctry
			on ctry.country_alpha2 = c2c.country
	where c2c.version = (select max(version) from prod.working_forecast_country)
	group by
		base_product_number
		, region_5
		, cal_date
)
select distinct
	stf.geography
	, stf.base_product_number
	, rdma.pl
	, supplies_xref.technology
	, stf.cal_date
	, stf.units
	, coalesce((stf.units*supplies_xref.equivalents_multiplier), stf.units) as equivalent_units
	, (fin.baseprod_gru*stf.units) gross_revenue
	, (fin.baseprod_contra_per_unit*stf.units) as contra
	, (fin.baseprod_revenue_currency_hedge_unit * stf.units) as revenue_currency_hedge
	, (fin.baseprod_aru * stf.units) as net_revenue
	, (fin.baseprod_variable_cost_per_unit * stf.units) as variable_cost
	, (fin.baseprod_contribution_margin_unit * stf.units) as contribution_margin
	, (fin.baseprod_fixed_cost_per_unit * stf.units) as fixed_cost
	, (fin.baseprod_gross_margin_unit * stf.units) as gross_margin
	, c2c.cartridges as insights_units
	, stf.username

from 
	stage.supplies_stf_landing stf
	left join
	fin_prod.forecast_supplies_baseprod_region_stf fin
		on fin.base_product_number = stf.base_product_number
		and fin.region_5 = stf.geography
		and fin.cal_date = stf.cal_date
		and fin.version = (select max(version) from fin_prod.forecast_supplies_baseprod_region_stf)
	left join
	mdm.rdma rdma
		on rdma.base_prod_number = stf.base_product_number
	inner join
	mdm.iso_country_Code_xref ctry
		on ctry.region_5 = stf.geography
	left join
	c2c c2c
		on c2c.base_product_number = stf.base_product_number
		and c2c.region_5 = stf.geography
		and c2c.cal_date = stf.cal_date
	left join
	mdm.supplies_xref supplies_xref
		on supplies_xref.base_product_number = stf.base_product_number;

      
GRANT ALL ON TABLE financials.v_stf_dollarization TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query)

# COMMAND ----------

query = """
GRANT ALL ON TABLE fin_prod.adjusted_revenue_flash TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query)

# COMMAND ----------


