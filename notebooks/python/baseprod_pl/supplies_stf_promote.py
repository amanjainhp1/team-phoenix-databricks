# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

max_info = call_redshift_addversion_sproc(configs, 'SUPPLIES_STF', 'SUPPLIES_STF')

max_version = max_info[0]
max_load_date = (max_info[1])

# COMMAND ----------

stf_dollarization = f""" 
INSERT INTO fin_prod.stf_dollarization
SELECT 
	  'SUPPLIES_STF' AS record
   	  ,geography
      ,base_product_number
      ,pl
	  ,technology
      ,cal_date
      ,units
	  ,equivalent_units
      ,gross_revenue
      ,contra
      ,revenue_currency_hedge
      ,net_revenue
      ,variable_cost
      ,contribution_margin
      ,fixed_cost
      ,gross_margin
      ,insights_units
	  ,current_timestamp as load_date
	  ,'WORKING VERSION' AS version
	  ,username
FROM financials.v_stf_dollarization;

UPDATE fin_prod.stf_dollarization
	SET version = (SELECT version FROM prod.version WHERE record = 'SUPPLIES_STF' AND official=1),
	load_date = (SELECT load_date FROM prod.version WHERE record = 'SUPPLIES_STF' AND official=1)
WHERE version = 'WORKING VERSION';

"""

submit_remote_query(configs, stf_dollarization)
# stf_dollarization = spark.sql(stf_dollarization)
# write_df_to_redshift(configs, stf_dollarization, "fin_prod.stf_dollarization", "overwrite")
# stf_dollarization.createOrReplaceTempView("stf_dollarization")
