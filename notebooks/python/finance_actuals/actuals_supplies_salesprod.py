# Databricks notebook source
dbutils.widgets.text("load_to_redshift", "")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

#submit query to redshift

query = f"""

TRUNCATE fin_prod.actuals_supplies_salesprod;


CALL prod.addversion_sproc('ACTUALS - SUPPLIES SALES PRODUCT FINANCIALS', 'ACTUALS - SUPPLIES SALES PRODUCT FINANCIALS');
    

INSERT INTO fin_prod.actuals_supplies_salesprod
           (record
		   ,cal_date
           ,country_alpha2
		   ,currency
		   ,market10
           ,sales_product_number
           ,pl
		   ,L5_Description
           ,customer_engagement
           ,gross_revenue
           ,net_currency
           ,contractual_discounts
           ,discretionary_discounts
		   ,net_revenue
		   ,warranty
		   ,other_cos
           ,total_cos
		   ,gross_profit
           ,revenue_units
           ,official
		   ,load_date
		   ,version
           )
SELECT 
	 record
	,cal_date
	,country_alpha2
	,currency
	,market10
	,sales_product_number
	,pl
	,L5_Description
	,customer_engagement
	,SUM(gross_revenue)
	,SUM(net_currency)
	,SUM(contractual_discounts) 
	,SUM(discretionary_discounts)
	,SUM(net_revenue)
	,SUM(warranty)
	,SUM(other_cos)
	,SUM(total_cos) 
	,SUM(gross_profit)
	,SUM(revenue_units)
	,official
	,(SELECT distinct load_date from prod.version WHERE record = 'ACTUALS - SUPPLIES SALES PRODUCT FINANCIALS' 
		AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'ACTUALS - SUPPLIES SALES PRODUCT FINANCIALS')) AS load_date
	,(SELECT distinct version from prod.version WHERE record = 'ACTUALS - SUPPLIES SALES PRODUCT FINANCIALS'
		AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'ACTUALS - SUPPLIES SALES PRODUCT FINANCIALS')) AS version
	FROM fin_prod.edw_actuals_supplies_salesprod 
GROUP BY
	 record
	,cal_date
	,country_alpha2
	,currency
	,market10
	,sales_product_number
	,pl
	,L5_Description
	,customer_engagement
	,official;
  
  
INSERT INTO fin_prod.actuals_supplies_salesprod
           (record
		   ,cal_date
           ,country_alpha2
		   ,currency
		   ,market10
           ,sales_product_number
           ,pl
		   ,L5_Description
           ,customer_engagement
           ,gross_revenue
           ,net_currency
           ,contractual_discounts
           ,discretionary_discounts
		   ,net_revenue
		   ,warranty
		   ,other_cos
           ,total_cos
		   ,gross_profit
           ,revenue_units
           ,official
		   ,load_date
		   ,version
           )
SELECT 
	 record
	,cal_date
	,country_alpha2
	,currency
	,market10
	,sales_product_number
	,pl
	,L5_Description
	,customer_engagement
	,SUM(gross_revenue)
	,SUM(net_currency)
	,SUM(contractual_discounts) 
	,SUM(discretionary_discounts)
	,SUM(net_revenue)
	,SUM(warranty)
	,SUM(other_cos)
	,SUM(total_cos) 
	,SUM(gross_profit)
	,SUM(revenue_units)
	,official
	,(SELECT distinct load_date from prod.version WHERE record = 'ACTUALS - SUPPLIES SALES PRODUCT FINANCIALS' 
		AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'ACTUALS - SUPPLIES SALES PRODUCT FINANCIALS')) AS load_date
	,(SELECT distinct version from prod.version WHERE record = 'ACTUALS - SUPPLIES SALES PRODUCT FINANCIALS'
		AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'ACTUALS - SUPPLIES SALES PRODUCT FINANCIALS')) AS version
FROM fin_prod.odw_actuals_supplies_salesprod 
GROUP BY
	 record
	,cal_date
	,country_alpha2
	,currency
	,market10
	,sales_product_number
	,pl
	,L5_Description
	,customer_engagement
	,official;
"""

submit_remote_query(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], query)
