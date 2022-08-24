# Databricks notebook source
dbutils.widgets.text("load_to_redshift", "")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

if dbutils.widgets.get("load_to_redshift").lower() == "true": 
    
    #actuals_supplies_baseprod = read_sql_server_to_df(configs) \
     #   .option("dbtable", "IE2_Financials.dbo.actuals_supplies_baseprod") \
     #   .load()

    #write_df_to_redshift(configs, actuals_supplies_baseprod, "fin_prod.actuals_supplies_baseprod", "append", "", "truncate fin_prod.actuals_supplies_baseprod")
    
    #odw_actuals_supplies_baseprod = read_sql_server_to_df(configs) \
     #   .option("dbtable", "IE2_Financials.ms4.odw_actuals_supplies_baseprod") \
     #   .load() \
    #    .select("record", "cal_date", "country_alpha2", "market10", "platform_subset", "base_product_number", "pl", "l5_description", "customer_engagement", "gross_revenue", "net_currency", "contractual_discounts",
    #           "discretionary_discounts", "net_revenue", "warranty", "other_cos", "total_cos", "gross_profit", "revenue_units", "equivalent_units", "yield_x_units", "yield_x_units_black_only", "official",
     #          "load_date", "version")

   # write_df_to_redshift(configs, odw_actuals_supplies_baseprod, "fin_prod.odw_actuals_supplies_baseprod", "append", "", "truncate fin_prod.odw_actuals_supplies_baseprod") 

   # edw_actuals_supplies_baseprod = read_sql_server_to_df(configs) \
   #     .option("dbtable", "IE2_Financials.dbo.edw_actuals_supplies_baseprod") \
   #     .load() \
   #     .select("record", "cal_date", "country_alpha2", "market10", "platform_subset", "base_product_number", "pl", "l5_description", "customer_engagement", "gross_revenue", "net_currency", "contractual_discounts",
   #            "discretionary_discounts", "net_revenue", "warranty", "other_cos", "total_cos", "gross_profit", "revenue_units", "equivalent_units", "yield_x_units", "yield_x_units_black_only", "official",
   #            "load_date", "version")

   # write_df_to_redshift(configs, edw_actuals_supplies_baseprod, "fin_prod.edw_actuals_supplies_baseprod", "append",  "", "truncate fin_prod.edw_actuals_supplies_baseprod")

# COMMAND ----------

#submit query to redshift

query = f"""

TRUNCATE fin_prod.actuals_supplies_baseprod;


CALL prod.addversion_sproc('ACTUALS - SUPPLIES BASE PRODUCT FINANCIALS', 'ACTUALS - SUPPLIES BASE PRODUCT FINANCIALS');
    
   
INSERT INTO fin_prod.actuals_supplies_baseprod
           (record
           ,cal_date
           ,country_alpha2
           ,market10
           ,platform_subset
           ,base_product_number
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
           ,equivalent_units
           ,yield_x_units
           ,yield_x_units_black_only
           ,official
           ,load_date
           ,version
           )
SELECT 
     record
    ,cal_date
    ,country_alpha2
    ,market10
    ,platform_subset
    ,base_product_number
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
    ,SUM(equivalent_units)
    ,SUM(yield_x_units)
    ,SUM(yield_x_units_black_only)
    ,official
    ,(SELECT distinct load_date from prod.version WHERE record = 'ACTUALS - SUPPLIES BASE PRODUCT FINANCIALS' 
        AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'ACTUALS - SUPPLIES BASE PRODUCT FINANCIALS')) AS load_date
    ,(SELECT distinct version from prod.version WHERE record = 'ACTUALS - SUPPLIES BASE PRODUCT FINANCIALS'
        AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'ACTUALS - SUPPLIES BASE PRODUCT FINANCIALS')) AS version    
    FROM fin_prod.edw_actuals_supplies_baseprod
GROUP BY
    record
    ,cal_date
    ,country_alpha2
    ,market10
    ,platform_subset
    ,base_product_number
    ,pl
    ,L5_Description
    ,customer_engagement
    ,official;

  
INSERT INTO fin_prod.actuals_supplies_baseprod
           (record
           ,cal_date
           ,country_alpha2
           ,market10
           ,platform_subset
           ,base_product_number
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
           ,equivalent_units
           ,yield_x_units
           ,yield_x_units_black_only
           ,official
           ,load_date
           ,version
           )
SELECT 
     record
    ,cal_date
    ,country_alpha2
    ,market10
    ,platform_subset
    ,base_product_number
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
    ,SUM(equivalent_units)
    ,SUM(yield_x_units)
    ,SUM(yield_x_units_black_only)
    ,official
    ,(SELECT distinct load_date from prod.version WHERE record = 'ACTUALS - SUPPLIES BASE PRODUCT FINANCIALS' 
        AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'ACTUALS - SUPPLIES BASE PRODUCT FINANCIALS')) AS load_date
    ,(SELECT distinct version from prod.version WHERE record = 'ACTUALS - SUPPLIES BASE PRODUCT FINANCIALS'
        AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'ACTUALS - SUPPLIES BASE PRODUCT FINANCIALS')) AS version    
    FROM fin_prod.odw_actuals_supplies_baseprod
GROUP BY
    record
    ,cal_date
    ,country_alpha2
    ,market10
    ,platform_subset
    ,base_product_number
    ,pl
    ,L5_Description
    ,customer_engagement
    ,official;
"""

submit_remote_query(configs, query)