# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Transfer Tables from Redshift to SFAI

# COMMAND ----------

# MAGIC %run ../common/configs 

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Dataframes for Each Table

# COMMAND ----------

#--------- fin_stage --------
    
    
#--------- fin_prod ---------
actuals_supplies_baseprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_supplies_baseprod") \
    .load()\
    .select(col("record")
      ,col("cal_date")
      ,col("country_alpha2")
      ,col("market10")
      ,col("platform_subset")
      ,col("base_product_number")
      ,col("pl")
      ,col("l5_description").alias("L5_Description")
      ,col("customer_engagement")
      ,col("gross_revenue")
      ,col("net_currency")
      ,col("contractual_discounts")
      ,col("discretionary_discounts")
      ,col("net_revenue")
      ,col("total_cos")
      ,col("gross_profit")
      ,col("revenue_units")
      ,col("equivalent_units")
      ,col("yield_x_units")
      ,col("yield_x_units_black_only")
      ,col("official")
      ,col("load_date")
      ,col("version")
      ,col("warranty")
      ,col("other_cos"))

odw_report_rac_product_financials_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_report_rac_product_financials_actuals") \
    .load()\
    .select(col("fiscal_year_period").alias("Fiscal Year/Period")
      , col("material_number").alias("Material Number")
      , col("profit_center_code").alias("Profit Center Code")
      , col("segment_code").alias("Segment Code")
      , col("segment_name").alias("Segment Name")
      , col("gross_trade_revenues_usd").alias("Gross Trade Revenues USD")
      , col("contractual_discounts_usd").alias("Contractual Discounts USD")
      , col("discretionary_discounts_usd").alias("Discretionary Discounts USD")
      , col("net_currency_usd").alias("Net Currency USD")
      , col("net_revenues_usd").alias("Net Revenues USD")
      , col("warr").alias("WARR")
      , col("total_cost_of_sales_usd").alias("TOTAL COST OF SALES USD")
      , col("gross_margin_usd").alias("GROSS MARGIN USD")
      , col("load_date").alias("load_date"))

#odw_report_ships_deliveries_actuals = read_redshift_to_df(configs) \
#    .option("dbtable", "stage.odw_report_ships_deliveries_actuals") \
#    .load() \
#    .select(col("fiscal_year_period").alias("Fiscal Year Month")
#        , col("calendar_year_month").alias("Calendar Year Month")
#        , col("unit_reporting_code").alias("Unit Reporting Code")
#        , col("unit_reporting_description").alias("Unit Reporting Description"))
#        , col("parent_explosion").alias("Parent Explosion")
#        , col("material_nr").alias("Material Nr")
#        , col("material_desc").alias("Material Desc")
#        , col("trade_or_non_trade").alias("Trade/Non-Trade")
#        , col("profit_center_code").alias("Profit Center Code")
#        , col("segment_code").alias("Segment Code")
#        , col("segment_hier_desc_level2").alias("Segment Hier Desc Level2")
#        , col("delivery_item_qty").alias("Delivery Item Qty")
#        , col("load_date").alias("load_date")
#        , col("delivery_item").alias("Delivery Item")
#        , col("bundled_qty").alias("Bundled Qty")
#        , col("unbundled_qty").alias("Unbundled Qty")

#odw_revenue_units_sales_actuals_prelim = read_redshift_to_df(configs) \
#    .option("dbtable", "fin_prod.odw_revenue_units_sales_actuals_prelim") \
#    .load()\
#    .select(col("fiscal_year_period").alias("Fiscal Year/Period")
#        , col("profit_center_hier_desc_level4").alias("Profit Center Hier Desc Level4")
#        , col("segment_hier_desc_level4").alias("Segment Hier Desc Level4")
#        , col("segment_code").alias("Segment Code")
#        , col("segment_name").alias("Segment Name")
#        , col("profit_center_code").alias("Profit Center Code")
#        , col("material_number").alias("Material Number")
#        , col("unit_quantity_sign_flip").alias("Unit Quantity (Sign-Flip)")
#        , col("load_date").alias("load_date")
#        , col("unit_reporting_code").alias("Unit Reporting Code")
#        , col("unit_reporting_description").alias("Unit Reporting Description"))

odw_revenue_units_sales_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_revenue_units_sales_actuals") \
    .load()\
    .select(col("fiscal_year_period").alias("Fiscal Year/Period")
        , col("profit_center_hier_desc_level4").alias("Profit Center Hier Desc Level4")
        , col("segment_hier_desc_level4").alias("Segment Hier Desc Level4")
        , col("segment_code").alias("Segment Code")
        , col("segment_name").alias("Segment Name")
        , col("profit_center_code").alias("Profit Center Code")
        , col("material_number").alias("Material Number")
        , col("unit_quantity_sign_flip").alias("Unit Quantity (Sign-Flip)")
        , col("load_date").alias("load_date")
        , col("unit_reporting_code").alias("Unit Reporting Code")
        , col("unit_reporting_description").alias("Unit Reporting Description"))

odw_sacp_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_sacp_actuals") \
    .load()

actuals_supplies_salesprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_supplies_salesprod") \
    .load()\
    .select(col("record")
      , col("cal_date")
      , col("country_alpha2")
      , col("currency")
      , col("market10")
      , col("sales_product_number")
      , col("pl")
      , col("l5_description").alias("L5_Description")
      , col("customer_engagement")
      , col("gross_revenue")
      , col("net_currency")
      , col("contractual_discounts")
      , col("discretionary_discounts")
      , col("net_revenue")
      , col("total_cos")
      , col("gross_profit")
      , col("revenue_units")
      , col("official")
      , col("load_date")
      , col("version")
      , col("warranty")
      , col("other_cos"))

#---------   mdm  -----------
    
#---------   prod   ---------
actuals_supplies = read_redshift_to_df(configs) \
    .option("dbtable", "prod.actuals_supplies") \
    .load()\
    .select("record"
      , "cal_date"
      , "country_alpha2"
      , "market10"
      , "platform_subset"
      , "base_product_number"
      , "customer_engagement"
      , "base_quantity"
      , "equivalent_units"
      , "yield_x_units"
      , "yield_x_units_black_only"
      , "official"
      , "load_date"
      , "version")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SFAI Table Names

# COMMAND ----------

tables = [
    ['IE2_Landing.ms4.odw_report_rac_product_financials_actuals_landing', odw_report_rac_product_financials_actuals, "overwrite"],
    ['IE2_Landing.ms4.odw_revenue_units_sales_actuals_landing', odw_revenue_units_sales_actuals, "overwrite"],
   # ['IE2_Landing.ms4.odw_revenue_units_sales_actuals_prelim_landing', odw_revenue_units_sales_actuals_prelim, "overwrite"],
   # ['IE2_Landing.ms4.odw_report_ships_deliveries_actuals_landing', odw_report_ships_deliveries_actuals, "overwrite"],
    ['IE2_Financials.ms4.odw_sacp_actuals', odw_sacp_actuals, "overwrite"],
    ['IE2_Financials.dbo.actuals_supplies_baseprod', actuals_supplies_baseprod, "overwrite"],
    ['IE2_Financials.dbo.actuals_supplies_salesprod', actuals_supplies_salesprod, "overwrite"],
    ['IE2_Prod.dbo.actuals_supplies', actuals_supplies, "overwrite"]
]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write to SFAI

# COMMAND ----------

for t_name, df, mode in tables:
    write_df_to_sqlserver(configs, df, t_name, mode)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct cal_date
# MAGIC from fin_stage.actuals_supplies_salesprod
# MAGIC order by 1;
