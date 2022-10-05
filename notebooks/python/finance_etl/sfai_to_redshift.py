# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Finance Tables from SFAI to Redshift

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

 #--------- fin_stage ---------
supplies_manual_mcode_jv_detail_landing = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Landing.dbo.supplies_manual_mcode_jv_detail_landing") \
    .load()

supplies_iink_units_landing = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Landing.dbo.supplies_iink_units_landing") \
    .load()

odw_actuals_supplies_baseprod_staging_interim_supplies_only = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Staging.ms4.odw_actuals_supplies_baseprod_staging_interim_supplies_only") \
    .load()\
    .select("cal_date", "country_alpha2", "market10", "base_product_number", "pl", "l5_description", "customer_engagement", "gross_revenue", "net_currency", "contractual_discounts", "discretionary_discounts", "net_revenue", "total_cos", "gross_profit", "revenue_units", "equivalent_units", "yield_x_units", "yield_x_units_black_only", "warranty", "other_cos")

itp_laser_landing = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Landing.dbo.itp_laser_landing") \
    .load()

edw_actuals_supplies_baseprod_staging_interim_supplies_only = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Staging.dbo.edw_actuals_supplies_baseprod_staging_interim_supplies_only") \
    .load()\
    .select("cal_date", "country_alpha2", "market10", "base_product_number", "pl", "l5_description", "customer_engagement", "gross_revenue", "net_currency", "contractual_discounts", "discretionary_discounts", "net_revenue", "total_cos", "gross_profit", "revenue_units", "equivalent_units", "yield_x_units", "yield_x_units_black_only", "warranty", "other_cos")

#--------- fin_prod ---------
ci_history_supplies_finance_landing = read_sql_server_to_df(configs) \
    .option("dbtable", "ie2_landing.dbo.ci_history_supplies_finance_landing") \
    .load()

odw_sacp_actuals = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.ms4.odw_sacp_actuals") \
    .load()
    
odw_revenue_units_sales_actuals = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Landing.ms4.odw_revenue_units_sales_actuals_landing") \
    .load()\
    .select(col("Fiscal Year/Period").alias("fiscal_year_period")
      , col("Profit Center Hier Desc Level4").alias("profit_center_hier_desc_level4")
      , col("Segment Hier Desc Level4").alias("segment_hier_desc_level4")
      , col("Segment Code").alias("segment_code")
      , col("Segment Name").alias("segment_name")
      , col("Profit Center Code").alias("profit_center_code")
      , col("Material Number").alias("material_number")
      , col("Unit Quantity (Sign-Flip)").alias("unit_quantity_sign_flip")
      , col("load_date").alias("load_date")
      , col("Unit Reporting Code").alias("unit_reporting_code")
      , col("Unit Reporting Description").alias("unit_reporting_description"))

odw_revenue_units_base_actuals = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Landing.ms4.odw_revenue_units_base_actuals_landing") \
    .load()

odw_report_rac_product_financials_actuals = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Landing.ms4.odw_report_rac_product_financials_actuals_landing") \
    .load()\
    .select(col("Fiscal Year/Period").alias("fiscal_year_period")
      , col("Material Number").alias("material_number")
      , col("Profit Center Code").alias("profit_center_code")
      , col("Segment Code").alias("segment_code")
      , col("Segment Name").alias("segment_name")
      , col("Gross Trade Revenues USD").alias("gross_trade_revenues_usd")
      , col("Contractual Discounts USD").alias("contractual_discounts_usd")
      , col("Discretionary Discounts USD").alias("discretionary_discounts_usd")
      , col("Net Currency USD").alias("net_currency_usd")
      , col("Net Revenues USD").alias("net_revenues_usd")
      , col("WARR").alias("warr")
      , col("TOTAL COST OF SALES USD").alias("total_cost_of_sales_usd")
      , col("GROSS MARGIN USD").alias("gross_margin_usd")
      , col("load_date").alias("load_date"))

odw_document_currency = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.ms4.odw_document_currency") \
    .load()

odw_actuals_supplies_salesprod = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.ms4.odw_actuals_supplies_salesprod") \
    .load()\
    .select("record", "cal_date", "country_alpha2", "market10", "currency", "sales_product_number", "pl", "l5_Description", "customer_engagement", "gross_revenue", "net_currency", "contractual_discounts", "discretionary_discounts", "net_revenue", "warranty", "other_cos", "total_cos", "gross_profit", "revenue_units", "official", "load_date", "version")

odw_actuals_supplies_baseprod = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.ms4.odw_actuals_supplies_baseprod") \
    .load()\
    .select("record", "cal_date", "country_alpha2", "market10", "platform_subset", "base_product_number", "pl", "l5_Description", "customer_engagement", "gross_revenue", "net_currency", "contractual_discounts", "discretionary_discounts", "net_revenue", "warranty", "other_cos", "total_cos", "gross_profit", "revenue_units", "equivalent_units", "yield_x_units", "yield_x_units_black_only", "official", "load_date", "version")

edw_summary_actuals_plau = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Landing.dbo.actuals_edw_plau_bigdata_dashboard_landing") \
    .load()

edw_actuals_supplies_salesprod = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.dbo.edw_actuals_supplies_salesprod") \
    .load()\
    .select("record", "cal_date", "country_alpha2", "market10", "currency", "sales_product_number", "pl", "l5_description", "customer_engagement", "gross_revenue", "net_currency", "contractual_discounts", "discretionary_discounts", "net_revenue", "warranty", "other_cos", "total_cos", "gross_profit", "revenue_units", "official", "load_date", "version")

edw_actuals_supplies_baseprod = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.dbo.edw_actuals_supplies_baseprod") \
    .load()\
    .select("record", "cal_date", "country_alpha2", "market10", "platform_subset", "base_product_number", "pl", "l5_description", "customer_engagement", "gross_revenue", "net_currency", "contractual_discounts", "discretionary_discounts", "net_revenue", "warranty", "other_cos", "total_cos", "gross_profit", "revenue_units", "equivalent_units", "yield_x_units", "yield_x_units_black_only", "official", "load_date", "version")

actuals_supplies_salesprod = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.dbo.actuals_supplies_salesprod") \
    .load()\
    .select("record", "cal_date", "country_alpha2", "currency", "market10", "sales_product_number", "pl", "L5_Description", "customer_engagement", "gross_revenue", "net_currency", "contractual_discounts", "discretionary_discounts", "net_revenue", "warranty", "other_cos", "total_cos", "gross_profit", "revenue_units", "official", "load_date", "version")

actuals_supplies_baseprod = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Financials.dbo.actuals_supplies_baseprod") \
    .load()\
    .select("record", "cal_date", "country_alpha2", "market10", "platform_subset", "base_product_number", "pl", "L5_Description", "customer_engagement", "gross_revenue", "net_currency", "contractual_discounts", "discretionary_discounts", "net_revenue", "warranty", "other_cos", "total_cos", "gross_profit", "revenue_units", "equivalent_units", "yield_x_units", "yield_x_units_black_only", "official", "load_date", "version")

ci_flash_for_insights_supplies_temp = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Landing.dbo.ci_flash_for_insights_supplies_temp") \
    .load()\
    .select(col("Fiscal_Year_Qtr").alias("fiscal_year_qtr ")
            , col("PL").alias("pl")
            , col("Business_description").alias("business_description")
            , col("Market").alias("market")
            , col("Channel Inv $K").alias("channel_inv_k")
            , col("Ink/Toner").alias("ink_toner")
           )

rev_flash_for_insights_supplies_temp = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Landing.dbo.rev_flash_for_insights_supplies_temp") \
    .load()\
    .select(col("Fiscal_Year_Qtr").alias("fiscal_year_qtr")
            , col("PL").alias("pl")
            , col("Business_description").alias("business_description")
            , col("Market").alias("market")
            , col("Net_Revenues $K").alias("net_revenues_k")
            , col("Ink/Toner").alias("ink_toner")
            , col("Hedge $K").alias("hedge_k")
            , col("Concatenate").alias("concatenate")
           )

#--------- mdm ---------
ms4_functional_area_hierarchy = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Landing.ms4.HP_simplify_functional_area_hierarchy_v1") \
    .load()\
    .select(col("Functional Area Code").alias("functional_area_code")
      , col("Functional Area Name").alias("functional_area_name")
      , col("Func Area Hier Level0").alias("func_area_hier_level0")
      , col("Func Area Hier Desc Level0").alias("func_area_hier_desc_level0")
      , col("Func Area Hier Level1").alias("func_area_hier_level1")
      , col("Func Area Hier Desc Level1").alias("func_area_hier_desc_level1")
      , col("Func Area Hier Level2").alias("func_area_hier_level2")
      , col("Func Area Hier Desc Level2").alias("func_area_hier_desc_level2")
      , col("Func Area Hier Level3").alias("func_area_hier_level3")
      , col("Func Area Hier Desc Level3").alias("func_area_hier_desc_level3")
      , col("Func Area Hier Level4").alias("func_area_hier_level4")
      , col("Func Area Hier Desc Level4").alias("func_area_hier_desc_level4")
      , col("Func Area Hier Level5").alias("func_area_hier_level5")
      , col("Func Area Hier Desc Level5").alias("func_area_hier_desc_level5")
      , col("Func Area Hier Level6").alias("func_area_hier_level6")
      , col("Func Area Hier Desc Level6").alias("func_area_hier_desc_level6")
      , col("Func Area Hier Level7").alias("func_area_hier_level7")
      , col("Func Area Hier Desc Level7").alias("func_area_hier_desc_level7")
      , col("Func Area Hier Level8").alias("func_area_hier_level8")
      , col("Func Area Hier Desc Level8").alias("func_area_hier_desc_level8")
      , col("Func Area Hier Level9").alias("func_area_hier_level9")
      , col("Func Area Hier Desc Level9").alias("func_area_hier_desc_level9")
      , col("Func Area Hier Level10").alias("func_area_hier_level10")
      , col("Func Area Hier Desc Level10").alias("func_area_hier_desc_level10")
      , col("Func Area Hier Level11").alias("func_area_hier_level11")
      , col("Func Area Hier Desc Level11").alias("func_area_hier_des_level11")
      , col("Func Area Hier Level12").alias("func_area_hier_level12")
      , col("Func Area Hier Desc Level12").alias("func_area_hier_desc_level12")
      , col("FUNC AREA LEAF NODE").alias("func_area_leaf_node")
      , col("load_date").alias("load_date"))

exclusion = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Prod.dbo.exclusion") \
    .load()

#--------- prod ---------
actuals_supplies = read_sql_server_to_df(configs) \
    .option("dbtable", "IE2_Prod.dbo.actuals_supplies") \
    .load()


# COMMAND ----------

tables = [
    ['mdm.ms4_functional_area_hierarchy', ms4_functional_area_hierarchy, "overwrite"],
    ['mdm.exclusion', exclusion, "overwrite"],
    ['prod.actuals_supplies', actuals_supplies, "overwrite"],
    ['fin_stage.supplies_manual_mcode_jv_detail_landing', supplies_manual_mcode_jv_detail_landing, "overwrite"],
    ['fin_stage.supplies_iink_units_landing', supplies_iink_units_landing, "overwrite"],
    ['fin_stage.odw_actuals_supplies_baseprod_staging_interim_supplies_only', odw_actuals_supplies_baseprod_staging_interim_supplies_only, "overwrite"],
    ['fin_stage.itp_laser_landing', itp_laser_landing, "overwrite"],
    ['fin_stage.edw_actuals_supplies_baseprod_staging_interim_supplies_only', edw_actuals_supplies_baseprod_staging_interim_supplies_only, "overwrite"],
    ['fin_prod.ci_history_supplies_finance_landing', ci_history_supplies_finance_landing, "overwrite"],
    ['fin_prod.odw_sacp_actuals', odw_sacp_actuals, "overwrite"],
    ['fin_prod.odw_revenue_units_sales_actuals', odw_revenue_units_sales_actuals, "overwrite"],
    ['fin_prod.odw_revenue_units_base_actuals', odw_revenue_units_base_actuals, "overwrite"],
    ['fin_prod.odw_report_rac_product_financials_actuals', odw_report_rac_product_financials_actuals, "overwrite"],
    ['fin_prod.odw_document_currency', odw_document_currency, "overwrite"],
    ['fin_prod.odw_actuals_supplies_salesprod', odw_actuals_supplies_salesprod, "overwrite"],
    ['fin_prod.odw_actuals_supplies_baseprod', odw_actuals_supplies_baseprod, "overwrite"],
    ['fin_prod.edw_summary_actuals_plau', edw_summary_actuals_plau, "overwrite"],
    ['fin_prod.edw_actuals_supplies_salesprod', edw_actuals_supplies_salesprod, "overwrite"],
    ['fin_prod.edw_actuals_supplies_baseprod', edw_actuals_supplies_baseprod, "overwrite"],
    ['fin_prod.actuals_supplies_salesprod', actuals_supplies_salesprod, "overwrite"],
    ['fin_prod.actuals_supplies_baseprod', actuals_supplies_baseprod, "overwrite"],
    ['fin_prod.ci_flash_for_insights_supplies_temp', ci_flash_for_insights_supplies_temp, "overwrite"],
    ['fin_prod.rev_flash_for_insights_supplies_temp', rev_flash_for_insights_supplies_temp, "overwrite"],
]

# COMMAND ----------

for t_name, df, mode in tables:
    write_df_to_redshift(configs, df, t_name, mode)
