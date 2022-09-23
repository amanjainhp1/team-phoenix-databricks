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
    .load()

odw_report_rac_product_financials_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_report_rac_product_financials_actuals") \
    .load()

odw_report_ships_deliveries_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_report_ships_deliveries_actuals") \
    .load()

odw_revenue_units_sales_actuals_prelim = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_revenue_units_sales_actuals_prelim") \
    .load()

odw_revenue_units_sales_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_revenue_units_sales_actuals") \
    .load()

odw_sacp_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_sacp_actuals") \
    .load()

actuals_supplies_salesprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_supplies_salesprod") \
    .load()

#---------   mdm  -----------
    
#---------   prod   ---------
actuals_supplies = read_redshift_to_df(configs) \
    .option("dbtable", "prod.actuals_supplies") \
    .load()


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SFAI Table Names

# COMMAND ----------

tables = [
    ['IE2_Landing.ms4.odw_report_rac_product_financials_actuals_landing', odw_report_rac_product_financials_actuals, "append"],
    ['IE2_Financials.ms4.odw_revenue_units_sales_actuals_landing', odw_revenue_units_sales_actuals, "append"],
    ['IE2_Landing.ms4.odw_revenue_units_sales_actuals_prelim_landing', odw_revenue_units_sales_actuals_prelim, "append"],
    ['IE2_Landing.ms4.odw_report_ships_deliveries_actuals_landing', odw_report_ships_deliveries_actuals_landing, "append"],
    ['IE2_Financials.ms4.odw_sacp_actuals', odw_sacp_actuals, "append"],
    ['IE2_Financials.dbo.actuals_supplies_salesprod', actuals_supplies_salesprod, "append"],
    ['IE2_Financials.dbo.actuals_supplies_salesprod', actuals_supplies_salesprod, "append"],
    ['IE2_Prod.dbo.actuals_supplies', actuals_supplies, "append"]
]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write to SFAI

# COMMAND ----------

for t_name, df, mode in tables:
    write_df_to_sqlserver(configs, df, t_name, mode)
