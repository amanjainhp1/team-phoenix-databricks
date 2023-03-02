# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

#load cbm data from sfai
cbm_st_data = read_sql_server_to_df(configs) \
    .option("dbtable", "CBM.dbo.cbm_st_data") \
    .load()

# COMMAND ----------

# load S3 tables to df
odw_revenue_units_sales_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_revenue_units_sales_actuals") \
    .load()
odw_document_currency = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_document_currency") \
    .load()
odw_report_rac_product_financials_actuals = read_redshift_to_df(configs) \
   .option("dbtable", "fin_prod.odw_report_rac_product_financials_actuals") \
   .load()
mps_ww_shipped_supply = read_redshift_to_df(configs) \
    .option("dbtable", "prod.mps_ww_shipped_supply") \
    .load()
iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load()
calendar = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.calendar") \
    .load()
profit_center_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.profit_center_code_xref") \
    .load()
product_line_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.product_line_xref") \
    .load()
itp_laser_landing = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.itp_laser_landing") \
    .load()
supplies_iink_units_landing = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.supplies_iink_units_landing") \
    .load()
supplies_manual_mcode_jv_detail_landing = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.supplies_manual_mcode_jv_detail_landing") \
    .load()
country_currency_map_landing = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.country_currency_map") \
    .load()
list_price_eu_country_list = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.list_price_eu_country_list") \
    .load()
exclusion = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.exclusion") \
    .load()
rdma_base_to_sales_product_map = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.rdma_base_to_sales_product_map") \
    .load()
supplies_hw_mapping = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.supplies_hw_mapping") \
    .load()
ib = read_redshift_to_df(configs) \
    .option("dbtable", "prod.ib") \
    .load()
actuals_supplies_salesprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_supplies_salesprod") \
    .load()
odw_sacp_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_sacp_actuals") \
    .load()
supplies_finance_hier_restatements_2020_2021 = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.supplies_finance_hier_restatements_2020_2021") \
    .load()
supplies_hw_country_actuals_mapping = read_redshift_to_df(configs) \
    .option("dbtable", "stage.supplies_hw_country_actuals_mapping") \
    .load()

# COMMAND ----------

# delta tables

import re

tables = [
    ['fin_stage.odw_revenue_units_sales_actuals', odw_revenue_units_sales_actuals],
    ['fin_stage.odw_document_currency', odw_document_currency],
    ['fin_stage.odw_report_rac_product_financials_actuals', odw_report_rac_product_financials_actuals],
    ['fin_stage.mps_ww_shipped_supply_staging', mps_ww_shipped_supply],
    ['mdm.iso_country_code_xref', iso_country_code_xref],
    ['mdm.profit_center_code_xref', profit_center_code_xref],
    ['fin_stage.itp_laser_landing', itp_laser_landing],
    ['fin_stage.supplies_iink_units_landing', supplies_iink_units_landing],
   # ['stage.supplies_hw_country_actuals_mapping', supplies_hw_country_actuals_mapping],
    ['mdm.calendar', calendar],
    ['mdm.product_line_xref', product_line_xref],
    ['fin_stage.supplies_manual_mcode_jv_detail_landing', supplies_manual_mcode_jv_detail_landing],
    ['fin_stage.country_currency_map_landing', country_currency_map_landing],
    ['mdm.list_price_eu_country_list', list_price_eu_country_list],
    ['fin_stage.cbm_st_data', cbm_st_data],
    ['mdm.exclusion', exclusion],
    ['mdm.rdma_base_to_sales_product_map', rdma_base_to_sales_product_map],
    ['mdm.supplies_hw_mapping', supplies_hw_mapping],
    ['stage.ib', ib],
    ['fin_stage.odw_sacp_actuals', odw_sacp_actuals],
    ['fin_stage.supplies_finance_hier_restatements_2020_2021', supplies_finance_hier_restatements_2020_2021]
    ]

for table in tables:
    # Define the input and output formats and paths and the table name.
    schema = table[0].split(".")[0]
    table_name = table[0].split(".")[1]
    write_format = 'delta'
    save_path = f'/tmp/delta/{schema}/{table_name}'
    
    # Load the data from its source.
    df = table[1]    
    print(f'loading {table[0]}...')
    
    for column in df.dtypes:
        renamed_column = re.sub('\)', '', re.sub('\(', '', re.sub('-', '_', re.sub('/', '_', re.sub('\$', '_dollars', re.sub(' ', '_', column[0])))))).lower()
        df = df.withColumnRenamed(column[0], renamed_column)
        print(renamed_column) 
        
     # Write the data to its target.
    df.write \
       .format(write_format) \
       .option("overwriteSchema", "true") \
       .mode("overwrite") \
       .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
     # Create the table.
    spark.sql("CREATE TABLE IF NOT EXISTS " + table[0] + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')

# COMMAND ----------

actuals_supplies_salesprod.createOrReplaceTempView("actuals_supplies_salesprod")

# COMMAND ----------

supplies_hw_country_actuals_mapping.createOrReplaceTempView("supplies_hw_country_actuals_mapping")

# COMMAND ----------

addversion_info = call_redshift_addversion_sproc(configs, "ACTUALS - ODW SUPPLIES SALES PRODUCT FINANCIALS", "ACTUALS - ODW SUPPLIES SALES PRODUCT FINANCIALS")

# COMMAND ----------

# ODW REVENUE UNITS

# COMMAND ----------

odw_extended_quantity = f"""
SELECT cal.Date AS cal_date
      ,segment_code
      ,pl
      ,material_number as sales_product_option
      ,unit_reporting_code
      ,unit_reporting_description
      ,SUM(unit_quantity_sign_flip) as extended_quantity
  FROM fin_stage.odw_revenue_units_sales_actuals land
  LEFT JOIN mdm.calendar cal ON ms4_Fiscal_Year_Period = fiscal_year_period
  LEFT JOIN mdm.product_line_xref plx ON land.profit_center_code = plx.profit_center_code
  WHERE 1=1
  AND Day_of_Month = 1
  AND cal.Date > '2021-10-01'
  AND unit_quantity_sign_flip <> 0
  AND unit_quantity_sign_flip is not null
  GROUP BY cal.Date, pl, material_number, segment_code, unit_reporting_code, unit_reporting_description
"""

odw_extended_quantity = spark.sql(odw_extended_quantity)
odw_extended_quantity.createOrReplaceTempView("odw_extended_quantity")


odw_extended_quantity_country = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_option,
    unit_reporting_code,
    unit_reporting_description,
    SUM(extended_quantity) as extended_quantity
FROM odw_extended_quantity odw
LEFT JOIN mdm.profit_center_code_xref s ON segment_code = profit_center_code
GROUP BY cal_date, country_alpha2, pl, sales_product_option, unit_reporting_code, unit_reporting_description
"""

odw_extended_quantity_country = spark.sql(odw_extended_quantity_country)
odw_extended_quantity_country.createOrReplaceTempView("odw_extended_quantity_country")


#SELECT ONLY SUPPLIES AND LLC PRODUCT LINES 
supplies_units = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_option,
    0 as gross_revenue,
    0 as net_currency,
    0 as contractual_discounts,
    0 as discretionary_discounts,
    0 as warranty,
    0 as total_cos_without_warranty,
    SUM(extended_quantity) as extended_quantity
FROM odw_extended_quantity_country
WHERE pl IN (
    SELECT distinct pl 
    FROM mdm.product_line_xref 
    WHERE 1=1
    AND pl_category IN ('SUP') 
    AND technology IN ('PWA', 'LASER', 'INK', 'LF')
    )
    AND unit_reporting_code = 'S'
GROUP BY cal_date,
    country_alpha2,
    pl,
    sales_product_option
"""

supplies_units = spark.sql(supplies_units)
supplies_units.createOrReplaceTempView("supplies_units")


llcs_units = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_option,
    0 as gross_revenue,
    0 as net_currency,
    0 as contractual_discounts,
    0 as discretionary_discounts,
    0 as warranty,
    0 as total_cos_without_warranty,
    SUM(extended_quantity) as extended_quantity
FROM odw_extended_quantity_country
WHERE pl IN (
    SELECT distinct pl 
    FROM mdm.product_line_xref 
    WHERE 1=1
    AND pl_category IN ('LLC') 
    AND technology IN ('LLCS')
    )
    AND unit_reporting_code = 'O'
GROUP BY cal_date,
    country_alpha2,
    pl,
    sales_product_option
"""

llcs_units = spark.sql(llcs_units)
llcs_units.createOrReplaceTempView("llcs_units")


combined_unit_types = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_option,    
    SUM(gross_revenue) as gross_revenue,
    SUM(net_currency) as net_currency,
    SUM(contractual_discounts) as contractual_discounts,
    SUM(discretionary_discounts) as discretionary_discounts,
    SUM(warranty) as warranty,
    SUM(total_cos_without_warranty) as total_cos_without_warranty,
    SUM(extended_quantity) AS revenue_units
FROM supplies_units
GROUP BY cal_date, country_alpha2, pl, sales_product_option

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_option,    
    SUM(gross_revenue) as gross_revenue,
    SUM(net_currency) as net_currency,
    SUM(contractual_discounts) as contractual_discounts,
    SUM(discretionary_discounts) as discretionary_discounts,
    SUM(warranty) as warranty,
    SUM(total_cos_without_warranty) as total_cos_without_warranty,
    SUM(extended_quantity) AS revenue_units
FROM llcs_units
GROUP BY cal_date, country_alpha2, pl, sales_product_option
"""

combined_unit_types = spark.sql(combined_unit_types)
combined_unit_types.createOrReplaceTempView("combined_unit_types")


odw_revenue_unit_data = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_option,    
    COALESCE(SUM(gross_revenue), 0) as gross_revenue,
    COALESCE(SUM(net_currency), 0) as net_currency,
    COALESCE(SUM(contractual_discounts), 0) as contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) as discretionary_discounts,
    COALESCE(SUM(warranty),0) as warranty,
    COALESCE(SUM(total_cos_without_warranty), 0) as total_cos_without_warranty,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM combined_unit_types
GROUP BY cal_date, country_alpha2, pl, sales_product_option
"""

odw_revenue_unit_data = spark.sql(odw_revenue_unit_data)
odw_revenue_unit_data.createOrReplaceTempView("odw_revenue_unit_data")


# COMMAND ----------

# ODW FINANCIAL DATA (REVENUE AND CONSOLIDATIONS MODEL, "RAC")

# COMMAND ----------

odw_dollars_raw = f"""
  SELECT cal.Date AS cal_date
      ,segment_code
      ,pl
      ,material_number as sales_product_option
      ,SUM(gross_trade_revenues_usd) as gross_revenue    
      ,SUM(net_currency_usd) * -1 as net_currency  -- * -1 makes the data like it was in edw
      ,SUM(contractual_discounts_usd) * -1 as contractual_discounts
      ,SUM(discretionary_discounts_usd) * -1 as discretionary_discounts
      ,SUM(net_revenues_usd) as net_revenue
      ,SUM(warr) * -1 as warranty
      ,SUM(total_cost_of_sales_usd) * -1 as total_cos
      ,SUM(gross_margin_usd) as gross_profit
  FROM fin_stage.odw_report_rac_product_financials_actuals land
  LEFT JOIN mdm.calendar cal ON ms4_Fiscal_Year_Period = fiscal_year_period
  LEFT JOIN mdm.product_line_xref plx ON land.profit_center_code = plx.profit_center_code
  WHERE 1=1
  AND day_of_month = 1
  AND cal.Date > '2021-10-01'
  AND land.profit_center_code NOT IN ('P1082', 'PF001')
  GROUP BY cal.Date, pl, material_number, segment_code
"""

odw_dollars_raw = spark.sql(odw_dollars_raw)
odw_dollars_raw.createOrReplaceTempView("odw_dollars_raw")


odw_history_periods = f"""
SELECT distinct Edw_fiscal_yr_mo 
FROM odw_dollars_raw d
LEFT JOIN mdm.calendar cal ON d.cal_date = cal.Date
WHERE day_of_month = 1
"""

odw_history_periods = spark.sql(odw_history_periods)
odw_history_periods.createOrReplaceTempView("odw_history_periods")


odw_dollars = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    CASE
        WHEN sales_product_option is null THEN CONCAT('UNKN', pl)
        ELSE sales_product_option
    END AS sales_product_option,
    SUM(gross_revenue) as gross_revenue,
    SUM(net_currency) as net_currency, 
    SUM(contractual_discounts) as contractual_discounts,
    SUM(discretionary_discounts) as discretionary_discounts,
    SUM(warranty) as warranty,
    SUM(total_cos) as total_cos,
    SUM(total_cos) - SUM(warranty) as total_cos_without_warranty
FROM odw_dollars_raw odw
LEFT JOIN mdm.profit_center_code_xref s ON segment_code = profit_center_code
WHERE 1=1
GROUP BY cal_date, country_alpha2, pl, sales_product_option
"""

odw_dollars = spark.sql(odw_dollars)
odw_dollars.createOrReplaceTempView("odw_dollars")


#SELECT ONLY SUPPLIES AND LLC PRODUCT LINES 
supplies_dollars = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_option,
    SUM(gross_revenue) as gross_revenue,
    SUM(net_currency) as net_currency, 
    SUM(contractual_discounts) as contractual_discounts,
    SUM(discretionary_discounts) as discretionary_discounts,
    SUM(warranty) as warranty,
    SUM(total_cos_without_warranty) as total_cos_without_warranty,
    0 as revenue_units
FROM odw_dollars
WHERE pl IN (
    SELECT distinct pl 
    FROM mdm.product_line_xref 
    WHERE 1=1
    AND pl_category IN ('SUP', 'LLC') 
    AND technology IN ('LLCS', 'PWA', 'LASER', 'INK', 'LF')
    )
GROUP BY cal_date,
    country_alpha2,
    pl,
    sales_product_option
"""

supplies_dollars = spark.sql(supplies_dollars)
supplies_dollars.createOrReplaceTempView("supplies_dollars")

# COMMAND ----------

# COMBINE FINANCIAL AND UNIT DATA, CLEAN IT UP, FIX "MISTAKES" (per business input)

# COMMAND ----------

odw_data_join = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_option,
    SUM(gross_revenue) as gross_revenue,
    SUM(net_currency) as net_currency,
    SUM(contractual_discounts) as contractual_discounts,
    SUM(discretionary_discounts) as discretionary_discounts,
    SUM(warranty) as warranty,
    SUM(total_cos_without_warranty) as total_cos_without_warranty,
    SUM(revenue_units) AS revenue_units
FROM supplies_dollars
GROUP BY cal_date, country_alpha2, pl, sales_product_option

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_option,    
    SUM(gross_revenue) as gross_revenue,
    SUM(net_currency) as net_currency,
    SUM(contractual_discounts) as contractual_discounts,
    SUM(discretionary_discounts) as discretionary_discounts,
    SUM(warranty) as warranty,
    SUM(total_cos_without_warranty) as total_cos_without_warranty,
    SUM(revenue_units) AS revenue_units
FROM odw_revenue_unit_data
GROUP BY cal_date, country_alpha2, pl, sales_product_option
"""

odw_data_join = spark.sql(odw_data_join)
odw_data_join.createOrReplaceTempView("odw_data_join")

odw_data = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_option,    
    COALESCE(SUM(gross_revenue), 0) as gross_revenue,
    COALESCE(SUM(net_currency), 0) as net_currency,
    COALESCE(SUM(contractual_discounts), 0) as contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) as discretionary_discounts,
    COALESCE(SUM(warranty),0) as warranty,
    COALESCE(SUM(total_cos_without_warranty), 0) as other_cos,
    COALESCE(SUM(total_cos_without_warranty),0) + COALESCE(SUM(warranty), 0) as total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM odw_data_join
GROUP BY cal_date, country_alpha2, pl, sales_product_option
"""

odw_data = spark.sql(odw_data)
odw_data.createOrReplaceTempView("odw_data")


findata_clean_zeros = f"""
SELECT 
    cal_date,
    country_alpha2,
    pl, 
    sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) as warranty,
    SUM(other_cos) as other_cos,
    SUM(total_cos) as total_cos,
    SUM(revenue_units) AS revenue_units,
        SUM(gross_revenue) + SUM(net_currency) + SUM(contractual_discounts) + 
        SUM(discretionary_discounts) + SUM(warranty) + SUM(other_cos) + SUM(total_cos) + sum(revenue_units) AS total_sum            
FROM odw_data AS edw
GROUP BY cal_date, country_alpha2, pl, sales_product_option 
"""

findata_clean_zeros = spark.sql(findata_clean_zeros)
findata_clean_zeros.createOrReplaceTempView("findata_clean_zeros")

        
final_findata = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,        
    sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM findata_clean_zeros
WHERE total_sum != 0
    AND pl IN (
        SELECT DISTINCT (pl) 
        FROM mdm.product_line_xref 
        WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
        AND PL_category IN ('SUP', 'LLC')
        OR pl = 'IE'
        )
GROUP BY cal_date, country_alpha2, pl, sales_product_option
"""

final_findata = spark.sql(final_findata)
final_findata.createOrReplaceTempView("final_findata")
        

final_union_odw_data = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM final_findata edw
WHERE 1=1
GROUP BY cal_date, country_alpha2, pl, sales_product_option
"""

final_union_odw_data = spark.sql(final_union_odw_data)
final_union_odw_data.createOrReplaceTempView("final_union_odw_data")

# COMMAND ----------

# Write out final_union_edw_data to its delta table target.
final_union_odw_data.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/final_union_odw_data")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.final_union_odw_data USING DELTA LOCATION '/tmp/delta/fin_stage/final_union_odw_data'")
spark.table("fin_stage.final_union_odw_data").createOrReplaceTempView("final_union_odw_data")


# COMMAND ----------

# MPS SUPPLIES SHIPMENTS CLEAN UP

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM fin_stage.mps_ww_shipped_supply_staging
# MAGIC WHERE version <> (SELECT MAX(version) FROM fin_stage.mps_ww_shipped_supply_staging) 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- subset mps shipment data to supplies product lines only
# MAGIC DELETE FROM fin_stage.mps_ww_shipped_supply_staging
# MAGIC WHERE Prod_Line NOT IN 
# MAGIC     (
# MAGIC     SELECT DISTINCT pl
# MAGIC     FROM product_line_xref 
# MAGIC     WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
# MAGIC         AND PL_category IN ('SUP', 'LLC')
# MAGIC         OR PL IN ('GO', 'GN', 'IE', 'IX')  
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- specify that direct vs indirect distinction reflects fulfillment and not totals for Direct MPS and Partner MPS per se (5:20, 23:16)
# MAGIC UPDATE fin_stage.mps_ww_shipped_supply_staging
# MAGIC SET 
# MAGIC direct_or_indirect_sf = 'EST_DIRECT_FULFILLMENT'
# MAGIC WHERE 
# MAGIC direct_or_indirect_sf  = 'DIRECT'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.mps_ww_shipped_supply_staging
# MAGIC SET 
# MAGIC     direct_or_indirect_sf = 'EST_INDIRECT_FULFILLMENT'
# MAGIC WHERE 
# MAGIC     direct_or_indirect_sf  = 'INDIRECT'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.mps_ww_shipped_supply_staging
# MAGIC SET 
# MAGIC     country = 'UNKNOWN LA'
# MAGIC WHERE    
# MAGIC     country = 'CENTRAL AMERICA & CARIBBEAN'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.mps_ww_shipped_supply_staging
# MAGIC SET 
# MAGIC     country = 'MACEDONIA (THE FORMER YUGOSLAV REPUBLIC OF)'
# MAGIC WHERE    
# MAGIC     country = 'MACEDONIA'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.mps_ww_shipped_supply_staging
# MAGIC SET 
# MAGIC     country = 'SOUTH AFRICA'
# MAGIC WHERE    
# MAGIC     country = 'RSA'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.mps_ww_shipped_supply_staging
# MAGIC SET 
# MAGIC     country = 'RUSSIAN FEDERATION'
# MAGIC WHERE    
# MAGIC     country = 'RUSSIA'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.mps_ww_shipped_supply_staging
# MAGIC SET 
# MAGIC     country = 'MONTENEGRO'
# MAGIC WHERE    
# MAGIC     country = 'SERBIA-MONTENEGRO'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.mps_ww_shipped_supply_staging
# MAGIC SET 
# MAGIC   country = 'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND'
# MAGIC   WHERE    
# MAGIC   country = 'UK'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.mps_ww_shipped_supply_staging
# MAGIC SET 
# MAGIC     country = 'UNITED STATES OF AMERICA'
# MAGIC WHERE    
# MAGIC     country = 'USA'

# COMMAND ----------

#MAKE CORRECTIONS TO DATA PER INPUTS FROM BUSINESS (business assertions of wrong-ness)
# 1. Drop # to get to sales product number like RDMA
# 2. drop Brazil data for OPS for FY2020 forward (units) [HP acquired Simpress so Brazil OPS sales become 'hp direct']

# COMMAND ----------

odw_data_salesprod1 = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    CASE
        WHEN sales_product_option LIKE '%#%'
        THEN LEFT(sales_product_option, 7)
        ELSE sales_product_option
    END AS sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM final_union_odw_data
GROUP BY cal_date, country_alpha2, pl, sales_product_option
"""

odw_data_salesprod1 = spark.sql(odw_data_salesprod1)
odw_data_salesprod1.createOrReplaceTempView("odw_data_salesprod1")


odw_data_salesprod2 = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    CASE
        WHEN sales_product_number LIKE '%#%'
        THEN LEFT(sales_product_number, 6)
        ELSE sales_product_number
    END AS sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM odw_data_salesprod1
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

odw_data_salesprod2 = spark.sql(odw_data_salesprod2)
odw_data_salesprod2.createOrReplaceTempView("odw_data_salesprod2")

# COMMAND ----------

supplies_pls_update_brazil = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    sales_product_number,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    CASE 
        WHEN country_alpha2 = 'BR' 
            AND cal_date > '2019-10-01' 
            AND pl IN ('5T', 'GJ', 'GK', 'GL', 'IU', 'E5', 'EO')
        THEN NULL
        ELSE SUM(revenue_units)
    END AS revenue_units
FROM odw_data_salesprod2
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

supplies_pls_update_brazil = spark.sql(supplies_pls_update_brazil)
supplies_pls_update_brazil.createOrReplaceTempView("supplies_pls_update_brazil")


supplies_final_findata = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    sales_product_number,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM supplies_pls_update_brazil
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

supplies_final_findata = spark.sql(supplies_final_findata)
supplies_final_findata.createOrReplaceTempView("supplies_final_findata")

# COMMAND ----------

#SYSTEM GOAL = drillable financials, meaning PL-NON-SKU ITEMS SHOULD BE SPREAD TO SKU DATA; IDENTIFY PL-NON-SKU ITEMS HERE

# COMMAND ----------

supplies_final_findata2 = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_final_findata
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

supplies_final_findata2 = spark.sql(supplies_final_findata2)
supplies_final_findata2.createOrReplaceTempView("supplies_final_findata2")


#mps only
exclusion_list1 = f""" 
SELECT item_number
FROM mdm.exclusion
WHERE 1=1
-- We will come back to the MPS / clicks later -- these next four are associated with PPU/MPS
AND exclusion_reason = 'MPS TONER FEE' 
OR exclusion_reason = 'MPS SUPPLIES RECONCILIATION'
OR exclusion_reason = 'IPGS PPU SUPPLIES (TONER CLICKS)'
OR item_number = 'H7523A'  -- Tracy Phillips from MPS indicated this code is related to click charges for supplies
"""

exclusion_list1 = spark.sql(exclusion_list1)
exclusion_list1.createOrReplaceTempView("exclusion_list1")


supplies_findata_post_exclusions = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_final_findata2
WHERE sales_product_number NOT IN (SELECT DISTINCT(item_number) FROM exclusion_list1) 
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

supplies_findata_post_exclusions = spark.sql(supplies_findata_post_exclusions)
supplies_findata_post_exclusions.createOrReplaceTempView("supplies_findata_post_exclusions")


# The following item numbers that show up in the sales product field in edw appear to be charges to the PL, e.g. discretionary discount dollars charged to a PL,
# but which cannot be tied back to a SKU level.  These will need to be spread across skus (x sku, x month) later.  While included here, no expected impact to ODW system data. 

allocation_items = f"""
SELECT distinct sales_product_number
FROM supplies_findata_post_exclusions
WHERE 1=1 
    AND sales_product_number = '?'
    OR LENGTH(sales_product_number) = 2
    OR (LENGTH(sales_product_number) = 3 AND sales_product_number LIKE 'M%')
    OR sales_product_number = 'M-CHARGE'
    OR SUBSTRING(sales_product_number,1,7) = 'CDP-FCO' 
    OR SUBSTRING(sales_product_number,1,3) = 'NSN'
    OR SUBSTRING(sales_product_number, 1,2) = 'PL'
    OR SUBSTRING(sales_product_number,1,5) = 'PRICE'
    OR SUBSTRING(sales_product_number,1,5) = 'SACDP' 
    OR SUBSTRING(sales_product_number,1,5) = 'SALSP'
    OR SUBSTRING(sales_product_number,1,3) = 'SAM'
    OR SUBSTRING(sales_product_number,1,5) = 'SANSN'
    OR SUBSTRING(sales_product_number,1,5) = 'SASSP'
    OR SUBSTRING(sales_product_number,1,3) = 'SSP'
"""

allocation_items = spark.sql(allocation_items)
allocation_items.createOrReplaceTempView("allocation_items")


#rename allocation items as PL-CHARGE and add items not in the exclusion table or that the code doesn't appear to be excluding

supplies_findata_with_allocations = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_findata_post_exclusions 
WHERE 1=1
    AND sales_product_number NOT IN (SELECT sales_product_number FROM allocation_items)
GROUP BY cal_date,
    country_alpha2,
    pl,
    sales_product_number

UNION ALL

SELECT cal_date,
    country_alpha2,
    pl,
    'PL-CHARGE' AS sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_findata_post_exclusions 
WHERE 1=1
    AND sales_product_number IN (SELECT distinct sales_product_number FROM allocation_items)
    AND revenue_units = 0
GROUP BY cal_date, country_alpha2, pl

UNION ALL

SELECT cal_date,
    country_alpha2,
    pl,
    CONCAT('UNKN', pl) AS sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_findata_post_exclusions 
WHERE 1=1
    AND sales_product_number IN (SELECT sales_product_number FROM allocation_items)
    AND revenue_units <> 0
GROUP BY cal_date,
    country_alpha2,
    pl,
    sales_product_number
"""

supplies_findata_with_allocations = spark.sql(supplies_findata_with_allocations)
supplies_findata_with_allocations.createOrReplaceTempView("supplies_findata_with_allocations")


#drop base product from edw and continue to clean up the 5x5 sales products

edw_supplies_combined_findata_including_iink = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_findata_with_allocations
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

edw_supplies_combined_findata_including_iink = spark.sql(edw_supplies_combined_findata_including_iink)
edw_supplies_combined_findata_including_iink.createOrReplaceTempView("edw_supplies_combined_findata_including_iink")


odw_supplies_combined_findata = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_supplies_combined_findata_including_iink
WHERE pl != 'GD'
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

odw_supplies_combined_findata = spark.sql(odw_supplies_combined_findata)
odw_supplies_combined_findata.createOrReplaceTempView("odw_supplies_combined_findata")

# COMMAND ----------

# Write out edw_supplies_combined_findata to its delta table target.
odw_supplies_combined_findata.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/odw_supplies_combined_findata")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.odw_supplies_combined_findata USING DELTA LOCATION '/tmp/delta/fin_stage/odw_supplies_combined_findata'")

#spark.table("fin_stage.odw_supplies_combined_findata").createOrReplaceTempView("odw_supplies_combined_findata")

# COMMAND ----------

spark.table("fin_stage.odw_supplies_combined_findata").createOrReplaceTempView("odw_supplies_combined_findata")

# COMMAND ----------

# PUSH EMEA SUPPLIES FINANCIALS TO COUNTRY USING SELL-THRU DATA (HP FINANCIALS DO NOT PROVIDE COUNTRY SALES IN EMEA)

# COMMAND ----------

finance_sys_recorded_pl = f"""
SELECT distinct cal_date,
    sales_product_number,
    pl as fin_pl
FROM odw_supplies_combined_findata
"""

finance_sys_recorded_pl = spark.sql(finance_sys_recorded_pl)
finance_sys_recorded_pl.createOrReplaceTempView("finance_sys_recorded_pl")


salesprod_emea_supplies = f"""
SELECT cal_date,
    sup.country_alpha2,
    pl,
    sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM odw_supplies_combined_findata AS sup
JOIN mdm.iso_country_code_xref AS geo ON sup.country_alpha2 = geo.country_alpha2
WHERE region_3 = 'EMEA' 
GROUP BY cal_date, sup.country_alpha2, pl, sales_product_number
"""

salesprod_emea_supplies = spark.sql(salesprod_emea_supplies)
salesprod_emea_supplies.createOrReplaceTempView("salesprod_emea_supplies")


salesprod_emea_remove_edw_country = f"""
SELECT cal_date,
    pl,
    sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_emea_supplies
GROUP BY cal_date, pl, sales_product_number
"""

salesprod_emea_remove_edw_country = spark.sql(salesprod_emea_remove_edw_country)
salesprod_emea_remove_edw_country.createOrReplaceTempView("salesprod_emea_remove_edw_country")


# sell-thru data :: CBM database

cbm_st_database = f"""
SELECT 
    CASE
        WHEN data_type = 'ACTUALS' THEN 'ACTUALS - CBM_ST_BASE_QTY'
        WHEN data_type = 'PROXY ADJUSTMENT' THEN 'PROXY ADJUSTMENT'
     END AS record,
    CAST(Month AS DATE) AS cal_date,  
    CASE
        WHEN Country_Code = '0A' THEN 'XB'
        WHEN Country_Code = '0M' THEN 'XH'
        WHEN Country_Code = 'CS' THEN 'XA'
        WHEN Country_Code = 'KV' THEN 'XA'
    ELSE Country_Code
    END AS country_alpha2,
    Product_Number AS sales_product_number,
    Product_Line_ID AS pl, 
    partner,
    RTM_2 AS rtm2,
    SUM(CAST(COALESCE(sell_thru_usd,0) AS float)) AS sell_thru_usd,
    SUM(CAST(COALESCE(sell_thru_qty,0) AS float)) AS sell_thru_qty,
    SUM(CAST(COALESCE(channel_inventory_usd,0) AS float)) AS channel_inventory_usd,
    (SUM(CAST(COALESCE(channel_inventory_qty,0) AS float)) + 0.0) AS channel_inventory_qty
FROM fin_stage.cbm_st_data st
WHERE 1=1
    AND    Month > '2015-10-01' 
    AND (COALESCE(sell_thru_usd,0) + COALESCE(sell_thru_qty,0) + COALESCE(channel_inventory_usd,0) + COALESCE(channel_inventory_qty,0)) <> 0
GROUP BY Data_Type, Month, Country_Code, Product_Number, Product_Line_ID, partner, RTM_2
"""

cbm_st_database = spark.sql(cbm_st_database)
cbm_st_database.createOrReplaceTempView("cbm_st_database")


channel_inventory = f"""
SELECT 
    cal_date,
    country_alpha2,
    CASE
        WHEN sales_product_number LIKE '%#%'
        THEN LEFT(sales_product_number, 7)
        ELSE sales_product_number
    END AS sales_product_number,
    pl,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty
FROM cbm_st_database     
WHERE (cal_date BETWEEN (SELECT MIN(cal_date) FROM odw_supplies_combined_findata)
    AND (SELECT MAX(cal_date) FROM odw_supplies_combined_findata))
    AND pl IN 
    (
        SELECT DISTINCT (pl) 
        FROM mdm.product_line_xref 
        WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
            AND PL_category IN ('SUP', 'LLC')
            OR pl IN ('IE')
    )
GROUP BY cal_date, country_alpha2, sales_product_number, pl
"""      

channel_inventory = spark.sql(channel_inventory)
channel_inventory.createOrReplaceTempView("channel_inventory")


channel_inventory2 = f"""
SELECT 
    cal_date,
    country_alpha2,
    CASE
        WHEN sales_product_number LIKE '%#%'
        THEN LEFT(sales_product_number, 6)
        ELSE sales_product_number
    END AS sales_product_number,
    pl,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty
FROM channel_inventory
GROUP BY cal_date, country_alpha2, sales_product_number, pl
"""

channel_inventory2 = spark.sql(channel_inventory2)
channel_inventory2.createOrReplaceTempView("channel_inventory2")


channel_inventory_pl_restated = f"""
SELECT 
    ci2.cal_date,
    country_alpha2,
    ci2.sales_product_number,
    fin_pl as pl,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty
FROM channel_inventory2 ci2
JOIN finance_sys_recorded_pl fpl
    ON fpl.cal_date = ci2.cal_date
    AND fpl.sales_product_number = ci2.sales_product_number
GROUP BY ci2.cal_date, country_alpha2, ci2.sales_product_number, fin_pl
"""

channel_inventory_pl_restated = spark.sql(channel_inventory_pl_restated)
channel_inventory_pl_restated.createOrReplaceTempView("channel_inventory_pl_restated")


tier1_emea_raw = f"""
SELECT
    cal_date,
    st.country_alpha2,
    CASE
        WHEN sales_product_number = '4HY97A' AND pl = 'G0'
        THEN 'E5'
        ELSE pl
    END AS pl,
    sales_product_number,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty
FROM channel_inventory_pl_restated AS st
JOIN mdm.iso_country_code_xref AS geo ON st.country_alpha2 = geo.country_alpha2
WHERE region_3 = 'EMEA'
  AND sell_thru_usd > 0
  AND sell_thru_qty > 0
  AND st.country_alpha2 IN 
    (
        SELECT distinct country_alpha2
        FROM fin_stage.odw_document_currency doc
        WHERE 1=1
            AND revenue <> 0
            AND country_alpha2 <> 'XW'
            AND document_currency_code is not null
            AND region_5 = 'EU'
        GROUP BY country_alpha2
    )
GROUP BY cal_date, st.country_alpha2, pl, sales_product_number
"""

tier1_emea_raw = spark.sql(tier1_emea_raw)
tier1_emea_raw.createOrReplaceTempView("tier1_emea_raw")


emea_st = f"""
SELECT
    cal_date,
    country_alpha2,
    CASE
        WHEN pl = 'GN' THEN 'GJ'
        WHEN pl = 'GO' THEN '5T'
        ELSE pl                
    END AS pl,
    edw.sales_product_number,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty
FROM tier1_emea_raw edw
INNER JOIN mdm.rdma_base_to_sales_product_map rdma ON edw.sales_product_number = rdma.sales_product_number
WHERE pl IN (
        SELECT DISTINCT pl 
        FROM mdm.product_line_xref 
        WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
            AND PL_category IN ('SUP', 'LLC')
            OR pl IN ('IE')
    ) 
    AND (sell_thru_usd != 0
    OR sell_thru_qty != 0)
GROUP BY cal_date, country_alpha2, pl, edw.sales_product_number
"""            

emea_st = spark.sql(emea_st)
emea_st.createOrReplaceTempView("emea_st")


# calc emea st ratios
emea_st_no_country = f"""
SELECT
    cal_date,
    sales_product_number,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty
FROM emea_st
GROUP BY cal_date, sales_product_number    
"""

emea_st_no_country = spark.sql(emea_st_no_country)
emea_st_no_country.createOrReplaceTempView("emea_st_no_country")


emea_salesprod_with_st = f"""
SELECT e.cal_date,
    e.pl,
    e.sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty
FROM salesprod_emea_remove_edw_country AS e
LEFT JOIN emea_st_no_country AS st ON (
    e.cal_date = st.cal_date
    AND e.sales_product_number = st.sales_product_number)
GROUP BY e.cal_date, e.pl, e.sales_product_number        
"""

emea_salesprod_with_st = spark.sql(emea_salesprod_with_st)
emea_salesprod_with_st.createOrReplaceTempView("emea_salesprod_with_st")


emea_st_cntry_x_pl_usd = f"""
SELECT cal_date,
    pl,
    CONCAT(cal_date, pl) AS pl_date,
    country_alpha2,
    SUM(sell_thru_usd) AS sell_thru_usd
FROM emea_st
WHERE sell_thru_usd != 0 
GROUP BY cal_date, pl, CONCAT(cal_date, pl), country_alpha2
"""        

emea_st_cntry_x_pl_usd = spark.sql(emea_st_cntry_x_pl_usd)
emea_st_cntry_x_pl_usd.createOrReplaceTempView("emea_st_cntry_x_pl_usd")


emea_st_cntry_x_pl_qty = f"""
SELECT cal_date,
    pl,
    CONCAT(cal_date, pl) AS pl_date,
    country_alpha2,
    SUM(sell_thru_qty) AS sell_thru_qty
FROM emea_st
WHERE sell_thru_qty != 0 
GROUP BY cal_date, pl, CONCAT(cal_date, pl), country_alpha2
"""        

emea_st_cntry_x_pl_qty = spark.sql(emea_st_cntry_x_pl_qty)
emea_st_cntry_x_pl_qty.createOrReplaceTempView("emea_st_cntry_x_pl_qty")


emea_st_cntry_x_sp_usd = f"""
SELECT cal_date,
    sales_product_number,
    pl,
    CONCAT(cal_date, sales_product_number) AS sp_date,
    country_alpha2,
    SUM(sell_thru_usd) AS sell_thru_usd
FROM emea_st
WHERE sell_thru_usd != 0 
GROUP BY cal_date, sales_product_number, pl, CONCAT(cal_date, sales_product_number), country_alpha2
"""        

emea_st_cntry_x_sp_usd = spark.sql(emea_st_cntry_x_sp_usd)
emea_st_cntry_x_sp_usd.createOrReplaceTempView("emea_st_cntry_x_sp_usd")


emea_st_cntry_x_sp_qty = f"""
SELECT cal_date,
    sales_product_number,
    pl,
    CONCAT(cal_date, sales_product_number) AS sp_date,
    country_alpha2,
    SUM(sell_thru_qty) AS sell_thru_qty
FROM emea_st
WHERE sell_thru_qty != 0 
GROUP BY cal_date, sales_product_number, pl, CONCAT(cal_date, sales_product_number), country_alpha2
"""        

emea_st_cntry_x_sp_qty = spark.sql(emea_st_cntry_x_sp_qty)
emea_st_cntry_x_sp_qty.createOrReplaceTempView("emea_st_cntry_x_sp_qty")


emea_pl_ratio_usd = f"""
SELECT cal_date,
    pl,
    country_alpha2,
    pl_date,
    CASE
        WHEN SUM(sell_thru_usd) OVER(PARTITION BY pl_date) = 0 THEN NULL
        ELSE sell_thru_usd / SUM(sell_thru_usd) OVER(PARTITION BY pl_date)
    END AS pl_cnty_usd_mix
FROM emea_st_cntry_x_pl_usd
WHERE sell_thru_usd != 0
"""        

emea_pl_ratio_usd = spark.sql(emea_pl_ratio_usd)
emea_pl_ratio_usd.createOrReplaceTempView("emea_pl_ratio_usd")


emea_pl_ratio_qty = f"""
SELECT cal_date,
    pl,
    country_alpha2,
    pl_date,
    CASE    
        WHEN SUM(sell_thru_qty) OVER(PARTITION BY pl_date)  = 0 THEN NULL
        ELSE sell_thru_qty / SUM(sell_thru_qty) OVER(PARTITION BY pl_date) 
    END AS pl_cnty_qty_mix
FROM emea_st_cntry_x_pl_qty
WHERE sell_thru_qty != 0
"""

emea_pl_ratio_qty = spark.sql(emea_pl_ratio_qty)
emea_pl_ratio_qty.createOrReplaceTempView("emea_pl_ratio_qty")


emea_pl_ratios = f"""
SELECT cal_date,
    pl, 
    country_alpha2,
    pl_date,
    SUM(pl_cnty_usd_mix) AS pl_cnty_usd_mix,
    0 AS pl_cnty_qty_mix
FROM emea_pl_ratio_usd
GROUP BY cal_date, pl, country_alpha2, pl_date

UNION ALL

SELECT cal_date,
    pl, 
    country_alpha2,
    pl_date,                
    0 AS pl_cnty_usd_mix,
    SUM(pl_cnty_qty_mix) AS pl_cnty_qty_mix
FROM emea_pl_ratio_qty
GROUP BY cal_date, pl, country_alpha2, pl_date
"""

emea_pl_ratios = spark.sql(emea_pl_ratios)
emea_pl_ratios.createOrReplaceTempView("emea_pl_ratios")


emea_pl_ratios2 = f"""
SELECT cal_date,
    pl, 
    country_alpha2,
    pl_date,
    COALESCE(SUM(pl_cnty_usd_mix), 0) AS pl_cnty_usd_mix,
    COALESCE(SUM(pl_cnty_qty_mix), 0) AS pl_cnty_qty_mix
FROM emea_pl_ratios
GROUP BY cal_date, pl, country_alpha2, pl_date        
"""

emea_pl_ratios2 = spark.sql(emea_pl_ratios2)
emea_pl_ratios2.createOrReplaceTempView("emea_pl_ratios2")


# sales product based ratios using sell thru with sales product history available
emea_sp_ratio_usd = f"""
SELECT cal_date,
    pl,
    sales_product_number,
    country_alpha2,
    sp_date,
    CASE
        WHEN SUM(sell_thru_usd) OVER(PARTITION BY sp_date) = 0 THEN NULL
        ELSE sell_thru_usd / SUM(sell_thru_usd) OVER(PARTITION BY sp_date) 
    END AS sp_cnty_usd_mix
FROM emea_st_cntry_x_sp_usd
WHERE sell_thru_usd != 0
"""

emea_sp_ratio_usd = spark.sql(emea_sp_ratio_usd)
emea_sp_ratio_usd.createOrReplaceTempView("emea_sp_ratio_usd")



emea_sp_ratio_qty = f"""
SELECT cal_date,
    pl,
    sales_product_number,
    country_alpha2,
    sp_date,
    CASE    
        WHEN SUM(sell_thru_qty) OVER(PARTITION BY sp_date) = 0 THEN NULL
        ELSE sell_thru_qty / SUM(sell_thru_qty) OVER(PARTITION BY sp_date) 
    END AS sp_cnty_qty_mix
FROM emea_st_cntry_x_sp_qty
WHERE sell_thru_qty != 0
"""

emea_sp_ratio_qty = spark.sql(emea_sp_ratio_qty)
emea_sp_ratio_qty.createOrReplaceTempView("emea_sp_ratio_qty")


emea_sp_ratios_mash0 = f"""
SELECT cal_date,
    pl,
    sales_product_number,
    country_alpha2,
    sp_date,
    COALESCE(SUM(sp_cnty_usd_mix), 0) AS sp_cnty_usd_mix,
    0 AS sp_cnty_qty_mix
FROM emea_sp_ratio_usd
GROUP BY cal_date, pl, sales_product_number, country_alpha2, sp_date

UNION ALL

SELECT cal_date,
    pl,
    sales_product_number,
    country_alpha2,
    sp_date,
    0 AS sp_cnty_usd_mix,
    COALESCE(SUM(sp_cnty_qty_mix), 0) AS sp_cnty_qty_mix
FROM emea_sp_ratio_qty            
GROUP BY cal_date, pl, sales_product_number, country_alpha2, sp_date
"""

emea_sp_ratios_mash0 = spark.sql(emea_sp_ratios_mash0)
emea_sp_ratios_mash0.createOrReplaceTempView("emea_sp_ratios_mash0")


emea_sp_ratios_mash1 = f"""    
SELECT cal_date,
    pl,
    sales_product_number,
    country_alpha2,
    sp_date,
    COALESCE(SUM(sp_cnty_usd_mix), 0) AS sp_cnty_usd_mix,
    COALESCE(SUM(sp_cnty_qty_mix), 0) AS sp_cnty_qty_mix
FROM emea_sp_ratios_mash0
GROUP BY cal_date, pl, sales_product_number, country_alpha2, sp_date
"""

emea_sp_ratios_mash1 = spark.sql(emea_sp_ratios_mash1)
emea_sp_ratios_mash1.createOrReplaceTempView("emea_sp_ratios_mash1")


emea_sp_ratios_mash2 = f"""
SELECT cal_date,
    pl,
    sales_product_number,
    country_alpha2,
    sp_date,
    SUM(sp_cnty_usd_mix) AS sp_cnty_usd_mix,
    SUM(sp_cnty_qty_mix) AS sp_cnty_qty_mix
FROM emea_sp_ratios_mash1
WHERE sp_cnty_usd_mix < 1
GROUP BY cal_date, pl, sales_product_number, country_alpha2, sp_date
"""

emea_sp_ratios_mash2 = spark.sql(emea_sp_ratios_mash2)
emea_sp_ratios_mash2.createOrReplaceTempView("emea_sp_ratios_mash2")


emea_sp_ratios_mash3 = f"""
SELECT cal_date,
    pl,
    sales_product_number,
    country_alpha2,
    sp_date,
    SUM(sp_cnty_usd_mix) AS sp_cnty_usd_mix,
    SUM(sp_cnty_qty_mix) AS sp_cnty_qty_mix
FROM emea_sp_ratios_mash2
WHERE sp_cnty_qty_mix < 1
GROUP BY cal_date, pl, sales_product_number, country_alpha2, sp_date
"""

emea_sp_ratios_mash3 = spark.sql(emea_sp_ratios_mash3)
emea_sp_ratios_mash3.createOrReplaceTempView("emea_sp_ratios_mash3")


emea_sp_ratios_mash4 = f"""
SELECT cal_date,
    pl,
    sales_product_number,
    country_alpha2,
    sp_date,
    SUM(sp_cnty_usd_mix) AS sp_cnty_usd_mix,
    SUM(sp_cnty_qty_mix) AS sp_cnty_qty_mix
FROM emea_sp_ratios_mash3
WHERE sp_cnty_usd_mix > -1
GROUP BY cal_date, pl, sales_product_number, country_alpha2, sp_date
"""    

emea_sp_ratios_mash4 = spark.sql(emea_sp_ratios_mash4)
emea_sp_ratios_mash4.createOrReplaceTempView("emea_sp_ratios_mash4")


emea_sp_ratios_mash5 = f"""
SELECT cal_date,
    pl,
    sales_product_number,
    country_alpha2,
    sp_date,
    SUM(sp_cnty_usd_mix) AS sp_cnty_usd_mix,
    SUM(sp_cnty_qty_mix) AS sp_cnty_qty_mix
FROM emea_sp_ratios_mash4
WHERE sp_cnty_qty_mix > -1
GROUP BY cal_date, pl, sales_product_number, country_alpha2, sp_date
"""        
    
emea_sp_ratios_mash5 = spark.sql(emea_sp_ratios_mash5)
emea_sp_ratios_mash5.createOrReplaceTempView("emea_sp_ratios_mash5")


emea_sp_ratios = f"""
SELECT cal_date,
    pl,
    sales_product_number,
    country_alpha2,
    sp_date,
    SUM(sp_cnty_usd_mix) AS sp_cnty_usd_mix,
    SUM(sp_cnty_qty_mix) AS sp_cnty_qty_mix
FROM emea_sp_ratios_mash5
GROUP BY cal_date, pl, sales_product_number, country_alpha2, sp_date
"""

emea_sp_ratios = spark.sql(emea_sp_ratios)
emea_sp_ratios.createOrReplaceTempView("emea_sp_ratios")


emea_actuals_add_country_mash_columns = f"""
SELECT cal_date,
    pl, 
    sales_product_number, 
    CONCAT(cal_date, pl) AS pl_date,
    CONCAT(cal_date, sales_product_number) AS sp_date,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units,
    COALESCE(SUM(sell_thru_usd), 0) AS sell_thru_usd,
    COALESCE(SUM(sell_thru_qty), 0) AS sell_thru_qty
FROM emea_salesprod_with_st
GROUP BY cal_date, sales_product_number, pl, CONCAT(cal_date, pl), CONCAT(cal_date, sales_product_number)
"""        

emea_actuals_add_country_mash_columns = spark.sql(emea_actuals_add_country_mash_columns)
emea_actuals_add_country_mash_columns.createOrReplaceTempView("emea_actuals_add_country_mash_columns")


emea_salesprod_by_country1a = f"""
SELECT e.cal_date,
    country_alpha2,
    e.pl,
    sales_product_number,
    e.pl_date,
    e.sp_date,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty,
    COALESCE(SUM(pl_cnty_usd_mix), 1) AS pl_cnty_usd_mix,
    COALESCE(SUM(pl_cnty_qty_mix), 1) AS pl_cnty_qty_mix
FROM emea_actuals_add_country_mash_columns AS e
LEFT JOIN emea_pl_ratios2 AS t2pl ON (e.cal_date = t2pl.cal_date AND e.pl = t2pl.pl AND e.pl_date = t2pl.pl_date)
GROUP BY e.cal_date, country_alpha2, e.pl, sales_product_number, e.pl_date, e.sp_date
"""        

emea_salesprod_by_country1a = spark.sql(emea_salesprod_by_country1a)
emea_salesprod_by_country1a.createOrReplaceTempView("emea_salesprod_by_country1a")


emea_salesprod_by_country1b = f"""
SELECT cal_date,
    CASE
        WHEN country_alpha2 IS NULL THEN 'XA'
        ELSE country_alpha2
    END AS country_alpha2,
    pl,
    sales_product_number,
    pl_date,
    sp_date,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty,
    SUM(pl_cnty_usd_mix) AS pl_cnty_usd_mix,
    SUM(pl_cnty_qty_mix) AS pl_cnty_qty_mix
FROM emea_salesprod_by_country1a
GROUP BY cal_date, country_alpha2, pl, sales_product_number, pl_date, sp_date
"""

emea_salesprod_by_country1b = spark.sql(emea_salesprod_by_country1b)
emea_salesprod_by_country1b.createOrReplaceTempView("emea_salesprod_by_country1b")


emea_salesprod_by_country4_table = f"""
SELECT e.cal_date,
    e.country_alpha2,
    e.pl,
    e.sales_product_number,
    e.pl_date,
    e.sp_date,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty,
    SUM(pl_cnty_usd_mix) AS pl_cnty_usd_mix,
    SUM(pl_cnty_qty_mix) AS pl_cnty_qty_mix,
    SUM(sp_cnty_usd_mix) AS sp_cnty_usd_mix,
    SUM(sp_cnty_qty_mix) AS sp_cnty_qty_mix
FROM emea_salesprod_by_country1b AS e
LEFT JOIN emea_sp_ratios AS t2 ON (e.cal_date = t2.cal_date AND e.sales_product_number = t2.sales_product_number 
    AND e.sp_date = t2.sp_date AND e.country_alpha2 = t2.country_alpha2)
GROUP BY e.cal_date, e.country_alpha2, e.pl, e.sales_product_number, e.pl_date, e.sp_date
"""        

emea_salesprod_by_country4_table = spark.sql(emea_salesprod_by_country4_table)
emea_salesprod_by_country4_table.createOrReplaceTempView("emea_salesprod_by_country4_table")

emea_salesprod_by_country3_sp_date = f"""
SELECT e.cal_date,
    e.country_alpha2,
    e.pl,
    e.sales_product_number,
    e.pl_date,
    e.sp_date,
    SUM(gross_revenue * sp_cnty_usd_mix) AS gross_revenue,
    SUM(net_currency *  sp_cnty_usd_mix) AS net_currency,
    SUM(contractual_discounts *  sp_cnty_usd_mix) AS contractual_discounts,
    SUM(discretionary_discounts *  sp_cnty_usd_mix) AS discretionary_discounts,
    SUM(warranty *  sp_cnty_usd_mix) AS warranty,
    SUM(other_cos * sp_cnty_usd_mix) AS other_cos,
    SUM(total_cos *  sp_cnty_usd_mix) AS total_cos,
    SUM(revenue_units *  sp_cnty_qty_mix) AS revenue_units
FROM emea_salesprod_by_country4_table AS e
WHERE sp_cnty_usd_mix IS NOT NULL AND sp_cnty_qty_mix IS NOT NULL
GROUP BY e.cal_date, e.country_alpha2, e.pl, e.sales_product_number, e.pl_date, e.sp_date
"""

emea_salesprod_by_country3_sp_date = spark.sql(emea_salesprod_by_country3_sp_date)
emea_salesprod_by_country3_sp_date.createOrReplaceTempView("emea_salesprod_by_country3_sp_date")

# row count fail here::  1122215 v 1096357
emea_salesprod_by_country4_pl_date = f"""
SELECT e.cal_date,
    e.country_alpha2,
    e.pl,
    e.sales_product_number,
    e.pl_date,
    e.sp_date,
    SUM(gross_revenue * pl_cnty_usd_mix) AS gross_revenue,
    SUM(net_currency *  pl_cnty_usd_mix) AS net_currency,
    SUM(contractual_discounts *  pl_cnty_usd_mix) AS contractual_discounts,
    SUM(discretionary_discounts *  pl_cnty_usd_mix) AS discretionary_discounts,
    SUM(warranty *  pl_cnty_usd_mix) AS warranty,
    SUM(other_cos * pl_cnty_usd_mix) AS other_cos,
    SUM(total_cos *  pl_cnty_usd_mix) AS total_cos,
    SUM(revenue_units *  pl_cnty_qty_mix) AS revenue_units
FROM emea_salesprod_by_country4_table AS e
WHERE sp_date NOT IN (SELECT DISTINCT sp_date FROM emea_salesprod_by_country3_sp_date)
GROUP BY e.cal_date, e.country_alpha2, e.pl, e.sales_product_number, e.pl_date, e.sp_date
"""

emea_salesprod_by_country4_pl_date = spark.sql(emea_salesprod_by_country4_pl_date)
emea_salesprod_by_country4_pl_date.createOrReplaceTempView("emea_salesprod_by_country4_pl_date")


emea_country_by_salesprod = f"""
SELECT cal_date,
    country_alpha2,
    pl, 
    sales_product_number, 
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_COS), 0) AS total_COS,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM emea_salesprod_by_country3_sp_date
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

emea_country_by_salesprod = spark.sql(emea_country_by_salesprod)
emea_country_by_salesprod.createOrReplaceTempView("emea_country_by_salesprod")


emea_country_by_product_line = f"""
SELECT cal_date,
    country_alpha2,
    pl, 
    sales_product_number, 
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM emea_salesprod_by_country4_pl_date
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

emea_country_by_product_line = spark.sql(emea_country_by_product_line)
emea_country_by_product_line.createOrReplaceTempView("emea_country_by_product_line")


all_emea_salesprod_country = f"""
SELECT * FROM emea_country_by_salesprod

UNION ALL

SELECT * FROM emea_country_by_product_line
"""

all_emea_salesprod_country = spark.sql(all_emea_salesprod_country)
all_emea_salesprod_country.createOrReplaceTempView("all_emea_salesprod_country")


#join back EMEA with ROW 

supplies_salesprod2 = f"""
SELECT 
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM odw_supplies_combined_findata
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

supplies_salesprod2 = spark.sql(supplies_salesprod2)
supplies_salesprod2.createOrReplaceTempView("supplies_salesprod2")


salesprod_row = f""" 
SELECT cal_date,
    sup.country_alpha2,
    pl,
    sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_salesprod2 AS sup
JOIN mdm.iso_country_code_xref AS geo ON sup.country_alpha2 = geo.country_alpha2
WHERE region_3 != 'EMEA'
GROUP BY cal_date, sup.country_alpha2, pl, sales_product_number        
"""

salesprod_row = spark.sql(salesprod_row)
salesprod_row.createOrReplaceTempView("salesprod_row")


all_salesprod_country = f"""
SELECT * FROM all_emea_salesprod_country

UNION ALL

SELECT * FROM salesprod_row
"""

all_salesprod_country = spark.sql(all_salesprod_country)
all_salesprod_country.createOrReplaceTempView("all_salesprod_country")


supplies_findata_emea_adjusted = f"""
SELECT 
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM all_salesprod_country
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

supplies_findata_emea_adjusted = spark.sql(supplies_findata_emea_adjusted)
supplies_findata_emea_adjusted.createOrReplaceTempView("supplies_findata_emea_adjusted")

# COMMAND ----------

#additional clean up the odw data after pushing emea to country; in preparation for adding the additional info
all_salesprod_region5 = f"""
SELECT
    cal_date,
    sp.country_alpha2,
    region_5,
    pl,
    sales_product_number,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units),0) AS revenue_units
FROM supplies_findata_emea_adjusted AS sp 
JOIN mdm.iso_country_code_xref AS geo ON (sp.country_alpha2 = geo.country_alpha2)
WHERE region_5 IN ('AP', 'EU', 'JP', 'LA', 'NA', 'XW')
GROUP BY cal_date, sp.country_alpha2, pl, sales_product_number, region_5
"""

all_salesprod_region5 = spark.sql(all_salesprod_region5)
all_salesprod_region5.createOrReplaceTempView("all_salesprod_region5")


odw_salesprod_all_cleaned = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM all_salesprod_region5
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number
"""

odw_salesprod_all_cleaned = spark.sql(odw_salesprod_all_cleaned)
odw_salesprod_all_cleaned.createOrReplaceTempView("odw_salesprod_all_cleaned")

# COMMAND ----------

#write out salesprod_all_cleaned2 to a delta table target.
odw_salesprod_all_cleaned.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/odw_salesprod_all_cleaned")

#create the table
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.odw_salesprod_all_cleaned USING DELTA LOCATION '/tmp/delta/fin_stage/odw_salesprod_all_cleaned'")

# COMMAND ----------

spark.table("fin_stage.odw_salesprod_all_cleaned").createOrReplaceTempView("odw_salesprod_all_cleaned")

# COMMAND ----------

# ADD DETAIL TO FINANCIAL TRANSACTIONS WHERE EDW SOURCE SYSTEMS OBSCURE IT

# COMMAND ----------

# mps data
mps_time_series_data1 = f"""
SELECT 
    cal.Date AS cal_date,
    country,
    direct_or_indirect_sf AS ce_split,
    Product_Nbr AS sales_product_number,
    Prod_Line AS pl,
    category,
    SUM(Shipped_Qty) AS shipped_qty
FROM fin_stage.mps_ww_shipped_supply_staging AS mps
JOIN mdm.calendar AS cal ON mps.Month = cal.Date
WHERE Prod_Line IN 
    (
    SELECT DISTINCT pl 
    FROM mdm.product_line_xref 
    WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
        AND PL_category IN ('SUP', 'LLC') 
    -- excludes GD in case there are any; GD has a different business model; would not expect GD volumes from mps
        AND pl NOT IN ('GD') -- would we expect there to be GY or LZ units for mps?
        OR PL IN ('GO', 'GN', 'IE')
    ) 
    AND Fiscal_Yr > '2015' AND Day_of_Month = 1
    AND cal.Date > '2021-10-01'
GROUP BY cal.Date, country, direct_or_indirect_sf, Product_Nbr, Prod_Line, category
"""

mps_time_series_data1 = spark.sql(mps_time_series_data1)
mps_time_series_data1.createOrReplaceTempView("mps_time_series_data1")


# mps processing
mps_shipped_qty2 = f"""
SELECT
    cal_date,
    country,
    ce_split,
    sales_product_number,
    pl,
    SUM(shipped_qty) AS shipped_qty
FROM mps_time_series_data1
GROUP BY cal_date, country, ce_split, sales_product_number, pl        
"""

mps_shipped_qty2 = spark.sql(mps_shipped_qty2)
mps_shipped_qty2.createOrReplaceTempView("mps_shipped_qty2")


mps_data_add_country_alpha = f"""
SELECT
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,
    ce_split,
    COALESCE(SUM(shipped_qty), 0) AS shipped_qty
FROM mps_shipped_qty2 AS mps     
JOIN mdm.iso_country_code_xref AS geo ON mps.country = geo.country
GROUP BY cal_date, country_alpha2, ce_split, sales_product_number, pl
"""

mps_data_add_country_alpha = spark.sql(mps_data_add_country_alpha)
mps_data_add_country_alpha.createOrReplaceTempView("mps_data_add_country_alpha")


mps_indirect = f"""
SELECT
    cal_date,
    country_alpha2,
    sales_product_number,
    SUM(shipped_qty) AS indirect_units
FROM mps_data_add_country_alpha
WHERE ce_split = 'EST_INDIRECT_FULFILLMENT'
and shipped_qty > 0
GROUP BY cal_date, country_alpha2, sales_product_number
"""

mps_indirect = spark.sql(mps_indirect)
mps_indirect.createOrReplaceTempView("mps_indirect")


mps_direct = f"""
SELECT
    cal_date,
    country_alpha2,
    sales_product_number,
    SUM(shipped_qty) AS direct_units
FROM mps_data_add_country_alpha
WHERE ce_split = 'EST_DIRECT_FULFILLMENT'
AND shipped_qty > 0
GROUP BY cal_date, country_alpha2, sales_product_number
"""

mps_direct = spark.sql(mps_direct)
mps_direct.createOrReplaceTempView("mps_direct")


# add mps data to the supplies sales product data

supplies_salesprod3 = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM odw_salesprod_all_cleaned
WHERE revenue_units > 0
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

supplies_salesprod3 = spark.sql(supplies_salesprod3)
supplies_salesprod3.createOrReplaceTempView("supplies_salesprod3")


supplies_salesprod3a_zero_below_units = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM odw_salesprod_all_cleaned
where revenue_units <= 0
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

supplies_salesprod3a_zero_below_units = spark.sql(supplies_salesprod3a_zero_below_units)
supplies_salesprod3a_zero_below_units.createOrReplaceTempView("supplies_salesprod3a_zero_below_units")


supplies_join_mps_indirect = f"""
SELECT sup.cal_date,
    sup.country_alpha2,
    sup.pl, 
    sup.sales_product_number,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts),0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units,
    COALESCE(SUM(indirect_units), 0) AS indirect_units
FROM supplies_salesprod3 AS sup
LEFT JOIN mps_indirect AS mps ON (sup.cal_date = mps.cal_date AND sup.country_alpha2 = mps.country_alpha2
        AND sup.sales_product_number = mps.sales_product_number)
GROUP BY sup.cal_date, sup.country_alpha2, sup.pl, sup.sales_product_number
"""

supplies_join_mps_indirect = spark.sql(supplies_join_mps_indirect)
supplies_join_mps_indirect.createOrReplaceTempView("supplies_join_mps_indirect")


supplies_join_mps_direct = f"""
SELECT sup.cal_date,
    sup.country_alpha2,
    sup.pl, 
    sup.sales_product_number,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts),0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units,
    COALESCE(SUM(indirect_units), 0) AS indirect_units,
    COALESCE(SUM(direct_units), 0) AS direct_units
FROM supplies_join_mps_indirect AS sup
LEFT JOIN mps_direct AS mps ON (sup.cal_date = mps.cal_date AND sup.country_alpha2 = mps.country_alpha2
        AND sup.sales_product_number = mps.sales_product_number)
GROUP BY sup.cal_date, sup.country_alpha2, sup.pl, sup.sales_product_number
"""

supplies_join_mps_direct = spark.sql(supplies_join_mps_direct)
supplies_join_mps_direct.createOrReplaceTempView("supplies_join_mps_direct")


date_helper = f"""
SELECT date_key, 
 Date as cal_date
FROM mdm.calendar
WHERE day_of_month = 1
"""

date_helper = spark.sql(date_helper)
date_helper.createOrReplaceTempView("date_helper")


supplies_mps_analytic_setup = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    gross_revenue,
    net_currency,
    contractual_discounts,
    discretionary_discounts,
    warranty,
    other_cos,
    total_cos,
    revenue_units,
    indirect_units,
    direct_units,
        (select min(cal_date) from odw_salesprod_all_cleaned) as min_cal_date,
        (select max(cal_date) from odw_salesprod_all_cleaned) as max_cal_date
FROM supplies_join_mps_direct
"""

supplies_mps_analytic_setup = spark.sql(supplies_mps_analytic_setup)
supplies_mps_analytic_setup.createOrReplaceTempView("supplies_mps_analytic_setup")


supplies_mps_full_calendar_cross_join = f"""
SELECT distinct d.cal_date,
    country_alpha2,
    pl,
    sales_product_number
FROM date_helper d
CROSS JOIN supplies_mps_analytic_setup mps
WHERE d.cal_date between mps.min_cal_date and mps.max_cal_date
"""

supplies_mps_full_calendar_cross_join = spark.sql(supplies_mps_full_calendar_cross_join)
supplies_mps_full_calendar_cross_join.createOrReplaceTempView("supplies_mps_full_calendar_cross_join")


fill_gap1 = f"""
SELECT
    mps.cal_date,
    coalesce(mps.country_alpha2, setup.country_alpha2) as country_alpha2,
    coalesce(mps.pl, setup.pl) as pl,
    coalesce(mps.sales_product_number, setup.sales_product_number) as sales_product_number,
    gross_revenue,
    net_currency,
    contractual_discounts,
    discretionary_discounts,
    warranty,
    other_cos,
    total_cos,
    revenue_units,
    indirect_units,
    direct_units
from supplies_mps_full_calendar_cross_join mps
left join supplies_mps_analytic_setup setup on
    mps.cal_date = setup.cal_date and
    mps.country_alpha2 = setup.country_alpha2 and
    mps.pl = setup.pl and
    mps.sales_product_number = setup.sales_product_number
"""

fill_gap1 = spark.sql(fill_gap1)
fill_gap1.createOrReplaceTempView("fill_gap1")


fill_gap2 = f"""
select 
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    gross_revenue,
    net_currency,
    contractual_discounts,
    discretionary_discounts,
    warranty,
    other_cos,
    total_cos,
    revenue_units,
    indirect_units,
    direct_units,
        min(cal_date) over (partition by sales_product_number, pl, country_alpha2 order by cal_date) as min_cal_date
from fill_gap1
"""

fill_gap2 = spark.sql(fill_gap2)
fill_gap2.createOrReplaceTempView("fill_gap2")


supplies_mps_data_time_series = f"""
select 
    cal_date,
    min_cal_date,
        country_alpha2,
        pl,
        sales_product_number,
        gross_revenue,
        net_currency,
        contractual_discounts,
        discretionary_discounts,
        warranty,
        other_cos,
        total_cos,
        revenue_units,
        indirect_units,
        direct_units,
        --CASE WHEN min_cal_date < CAST(GETDATE() - DAY(GETDATE()) + 1 AS DATE) THEN 1 ELSE 0 END as actuals_flag,
        COUNT(cal_date) OVER (PARTITION BY sales_product_number, pl, country_alpha2 
            ORDER BY cal_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_count
from fill_gap2
"""

supplies_mps_data_time_series = spark.sql(supplies_mps_data_time_series)
supplies_mps_data_time_series.createOrReplaceTempView("supplies_mps_data_time_series")


supplies_mps_full_calendar = f"""
select
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts),0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,    
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units,
    COALESCE(SUM(indirect_units), 0) AS indirect_units,
    COALESCE(SUM(direct_units), 0) AS direct_units
FROM fill_gap2
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

supplies_mps_full_calendar = spark.sql(supplies_mps_full_calendar)
supplies_mps_full_calendar.createOrReplaceTempView("supplies_mps_full_calendar")


mps_data_lagged = f"""
SELECT
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
    LAG(COALESCE(SUM(indirect_units), 0), 1) OVER (PARTITION BY sales_product_number, country_alpha2, pl ORDER BY cal_date) AS lagged_indirect_ships,
    LAG(COALESCE(SUM(direct_units), 0), 1) OVER (PARTITION BY sales_product_number, country_alpha2, pl ORDER BY cal_date) AS lagged_direct_ships
FROM supplies_mps_full_calendar
GROUP BY cal_date, country_alpha2, pl, sales_product_number, indirect_units, direct_units
"""

mps_data_lagged = spark.sql(mps_data_lagged)
mps_data_lagged.createOrReplaceTempView("mps_data_lagged")


mps_data_average = f"""
SELECT 
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,                
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
    AVG(lagged_indirect_ships) OVER 
        (PARTITION BY sales_product_number, pl, country_alpha2 ORDER BY cal_date ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS indirect_ships,
    AVG(lagged_direct_ships) OVER 
        (PARTITION BY sales_product_number, pl, country_alpha2 ORDER BY cal_date ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS direct_ships
FROM mps_data_lagged
GROUP BY cal_date, pl, country_alpha2, sales_product_number, lagged_indirect_ships, lagged_direct_ships
"""

mps_data_average = spark.sql(mps_data_average)
mps_data_average.createOrReplaceTempView("mps_data_average")


salesprod_pre_rtm = f"""
SELECT 
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,
    CASE 
        WHEN SUM(revenue_units) >= 0 AND SUM(indirect_ships) >= 0
        THEN 'Y'
    ELSE 'N'
    END AS rboth_positive,
    CASE 
        WHEN SUM(revenue_units) < 0 AND SUM(indirect_ships) < 0
        THEN 'Y'
    ELSE 'N'
    END AS rboth_negative,
    CASE 
        WHEN SUM(revenue_units) > SUM(indirect_ships)
        THEN 'Y'
    ELSE 'N'
    END AS edw_greater_than_pmps,                
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
    SUM(indirect_ships) AS indirect_ships,
    SUM(direct_ships) AS direct_ships
FROM mps_data_average
GROUP BY cal_date, pl, country_alpha2, sales_product_number
"""

salesprod_pre_rtm = spark.sql(salesprod_pre_rtm)
salesprod_pre_rtm.createOrReplaceTempView("salesprod_pre_rtm")



salesprod_pre_rtm2 = f"""
SELECT 
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,
    rboth_positive,
    rboth_negative,
    edw_greater_than_pmps,                
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
    SUM(indirect_ships) AS indirect_ships,
    SUM(direct_ships) AS direct_ships,
    CASE
        WHEN SUM(indirect_ships) = 0 THEN SUM(revenue_units)
        WHEN rboth_positive = 'Y' AND edw_greater_than_pmps = 'Y'
        THEN SUM(revenue_units) - SUM(indirect_ships)
        WHEN rboth_positive = 'Y' AND edw_greater_than_pmps = 'N'
        THEN 0
        WHEN rboth_negative = 'Y' AND edw_greater_than_pmps = 'Y'
        THEN 0
        WHEN rboth_negative = 'Y' AND edw_greater_than_pmps = 'N'
        THEN SUM(revenue_units) - SUM(indirect_ships)
    ELSE SUM(revenue_units)
    END AS trade_units,
    CASE 
        WHEN SUM(indirect_ships) = 0 THEN 0
        WHEN rboth_positive = 'Y' AND edw_greater_than_pmps = 'Y'
        THEN SUM(indirect_ships)
        WHEN rboth_positive = 'Y' AND edw_greater_than_pmps = 'N'
        THEN SUM(revenue_units)
        WHEN rboth_negative = 'Y' AND edw_greater_than_pmps = 'Y'
        THEN SUM(revenue_units)
        WHEN rboth_negative = 'Y' AND edw_greater_than_pmps = 'N'
        THEN SUM(indirect_ships)
    ELSE 0
    END AS indirect_ships_adjusted
FROM salesprod_pre_rtm
GROUP BY cal_date, pl, country_alpha2, sales_product_number, rboth_negative, rboth_positive, edw_greater_than_pmps
"""

salesprod_pre_rtm2 = spark.sql(salesprod_pre_rtm2)
salesprod_pre_rtm2.createOrReplaceTempView("salesprod_pre_rtm2")


salesprod_pre_rtm2_cleaned = f"""
SELECT 
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,            
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,                
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units,
    COALESCE(SUM(indirect_ships), 0) AS indirect_ships,
    COALESCE(SUM(direct_ships), 0) AS direct_ships,
    COALESCE(SUM(trade_units), 0) AS trade_units,
    COALESCE(SUM(indirect_ships_adjusted), 0) AS indirect_ships_adjusted,
    SUM(indirect_ships_adjusted) + SUM(trade_units) AS sales_quantity
FROM salesprod_pre_rtm2
GROUP BY cal_date, pl, country_alpha2, sales_product_number

UNION ALL

SELECT
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,        
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
    0 as indirect_ships,
    0 as direct_ships,
    SUM(revenue_units) as trade_units,
    0 as indirect_ships_adjusted,
    SUM(revenue_units) as sales_quantity
FROM supplies_salesprod3a_zero_below_units
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

salesprod_pre_rtm2_cleaned = spark.sql(salesprod_pre_rtm2_cleaned)
salesprod_pre_rtm2_cleaned.createOrReplaceTempView("salesprod_pre_rtm2_cleaned")


supplies_salesprod_rtm1= f"""
SELECT 
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,                
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,    
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units,
    COALESCE(SUM(indirect_ships), 0) AS indirect_ships,
    COALESCE(SUM(direct_ships), 0) AS direct_ships,
    COALESCE(SUM(trade_units), 0) AS trade_units,
    COALESCE(SUM(indirect_ships_adjusted), 0) AS indirect_ships_adjusted,
    COALESCE(SUM(sales_quantity), 0) AS sales_quantity
FROM salesprod_pre_rtm2_cleaned
GROUP BY cal_date, pl, country_alpha2, sales_product_number
"""

supplies_salesprod_rtm1 = spark.sql(supplies_salesprod_rtm1)
supplies_salesprod_rtm1.createOrReplaceTempView("supplies_salesprod_rtm1")


salesprod_pre_rtm2_cleaned2 = f"""
SELECT 
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,            
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units,
    COALESCE(SUM(indirect_ships), 0) AS indirect_ships,
    COALESCE(SUM(direct_ships), 0) AS direct_ships,
    COALESCE(SUM(trade_units), 0) AS trade_units,
    COALESCE(SUM(indirect_ships_adjusted), 0) AS indirect_ships_adjusted,
    COALESCE(SUM(sales_quantity), 0) AS sales_quantity
FROM supplies_salesprod_rtm1
WHERE pl != 'GD'
GROUP BY cal_date, pl, country_alpha2, sales_product_number
"""

salesprod_pre_rtm2_cleaned2 = spark.sql(salesprod_pre_rtm2_cleaned2)
salesprod_pre_rtm2_cleaned2.createOrReplaceTempView("salesprod_pre_rtm2_cleaned2")


salesprod_pre_rtm3 = f"""
SELECT 
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,                
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,            
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
    SUM(trade_units) AS trade_units,
    SUM(indirect_ships_adjusted) AS indirect_ships_adjusted,
    SUM(direct_ships) AS direct_ships,
    SUM(sales_quantity) AS sales_quantity,
    CASE
        WHEN COALESCE(SUM(gross_revenue) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN SUM(gross_revenue)
        ELSE SUM(gross_revenue) / SUM(sales_quantity) * SUM(trade_units)
    END AS trade_gross_rev,
    CASE
        WHEN COALESCE(SUM(gross_revenue) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN 0
        ELSE SUM(gross_revenue) / SUM(sales_quantity) * SUM(indirect_ships_adjusted)
    END AS indirect_gross_rev,
    CASE
        WHEN COALESCE(SUM(contractual_discounts) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN SUM(contractual_discounts)
        ELSE SUM(contractual_discounts) / SUM(sales_quantity) * SUM(trade_units)
    END AS trade_contractual_discounts,
    CASE
        WHEN COALESCE(SUM(contractual_discounts) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN 0
        ELSE SUM(contractual_discounts) / SUM(sales_quantity) * SUM(indirect_ships_adjusted)
    END AS indirect_contractual_discounts,
    CASE
        WHEN COALESCE(SUM(discretionary_discounts) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN SUM(discretionary_discounts)
        ELSE SUM(discretionary_discounts) / SUM(sales_quantity) * SUM(trade_units)
    END AS trade_discretionary_discounts,
    CASE
        WHEN COALESCE(SUM(discretionary_discounts) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN 0
        ELSE SUM(discretionary_discounts) / SUM(sales_quantity) * SUM(indirect_ships_adjusted)
    END AS indirect_discretionary_discounts,
    CASE
        WHEN COALESCE(SUM(warranty) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN SUM(warranty)
        ELSE SUM(warranty) / SUM(sales_quantity) * SUM(trade_units)
    END AS trade_warranty,
    CASE
        WHEN COALESCE(SUM(warranty) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN 0
        ELSE SUM(warranty) / SUM(sales_quantity) * SUM(indirect_ships_adjusted)
    END AS indirect_warranty,
    CASE
        WHEN COALESCE(SUM(other_cos) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN SUM(other_cos)
        ELSE SUM(other_cos) / SUM(sales_quantity) * SUM(trade_units)
    END AS trade_other_cos,
    CASE
        WHEN COALESCE(SUM(other_cos) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN 0
        ELSE SUM(other_cos) / SUM(sales_quantity) * SUM(indirect_ships_adjusted)
    END AS indirect_other_cos,
   CASE
        WHEN COALESCE(SUM(total_cos) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN SUM(total_cos)
        ELSE SUM(total_cos) / SUM(sales_quantity) * SUM(trade_units)
    END AS trade_cos,
    CASE
        WHEN COALESCE(SUM(total_cos) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN 0
        ELSE SUM(total_cos) / SUM(sales_quantity) * SUM(indirect_ships_adjusted)
    END AS indirect_cos,
    CASE
        WHEN COALESCE(SUM(net_currency) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN SUM(net_currency)
        ELSE SUM(net_currency) / SUM(sales_quantity) * SUM(trade_units)
    END AS trade_currency,
    CASE
        WHEN COALESCE(SUM(net_currency) / NULLIF(SUM(sales_quantity), 0), 0) = 0
        THEN 0
        ELSE SUM(net_currency) / SUM(sales_quantity) * SUM(indirect_ships_adjusted)
    END AS indirect_currency            
FROM salesprod_pre_rtm2_cleaned2
GROUP BY cal_date, pl, country_alpha2, sales_product_number
"""

salesprod_pre_rtm3 = spark.sql(salesprod_pre_rtm3)
salesprod_pre_rtm3.createOrReplaceTempView("salesprod_pre_rtm3")


trade_salesprod = f"""
SELECT
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,
    'TRAD' AS ce_split,                
    SUM(trade_gross_rev) AS gross_revenue,
    SUM(trade_currency) AS net_currency,
    SUM(trade_contractual_discounts) AS contractual_discounts,
    SUM(trade_discretionary_discounts) AS discretionary_discounts,
    SUM(trade_warranty) AS warranty,
    SUM(trade_other_cos) AS other_cos,
    SUM(trade_cos) AS total_cos,
    SUM(trade_units) AS revenue_units
FROM salesprod_pre_rtm3
GROUP BY cal_date, pl, country_alpha2, sales_product_number
"""

trade_salesprod = spark.sql(trade_salesprod)
trade_salesprod.createOrReplaceTempView("trade_salesprod")


indirect_salesprod = f"""
SELECT
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,
    'EST_INDIRECT_FULFILLMENT' AS ce_split,                
    SUM(indirect_gross_rev) AS gross_revenue,
    SUM(indirect_currency) AS net_currency,
    SUM(indirect_contractual_discounts) AS contractual_discounts,
    SUM(indirect_discretionary_discounts) AS discretionary_discounts,
    SUM(indirect_warranty) AS warranty,
    SUM(indirect_other_cos) AS other_cos,
    SUM(indirect_cos) AS total_cos,
    SUM(indirect_ships_adjusted) AS revenue_units
FROM salesprod_pre_rtm3
GROUP BY cal_date, pl, country_alpha2, sales_product_number
"""

indirect_salesprod = spark.sql(indirect_salesprod)
indirect_salesprod.createOrReplaceTempView("indirect_salesprod")

direct_salesprod = f"""
SELECT
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,
    'EST_DIRECT_FULFILLMENT' AS ce_split,                
    0 AS gross_revenue,
    0 AS net_currency,
    0 AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS warranty,    
    0 AS other_cos,
    0 AS total_cos,
    SUM(direct_ships) AS revenue_units
FROM salesprod_pre_rtm3
WHERE direct_ships != 0
GROUP BY cal_date, pl, country_alpha2, sales_product_number
"""

direct_salesprod = spark.sql(direct_salesprod)
direct_salesprod.createOrReplaceTempView("direct_salesprod")


salesprod_with_ce_splits = f"""
SELECT *
FROM trade_salesprod

UNION ALL

SELECT *
FROM indirect_salesprod

UNION ALL

SELECT *
FROM direct_salesprod
"""

salesprod_with_ce_splits = spark.sql(salesprod_with_ce_splits)
salesprod_with_ce_splits.createOrReplaceTempView("salesprod_with_ce_splits")


salesprod_join_mps_data = f"""
SELECT
    cal_date,
    pl,
    country_alpha2,
    sales_product_number,
    ce_split,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM salesprod_with_ce_splits
GROUP BY cal_date, pl, country_alpha2, sales_product_number, ce_split
"""

salesprod_join_mps_data = spark.sql(salesprod_join_mps_data)
salesprod_join_mps_data.createOrReplaceTempView("salesprod_join_mps_data")

# COMMAND ----------

# ADDING IN THE OFFLINE MCODES

# COMMAND ----------

# add the iink units as provided by iink team for PL GD (units only)
# 1. start with iink units in ie2_edw

iink_units = f"""
SELECT cal_date,
    CASE
        WHEN iink.country = 'UNKNOWN AP' THEN 'XI'
        WHEN iink.country = 'UNKNOWN EMEA' THEN 'XA'
        WHEN iink.country = 'CANADA' THEN 'CA'
    ELSE 'US'
    END AS country_alpha2,
    pl, 
    sales_product_number,
    'I-INK' AS ce_split,
    0 AS gross_revenue,
    0 AS net_currency,
    0 AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS warranty,
    0 AS other_cos,
    0 AS total_cos,
    SUM(sales_quantity) AS revenue_units
FROM fin_stage.supplies_iink_units_landing AS iink
LEFT JOIN mdm.iso_country_code_xref AS c ON iink.country = c.country
LEFT JOIN mdm.calendar AS cal ON cal_Date = cal.Date
WHERE Fiscal_Yr > 2015 AND sales_quantity != 0 AND Day_of_Month = 1
AND cal_date > '2021-10-01'
GROUP BY cal_date, iink.country, pl, sales_product_number
"""

iink_units = spark.sql(iink_units)
iink_units.createOrReplaceTempView("iink_units")


# 2. add region_5 so we can pull in salesprod :: printer using supplies hw map
iink_region_5 = f"""
SELECT
    cal_Date,
    iink.country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    sum(gross_revenue) as gross_revenue,
    sum(net_currency) as net_currency,
    sum(contractual_discounts) as contractual_discounts,
    SUM(discretionary_discounts) as discretionary_discounts,
    sum(warranty) as warranty,
    SUM(other_cos) as other_cos,
    sum(total_cos) as total_cos,
    sum(revenue_units) as revenue_units
FROM iink_units iink
LEFT JOIN mdm.iso_country_code_xref iso ON iink.country_alpha2 = iso.country_alpha2
WHERE 1=1
GROUP BY cal_date, iink.country_alpha2, pl, sales_product_number, ce_split, region_5
"""        

iink_region_5 = spark.sql(iink_region_5)
iink_region_5.createOrReplaceTempView("iink_region_5")


#3. divide between AMS, which has country detail, and ROW which does not
iink_ams = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl, 
    sales_product_number,
    ce_split,
    sum(gross_revenue) as gross_revenue,
    sum(net_currency) as net_currency,
    sum(contractual_discounts) as contractual_discounts,
    SUM(discretionary_discounts) as discretionary_discounts,
    sum(warranty) as warranty,
    SUM(other_cos) as other_cos,
    sum(total_cos) as total_cos,
    sum(revenue_units) as revenue_units
FROM iink_region_5 
WHERE 1=1
    AND region_5 NOT IN ('EU', 'AP')
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

iink_ams = spark.sql(iink_ams)
iink_ams.createOrReplaceTempView("iink_ams")


iink_row = f"""
SELECT cal_date,
    region_5, -- dropped country_alpha2 as it was XI, XA 100% region HQ
    pl, 
    sales_product_number,
    ce_split,
    sum(gross_revenue) as gross_revenue,
    sum(net_currency) as net_currency,
    sum(contractual_discounts) as contractual_discounts,
    SUM(discretionary_discounts) as discretionary_discounts,
    sum(warranty) as warranty,
    SUM(other_cos) as other_cos,
    sum(total_cos) as total_cos,
    sum(revenue_units) as revenue_units
FROM iink_region_5 
WHERE 1=1
AND region_5 IN ('EU', 'AP')
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split
"""

iink_row = spark.sql(iink_row)
iink_row.createOrReplaceTempView("iink_row")


# 4.  ib country mix by month, platform, region
ib_data = f"""
SELECT
    cal_date,
    ib.country_alpha2,
    region_5,
    platform_subset,
    sum(units) as ib
FROM stage.ib ib
LEFT JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2 = ib.country_alpha2
WHERE 1=1
    AND ib.version = (select max(version) from stage.ib)
    AND measure = 'IB'
    AND customer_engagement = 'I-INK'
GROUP BY cal_date, ib.country_alpha2, platform_subset, region_5
"""

ib_data = spark.sql(ib_data)
ib_data.createOrReplaceTempView("ib_data")


# 5. add ib data to sales product via rdma sales to base mapping and supplies to hw mapping
salesprod_to_baseprod = f"""
SELECT cal_date,
    region_5,
    iink.sales_product_number,
    rdma.base_product_number
FROM iink_row iink
LEFT JOIN mdm.rdma_base_to_sales_product_map rdma ON iink.sales_product_number = rdma.sales_product_number
"""

salesprod_to_baseprod = spark.sql(salesprod_to_baseprod)
salesprod_to_baseprod.createOrReplaceTempView("salesprod_to_baseprod")


baseprod_to_printer = f"""
SELECT 
        distinct iink.base_product_number, 
        cal_date,
        region_5,
        sales_product_number,
        platform_subset
FROM salesprod_to_baseprod iink
LEFT JOIN mdm.supplies_hw_mapping map ON iink.base_product_number = map.base_product_number
"""

baseprod_to_printer = spark.sql(baseprod_to_printer)
baseprod_to_printer.createOrReplaceTempView("baseprod_to_printer")


printer_to_ibdata = f"""
SELECT
    iink.cal_date,
    iink.region_5,
    sales_product_number,
    base_product_number,
    iink.platform_subset,
    country_alpha2,
    COALESCE(sum(ib), 0) as ib
FROM baseprod_to_printer iink
LEFT JOIN ib_data ib ON iink.cal_date = ib.cal_date AND iink.region_5 = ib.region_5 AND iink.platform_subset = ib.platform_subset
WHERE 1=1
    AND country_alpha2 is not null
GROUP BY iink.cal_date, iink.region_5, sales_product_number, base_product_number, iink.platform_subset, country_alpha2
"""

printer_to_ibdata = spark.sql(printer_to_ibdata)
printer_to_ibdata.createOrReplaceTempView("printer_to_ibdata")


# 6. compute ib mix at country
iink_country_ib_mix = f"""
SELECT cal_date,
    region_5,
    sales_product_number,
    country_alpha2,
    CASE
        WHEN SUM(ib) OVER (PARTITION BY cal_date, region_5, sales_product_number) = 0 THEN NULL
        ELSE ib / SUM(ib) OVER (PARTITION BY cal_date, region_5, sales_product_number)
    END AS ib_country_mix
FROM printer_to_ibdata iink
GROUP BY cal_date,
    region_5,
    sales_product_number,
    country_alpha2,
    ib
"""

iink_country_ib_mix = spark.sql(iink_country_ib_mix)
iink_country_ib_mix.createOrReplaceTempView("iink_country_ib_mix")


# 7. add country to row dataset
iink_row_new = f"""
SELECT iink.cal_date,
    country_alpha2,
    iink.region_5, 
    pl, 
    iink.sales_product_number,
    ce_split,
    sum(gross_revenue * COALESCE(ib_country_mix, 1)) as gross_revenue,
    sum(net_currency * COALESCE(ib_country_mix, 1)) as net_currency,
    sum(contractual_discounts * COALESCE(ib_country_mix, 1)) as contractual_discounts,
    SUM(discretionary_discounts * COALESCE(ib_country_mix, 1)) as discretionary_discounts,
    sum(warranty * COALESCE(ib_country_mix, 1)) as warranty,
    sum(other_cos * COALESCE(ib_country_mix, 1)) as other_cos,
    sum(total_cos * COALESCE(ib_country_mix, 1)) as total_cos,
    sum(revenue_units * COALESCE(ib_country_mix, 1)) as revenue_units
FROM iink_row iink
LEFT JOIN iink_country_ib_mix mix ON
    iink.cal_date = mix.cal_date AND
    iink.region_5 = mix.region_5 AND
    iink.sales_product_number = mix.sales_product_number
WHERE 1=1
GROUP BY iink.cal_date, iink.region_5, pl, iink.sales_product_number, ce_split, country_alpha2
"""

iink_row_new = spark.sql(iink_row_new)
iink_row_new.createOrReplaceTempView("iink_row_new")


# 8. Add back ROW with AMS

iink_country_join = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl, 
    sales_product_number,
    ce_split,
    sum(gross_revenue) as gross_revenue,
    sum(net_currency) as net_currency,
    sum(contractual_discounts) as contractual_discounts,
    SUM(discretionary_discounts) as discretionary_discounts,
    sum(warranty) as warranty,
    sum(other_cos) as other_cos,
    sum(total_cos) as total_cos,
    sum(revenue_units) as revenue_units
FROM iink_ams
WHERE 1=1
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split

UNION ALL

SELECT cal_date,
    country_alpha2,
    region_5,
    pl, 
    sales_product_number,
    ce_split,
    sum(gross_revenue) as gross_revenue,
    sum(net_currency) as net_currency,
    sum(contractual_discounts) as contractual_discounts,
    SUM(discretionary_discounts) as discretionary_discounts,
    sum(warranty) as warranty,
    sum(other_cos) as other_cos,
    sum(total_cos) as total_cos,
    sum(revenue_units) as revenue_units
FROM iink_row_new
WHERE 1=1
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

iink_country_join = spark.sql(iink_country_join)
iink_country_join.createOrReplaceTempView("iink_country_join")


iink_country = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl, 
    sales_product_number,
    ce_split,
    coalesce(sum(gross_revenue), 0) as gross_revenue,
    coalesce(sum(net_currency), 0) as net_currency,
    coalesce(sum(contractual_discounts), 0) as contractual_discounts,        
    coalesce(sum(discretionary_discounts), 0) as discretionary_discounts,
    coalesce(sum(warranty), 0) as warranty,
    coalesce(sum(other_cos), 0) as other_cos,
    coalesce(sum(total_cos), 0) as total_cos,
    coalesce(sum(revenue_units), 0) as revenue_units
FROM iink_country_join
WHERE 1=1
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

iink_country = spark.sql(iink_country)
iink_country.createOrReplaceTempView("iink_country")

# COMMAND ----------

#itp actuals
# 1. data from ibp for itp

itp_units_ibp = f"""    
SELECT 
    cal_date,
    CASE
        WHEN market10 = 'CENTRAL AND EASTERN EUROPE'
        THEN 'CENTRAL EUROPE'
        ELSE market10
    END AS market10,
    sales_product as sales_product_number,
    sum(units) as revenue_units
FROM fin_stage.itp_laser_landing
WHERE 1=1
AND units <> 0
AND cal_date > '2021-10-01'
GROUP BY cal_date, market10, sales_product
"""

itp_units_ibp = spark.sql(itp_units_ibp)
itp_units_ibp.createOrReplaceTempView("itp_units_ibp")

# add region_5
itp_mkt10_reg5 = f"""
SELECT
	distinct region_5, market10	
FROM mdm.iso_country_code_xref
WHERE 1=1 
AND market10 IN (SELECT distinct market10 FROM itp_units_ibp)
AND region_5 <> 'JP'
"""

itp_mkt10_reg5 = spark.sql(itp_mkt10_reg5)
itp_mkt10_reg5.createOrReplaceTempView("itp_mkt10_reg5")


itp_units_ibp2 = f"""
SELECT
	cal_date,
	ibp.market10,
	region_5,
	sales_product_number,
	sum(revenue_units) as revenue_units
FROM itp_units_ibp ibp
LEFT JOIN itp_mkt10_reg5 map
ON ibp.market10 = map.market10
GROUP BY cal_date, ibp.market10, region_5, sales_product_number
"""    

itp_units_ibp2 = spark.sql(itp_units_ibp2)
itp_units_ibp2.createOrReplaceTempView("itp_units_ibp2")
    
    
# 2. create a market10 to country alpha 2 map for itp
# what are the top ib countries by market10?
# first, map salesprod to baseprod for itp skus

itp_salesprod_to_baseprod = f"""
SELECT cal_date,
    region_5,
    itp.sales_product_number,
    rdma.base_product_number
FROM itp_units_ibp2 itp
LEFT JOIN mdm.rdma_base_to_sales_product_map rdma ON itp.sales_product_number = rdma.sales_product_number
"""

itp_salesprod_to_baseprod = spark.sql(itp_salesprod_to_baseprod)
itp_salesprod_to_baseprod.createOrReplaceTempView("itp_salesprod_to_baseprod")


#map baseprod to platform subset to get the platform subsets

itp_baseprod_to_printer = f"""
SELECT 
    distinct itp.base_product_number, 
    cal_date,
    region_5,
    sales_product_number,
    platform_subset
FROM itp_salesprod_to_baseprod itp
LEFT JOIN mdm.supplies_hw_mapping map 
    ON itp.base_product_number = map.base_product_number
    AND itp.region_5 = map.geography
    AND map.geography_grain = 'REGION_5'
"""

itp_baseprod_to_printer = spark.sql(itp_baseprod_to_printer)
itp_baseprod_to_printer.createOrReplaceTempView("itp_baseprod_to_printer")


# what is the itp ib?
itp_ib = f"""
SELECT
    cal_date,
    ib.country_alpha2,
    market10,
    region_5,
    iso.country,
    platform_subset,
    sum(units) as ib
FROM stage.ib ib
LEFT JOIN mdm.iso_country_code_xref iso ON iso.country_alpha2 = ib.country_alpha2
WHERE 1=1
    AND ib.version = (select max(version) from stage.ib)
    AND measure = 'IB'
    AND platform_subset IN (select distinct platform_subset from itp_baseprod_to_printer)
GROUP BY cal_date, ib.country_alpha2, platform_subset, region_5, market10, iso.country
"""

itp_ib = spark.sql(itp_ib)
itp_ib.createOrReplaceTempView("itp_ib")


# pick top countries/ some day replace with mix

itp_salesprod_to_printer = f"""    
SELECT  
    itp.cal_date,
    country_alpha2,
	itp.region_5,
	itp.sales_product_number,
	itp.platform_subset,
	SUM(ib) as ib
FROM itp_baseprod_to_printer itp
LEFT JOIN itp_ib ib
	ON itp.cal_date = ib.cal_date
	AND itp.region_5 = ib.region_5
	AND itp.platform_subset = ib.platform_subset
GROUP BY itp.cal_date, itp.region_5, itp.sales_product_number, itp.platform_subset, country_alpha2
"""

itp_salesprod_to_printer = spark.sql(itp_salesprod_to_printer)
itp_salesprod_to_printer.createOrReplaceTempView("itp_salesprod_to_printer")


itp_ib_country_mix = f"""
SELECT cal_date,
	country_alpha2,
	region_5,
	sales_product_number,
	CASE
		WHEN SUM(ib) OVER (PARTITION BY cal_date, region_5, sales_product_number) = 0 THEN NULL
		ELSE ib / SUM(ib) OVER (PARTITION BY cal_date, region_5, sales_product_number)
	END AS ib_country_mix
FROM itp_salesprod_to_printer
GROUP BY cal_date,
	country_alpha2,
	region_5,
	sales_product_number,
	ib
"""

itp_ib_country_mix = spark.sql(itp_ib_country_mix)
itp_ib_country_mix.createOrReplaceTempView("itp_ib_country_mix")


# 3. add region and country to itp data
    
itp_country = f"""
SELECT
	itp.cal_date,
	c.country_alpha2,
	itp.region_5,
	itp.sales_product_number,
	sum(revenue_units * COALESCE(ib_country_mix, 0)) as revenue_units
FROM itp_units_ibp2 itp
LEFT JOIN itp_ib_country_mix c 
	ON c.region_5 = itp.region_5
	AND c.cal_date = itp.cal_date
	AND c.sales_product_number = itp.sales_product_number
WHERE country_alpha2 is not null
GROUP BY itp.cal_date,
	c.country_alpha2,
	itp.region_5,
	itp.sales_product_number
"""

itp_country = spark.sql(itp_country)
itp_country.createOrReplaceTempView("itp_country")


# final itp

itp_final_input = f"""
SELECT
    cal_date,
    country_alpha2,
    'N5' as pl,
    sales_product_number,
    'TRAD' AS ce_split,
    0 AS gross_revenue,
    0 AS net_currency,
    0 AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS warranty,
    0 as other_cos,
    0 AS total_cos,
    sum(revenue_units) as revenue_units
FROM itp_country
GROUP BY cal_date, country_alpha2, sales_product_number
"""

itp_final_input = spark.sql(itp_final_input)
itp_final_input.createOrReplaceTempView("itp_final_input")

# COMMAND ----------

#add itp, iink to current data set
salesprod_with_ce_splits2  = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,            
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_join_mps_data
WHERE pl <> 'GD'
GROUP BY cal_date, pl, country_alpha2, sales_product_number, ce_split

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,            
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM iink_country
GROUP BY cal_date, pl, country_alpha2, sales_product_number, ce_split

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,            
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM itp_final_input
GROUP BY cal_date, pl, country_alpha2, sales_product_number, ce_split
"""

salesprod_with_ce_splits2 = spark.sql(salesprod_with_ce_splits2)
salesprod_with_ce_splits2.createOrReplaceTempView("salesprod_with_ce_splits2")


salesprod_with_ce_splits2a  = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,    
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM salesprod_with_ce_splits2
GROUP BY cal_date, pl, country_alpha2, sales_product_number, ce_split
"""

salesprod_with_ce_splits2a = spark.sql(salesprod_with_ce_splits2a)
salesprod_with_ce_splits2a.createOrReplaceTempView("salesprod_with_ce_splits2a")

# COMMAND ----------

# DESIGNATED M-CODES

# COMMAND ----------

designated_mcodes = f"""
SELECT cal.Date AS cal_date,
    pl.pl,
    jv.country,
    country_alpha2,
    sales_product_number,
    SUM(gross_revenue) AS gross_revenue,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(total_COS) AS other_cos,
    SUM(total_COS) AS total_cos
FROM fin_stage.supplies_manual_mcode_jv_detail_landing AS jv
JOIN mdm.product_line_xref AS pl ON jv.pl = pl.plxx
JOIN mdm.calendar AS cal ON jv.yearmon = cal.Calendar_Yr_Mo
LEFT JOIN mdm.iso_country_code_xref AS iso ON jv.country = iso.country
WHERE 1=1 
    AND Day_of_Month = 1
GROUP BY cal.Date, pl.pl, jv.country, country_alpha2, sales_product_number
"""

designated_mcodes = spark.sql(designated_mcodes)
designated_mcodes.createOrReplaceTempView("designated_mcodes")


format_mcodes = f"""
SELECT
    cal_date,
    CASE
        WHEN sales_product_number = 'CISS' AND pl = '1N' THEN 'LU' -- restate PL per 2021 hierarchy changes
        --WHEN sales_product_number = 'CTSS' AND pl = 'G0' THEN 'N4' -- restate PL per 2022 hierarchy changes
        ELSE pl
    END AS pl,
    country_alpha2,
    sales_product_number,
    'TRAD' AS ce_split,                
    SUM(gross_revenue) AS gross_revenue,
    0 AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM designated_mcodes
WHERE cal_date in (SELECT distinct cal_date FROM odw_salesprod_all_cleaned)
GROUP BY cal_date, pl, country_alpha2, sales_product_number        
"""

format_mcodes = spark.sql(format_mcodes)
format_mcodes.createOrReplaceTempView("format_mcodes")


mcodes_offset = f"""
SELECT
    cal_date,
    pl,
    country_alpha2,
    CONCAT('UNKN', pl) AS sales_product_number,
    'TRAD' AS ce_split,                
    SUM(gross_revenue) * -1 AS gross_revenue,
    0 AS net_currency,
    SUM(contractual_discounts) * -1 AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS warranty,
    SUM(other_cos) * -1 AS other_cos,
    SUM(total_cos) * -1 AS total_cos,
    0 AS revenue_units
FROM format_mcodes
GROUP BY cal_date, pl, country_alpha2, sales_product_number        
"""

mcodes_offset = spark.sql(mcodes_offset)
mcodes_offset.createOrReplaceTempView("mcodes_offset")


mps_revenue_from_edw = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    'EST_MPS_REVENUE_JV' AS sales_product_number,
    'TRAD' AS ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_final_findata2
WHERE 1=1
    AND sales_product_number IN ('H7503A', 'H7509A', 'U1001AC', 'H7523A') -- from RDMA and confirmed by MPS
GROUP BY cal_date,
    country_alpha2,
    pl
"""

mps_revenue_from_edw = spark.sql(mps_revenue_from_edw)
mps_revenue_from_edw.createOrReplaceTempView("mps_revenue_from_edw")


estimated_mps_revenue = f"""
SELECT 
    cal_date,
    CASE
        WHEN country_alpha2 = 'XS' THEN 'CZ'
        WHEN country_alpha2 = 'XW' THEN 'US'
        ELSE country_alpha2
    END AS country_alpha2,
    pl,
    sales_product_number,                
    ce_split,    
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    0 AS net_currency,
    0 AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS warranty,
    0 as other_cos,
    0 AS total_cos,
    0 AS revenue_units
FROM mps_revenue_from_edw
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split
"""

estimated_mps_revenue = spark.sql(estimated_mps_revenue)
estimated_mps_revenue.createOrReplaceTempView("estimated_mps_revenue")


salesprod_add_mcodes = f"""
SELECT 
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,                            
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_with_ce_splits2a
GROUP BY cal_date, pl, country_alpha2, sales_product_number, ce_split

UNION ALL

SELECT 
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,                            
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM format_mcodes
GROUP BY cal_date, pl, country_alpha2, sales_product_number, ce_split

UNION ALL

SELECT 
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,                            
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM mcodes_offset
GROUP BY cal_date, pl, country_alpha2, sales_product_number, ce_split

UNION ALL

SELECT 
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,                            
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM estimated_mps_revenue 
GROUP BY cal_date, pl, country_alpha2, sales_product_number, ce_split
"""

salesprod_add_mcodes = spark.sql(salesprod_add_mcodes)
salesprod_add_mcodes.createOrReplaceTempView("salesprod_add_mcodes")


salesprod_final_before_charges_spread = f"""
SELECT 
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,    
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts),0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM salesprod_add_mcodes
WHERE country_alpha2 <> 'XW'
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split
"""    

salesprod_final_before_charges_spread = spark.sql(salesprod_final_before_charges_spread)
salesprod_final_before_charges_spread.createOrReplaceTempView("salesprod_final_before_charges_spread")


# COMMAND ----------

# SPREAD CHARGES

# COMMAND ----------

odw_salesprod_before_plcharges_temp = f"""
SELECT 
    cal_date,
    edw.country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,    
    Fiscal_Month,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_final_before_charges_spread edw
LEFT JOIN mdm.calendar cal 
    ON cal.Date = edw.cal_date
LEFT JOIN mdm.iso_country_code_xref iso
    ON edw.country_alpha2 = iso.country_alpha2
WHERE 1=1
AND day_of_month = 1
GROUP BY cal_date, pl, edw.country_alpha2, sales_product_number, ce_split, Fiscal_Month, region_5
"""    

odw_salesprod_before_plcharges_temp = spark.sql(odw_salesprod_before_plcharges_temp)
odw_salesprod_before_plcharges_temp.createOrReplaceTempView("odw_salesprod_before_plcharges_temp")

# COMMAND ----------

# Write out salesprod_before_plcharges_temp to its delta table target.
odw_salesprod_before_plcharges_temp.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/odw_salesprod_before_plcharges_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.odw_salesprod_before_plcharges_temp USING DELTA LOCATION '/tmp/delta/fin_stage/odw_salesprod_before_plcharges_temp'")

#spark.table("fin_stage.odw_salesprod_before_plcharges_temp").createOrReplaceTempView("odw_salesprod_before_plcharges_temp")

# COMMAND ----------


spark.table("fin_stage.odw_salesprod_before_plcharges_temp").createOrReplaceTempView("odw_salesprod_before_plcharges_temp2")

# COMMAND ----------

unknown_skus = f"""
select distinct sales_product_number 
from odw_salesprod_before_plcharges_temp2
where sales_product_number like 'UNK%'
and sales_product_number <> 'UNKNLU'
"""

unknown_skus = spark.sql(unknown_skus)
unknown_skus.createOrReplaceTempView("unknown_skus")

            
salesprod_pre_spread_unadjusted = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    'PL-CHARGE' AS sales_product_number,
    ce_split,                            
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM odw_salesprod_before_plcharges_temp2
WHERE 1=1
AND sales_product_number IN (select sales_product_number from unknown_skus)
GROUP BY cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split
    
UNION ALL

SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,                            
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM odw_salesprod_before_plcharges_temp2
WHERE 1=1
AND sales_product_number NOT IN (select sales_product_number from unknown_skus)
GROUP BY cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split
"""

salesprod_pre_spread_unadjusted = spark.sql(salesprod_pre_spread_unadjusted)
salesprod_pre_spread_unadjusted.createOrReplaceTempView("salesprod_pre_spread_unadjusted")


salesprod_pre_spread = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units),0) AS revenue_units
FROM salesprod_pre_spread_unadjusted
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, revenue_units
"""

salesprod_pre_spread = spark.sql(salesprod_pre_spread)
salesprod_pre_spread.createOrReplaceTempView("salesprod_pre_spread")

# COMMAND ----------

salesprod_acct_items2 = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_pre_spread
WHERE sales_product_number IN ('BIRDS', 'CISS', 'CTSS', 'EST_MPS_REVENUE_JV', 'LFMPS')
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

salesprod_acct_items2 = spark.sql(salesprod_acct_items2)
salesprod_acct_items2.createOrReplaceTempView("salesprod_acct_items2")


salesprod_spread_able = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_pre_spread
WHERE sales_product_number NOT IN ('BIRDS', 'CISS', 'CTSS', 'EST_MPS_REVENUE_JV', 'LFMPS')
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

salesprod_spread_able = spark.sql(salesprod_spread_able)
salesprod_spread_able.createOrReplaceTempView("salesprod_spread_able")


salesprod_spread_able_without_charges2 = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_spread_able
WHERE sales_product_number <> 'PL-CHARGE'
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

salesprod_spread_able_without_charges2 = spark.sql(salesprod_spread_able_without_charges2)
salesprod_spread_able_without_charges2.createOrReplaceTempView("salesprod_spread_able_without_charges2")


salesprod_spread_able_with_charges2 = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos
FROM salesprod_spread_able
WHERE sales_product_number = 'PL-CHARGE'
GROUP BY cal_date, country_alpha2, region_5, pl, ce_split
"""

salesprod_spread_able_with_charges2 = spark.sql(salesprod_spread_able_with_charges2)
salesprod_spread_able_with_charges2.createOrReplaceTempView("salesprod_spread_able_with_charges2")

   
gross_rev_product_mix_by_country = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,
    CASE
        WHEN SUM(gross_revenue) OVER(PARTITION BY cal_date, country_alpha2, pl, ce_split) = 0 THEN NULL
        ELSE gross_revenue / SUM(gross_revenue) OVER(PARTITION BY cal_date, country_alpha2, pl, ce_split)
    END AS edw_gross_rev_mix
FROM salesprod_spread_able_without_charges2
WHERE gross_revenue > 0
"""

gross_rev_product_mix_by_country = spark.sql(gross_rev_product_mix_by_country)
gross_rev_product_mix_by_country.createOrReplaceTempView("gross_rev_product_mix_by_country")


pl_charges_spread_to_SKU1 = f"""
SELECT
    p.cal_date,
    p.country_alpha2,
    region_5,
    p.pl,
    sales_product_number,
    p.ce_split,                
    SUM(gross_revenue * COALESCE(edw_gross_rev_mix, 1)) AS gross_revenue,
    SUM(net_currency * COALESCE(edw_gross_rev_mix, 1)) AS net_currency,
    SUM(contractual_discounts * COALESCE(edw_gross_rev_mix, 1)) AS contractual_discounts,
    SUM(discretionary_discounts * COALESCE(edw_gross_rev_mix, 1)) AS discretionary_discounts,
    SUM(warranty * COALESCE(edw_gross_rev_mix, 1)) AS warranty,
    SUM(other_cos * COALESCE(edw_gross_rev_mix, 1)) AS other_cos,
    SUM(total_cos * COALESCE(edw_gross_rev_mix, 1)) AS total_cos
FROM salesprod_spread_able_with_charges2 p
LEFT JOIN gross_rev_product_mix_by_country mix ON
    p.cal_date = mix.cal_date AND
    p.country_alpha2 = mix.country_alpha2 AND
    p.pl = mix.pl AND
    p.ce_split = mix.ce_split
GROUP BY p.cal_date, p.country_alpha2, region_5, p.pl, p.ce_split, sales_product_number
"""

pl_charges_spread_to_SKU1 = spark.sql(pl_charges_spread_to_SKU1)
pl_charges_spread_to_SKU1.createOrReplaceTempView("pl_charges_spread_to_SKU1")


pl_charges_spread_to_SKU2 = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(warranty) + SUM(other_cos) AS total_cos
FROM pl_charges_spread_to_SKU1
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

pl_charges_spread_to_SKU2 = spark.sql(pl_charges_spread_to_SKU2)
pl_charges_spread_to_SKU2.createOrReplaceTempView("pl_charges_spread_to_SKU2")


findata_mash = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_acct_items2
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_spread_able_without_charges2
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split

UNION ALL 

SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    CASE
        WHEN sales_product_number is null THEN CONCAT('UNKN', pl)
        ELSE sales_product_number
    END AS sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM pl_charges_spread_to_SKU2
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

findata_mash = spark.sql(findata_mash)
findata_mash.createOrReplaceTempView("findata_mash")


salesprod_edw_spread = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM findata_mash
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

salesprod_edw_spread = spark.sql(salesprod_edw_spread)
salesprod_edw_spread.createOrReplaceTempView("salesprod_edw_spread")


# COMMAND ----------

# UPDATE TRANSACTION DATA TO REFLECT CURRENT PRODUCT HIERARCHY 

# COMMAND ----------

rdma_correction_2023_restatements = f"""
SELECT 
    sales_product_number,
    CASE
      WHEN sales_product_line_code = '65' THEN 'UD'
      WHEN sales_product_line_code = 'EO' THEN 'GL'
      WHEN sales_product_line_code = 'GM' THEN 'K6'
      ELSE sales_product_line_code
    END AS sales_product_line_code
FROM mdm.rdma_base_to_sales_product_map
WHERE 1=1
"""

rdma_correction_2023_restatements = spark.sql(rdma_correction_2023_restatements)
rdma_correction_2023_restatements.createOrReplaceTempView("rdma_correction_2023_restatements")


rdma_updated_sku_PLs = f"""
SELECT 
    DISTINCT sales_product_number,
    sales_product_line_code
FROM rdma_correction_2023_restatements
WHERE 1=1
"""

rdma_updated_sku_PLs = spark.sql(rdma_updated_sku_PLs)
rdma_updated_sku_PLs.createOrReplaceTempView("rdma_updated_sku_PLs")


salesprod_edw_spread2 = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    edw.pl AS edw_recorded_pl,
    rdma.sales_product_line_code AS rdma_pl,
    edw.sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_edw_spread edw
LEFT JOIN rdma_updated_sku_PLs rdma ON edw.sales_product_number = rdma.sales_product_number
WHERE edw.pl <> 'GD' OR rdma.sales_product_line_code <> 'GD'
GROUP BY cal_date, country_alpha2, region_5, edw.pl, edw.sales_product_number, rdma.sales_product_line_code, ce_split
"""

salesprod_edw_spread2 = spark.sql(salesprod_edw_spread2)
salesprod_edw_spread2.createOrReplaceTempView("salesprod_edw_spread2")


edw_product_line_restated = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    edw_recorded_pl,
    rdma_pl,
    sales_product_number,
    CASE
        WHEN rdma_pl is null THEN edw_recorded_pl
        ELSE rdma_pl
    END AS pl,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_edw_spread2 edw
GROUP BY cal_date, country_alpha2, region_5, edw_recorded_pl, rdma_pl, sales_product_number, ce_split
"""

edw_product_line_restated = spark.sql(edw_product_line_restated)
edw_product_line_restated.createOrReplaceTempView("edw_product_line_restated")


edw_product_line_restated2 = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    edw_recorded_pl,
    CASE
        WHEN pl = '65' THEN 'UD'
        WHEN pl = 'GM' THEN 'K6'
        WHEN pl = 'EO' THEN 'GL'
        ELSE pl
    END AS pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_product_line_restated edw
WHERE 1=1 
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, edw_recorded_pl
"""

edw_product_line_restated2 = spark.sql(edw_product_line_restated2)
edw_product_line_restated2.createOrReplaceTempView("edw_product_line_restated2")


updated_pl_plus_gd = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_product_line_restated2 edw
WHERE pl <> 'GD'
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split

UNION ALL

SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_edw_spread
WHERE pl = 'GD'
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

updated_pl_plus_gd = spark.sql(updated_pl_plus_gd)
updated_pl_plus_gd.createOrReplaceTempView("updated_pl_plus_gd")


edw_data_with_updated_rdma_pl2 = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM updated_pl_plus_gd
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

edw_data_with_updated_rdma_pl2 = spark.sql(edw_data_with_updated_rdma_pl2)
edw_data_with_updated_rdma_pl2.createOrReplaceTempView("edw_data_with_updated_rdma_pl2")


edw_data_with_updated_rdma_pl3 = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_data_with_updated_rdma_pl2
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

edw_data_with_updated_rdma_pl3 = spark.sql(edw_data_with_updated_rdma_pl3)
edw_data_with_updated_rdma_pl3.createOrReplaceTempView("edw_data_with_updated_rdma_pl3")

# COMMAND ----------

# CURRENCY 

#Currency commentary :: why various currencies are being screened (should be 1 currency per country)
#WHERE currency_name = 'Ghana Cedi' -- keep New Ghana Cedi (GHS) 2007 - present
#OR currency_iso_code IN ('VEB', 'VEF') -- Venezuela VEF replaced by VES after a transition within IE2 timeline (addressed below)
#OR currency_iso_code = 'CLF' -- keep Chilean peso, CFP, 1975 - present
#OR currency_iso_code = 'XAF' -- keep Congolese Franc, CDF, 1997 - present
#OR currency_iso_code = 'CYP' -- keep Cyprus with EUR since 2008 (former currency, pre EURO)
#OR currency_iso_code = 'EEK' -- keep Estonia with EUR since 2011 (former currency, pre EURO)
#OR currency_iso_code = 'LVL' -- keep Lativa with EUR since 2014 (former currency, pre EURO)
#OR currency_iso_code = 'LTL' -- keep Lituania with EUR since 2015 (former currency, pre EURO)
#OR currency_iso_code = 'MTL' -- keep Malta with EUR since 2004 (former currency, pre EURO)
#OR currency_iso_code = 'YUM' -- keep Serbia with Dinar since 2006
#OR currency_iso_code = 'SKK' -- keep Slovakia with EUR since 2009 (former currency, pre EURO)
#OR currency_iso_code = 'SIT' -- keep Slovenia with EUR since 2007 (former currency, pre EURO)
#OR currency_iso_code = 'ZMK' -- Zambia now uses kwacha, ZMW; ZMK expired as legal tender in June 2013
#OR currency_iso_code = 'TRL' -- Turkey now uses TRY lira since 2005

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM fin_stage.country_currency_map_landing
# MAGIC WHERE 1=1 
# MAGIC AND load_date <> (SELECT MAX(load_date) FROM fin_stage.country_currency_map_landing)
# MAGIC AND country_alpha2 = ''

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM fin_stage.country_currency_map_landing
# MAGIC WHERE currency_name = 'Ghana Cedi'
# MAGIC OR currency_iso_code IN ('VEB', 'VEF')
# MAGIC OR currency_iso_code = 'CLF'
# MAGIC OR currency_iso_code = 'XAF'
# MAGIC OR currency_iso_code = 'CYP' 
# MAGIC OR currency_iso_code = 'EEK' 
# MAGIC OR currency_iso_code = 'LVL'
# MAGIC OR currency_iso_code = 'LTL' 
# MAGIC OR currency_iso_code = 'MTL' 
# MAGIC OR currency_iso_code = 'YUM' 
# MAGIC OR currency_iso_code = 'SKK' 
# MAGIC OR currency_iso_code = 'SIT' 
# MAGIC OR currency_iso_code = 'ZMK' 
# MAGIC OR currency_iso_code = 'TRL' 

# COMMAND ----------

emea_currency_table = f"""
SELECT 
    CASE
        WHEN country = 'AFGHANISTAN' THEN 'AP'
        ELSE region_5
    END AS region_5,
    country_alpha2,
    country,
    CASE
        WHEN currency = 'EURO' THEN 'EUR'
        ELSE 'USD'                
    END AS currency
FROM mdm.list_price_eu_country_list
WHERE country_alpha2 NOT IN ('ZM', 'ZW')
"""

emea_currency_table = spark.sql(emea_currency_table)
emea_currency_table.createOrReplaceTempView("emea_currency_table")


row_currency_table = f"""
SELECT 
    region_5,
    cmap.country_alpha2,
    cmap.country,
    currency_iso_code AS currency 
FROM fin_stage.country_currency_map_landing cmap
LEFT JOIN mdm.iso_country_code_xref iso ON cmap.country_alpha2 = iso.country_alpha2 AND cmap.country = iso.country
WHERE cmap.country_alpha2 NOT IN (
    SELECT DISTINCT country_alpha2
    FROM emea_currency_table)
"""

row_currency_table = spark.sql(row_currency_table)
row_currency_table.createOrReplaceTempView("row_currency_table")


ALL_currencies = f"""
SELECT *
FROM emea_currency_table
UNION ALL
SELECT *
FROM row_currency_table
"""

ALL_currencies = spark.sql(ALL_currencies)
ALL_currencies.createOrReplaceTempView("ALL_currencies")


currency = f"""
SELECT distinct    region_5,
    country_alpha2,
    country,
    CASE
        WHEN currency IN ('ZMW', 'ZWD') THEN 'USD' 
        ELSE currency            
    END AS currency
FROM ALL_currencies
"""        

currency = spark.sql(currency)
currency.createOrReplaceTempView("currency")

# COMMAND ----------

#prepare data for x-code (financial system HQ, i.e., non-ISO) elimination
edw_restated_data2 = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_data_with_updated_rdma_pl2 redw
WHERE country_alpha2 <> 'XW'
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

edw_restated_data2 = spark.sql(edw_restated_data2)
edw_restated_data2.createOrReplaceTempView("edw_restated_data2")

# COMMAND ----------

# there are three classes of items that we are concerned with:  
# 1. accounting items that have no units, e.g. CISS, Birds, CTSS, and are manually input
# 2. regular data that should flow thru 'ok' which includes mps
# 3. PLs that have NO region_5 level data except for 'X'codes, e.g. GY and HF in EU

# COMMAND ----------

# add currency to PLGD

xcode_adjusted_plgd2 = f"""
SELECT
    cal_date,
    country_alpha2,
    CASE 
        WHEN country_alpha2 = 'US' THEN 'USD' 
        WHEN country_alpha2 = 'CA' THEN 'CAD'
        WHEN region_5 = 'EU' THEN 'EUR'
        ELSE 'USD'
    END as currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,    
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM edw_restated_data2
WHERE country_alpha2 is not null
AND pl = 'GD'
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

xcode_adjusted_plgd2 = spark.sql(xcode_adjusted_plgd2)
xcode_adjusted_plgd2.createOrReplaceTempView("xcode_adjusted_plgd2")


accounting_items = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_restated_data2 acct
WHERE 1=1
    AND sales_product_number IN ('BIRDS', 'CISS', 'CTSS', 'LFMPS')
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2
"""

accounting_items = spark.sql(accounting_items)
accounting_items.createOrReplaceTempView("accounting_items")


birds = f"""
SELECT
    cal_date,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM accounting_items 
WHERE sales_product_number = 'BIRDS'
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split
"""

birds = spark.sql(birds)
birds.createOrReplaceTempView("birds")    

mix_GP = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    CASE
        WHEN SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5) = 0 THEN NULL
        ELSE gross_revenue / SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5)
    END AS country_gross_mix
FROM edw_restated_data2
WHERE pl = 'GP'
    AND country_alpha2 NOT IN (
                                SELECT country_alpha2
                                FROM mdm.iso_country_code_xref
                                WHERE country_alpha2 LIKE 'X%'
                                AND country_alpha2 != 'XK'
                            )
GROUP BY cal_date, country_alpha2, region_5, gross_revenue
"""

mix_GP = spark.sql(mix_GP)
mix_GP.createOrReplaceTempView("mix_GP")    


mix_1NLU = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    CASE
        WHEN SUM(revenue_units) OVER (PARTITION BY cal_date, region_5) = 0 THEN NULL
        ELSE revenue_units / SUM(revenue_units) OVER (PARTITION BY cal_date, region_5)
    END AS country_unit_mix,
    CASE
        WHEN SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5) = 0 THEN NULL
        ELSE gross_revenue / SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5)
    END AS country_gross_mix
FROM edw_restated_data2
WHERE pl IN ('LU', '1N') -- blending these PLs for fuller history
    AND country_alpha2 NOT IN (
                                SELECT country_alpha2
                                FROM mdm.iso_country_code_xref
                                WHERE country_alpha2 LIKE 'X%'
                                AND country_alpha2 != 'XK'
                            )
GROUP BY cal_date, country_alpha2, region_5, revenue_units, gross_revenue
"""

mix_1NLU = spark.sql(mix_1NLU)
mix_1NLU.createOrReplaceTempView("mix_1NLU")    


mix_sprint = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    CASE
        WHEN SUM(revenue_units) OVER (PARTITION BY cal_date, region_5) = 0 THEN NULL
        ELSE revenue_units / SUM(revenue_units) OVER (PARTITION BY cal_date, region_5)
    END AS country_unit_mix,
    CASE
        WHEN SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5) = 0 THEN NULL
        ELSE gross_revenue / SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5)
    END AS country_gross_mix
FROM edw_restated_data2
WHERE pl IN ('G0', 'E5', 'GL', 'EO') -- blending these PLs for fuller history
AND country_alpha2 NOT IN (
                                SELECT country_alpha2
                                FROM mdm.iso_country_code_xref
                                WHERE country_alpha2 LIKE 'X%'
                                AND country_alpha2 != 'XK'
                            )
GROUP BY cal_date, country_alpha2, region_5, revenue_units, gross_revenue
"""

mix_sprint = spark.sql(mix_sprint)
mix_sprint.createOrReplaceTempView("mix_sprint")    


birdsx = f"""
-- add back
SELECT
    b.cal_date,
    country_alpha2,
    'USD' as currency,
    b.region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue * country_gross_mix) AS gross_revenue,
    SUM(net_currency * country_gross_mix) AS net_currency,
    SUM(contractual_discounts * country_gross_mix) AS contractual_discounts,
    SUM(discretionary_discounts * country_gross_mix) AS discretionary_discounts,
    SUM(warranty * country_gross_mix) AS warranty,
    SUM(other_cos * country_gross_mix) AS other_cos,
    SUM(total_cos * country_gross_mix) AS total_cos,
    SUM(revenue_units * country_gross_mix) AS revenue_units
FROM birds b
LEFT JOIN mix_GP gp ON gp.cal_date = b.cal_date AND b.region_5 = gp.region_5
GROUP BY b.cal_date, b.region_5, pl, sales_product_number, ce_split, country_alpha2
"""

birdsx = spark.sql(birdsx)
birdsx.createOrReplaceTempView("birdsx")    


ciss = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM accounting_items c
WHERE 1=1
    AND sales_product_number = 'CISS'
    AND country_alpha2 NOT LIKE 'X%'
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2
"""

ciss = spark.sql(ciss)
ciss.createOrReplaceTempView("ciss")    


cissx = f"""
SELECT
    cal_date,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM accounting_items cx
WHERE 1=1
    AND sales_product_number = 'CISS'
    AND country_alpha2 LIKE 'X%'
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split
"""

cissx = spark.sql(cissx)
cissx.createOrReplaceTempView("cissx")    


cissx_fix = f"""
SELECT
    cx.cal_date,
    country_alpha2,
    cx.region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue * country_gross_mix) AS gross_revenue,
    SUM(net_currency * country_gross_mix) AS net_currency,
    SUM(contractual_discounts * country_gross_mix) AS contractual_discounts,
    SUM(discretionary_discounts * country_gross_mix) AS discretionary_discounts,
    SUM(warranty * country_gross_mix) AS warranty,
    SUM(other_cos * country_gross_mix) AS other_cos,
    SUM(total_cos * country_gross_mix) AS total_cos,
    SUM(revenue_units * country_gross_mix) AS revenue_units
FROM cissx cx
LEFT JOIN mix_1NLU n ON cx.cal_date = n.cal_Date AND cx.region_5 = n.region_5
GROUP BY cx.cal_date, cx.region_5, pl, sales_product_number, ce_split, country_alpha2
"""

cissx_fix = spark.sql(cissx_fix)
cissx_fix.createOrReplaceTempView("cissx_fix")    


xcode_adjusted_ciss = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM ciss c
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM cissx_fix cx
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2
"""

xcode_adjusted_ciss = spark.sql(xcode_adjusted_ciss)
xcode_adjusted_ciss.createOrReplaceTempView("xcode_adjusted_ciss")    


xcode_adjusted_ciss2 = f"""
-- addback
SELECT
    cal_date,
    country_alpha2,
    'USD' AS currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,                
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM xcode_adjusted_ciss c
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2
"""

xcode_adjusted_ciss2 = spark.sql(xcode_adjusted_ciss2)
xcode_adjusted_ciss2.createOrReplaceTempView("xcode_adjusted_ciss2")    


ctss = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM accounting_items c
WHERE 1=1
    AND sales_product_number = 'CTSS'
    AND country_alpha2 NOT LIKE 'X%'
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2
"""

ctss = spark.sql(ctss)
ctss.createOrReplaceTempView("ctss")    


ctssx = f"""
SELECT
    cal_date,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM accounting_items cx
WHERE 1=1
    AND sales_product_number = 'CTSS'
    AND country_alpha2 LIKE 'X%'
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split
"""

ctssx = spark.sql(ctssx)
ctssx.createOrReplaceTempView("ctssx")    


ctssx_fix = f"""
SELECT
    cx.cal_date,
    country_alpha2,
    cx.region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue * country_gross_mix) AS gross_revenue,
    SUM(net_currency * country_gross_mix) AS net_currency,
    SUM(contractual_discounts * country_gross_mix) AS contractual_discounts,
    SUM(discretionary_discounts * country_gross_mix) AS discretionary_discounts,
    SUM(warranty * country_gross_mix) AS warranty,
    SUM(other_cos * country_gross_mix) AS other_cos,
    SUM(total_cos * country_gross_mix) AS total_cos,
    SUM(revenue_units * country_gross_mix) AS revenue_units
FROM ctssx cx
LEFT JOIN mix_sprint n ON cx.cal_date = n.cal_Date AND cx.region_5 = n.region_5
GROUP BY cx.cal_date, cx.region_5, pl, sales_product_number, ce_split, country_alpha2
"""


ctssx_fix = spark.sql(ctssx_fix)
ctssx_fix.createOrReplaceTempView("ctssx_fix")    


xcode_adjusted_ctss = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM ctss c
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM ctssx_fix cx
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2
"""


xcode_adjusted_ctss = spark.sql(xcode_adjusted_ctss)
xcode_adjusted_ctss.createOrReplaceTempView("xcode_adjusted_ctss")    


xcode_adjusted_ctss2 = f"""
--addback

SELECT
    cal_date,
    country_alpha2,
    'USD' AS currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,                
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM xcode_adjusted_ctss c
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2
"""


xcode_adjusted_ctss2 = spark.sql(xcode_adjusted_ctss2)
xcode_adjusted_ctss2.createOrReplaceTempView("xcode_adjusted_ctss2")    


xcode_adjusted_accounting_items = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM birdsx b
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2, currency
    
UNION ALL

SELECT 
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM xcode_adjusted_ciss2 ciss
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2, currency

UNION ALL

SELECT 
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM xcode_adjusted_ctss2 ctss
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, country_alpha2, currency
"""


xcode_adjusted_accounting_items = spark.sql(xcode_adjusted_accounting_items)
xcode_adjusted_accounting_items.createOrReplaceTempView("xcode_adjusted_accounting_items")    


xcode_adjusted_account_jv = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,        
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,    
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM xcode_adjusted_accounting_items
GROUP BY cal_date, currency, region_5, pl, sales_product_number, ce_split, country_alpha2
"""

xcode_adjusted_account_jv = spark.sql(xcode_adjusted_account_jv)
xcode_adjusted_account_jv.createOrReplaceTempView("xcode_adjusted_account_jv")    


salesprod_data_normalish_items = f"""
SELECT
    cal_date,
    CASE
        WHEN pl = 'GY' AND country_alpha2 = 'XA' THEN 'CH'
        ELSE country_alpha2
    END AS country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_restated_data2
WHERE 1=1
    AND pl <> 'GD'
    AND sales_product_number NOT IN ('BIRDS', 'CISS', 'CTSS', 'LFMPS')
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

salesprod_data_normalish_items = spark.sql(salesprod_data_normalish_items)
salesprod_data_normalish_items.createOrReplaceTempView("salesprod_data_normalish_items")    

# COMMAND ----------

salesprod_currency_emea = f"""
SELECT
    cal_date,
    Fiscal_Yr,
    act.country_alpha2,
    currency,
    act.region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_data_normalish_items  AS act
LEFT JOIN mdm.calendar AS cal ON cal_date = cal.Date
LEFT JOIN currency cmap 
    ON act.country_alpha2 = cmap.country_alpha2
WHERE 1=1
    AND Day_of_Month = 1 
    AND act.region_5 = 'EU'
GROUP BY cal_date, act.country_alpha2, currency, act.region_5, pl, sales_product_number, ce_split, Fiscal_Yr
"""

salesprod_currency_emea = spark.sql(salesprod_currency_emea)
salesprod_currency_emea.createOrReplaceTempView("salesprod_currency_emea")    


salesprod_currency_emea2 = f"""
SELECT
    cal_date,
    Fiscal_Yr,
    country_alpha2,
    CASE
        WHEN currency is null THEN 'USD'
        ELSE currency
    END AS currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_currency_emea act
GROUP BY cal_date, country_alpha2, currency, region_5, pl, sales_product_number, ce_split, Fiscal_Yr
"""        

salesprod_currency_emea2 = spark.sql(salesprod_currency_emea2)
salesprod_currency_emea2.createOrReplaceTempView("salesprod_currency_emea2")    


salesprod_emea_with_country_detail  = f"""
SELECT
    cal_date,
    Fiscal_Yr,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_currency_emea2
WHERE country_alpha2 NOT IN (
                                SELECT country_alpha2
                                FROM mdm.iso_country_code_xref
                                WHERE country_alpha2 LIKE 'X%'
                                AND country_alpha2 != 'XK'
                            )
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, Fiscal_Yr, currency
"""

salesprod_emea_with_country_detail = spark.sql(salesprod_emea_with_country_detail)
salesprod_emea_with_country_detail.createOrReplaceTempView("salesprod_emea_with_country_detail")    


salesprod_emea_with_xcodes = f"""
SELECT
    cal_date,
    Fiscal_Yr,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_currency_emea2
WHERE country_alpha2 IN (
                                SELECT country_alpha2
                                FROM mdm.iso_country_code_xref
                                WHERE country_alpha2 LIKE 'X%'
                                AND country_alpha2 != 'XK'
                            )
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split, Fiscal_Yr
"""
    
salesprod_emea_with_xcodes = spark.sql(salesprod_emea_with_xcodes)
salesprod_emea_with_xcodes.createOrReplaceTempView("salesprod_emea_with_xcodes")    


emea_country_mix_by_PL_dollar_data = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue
FROM salesprod_emea_with_country_detail
WHERE gross_revenue != 0
AND pl != 'GD'
GROUP BY cal_date, country_alpha2, region_5, pl
"""    

emea_country_mix_by_PL_dollar_data = spark.sql(emea_country_mix_by_PL_dollar_data)
emea_country_mix_by_PL_dollar_data.createOrReplaceTempView("emea_country_mix_by_PL_dollar_data")    


emea_country_mix_by_PL_dollar_data_noUSA = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue
FROM salesprod_emea_with_country_detail
WHERE gross_revenue != 0
AND country_alpha2 != 'US'
AND pl != 'GD'
GROUP BY cal_date, country_alpha2, region_5, pl
"""    
    
emea_country_mix_by_PL_dollar_data_noUSA = spark.sql(emea_country_mix_by_PL_dollar_data_noUSA)
emea_country_mix_by_PL_dollar_data_noUSA.createOrReplaceTempView("emea_country_mix_by_PL_dollar_data_noUSA")    


emea_country_mix_by_PL_units_data = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    CAST(COALESCE(SUM(revenue_units), 0) as numeric) AS revenue_units
FROM salesprod_emea_with_country_detail
WHERE revenue_units != 0
AND pl != 'GD'
GROUP BY cal_date, country_alpha2, region_5, pl
"""

emea_country_mix_by_PL_units_data = spark.sql(emea_country_mix_by_PL_units_data)
emea_country_mix_by_PL_units_data.createOrReplaceTempView("emea_country_mix_by_PL_units_data")    


emea_country_dollar_mix = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    CASE
        WHEN SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5, pl) = 0 THEN NULL
        ELSE gross_revenue / SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5, pl)
    END AS country_rev_mix
FROM emea_country_mix_by_PL_dollar_data
GROUP BY cal_date, country_alpha2, region_5, pl, gross_revenue
"""

emea_country_dollar_mix = spark.sql(emea_country_dollar_mix)
emea_country_dollar_mix.createOrReplaceTempView("emea_country_dollar_mix")    


emea_country_dollar_mix_currency = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    CASE
        WHEN SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5, pl) = 0 THEN NULL
        ELSE gross_revenue / SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5, pl)
    END AS country_currency_mix
FROM emea_country_mix_by_PL_dollar_data_noUSA
GROUP BY cal_date, country_alpha2, region_5, pl, gross_revenue
"""    

emea_country_dollar_mix_currency = spark.sql(emea_country_dollar_mix_currency)
emea_country_dollar_mix_currency.createOrReplaceTempView("emea_country_dollar_mix_currency")



emea_combined_dollar_mix = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    SUM(country_rev_mix) AS country_rev_mix,
    0 AS country_currency_mix
FROM emea_country_dollar_mix
GROUP BY cal_date, country_alpha2, region_5, pl
            
UNION ALL

SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    0 AS country_rev_mix,
    SUM(country_currency_mix) AS country_currency_mix
FROM emea_country_dollar_mix_currency
GROUP BY cal_date, country_alpha2, region_5, pl
"""

emea_combined_dollar_mix = spark.sql(emea_combined_dollar_mix)
emea_combined_dollar_mix.createOrReplaceTempView("emea_combined_dollar_mix")    


emea_combined_dollar_mix2 = f"""
SELECT 
    cal_date,
    country_alpha2,
    region_5,
    pl,
    COALESCE(SUM(country_rev_mix), 0) AS country_rev_mix,
    COALESCE(SUM(country_currency_mix), 0) AS country_currency_mix
FROM emea_combined_dollar_mix
GROUP BY cal_date, country_alpha2, region_5, pl        
"""

emea_combined_dollar_mix2 = spark.sql(emea_combined_dollar_mix2)
emea_combined_dollar_mix2.createOrReplaceTempView("emea_combined_dollar_mix2")    


emea_country_unit_mix = f"""    
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    0 AS country_rev_mix,
    0 AS country_currency_mix,
    CASE
        WHEN SUM(revenue_units) OVER (PARTITION BY cal_date, region_5, pl) = 0 THEN NULL
        ELSE revenue_units / SUM(revenue_units) OVER (PARTITION BY cal_date, region_5, pl)
    END AS country_unit_mix
FROM emea_country_mix_by_PL_units_data
GROUP BY cal_date, country_alpha2, region_5, pl, revenue_units
"""

emea_country_unit_mix = spark.sql(emea_country_unit_mix)
emea_country_unit_mix.createOrReplaceTempView("emea_country_unit_mix")    


emea_combined_mix_3 = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    SUM(country_rev_mix) AS country_rev_mix,
    SUM(country_currency_mix) AS country_currency_mix,
    0 AS country_unit_mix
FROM emea_combined_dollar_mix2
GROUP BY cal_date, country_alpha2, region_5, pl    
            
UNION ALL
    
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    0 AS country_rev_mix,
    0 AS country_currency_mix,
    SUM(country_unit_mix) AS country_unit_mix
FROM emea_country_unit_mix
GROUP BY cal_date, country_alpha2, region_5, pl            
"""

emea_combined_mix_3 = spark.sql(emea_combined_mix_3)
emea_combined_mix_3.createOrReplaceTempView("emea_combined_mix_3")    


emea_combined_mix_4 = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    COALESCE(SUM(country_rev_mix), 0) AS country_rev_mix,
    COALESCE(SUM(country_currency_mix), 0) AS country_currency_mix,                
    COALESCE(SUM(country_unit_mix), 0) AS country_unit_mix
FROM emea_combined_mix_3
GROUP BY cal_date, country_alpha2, region_5, pl        
"""

emea_combined_mix_4 = spark.sql(emea_combined_mix_4)
emea_combined_mix_4.createOrReplaceTempView("emea_combined_mix_4")    


emea_salesprod_xcode_mash1 = f"""
SELECT
    sp.cal_date,
    country_alpha2,
    sp.region_5,
    sp.pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue * COALESCE(country_rev_mix, 0)) AS gross_revenue,
    SUM(net_currency * COALESCE(country_currency_mix, 0)) AS net_currency,
    SUM(contractual_discounts * COALESCE(country_rev_mix, 0)) AS contractual_discounts,
    SUM(discretionary_discounts * COALESCE(country_rev_mix, 0)) AS discretionary_discounts,                
    SUM(warranty * COALESCE(country_rev_mix, 0)) AS warranty,
    SUM(other_cos * COALESCE(country_rev_mix, 0)) AS other_cos,
    SUM(total_cos * COALESCE(country_rev_mix, 0)) AS total_cos,
    SUM(revenue_units * COALESCE(country_unit_mix, 0)) AS revenue_units
FROM salesprod_emea_with_xcodes AS sp
LEFT JOIN emea_combined_mix_4 AS cd ON (sp.cal_date = cd.cal_date AND sp.region_5 = cd.region_5 AND sp.pl = cd.pl)
GROUP BY sp.cal_date, country_alpha2, sp.region_5, sp.pl, sales_product_number, ce_split
"""        
    
emea_salesprod_xcode_mash1 = spark.sql(emea_salesprod_xcode_mash1)
emea_salesprod_xcode_mash1.createOrReplaceTempView("emea_salesprod_xcode_mash1")    


emea_salesprod_xcode_fix1 = f"""    
SELECT
    cal_date,
    country_alpha2,
    'USD' AS currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,    
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM emea_salesprod_xcode_mash1
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, region_5
"""

emea_salesprod_xcode_fix1 = spark.sql(emea_salesprod_xcode_fix1)
emea_salesprod_xcode_fix1.createOrReplaceTempView("emea_salesprod_xcode_fix1")    


emea_salesprod_xcode_adjusted = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,                
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_emea_with_country_detail
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency
            
UNION ALL
            
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,            
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM emea_salesprod_xcode_fix1
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency
"""

emea_salesprod_xcode_adjusted = spark.sql(emea_salesprod_xcode_adjusted)
emea_salesprod_xcode_adjusted.createOrReplaceTempView("emea_salesprod_xcode_adjusted")    


emea_salesprod_xcode_adjusted2 = f"""
SELECT
    cal_date,
    country_alpha2,
    CASE
        WHEN sales_product_number LIKE 'MPS%' THEN 'USD'
        ELSE currency
    END AS currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,                            
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM emea_salesprod_xcode_adjusted
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency
"""

emea_salesprod_xcode_adjusted2 = spark.sql(emea_salesprod_xcode_adjusted2)
emea_salesprod_xcode_adjusted2.createOrReplaceTempView("emea_salesprod_xcode_adjusted2")    

# COMMAND ----------

edw_document_currency_2023_restatements = f"""
SELECT
    cal_date,
    Fiscal_year_qtr,
    Fiscal_yr,
    CASE
        WHEN pl = '65' THEN 'UD'
        WHEN pl = 'EO' THEN 'GL'
        WHEN pl = 'GM' THEN 'K6'
        ELSE pl
    END AS pl,
    country_alpha2,
    region_5,
    document_currency_code,
    SUM(revenue) AS revenue -- at net revenue level but sources does not have hedge, so equivalent to revenue before hedge
FROM fin_stage.odw_document_currency doc
LEFT JOIN mdm.calendar cal ON doc.cal_date = cal.Date
WHERE 1=1
    AND revenue <> 0
    AND country_alpha2 <> 'XW'
    AND cal_date > '2021-10-01'
    AND day_of_month = 1
    --AND cal_date = (SELECT MAX(cal_date) FROM fin_stage.odw_document_currency)
GROUP BY cal_date,
    Fiscal_year_qtr,
    Fiscal_yr,
    pl,
    country_alpha2,
    region_5,
    document_currency_code
"""

edw_document_currency_2023_restatements = spark.sql(edw_document_currency_2023_restatements)
edw_document_currency_2023_restatements.createOrReplaceTempView("edw_document_currency_2023_restatements")


edw_document_currency_raw = f"""
SELECT
    cal_date,
    Fiscal_year_qtr,
    Fiscal_yr,
    pl,
    country_alpha2,
    region_5,
    document_currency_code,
    SUM(revenue) AS revenue 
FROM edw_document_currency_2023_restatements
GROUP BY cal_date,
    Fiscal_year_qtr,
    Fiscal_yr,
    pl,
    country_alpha2,
    region_5,
    document_currency_code
"""

edw_document_currency_raw = spark.sql(edw_document_currency_raw)
edw_document_currency_raw.createOrReplaceTempView("edw_document_currency_raw")


country_revenue = f"""
-- where is revenue at a country level essentially zero?
SELECT
    cal_date,
    pl,
    country_alpha2,
    CONCAT(cal_date, country_alpha2, pl) as filter_me,
    SUM(revenue) AS revenue
FROM edw_document_currency_raw
WHERE 1=1    
    AND country_alpha2 NOT LIKE 'X%' -- X-codes will be set to USD per finance 
GROUP BY cal_date, country_alpha2, pl
"""

country_revenue = spark.sql(country_revenue)
country_revenue.createOrReplaceTempView("country_revenue")


above_zero_country_revenue1 = f"""
SELECT
    cal_date,
    pl,
    country_alpha2,
    filter_me,
    SUM(revenue) AS revenue
FROM country_revenue
WHERE 1=1
    AND revenue > 100
GROUP BY cal_date, country_alpha2, pl, filter_me
"""

above_zero_country_revenue1 = spark.sql(above_zero_country_revenue1)
above_zero_country_revenue1.createOrReplaceTempView("above_zero_country_revenue1")


edw_document_currency1 = f"""
-- eliminate corner cases 
SELECT
    cal_date,
    Fiscal_Year_Qtr,
    Fiscal_Yr,
    pl,
    country_alpha2,
    region_5,
    document_currency_code,
    SUM(revenue) AS revenue
FROM edw_document_currency_raw
WHERE 1=1
    AND CONCAT(cal_date, country_alpha2, pl) IN (select distinct filter_me from above_zero_country_revenue1)
GROUP BY cal_date, country_alpha2, document_currency_code, pl, region_5, Fiscal_Year_Qtr, Fiscal_Yr
"""

edw_document_currency1 = spark.sql(edw_document_currency1)
edw_document_currency1.createOrReplaceTempView("edw_document_currency1")


doc_currency_mix1 = f"""
-- calculate the currency as a mix of a region
SELECT
    cal_date,
    Fiscal_Year_Qtr,
    Fiscal_Yr,
    pl,
    region_5,
    document_currency_code,
    CONCAT(cal_date, pl, region_5, document_currency_code) AS doc_date,
    SUM(revenue) AS revenue,
    CASE
        WHEN SUM(revenue) OVER (PARTITION BY cal_date, region_5, pl) = 0 THEN NULL
        ELSE revenue / SUM(revenue) OVER (PARTITION BY cal_date, region_5, pl)
    END AS document_currency_mix1_ratio
FROM edw_document_currency1
GROUP BY cal_date,
    Fiscal_Year_Qtr,
    Fiscal_Yr,
    pl,
    region_5,
    document_currency_code,
    revenue
"""


doc_currency_mix1 = spark.sql(doc_currency_mix1)
doc_currency_mix1.createOrReplaceTempView("doc_currency_mix1")

doc_currency_mix2_absval = f"""
-- absolute value
SELECT
    cal_date,
    Fiscal_Year_Qtr,
    Fiscal_Yr,
    pl,
    region_5,
    document_currency_code,
    doc_date,
    revenue,
    ABS(CAST(document_currency_mix1_ratio as decimal (11, 5))) AS document_currency_mix1_ratio
FROM doc_currency_mix1
"""

doc_currency_mix2_absval = spark.sql(doc_currency_mix2_absval)
doc_currency_mix2_absval.createOrReplaceTempView("doc_currency_mix2_absval")


doc_currency_mix3 = f"""
-- exclude outlier currencies
SELECT
    cal_date,
    Fiscal_Year_Qtr,
    Fiscal_Yr,
    pl,
    region_5,
    document_currency_code,
    doc_date,
    SUM(revenue) AS revenue
FROM doc_currency_mix2_absval
WHERE document_currency_mix1_ratio > 0.01
GROUP BY cal_date,
    Fiscal_Year_Qtr,
    Fiscal_Yr,
    pl,
    region_5,
    document_currency_code,
    doc_date
"""

doc_currency_mix3 = spark.sql(doc_currency_mix3)
doc_currency_mix3.createOrReplaceTempView("doc_currency_mix3")


edw_doc_currency_non_EU = f"""
-- edw currency mix excluding EMEA
SELECT
    cal_date,
    country_alpha2,
    region_5,
    document_currency_code,
    CONCAT(cal_date, pl, region_5, document_currency_code) AS doc_date,
    pl,
    SUM(revenue) AS revenue
FROM edw_document_currency1
WHERE 1=1
    AND region_5 <> 'EU'
GROUP BY cal_date, country_alpha2, document_currency_code, pl, revenue, region_5
"""

edw_doc_currency_non_EU = spark.sql(edw_doc_currency_non_EU)
edw_doc_currency_non_EU.createOrReplaceTempView("edw_doc_currency_non_EU")


country_currency_mix = f"""
-- calc currency mix for countries
SELECT
    cal_date,
    country_alpha2,
    region_5,
    document_currency_code,
    pl,
    SUM(revenue) AS revenue,
    CASE
        WHEN SUM(revenue) OVER (PARTITION BY cal_date, country_alpha2, pl) = 0 THEN NULL
        ELSE revenue / SUM(revenue) OVER (PARTITION BY cal_date, country_alpha2, pl)
    END AS country_proxy_mix
FROM edw_doc_currency_non_EU
WHERE 1=1
    AND doc_date IN (SELECT distinct doc_date FROM doc_currency_mix3)
GROUP BY cal_date, country_alpha2, document_currency_code, pl, revenue, region_5
"""

country_currency_mix = spark.sql(country_currency_mix)
country_currency_mix.createOrReplaceTempView("country_currency_mix")


country_currency_mix2 = f"""
-- mix only
SELECT
    cal_date,
    country_alpha2,
    document_currency_code as currency,
    region_5,
    pl,
    SUM(country_proxy_mix) as country_currency_mix
FROM country_currency_mix c
WHERE 1=1
GROUP BY cal_date, country_alpha2, document_currency_code, pl, region_5
"""

country_currency_mix2 = spark.sql(country_currency_mix2)
country_currency_mix2.createOrReplaceTempView("country_currency_mix2")


country_currency_mix3 = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    country_currency_mix
FROM country_currency_mix2 c

"""

country_currency_mix3 = spark.sql(country_currency_mix3)
country_currency_mix3.createOrReplaceTempView("country_currency_mix3")


country_currency_mix4 = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    COALESCE(SUM(country_currency_mix), 0) as country_currency_mix
FROM country_currency_mix3 c
GROUP BY cal_date, country_alpha2, currency, region_5, pl
"""

country_currency_mix4 = spark.sql(country_currency_mix4)
country_currency_mix4.createOrReplaceTempView("country_currency_mix4")


country_currency_mix5 = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    SUM(country_currency_mix) as country_currency_mix
FROM country_currency_mix4
WHERE country_alpha2 NOT LIKE 'X%'
GROUP BY cal_date, country_alpha2, currency, region_5, pl
"""

country_currency_mix5 = spark.sql(country_currency_mix5)
country_currency_mix5.createOrReplaceTempView("country_currency_mix5")


salesprod_with_country_detail_ams_ap = f"""
-- transition back to salesprod code
SELECT
    c.cal_date,
    c.country_alpha2,
    currency,
    c.region_5,
    c.pl,
    sales_product_number,
    ce_split,        
    country_currency_mix,
    SUM(gross_revenue * COALESCE(country_currency_mix, 1)) AS gross_revenue,
    SUM(net_currency * COALESCE(country_currency_mix, 1)) AS net_currency,
    SUM(contractual_discounts * COALESCE(country_currency_mix, 1)) AS contractual_discounts,
    SUM(discretionary_discounts * COALESCE(country_currency_mix, 1)) AS discretionary_discounts,
    SUM(warranty * COALESCE(country_currency_mix, 1)) AS warranty,
    SUM(other_cos * COALESCE(country_currency_mix, 1)) AS other_cos,
    SUM(total_cos * COALESCE(country_currency_mix, 1)) AS total_cos,
    SUM(revenue_units * COALESCE(country_currency_mix, 1)) AS revenue_units
FROM salesprod_data_normalish_items c
LEFT JOIN country_currency_mix4 mix ON
    c.cal_date = mix.cal_date AND
    c.country_alpha2 = mix.country_alpha2 AND
    c.pl = mix.pl
WHERE c.region_5 <> 'EU'
GROUP BY c.cal_date, c.country_alpha2, c.region_5, c.pl, sales_product_number, ce_split, currency, country_currency_mix
"""    

salesprod_with_country_detail_ams_ap = spark.sql(salesprod_with_country_detail_ams_ap)
salesprod_with_country_detail_ams_ap.createOrReplaceTempView("salesprod_with_country_detail_ams_ap")


salesprod_with_country_detail_exclude_emea = f"""
SELECT
    cal_date,
    country_alpha2,
    CASE
        WHEN currency is null THEN 'USD'
        WHEN country_alpha2 LIKE 'X%' THEN 'USD'
        ELSE currency
    END AS currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_with_country_detail_ams_ap
WHERE 1=1
GROUP BY cal_date, country_alpha2, currency, region_5, pl, sales_product_number, ce_split
"""        
        
salesprod_with_country_detail_exclude_emea = spark.sql(salesprod_with_country_detail_exclude_emea)
salesprod_with_country_detail_exclude_emea.createOrReplaceTempView("salesprod_with_country_detail_exclude_emea")


odw_salesprod_with_country_detail3 = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units),0) AS revenue_units
FROM salesprod_with_country_detail_exclude_emea
GROUP BY cal_date, country_alpha2, currency, region_5, pl, sales_product_number, ce_split
"""

odw_salesprod_with_country_detail3 = spark.sql(odw_salesprod_with_country_detail3)
odw_salesprod_with_country_detail3.createOrReplaceTempView("odw_salesprod_with_country_detail3")

# COMMAND ----------

# Write out salesprod_with_country_detail3 to its delta table target.
odw_salesprod_with_country_detail3.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/odw_salesprod_with_country_detail3")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.odw_salesprod_with_country_detail3 USING DELTA LOCATION '/tmp/delta/fin_stage/odw_salesprod_with_country_detail3'")

#spark.table("fin_stage.odw_salesprod_with_country_detail3").createOrReplaceTempView("odw_salesprod_with_country_detail3")

# COMMAND ----------

spark.table("fin_stage.odw_salesprod_with_country_detail3").createOrReplaceTempView("odw_salesprod_with_country_detail3")

# COMMAND ----------

salesprod_with_country_detail4 = f"""    
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,    
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM odw_salesprod_with_country_detail3  AS act
WHERE 1=1
    AND country_alpha2 NOT IN (
                                SELECT country_alpha2
                                FROM mdm.iso_country_code_xref
                                WHERE country_alpha2 LIKE 'X%'
                                AND country_alpha2 != 'XK'
                            )
GROUP BY cal_date, country_alpha2, currency, region_5, pl, sales_product_number, ce_split
"""

salesprod_with_country_detail4 = spark.sql(salesprod_with_country_detail4)
salesprod_with_country_detail4.createOrReplaceTempView("salesprod_with_country_detail4")


salesprod_with_xcodes_apams = f"""
SELECT
    cal_date,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM odw_salesprod_with_country_detail3
WHERE 1=1
    AND country_alpha2 IN (
                                SELECT country_alpha2
                                FROM mdm.iso_country_code_xref
                                WHERE country_alpha2 LIKE 'X%'
                                AND country_alpha2 != 'XK'
                            )
GROUP BY cal_date, region_5, pl, sales_product_number, ce_split
"""        

salesprod_with_xcodes_apams = spark.sql(salesprod_with_xcodes_apams)
salesprod_with_xcodes_apams.createOrReplaceTempView("salesprod_with_xcodes_apams")


country_mix_by_PL_dollar_data_apams = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue
FROM salesprod_with_country_detail4
WHERE gross_revenue != 0
AND pl != 'GD'
AND country_alpha2 NOT IN ('BR', 'MX')
GROUP BY cal_date, country_alpha2, region_5, pl
"""        

country_mix_by_PL_dollar_data_apams = spark.sql(country_mix_by_PL_dollar_data_apams)
country_mix_by_PL_dollar_data_apams.createOrReplaceTempView("country_mix_by_PL_dollar_data_apams")


country_mix_by_PL_dollar_data_no_usa_apams = f"""         
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue
FROM salesprod_with_country_detail4
WHERE gross_revenue != 0
AND country_alpha2 != 'US'
AND pl != 'GD'
AND country_alpha2 NOT IN ('BR', 'MX')
GROUP BY cal_date, country_alpha2, region_5, pl
"""    

country_mix_by_PL_dollar_data_no_usa_apams = spark.sql(country_mix_by_PL_dollar_data_no_usa_apams)
country_mix_by_PL_dollar_data_no_usa_apams.createOrReplaceTempView("country_mix_by_PL_dollar_data_no_usa_apams")


country_mix_by_PL_units_data_apams = f"""        
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    CAST(COALESCE(SUM(revenue_units), 0) as numeric) AS revenue_units
FROM salesprod_with_country_detail4
WHERE revenue_units != 0
AND pl != 'GD'
AND country_alpha2 NOT IN ('BR', 'MX')
GROUP BY cal_date, country_alpha2, region_5, pl
"""        

country_mix_by_PL_units_data_apams = spark.sql(country_mix_by_PL_units_data_apams)
country_mix_by_PL_units_data_apams.createOrReplaceTempView("country_mix_by_PL_units_data_apams")


country_dollar_mix_apams = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    CASE
        WHEN SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5, pl) = 0 THEN NULL
        ELSE gross_revenue / SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5, pl)
    END AS country_rev_mix
FROM country_mix_by_PL_dollar_data_apams
GROUP BY cal_date, country_alpha2, region_5, pl, gross_revenue
"""        

country_dollar_mix_apams = spark.sql(country_dollar_mix_apams)
country_dollar_mix_apams.createOrReplaceTempView("country_dollar_mix_apams")


country_dollar_mix_currency = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    CASE
        WHEN SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5, pl) = 0 THEN NULL
        ELSE gross_revenue / SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5, pl)
    END AS country_currency_mix
FROM country_mix_by_PL_dollar_data_no_usa_apams
GROUP BY cal_date, country_alpha2, region_5, pl, gross_revenue
"""

country_dollar_mix_currency = spark.sql(country_dollar_mix_currency)
country_dollar_mix_currency.createOrReplaceTempView("country_dollar_mix_currency")


combined_dollar_mix_apams = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    SUM(country_rev_mix) AS country_rev_mix,
    0 AS country_currency_mix
FROM country_dollar_mix_apams
GROUP BY cal_date, country_alpha2, region_5, pl
            
UNION ALL

SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    0 AS country_rev_mix,
    SUM(country_currency_mix) AS country_currency_mix
FROM country_dollar_mix_currency
GROUP BY cal_date, country_alpha2, region_5, pl
"""

combined_dollar_mix_apams = spark.sql(combined_dollar_mix_apams)
combined_dollar_mix_apams.createOrReplaceTempView("combined_dollar_mix_apams")


combined_dollar_mix2_apams = f"""
SELECT 
    cal_date,
    country_alpha2,
    region_5,
    pl,
    COALESCE(SUM(country_rev_mix), 0) AS country_rev_mix,
    COALESCE(SUM(country_currency_mix), 0) AS country_currency_mix
FROM combined_dollar_mix_apams
GROUP BY cal_date, country_alpha2, region_5, pl        
"""

combined_dollar_mix2_apams = spark.sql(combined_dollar_mix2_apams)
combined_dollar_mix2_apams.createOrReplaceTempView("combined_dollar_mix2_apams")


country_unit_mix = f"""        
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    0 AS country_rev_mix,
    0 AS country_currency_mix,
    CASE
        WHEN SUM(revenue_units) OVER (PARTITION BY cal_date, region_5, pl) = 0 THEN NULL
        ELSE revenue_units / SUM(revenue_units) OVER (PARTITION BY cal_date, region_5, pl)
    END AS country_unit_mix
FROM country_mix_by_PL_units_data_apams
GROUP BY cal_date, country_alpha2, region_5, pl, revenue_units
"""        

country_unit_mix = spark.sql(country_unit_mix)
country_unit_mix.createOrReplaceTempView("country_unit_mix")


combined_mix_3_apams = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    SUM(country_rev_mix) AS country_rev_mix,
    SUM(country_currency_mix) AS country_currency_mix,
    0 AS country_unit_mix
FROM combined_dollar_mix2_apams
GROUP BY cal_date, country_alpha2, region_5, pl    
            
UNION ALL
            
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    0 AS country_rev_mix,
    0 AS country_currency_mix,
    SUM(country_unit_mix) AS country_unit_mix
FROM country_unit_mix
GROUP BY cal_date, country_alpha2, region_5, pl            
"""

combined_mix_3_apams = spark.sql(combined_mix_3_apams)
combined_mix_3_apams.createOrReplaceTempView("combined_mix_3_apams")


combined_mix_4_apams = f"""
SELECT cal_date,
    country_alpha2,
    region_5,
    pl,
    COALESCE(SUM(country_rev_mix), 0) AS country_rev_mix,
    COALESCE(SUM(country_currency_mix), 0) AS country_currency_mix,                
    COALESCE(SUM(country_unit_mix), 0) AS country_unit_mix
FROM combined_mix_3_apams
GROUP BY cal_date, country_alpha2, region_5, pl        
"""

combined_mix_4_apams = spark.sql(combined_mix_4_apams)
combined_mix_4_apams.createOrReplaceTempView("combined_mix_4_apams")


salesprod_xcode_mash1_apams = f"""
SELECT
    sp.cal_date,
    country_alpha2,
    sp.region_5,
    sp.pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue * COALESCE(country_rev_mix, 0)) AS gross_revenue,
    SUM(net_currency * COALESCE(country_currency_mix, 0)) AS net_currency,
    SUM(contractual_discounts * COALESCE(country_rev_mix, 0)) AS contractual_discounts,
    SUM(discretionary_discounts * COALESCE(country_rev_mix, 0)) AS discretionary_discounts,
    SUM(warranty * COALESCE(country_rev_mix, 0)) AS warranty,    
    SUM(other_cos * COALESCE(country_rev_mix, 0)) AS other_cos,
    SUM(total_cos * COALESCE(country_rev_mix, 0)) AS total_cos,
    SUM(revenue_units * COALESCE(country_unit_mix, 0)) AS revenue_units
FROM salesprod_with_xcodes_apams AS sp
LEFT JOIN combined_mix_4_apams AS cd ON (sp.cal_date = cd.cal_date AND sp.region_5 = cd.region_5 AND sp.pl = cd.pl)
GROUP BY sp.cal_date, country_alpha2, sp.region_5, sp.pl, sales_product_number, ce_split
"""

salesprod_xcode_mash1_apams = spark.sql(salesprod_xcode_mash1_apams)
salesprod_xcode_mash1_apams.createOrReplaceTempView("salesprod_xcode_mash1_apams")


salesprod_xcode_fix1_apams = f"""
SELECT
    cal_date,
    country_alpha2,
    'USD' AS currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,    
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM salesprod_xcode_mash1_apams
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, region_5
"""

salesprod_xcode_fix1_apams = spark.sql(salesprod_xcode_fix1_apams)
salesprod_xcode_fix1_apams.createOrReplaceTempView("salesprod_xcode_fix1_apams")


salesprod_xcode_adjusted = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_with_country_detail4
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_xcode_fix1_apams
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM emea_salesprod_xcode_adjusted2
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency
"""

salesprod_xcode_adjusted = spark.sql(salesprod_xcode_adjusted)
salesprod_xcode_adjusted.createOrReplaceTempView("salesprod_xcode_adjusted")


salesprod_xcode_adjusted2 = f"""
SELECT
    cal_date,
    country_alpha2,
    CASE
        WHEN sales_product_number LIKE 'mps%' THEN 'USD'
        ELSE currency
    END AS currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,                            
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,  
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM salesprod_xcode_adjusted
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency
"""

salesprod_xcode_adjusted2 = spark.sql(salesprod_xcode_adjusted2)
salesprod_xcode_adjusted2.createOrReplaceTempView("salesprod_xcode_adjusted2")


salesprod_normal_with_currency1 = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,  
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM salesprod_xcode_adjusted2
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, currency, region_5
"""

salesprod_normal_with_currency1 = spark.sql(salesprod_normal_with_currency1)
salesprod_normal_with_currency1.createOrReplaceTempView("salesprod_normal_with_currency1")


data_exclude_net_currency = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,    
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_normal_with_currency1
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency
"""

data_exclude_net_currency = spark.sql(data_exclude_net_currency)
data_exclude_net_currency.createOrReplaceTempView("data_exclude_net_currency")


net_currency_set_USD = f"""
SELECT
    cal_date,
    country_alpha2,
    'USD' AS currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    COALESCE(SUM(net_currency), 0) AS net_currency
FROM salesprod_normal_with_currency1
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency
"""

net_currency_set_USD = spark.sql(net_currency_set_USD)
net_currency_set_USD.createOrReplaceTempView("net_currency_set_USD")


data_set_reunioned = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    0 AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty, 
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM data_exclude_net_currency
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    0 AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    0 AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS warranty,
    0 AS other_cos,
    0 AS total_cos,
    0 AS revenue_units
FROM net_currency_set_USD
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency
"""

data_set_reunioned = spark.sql(data_set_reunioned)
data_set_reunioned.createOrReplaceTempView("data_set_reunioned")


salesprod_normal_with_currency2 = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM data_set_reunioned
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, currency, region_5
"""

salesprod_normal_with_currency2 = spark.sql(salesprod_normal_with_currency2)
salesprod_normal_with_currency2.createOrReplaceTempView("salesprod_normal_with_currency2")


odw_xcode_adjusted_data = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty, 
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM xcode_adjusted_plgd2
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty, 
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM xcode_adjusted_account_jv
GROUP BY cal_date, currency, region_5, pl, sales_product_number, ce_split, country_alpha2

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,                
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty, 
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_normal_with_currency2
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, currency, region_5
"""

odw_xcode_adjusted_data = spark.sql(odw_xcode_adjusted_data)
odw_xcode_adjusted_data.createOrReplaceTempView("odw_xcode_adjusted_data")

# COMMAND ----------

# Write out salesprod_with_country_detail3 to its delta table target. 
odw_xcode_adjusted_data.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/odw_xcode_adjusted_data")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.odw_xcode_adjusted_data USING DELTA LOCATION '/tmp/delta/fin_stage/odw_xcode_adjusted_data'")

#spark.table("fin_stage.odw_xcode_adjusted_data").createOrReplaceTempView("odw_xcode_adjusted_data")

# COMMAND ----------

spark.table("fin_stage.odw_xcode_adjusted_data").createOrReplaceTempView("odw_xcode_adjusted_data")

# COMMAND ----------

xcode_adjusted_data2 = f"""
SELECT
    cal_date,
    country_alpha2,
    CASE
        WHEN country_alpha2 = 'BR'
            AND currency <> 'USD' 
            AND (pl ='E5' OR pl ='5T' OR pl = 'EO' OR pl = 'GJ' OR pl = 'GK' OR pl = 'GM' OR pl = 'IU' OR pl = 'K6' OR pl = 'GL')
            THEN 'USD'
        ELSE currency
    END AS currency,
    region_5,
    pl,
    sales_product_number,
        ce_split,        
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units,
    COALESCE(SUM(gross_revenue), 0) + COALESCE(SUM(net_currency), 0) + COALESCE(SUM(contractual_discounts), 0) + COALESCE(SUM(warranty), 0) + COALESCE(SUM(other_cos), 0)
                    + COALESCE(SUM(discretionary_discounts), 0) + COALESCE(SUM(total_cos), 0) + COALESCE(SUM(revenue_units), 0) AS total_sums
FROM odw_xcode_adjusted_data
GROUP BY cal_date, currency, region_5, pl, sales_product_number, ce_split, country_alpha2
"""
        
xcode_adjusted_data2 = spark.sql(xcode_adjusted_data2)
xcode_adjusted_data2.createOrReplaceTempView("xcode_adjusted_data2")


salesprod_preplanet_with_currency_map1 = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    CASE
        WHEN pl = 'IX' THEN 'TX' 
        ELSE pl
    END AS pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) * -1 AS contractual_discounts,
    SUM(discretionary_discounts) * -1 AS discretionary_discounts,
    SUM(warranty) * -1 AS warranty,
    SUM(other_cos) * -1 AS other_cos,
    SUM(total_cos) * -1 AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM xcode_adjusted_data2
WHERE 1=1
AND total_sums <> 0
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, currency, region_5
"""

salesprod_preplanet_with_currency_map1 = spark.sql(salesprod_preplanet_with_currency_map1)
salesprod_preplanet_with_currency_map1.createOrReplaceTempView("salesprod_preplanet_with_currency_map1")

# COMMAND ----------

# TIE OUT TO OFFICIAL FINANCIALS

# COMMAND ----------

# because of a conflict in requirements (finance at region 5; drivers at market), we need to push "official" targets to country level
# options for mix
# EMEA (because financials don't include EMEA data at country grain)
cbm_country_actuals_mapping_mix = f"""
SELECT cal.Date AS cal_date,
    region_5,
    cbm.country_code AS country_alpha2,
    product_line_id AS pl,
    CASE
       WHEN SUM(sell_thru_usd) OVER (PARTITION BY cal.Date, region_5, product_line_id) = 0 THEN NULL
       ELSE sell_thru_usd / SUM(sell_thru_usd) OVER (PARTITION BY cal.Date, region_5, product_line_id)
    END AS country_mix
FROM fin_stage.cbm_st_data cbm
JOIN mdm.calendar cal
    ON cbm.month = cal.Date
JOIN mdm.iso_country_code_xref iso
    ON iso.country_alpha2 = cbm.country_code
WHERE 1=1
AND region_5 = 'EU'
AND sell_thru_usd > 0
GROUP BY cal.Date,
    region_5,
    cbm.country_code,
    product_line_id,
    sell_thru_usd
"""

cbm_country_actuals_mapping_mix = spark.sql(cbm_country_actuals_mapping_mix)
cbm_country_actuals_mapping_mix.createOrReplaceTempView("cbm_country_actuals_mapping_mix")


# usage share country as mix source
rdma_correction_2023_restatements_baseprod = f"""
SELECT 
    base_product_number,
    CASE
      WHEN base_product_line_code = '65' THEN 'UD'
      WHEN base_product_line_code = 'EO' THEN 'GL'
      WHEN base_product_line_code = 'GM' THEN 'K6'
      ELSE base_product_line_code
    END AS base_product_line_code
FROM mdm.rdma_base_to_sales_product_map
WHERE 1=1
"""

rdma_correction_2023_restatements_baseprod = spark.sql(rdma_correction_2023_restatements_baseprod)
rdma_correction_2023_restatements_baseprod.createOrReplaceTempView("rdma_correction_2023_restatements_baseprod")


supplies_hw_country_actuals_mapping_countries_x_region5 = f"""
SELECT cal_date,
    region_5,
    shcam.country_alpha2,
    base_product_line_code as pl,
    sum(hp_pages) as hp_pages
FROM stage.supplies_hw_country_actuals_mapping shcam
JOIN mdm.iso_country_code_xref iso
    ON iso.country_alpha2 = shcam.country_alpha2
JOIN rdma_correction_2023_restatements_baseprod rdma
    ON rdma.base_product_number = shcam.base_product_number
WHERE hp_pages > 0
GROUP BY cal_date, shcam.country_alpha2, base_product_line_code, region_5
"""

supplies_hw_country_actuals_mapping_countries_x_region5 = spark.sql(supplies_hw_country_actuals_mapping_countries_x_region5)
supplies_hw_country_actuals_mapping_countries_x_region5.createOrReplaceTempView("supplies_hw_country_actuals_mapping_countries_x_region5")


supplies_hw_country_actuals_mapping_mix = f"""
SELECT cal_date,
    region_5,
    country_alpha2,
    pl,
    CASE
       WHEN SUM(hp_pages) OVER (PARTITION BY cal_date, region_5, pl) = 0 THEN NULL
       ELSE hp_pages / SUM(hp_pages) OVER (PARTITION BY cal_date, region_5, pl)
    END AS country_mix
FROM supplies_hw_country_actuals_mapping_countries_x_region5
GROUP BY cal_date,
    region_5,
    country_alpha2,
    pl,
    hp_pages
"""

supplies_hw_country_actuals_mapping_mix = spark.sql(supplies_hw_country_actuals_mapping_mix)
supplies_hw_country_actuals_mapping_mix.createOrReplaceTempView("supplies_hw_country_actuals_mapping_mix")


# best source: use ODW detailed data to create country mix (AP, AMS)
general_ledger_mapping_mix = f"""
SELECT cal_date,
    region_5,
    fued.country_alpha2,
    pl,
    CASE
       WHEN SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5, pl) = 0 THEN NULL
       ELSE gross_revenue / SUM(gross_revenue) OVER (PARTITION BY cal_date, region_5, pl)
    END AS country_mix
FROM fin_stage.final_union_odw_data fued
LEFT JOIN mdm.iso_country_code_xref iso
    ON fued.country_alpha2 = iso.country_alpha2
WHERE fued.country_alpha2 NOT LIKE "%X%"
AND region_5 NOT IN ('EU', 'XW', 'XU')
AND fued.country_alpha2 NOT LIKE ('X%')
AND gross_revenue > 0
GROUP BY cal_date,
    region_5,
    fued.country_alpha2,
    pl,
    gross_revenue
"""

general_ledger_mapping_mix = spark.sql(general_ledger_mapping_mix)
general_ledger_mapping_mix.createOrReplaceTempView("general_ledger_mapping_mix")

# COMMAND ----------

planet_extract = f"""
SELECT 
    cal_date,
    Fiscal_Yr,
    region_5,
    pl,
    SUM(gross_revenue) AS p_gross_revenue, 
    SUM(net_currency) AS p_net_currency,  
    SUM(contractual_discounts) AS p_contractual_discounts, 
    SUM(discretionary_discounts) AS p_discretionary_discounts, 
    SUM(warranty) as p_warranty,
    SUM(other_cos) as p_other_cos,
    SUM(total_cos) AS p_total_cos
FROM fin_stage.odw_sacp_actuals AS p
JOIN mdm.calendar AS cal ON cal.Date = p.cal_date
WHERE pl IN 
    (
    SELECT DISTINCT (pl) 
    FROM mdm.product_line_xref 
    WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
        AND PL_category IN ('SUP', 'LLC')
    )
    AND Fiscal_Yr > '2016'
    AND Day_of_Month = 1 
    AND gross_revenue + net_currency + contractual_discounts + discretionary_discounts + other_cos + warranty != 0
    --AND cal_date = (SELECT MAX(cal_date) FROM IE2_Financials.ms4.odw_sacp_actuals)
    AND cal_date > '2021-10-01'
GROUP BY cal_date, pl, region_5, Fiscal_Yr
"""

planet_extract = spark.sql(planet_extract)
planet_extract.createOrReplaceTempView("planet_extract")


planet_cleaned = f"""
SELECT
    cal_date,
    Fiscal_Yr,
    region_5,
    CASE
        WHEN cal_date > '2020-10-01' AND pl = 'GM' THEN 'K6'
        WHEN cal_date > '2020-10-01' AND pl = 'EO' THEN 'GL'
        WHEN cal_date > '2020-10-01' AND pl = '65' THEN 'UD'
        ELSE pl
    END AS pl,
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts, 
    SUM(p_discretionary_discounts) AS p_discretionary_discounts, 
    SUM(p_warranty) AS p_warranty,
    SUM(p_other_cos) AS p_other_cos,
    SUM(p_total_cos) AS p_total_cos
FROM planet_extract
GROUP BY cal_date,
    Fiscal_Yr,
    region_5,
    pl
"""
        
planet_cleaned = spark.sql(planet_cleaned)
planet_cleaned.createOrReplaceTempView("planet_cleaned")


planet_targets = f"""
SELECT cal_date,
    Fiscal_Yr,
    region_5,
    pl,
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,
    SUM(p_other_cos) AS p_other_cos,
    SUM(p_total_cos) AS p_total_cos
FROM planet_cleaned
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets = spark.sql(planet_targets)
planet_targets.createOrReplaceTempView("planet_targets")

# COMMAND ----------

planet_targets_post_all_restatements_country1 = f"""
SELECT p.cal_date,
    Fiscal_Yr,
    country_alpha2,
    p.region_5,
    p.pl,    
    SUM(p_gross_revenue * country_mix) AS p_gross_revenue,
    SUM(p_net_currency  * country_mix) AS p_net_currency,
    SUM(p_contractual_discounts * country_mix) AS p_contractual_discounts,
    SUM(p_discretionary_discounts * country_mix) AS p_discretionary_discounts,
    SUM(p_warranty * country_mix) AS p_warranty,
    SUM(p_other_cos * country_mix) AS p_other_cos,
    SUM(p_total_cos * country_mix) AS p_total_cos
FROM planet_targets p
JOIN general_ledger_mapping_mix gl
    ON p.cal_date = gl.cal_date
    AND p.region_5 = gl.region_5
    AND p.pl = gl.pl
GROUP BY p.cal_date, p.region_5, p.pl, Fiscal_Yr, country_alpha2
"""

planet_targets_post_all_restatements_country1 = spark.sql(planet_targets_post_all_restatements_country1)
planet_targets_post_all_restatements_country1.createOrReplaceTempView("planet_targets_post_all_restatements_country1")


planet_targets_post_all_restatements_country2a = f"""
SELECT p.cal_date,
    Fiscal_Yr,
    p.region_5,
    p.pl,    
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,
    SUM(p_other_cos) AS p_other_cos,
    SUM(p_total_cos) AS p_total_cos
FROM planet_targets p
LEFT JOIN general_ledger_mapping_mix gl
    ON p.cal_date = gl.cal_date
    AND p.region_5 = gl.region_5
    AND p.pl = gl.pl
WHERE country_alpha2 is null
GROUP BY p.cal_date, p.region_5, p.pl, Fiscal_Yr
"""

planet_targets_post_all_restatements_country2a = spark.sql(planet_targets_post_all_restatements_country2a)
planet_targets_post_all_restatements_country2a.createOrReplaceTempView("planet_targets_post_all_restatements_country2a")


planet_targets_post_all_restatements_country2b = f"""
SELECT p.cal_date,
    Fiscal_Yr,
    country_alpha2,
    p.region_5,
    p.pl,    
    SUM(p_gross_revenue * country_mix) AS p_gross_revenue,
    SUM(p_net_currency  * country_mix) AS p_net_currency,
    SUM(p_contractual_discounts * country_mix) AS p_contractual_discounts,
    SUM(p_discretionary_discounts * country_mix) AS p_discretionary_discounts,
    SUM(p_warranty * country_mix) AS p_warranty,
    SUM(p_other_cos * country_mix) AS p_other_cos,
    SUM(p_total_cos * country_mix) AS p_total_cos
FROM planet_targets_post_all_restatements_country2a p
JOIN cbm_country_actuals_mapping_mix gl
    ON p.cal_date = gl.cal_date
    AND p.region_5 = gl.region_5
    AND p.pl = gl.pl
GROUP BY p.cal_date, p.region_5, p.pl, Fiscal_Yr, country_alpha2
"""

planet_targets_post_all_restatements_country2b = spark.sql(planet_targets_post_all_restatements_country2b)
planet_targets_post_all_restatements_country2b.createOrReplaceTempView("planet_targets_post_all_restatements_country2b")


planet_targets_post_all_restatements_country2c = f"""
SELECT p.cal_date,
    Fiscal_Yr,
    p.region_5,
    p.pl,    
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,
    SUM(p_other_cos) AS p_other_cos,
    SUM(p_total_cos) AS p_total_cos
FROM planet_targets_post_all_restatements_country2a p
LEFT JOIN cbm_country_actuals_mapping_mix gl
    ON p.cal_date = gl.cal_date
    AND p.region_5 = gl.region_5
    AND p.pl = gl.pl
WHERE country_alpha2 is null
GROUP BY p.cal_date, p.region_5, p.pl, Fiscal_Yr
"""

planet_targets_post_all_restatements_country2c = spark.sql(planet_targets_post_all_restatements_country2c)
planet_targets_post_all_restatements_country2c.createOrReplaceTempView("planet_targets_post_all_restatements_country2c")


planet_targets_post_all_restatements_country3 = f"""
SELECT cal_date,
    Fiscal_Yr,
    CASE
		WHEN region_5 = 'JP' THEN 'JP'
		WHEN region_5 = 'AP' THEN 'XI'
		WHEN region_5 = 'EU' THEN 'XA'
		WHEN region_5 = 'LA' THEN 'XH'
		WHEN region_5 = 'NA' THEN 'XG'
		ELSE 'XW' 
    END AS country_alpha2,
    region_5,
    pl,    
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,
    SUM(p_other_cos) AS p_other_cos,
    SUM(p_total_cos) AS p_total_cos
FROM planet_targets_post_all_restatements_country2c
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets_post_all_restatements_country3 = spark.sql(planet_targets_post_all_restatements_country3)
planet_targets_post_all_restatements_country3.createOrReplaceTempView("planet_targets_post_all_restatements_country3")

planet_targets_fully_restated_to_country = f"""
SELECT cal_date,
    Fiscal_Yr,
    country_alpha2,
    region_5,
    pl,    
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,
    SUM(p_other_cos) as p_other_cos,
    SUM(p_total_cos) AS p_total_cos
FROM planet_targets_post_all_restatements_country1 
GROUP BY cal_date,
    Fiscal_Yr,
    country_alpha2,
    region_5,
    pl

UNION ALL

SELECT cal_date,
    Fiscal_Yr,
    country_alpha2,
    region_5,
    pl,    
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,
    SUM(p_other_cos) as p_other_cos,
    SUM(p_total_cos) AS p_total_cos
FROM planet_targets_post_all_restatements_country3 
GROUP BY cal_date,
    Fiscal_Yr,
    country_alpha2,
    region_5,
    pl

UNION ALL

SELECT cal_date,
    Fiscal_Yr,
    country_alpha2,
    region_5,
    pl,    
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,
    SUM(p_other_cos) as p_other_cos,
    SUM(p_total_cos) AS p_total_cos
FROM planet_targets_post_all_restatements_country2b 
GROUP BY cal_date,
    Fiscal_Yr,
    country_alpha2,
    region_5,
    pl
"""

planet_targets_fully_restated_to_country = spark.sql(planet_targets_fully_restated_to_country)
planet_targets_fully_restated_to_country.createOrReplaceTempView("planet_targets_fully_restated_to_country")

# COMMAND ----------

salesprod_prep_for_planet_targets = f"""
SELECT
    sp.cal_date,
    country_alpha2,
    Fiscal_Yr,
    region_5,
    sp.pl,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos
FROM salesprod_preplanet_with_currency_map1 AS sp
JOIN mdm.calendar AS cal ON sp.cal_date = cal.Date
WHERE Fiscal_Yr > '2016'
    AND Day_of_Month = 1
GROUP BY sp.cal_date, sp.pl, region_5, Fiscal_Yr, country_alpha2
"""

salesprod_prep_for_planet_targets = spark.sql(salesprod_prep_for_planet_targets)
salesprod_prep_for_planet_targets.createOrReplaceTempView("salesprod_prep_for_planet_targets")


salesprod_add_planet = f"""
SELECT
    cal_date,
    country_alpha2,
    Fiscal_Yr,
    region_5,
    pl,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    0 AS p_gross_revenue,
    0 AS p_net_currency,
    0 AS p_contractual_discounts, 
    0 AS p_discretionary_discounts,
    0 AS p_warranty,
    0 AS p_other_cos,
    0 AS p_total_cos
FROM salesprod_prep_for_planet_targets 
GROUP BY cal_date, region_5, pl, Fiscal_Yr, country_alpha2

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    Fiscal_Yr,
    region_5,
    pl,
    0 AS gross_revenue,
    0 AS net_currency,
    0 AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS warranty,
    0 AS other_cos,
    0 AS total_cos,
    COALESCE(SUM(p_gross_revenue), 0) AS p_gross_revenue,
    COALESCE(SUM(p_net_currency), 0) AS p_net_currency,
    COALESCE(SUM(p_contractual_discounts), 0) AS p_contractual_discounts, 
    COALESCE(SUM(p_discretionary_discounts), 0) AS p_discretionary_discounts, 
    COALESCE(SUM(p_warranty), 0) AS p_warranty,
    COALESCE(SUM(p_other_cos), 0) AS p_other_cos,
    COALESCE(SUM(p_total_cos), 0) AS p_total_cos
FROM planet_targets_fully_restated_to_country
GROUP BY cal_date, region_5, pl, Fiscal_Yr, country_alpha2
"""

salesprod_add_planet = spark.sql(salesprod_add_planet)
salesprod_add_planet.createOrReplaceTempView("salesprod_add_planet")


salesprod_add_planet2 = f"""
SELECT
    cal_date,
    country_alpha2,
    Fiscal_Yr,
    region_5,
    pl,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(p_gross_revenue), 0) AS p_gross_revenue,
    COALESCE(SUM(p_net_currency), 0) AS p_net_currency,
    COALESCE(SUM(p_contractual_discounts), 0) AS p_contractual_discounts, 
    COALESCE(SUM(p_discretionary_discounts), 0) AS p_discretionary_discounts, 
    COALESCE(SUM(p_warranty), 0) AS p_warranty,
    COALESCE(SUM(p_other_cos), 0) AS p_other_cos,
    COALESCE(SUM(p_total_cos), 0) AS p_total_cos
FROM salesprod_add_planet
GROUP BY cal_date, region_5, pl, Fiscal_Yr, country_alpha2
"""

salesprod_add_planet2 = spark.sql(salesprod_add_planet2)
salesprod_add_planet2.createOrReplaceTempView("salesprod_add_planet2")


salesprod_calc_difference = f"""
SELECT
    cal_date,
    country_alpha2,
    Fiscal_Yr,
    region_5,
    pl,
    COALESCE(SUM(p_gross_revenue) - SUM(gross_revenue), 0) AS plug_gross_revenue,
    COALESCE(SUM(p_net_currency) - SUM(net_currency), 0) AS plug_net_currency,
    COALESCE(SUM(p_contractual_discounts) - SUM(contractual_discounts), 0) AS plug_contractual_discounts,
    COALESCE(SUM(p_discretionary_discounts) - SUM(discretionary_discounts), 0) AS plug_discretionary_discounts,
    COALESCE(SUM(p_warranty) - SUM(warranty), 0) AS plug_warranty,
    COALESCE(SUM(p_other_cos) - SUM(other_cos), 0) AS plug_other_cos,
    COALESCE(SUM(p_total_cos) - SUM(total_cos), 0) AS plug_total_cos
FROM salesprod_add_planet
GROUP BY cal_date, Fiscal_Yr, region_5, pl, country_alpha2
"""

salesprod_calc_difference = spark.sql(salesprod_calc_difference)
salesprod_calc_difference.createOrReplaceTempView("salesprod_calc_difference")


planet_tieout = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    COALESCE(SUM(plug_gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(plug_net_currency), 0) AS net_currency,
    COALESCE(SUM(plug_contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(plug_discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(plug_warranty), 0) AS warranty,    
    COALESCE(SUM(plug_other_cos), 0) AS other_cos,
    COALESCE(SUM(plug_total_cos), 0) AS total_cos
FROM salesprod_calc_difference 
GROUP BY cal_date, region_5, pl, country_alpha2
"""

planet_tieout = spark.sql(planet_tieout)
planet_tieout.createOrReplaceTempView("planet_tieout")


planet_tieout2 = f"""
SELECT
    cal_date,
    region_5,
    country_alpha2,
    pl,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos
FROM planet_tieout 
GROUP BY cal_date, pl, region_5, country_alpha2
"""

planet_tieout2 = spark.sql(planet_tieout2)
planet_tieout2.createOrReplaceTempView("planet_tieout2")


planet_adjust2 = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    'EDW_TIE_TO_PLANET' AS sales_product_number,
    'TRAD' AS ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM planet_tieout2
GROUP BY cal_date, country_alpha2, pl
"""

planet_adjust2 = spark.sql(planet_adjust2)
planet_adjust2.createOrReplaceTempView("planet_adjust2")


planet_adjusts = f"""
SELECT 
                cal_date,
                country_alpha2,
                pl,
                sales_product_number,
                ce_split,
                SUM(gross_revenue) AS gross_revenue,
                SUM(net_currency) AS net_currency,
                SUM(contractual_discounts) AS contractual_discounts,
                SUM(discretionary_discounts) AS discretionary_discounts,
                SUM(total_cos) AS total_cos,
                SUM(warranty) AS warranty,
                SUM(other_cos) AS other_cos,
                0 AS revenue_units
            FROM planet_adjust2
            GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split
"""

planet_adjusts = spark.sql(planet_adjusts)
planet_adjusts.createOrReplaceTempView("planet_adjusts")


planet_adjusts_supplies = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    CASE
        WHEN pl = 'GD' THEN 'I-INK'
        ELSE 'TRAD'
    END AS ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM planet_adjusts
WHERE pl IN 
    (
    SELECT DISTINCT (pl) 
    FROM mdm.product_line_xref 
    WHERE Technology IN ('INK', 'LASER', 'PWA', 'LF')
        AND PL_category IN ('SUP')
    )
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split
"""

planet_adjusts_supplies = spark.sql(planet_adjusts_supplies)
planet_adjusts_supplies.createOrReplaceTempView("planet_adjusts_supplies")


planet_adjusts_llc = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM planet_adjusts pa
JOIN mdm.calendar cal ON pa.cal_date = cal.Date
WHERE Day_of_month = 1
    AND pl IN 
                (
                SELECT DISTINCT (pl) 
                FROM mdm.product_line_xref 
                WHERE Technology IN ('LLCS')
                    AND PL_category IN ('LLC')
                )
    AND Fiscal_Yr NOT IN ('2016', '2017', '2018')
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split
"""

planet_adjusts_llc = spark.sql(planet_adjusts_llc)
planet_adjusts_llc.createOrReplaceTempView("planet_adjusts_llc")


planet_adjusts_final = f"""
SELECT cal_date,
    country_alpha2,
    'USD' AS currency,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM planet_adjusts_supplies
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split

UNION ALL

SELECT cal_date,
    country_alpha2,
    'USD' AS currency,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM planet_adjusts_llc
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split
"""
    
planet_adjusts_final = spark.sql(planet_adjusts_final)
planet_adjusts_final.createOrReplaceTempView("planet_adjusts_final")


all_salesprod_final_pre = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_preplanet_with_currency_map1
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, currency
"""

all_salesprod_final_pre = spark.sql(all_salesprod_final_pre)
all_salesprod_final_pre.createOrReplaceTempView("all_salesprod_final_pre")


all_salesprod = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM ALL_salesprod_final_pre
GROUP BY cal_date, country_alpha2, currency, pl, sales_product_number, ce_split

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    currency,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM planet_adjusts_final            
GROUP BY cal_date, country_alpha2, currency, pl, sales_product_number, ce_split

"""

all_salesprod = spark.sql(all_salesprod)
all_salesprod.createOrReplaceTempView("all_salesprod")


all_salesprod2 = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
                -- row clean up
                SUM(gross_revenue) + SUM(net_currency) + SUM(contractual_discounts) + SUM(discretionary_discounts) + SUM(total_cos) + SUM(warranty) + SUM(revenue_units) AS total_rows
FROM ALL_salesprod
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, currency
"""

all_salesprod2 = spark.sql(all_salesprod2)
all_salesprod2.createOrReplaceTempView("all_salesprod2")

# COMMAND ----------

all_salesprod3 = f"""
SELECT
    sp.cal_date,
    country_alpha2,
    currency,
    sp.pl,
    l5_description,
    sales_product_number,
    ce_split AS customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(gross_revenue) + SUM(net_currency) - SUM(contractual_discounts) - SUM(discretionary_discounts) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_revenue) + SUM(net_currency) - SUM(contractual_discounts) - SUM(discretionary_discounts) - SUM(total_cos) AS gross_profit,
    SUM(revenue_units) AS revenue_units,    
    '{addversion_info[1]}' AS load_date,
    '{addversion_info[0]}' AS version
FROM ALL_salesprod2 AS sp
JOIN mdm.product_line_xref AS plx ON sp.pl = plx.pl 
WHERE total_rows <> 0
GROUP BY sp.cal_date, country_alpha2, sp.pl, sales_product_number, ce_split, l5_description, currency
"""

all_salesprod3 = spark.sql(all_salesprod3)
all_salesprod3.createOrReplaceTempView("all_salesprod3")


all_salesprod4 = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    pl,
    l5_description,
    sales_product_number,
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    version,
    load_date
FROM ALL_salesprod3
WHERE pl IN (
        SELECT DISTINCT (pl) 
        FROM mdm.product_line_xref 
        WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
        AND PL_category IN ('SUP', 'LLC')
        )
GROUP BY cal_date, country_alpha2, pl, sales_product_number, customer_engagement, l5_description, version, load_date, currency
"""

all_salesprod4 = spark.sql(all_salesprod4)
all_salesprod4.createOrReplaceTempView("all_salesprod4")


salesprod_planet_precurrency = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    pl,
    l5_description,
    sales_product_number,
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    version,
    load_date
FROM ALL_salesprod4
GROUP BY cal_date, country_alpha2, pl, sales_product_number, customer_engagement, l5_description, version, load_date, currency
"""

salesprod_planet_precurrency = spark.sql(salesprod_planet_precurrency)
salesprod_planet_precurrency.createOrReplaceTempView("salesprod_planet_precurrency")

# COMMAND ----------

# special case due to business model differences :: currency plgd application
data_without_plgd_dollars = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    pl,
    l5_description,
    sales_product_number,
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    version,
    load_date
FROM  salesprod_planet_precurrency
WHERE (pl <> 'GD' OR sales_product_number NOT LIKE 'EDW%')
GROUP BY cal_date,
    country_alpha2,
    currency,
    pl,
    l5_description,
    sales_product_number,
    customer_engagement,
    version,
    load_date
"""

data_without_plgd_dollars = spark.sql(data_without_plgd_dollars)
data_without_plgd_dollars.createOrReplaceTempView("data_without_plgd_dollars")


data_with_plgd_dollars_only = f"""
SELECT
    cal_date,
    pre.country_alpha2,
    region_5,
    pl,
    l5_description,
    sales_product_number,
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units, 
    pre.version,
    pre.load_date
FROM salesprod_planet_precurrency pre
LEFT JOIN mdm.iso_country_code_xref iso ON pre.country_alpha2 = iso.country_alpha2
WHERE pl = 'GD' AND sales_product_number LIKE 'EDW%'
GROUP BY cal_date,
    pre.country_alpha2,
    pl,
    l5_description,
    sales_product_number,
    customer_engagement,
    pre.version,
    pre.load_date,
    region_5
"""

data_with_plgd_dollars_only = spark.sql(data_with_plgd_dollars_only)
data_with_plgd_dollars_only.createOrReplaceTempView("data_with_plgd_dollars_only")


data_with_plgd_dollars_only2 = f"""
SELECT
    cal_date,
    region_5,
    pl,
    l5_description,
    sales_product_number,
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    version,
    load_date
FROM data_with_plgd_dollars_only
GROUP BY cal_date,
    pl,
    l5_description,
    sales_product_number,
    customer_engagement,
    version,
    load_date,
    region_5
"""

data_with_plgd_dollars_only2 = spark.sql(data_with_plgd_dollars_only2)
data_with_plgd_dollars_only2.createOrReplaceTempView("data_with_plgd_dollars_only2")


edw_country_revenue_for_plgd = f"""
SELECT
    cal_date,
    iink.country_alpha2,
    pl,
    sum(gross_revenue) + sum(net_currency) + sum(contractual_discounts) + sum(discretionary_discounts) as net_revenue,
    region_5
FROM edw_supplies_combined_findata_including_iink iink
LEFT JOIN mdm.iso_country_code_xref iso on iink.country_alpha2 = iso.country_alpha2
WHERE 1=1 
AND pl = 'GD'
GROUP BY cal_date,
    iink.country_alpha2,
    pl,
    region_5
"""

edw_country_revenue_for_plgd = spark.sql(edw_country_revenue_for_plgd)
edw_country_revenue_for_plgd.createOrReplaceTempView("edw_country_revenue_for_plgd")


edw_country_mix_for_plgd = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    CASE
        WHEN SUM(net_revenue) OVER (PARTITION BY cal_date, region_5, pl) = 0 THEN NULL
        ELSE net_revenue / sum(net_revenue) OVER (PARTITION BY cal_date, region_5, pl)
    END as country_mix
FROM edw_country_revenue_for_plgd
WHERE 1=1 
AND net_revenue <> 0
GROUP BY cal_date,
    country_alpha2,
    pl,
    region_5,
    net_revenue
"""

edw_country_mix_for_plgd = spark.sql(edw_country_mix_for_plgd)
edw_country_mix_for_plgd.createOrReplaceTempView("edw_country_mix_for_plgd")


update_plgd_country = f"""
    SELECT
        gd.cal_date,
        mix.country_alpha2,
        gd.region_5,
        gd.pl,
        l5_description,
        sales_product_number,
        customer_engagement,
        sum(gross_revenue) * COALESCE(country_mix, 1) AS gross_revenue,
        sum(net_currency)  * COALESCE(country_mix, 1) AS net_currency,
        sum(contractual_discounts) * COALESCE(country_mix, 1) AS contractual_discounts,
        sum(discretionary_discounts)  * COALESCE(country_mix, 1) AS discretionary_discounts,
        sum(net_revenue) * COALESCE(country_mix, 1) AS net_revenue,
        sum(warranty) * COALESCE(country_mix, 1) AS warranty,
        sum(other_cos)  * COALESCE(country_mix, 1) AS other_cos,
        sum(total_cos)  * COALESCE(country_mix, 1) AS total_cos,
        sum(gross_profit)  * COALESCE(country_mix, 1) AS gross_profit,
        0 as revenue_units,
        version,
        load_date
    FROM data_with_plgd_dollars_only2 gd
    LEFT JOIN edw_country_mix_for_plgd mix ON
        gd.cal_date = mix.cal_date AND
        gd.region_5 = mix.region_5 AND
        gd.pl = mix.pl
    group by gd.cal_date, mix.country_alpha2, gd.region_5, gd.pl, l5_description, sales_product_number, customer_engagement, country_mix, version, load_date
"""

update_plgd_country = spark.sql(update_plgd_country)
update_plgd_country.createOrReplaceTempView("update_plgd_country")


restated_plgd_country_revenue = f"""
SELECT
        cal_date,
        CASE    
            WHEN country_alpha2 is null AND region_5 = 'XW' THEN 'XW'
            ELSE country_alpha2
        END AS country_alpha2,
        region_5,
        pl,
        l5_description,
        sales_product_number,
        customer_engagement,
        gross_revenue,
        net_currency,
        contractual_discounts,
        discretionary_discounts,
        net_revenue,
        warranty,
        other_cos,
        total_cos,
        gross_profit,
        revenue_units,
        version,
        load_date
FROM update_plgd_country
WHERE country_alpha2 is not null
"""

restated_plgd_country_revenue = spark.sql(restated_plgd_country_revenue)
restated_plgd_country_revenue.createOrReplaceTempView("restated_plgd_country_revenue")


plgd_document_currency = f"""
SELECT
    cal_date,
    pl,
    country_alpha2,
    region_5,
    document_currency_code,
    SUM(revenue) AS revenue -- at net revenue level but sources does not have hedge, so equivalent to revenue before hedge
FROM fin_stage.odw_document_currency doc
WHERE 1=1
    AND document_currency_code <> '?'
    AND country_alpha2 NOT LIKE 'X%'
    AND cal_date > '2021-10-01'
    --AND cal_date = (SELECT MAX(cal_date) FROM fin_stage.odw_document_currency)
    AND pl = 'GD'
GROUP BY cal_date,
    pl,
    region_5,
    country_alpha2,
    document_currency_code
"""

plgd_document_currency = spark.sql(plgd_document_currency)
plgd_document_currency.createOrReplaceTempView("plgd_document_currency")


plgd_doc_currency_mix = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    document_currency_code AS currency,
    pl,
    CASE
        WHEN SUM(revenue) OVER (PARTITION BY cal_date, region_5, country_alpha2, pl) = 0 THEN NULL
        ELSE revenue / SUM(revenue) OVER (PARTITION BY cal_date, region_5, country_alpha2, pl)
    END AS gd_proxy_mix
FROM plgd_document_currency doc
GROUP BY cal_date, document_currency_code, pl, revenue, region_5, country_alpha2
"""

plgd_doc_currency_mix = spark.sql(plgd_doc_currency_mix)
plgd_doc_currency_mix.createOrReplaceTempView("plgd_doc_currency_mix")


update_plgd_currency = f"""
    SELECT
        gd.cal_date,
        gd.country_alpha2,
        gd.region_5,
        currency,
        gd.pl,
        l5_description,
        sales_product_number,
        customer_engagement,
        gross_revenue * COALESCE(gd_proxy_mix, 1) AS gross_revenue,
        net_currency  * COALESCE(gd_proxy_mix, 1) AS net_currency,
        contractual_discounts * COALESCE(gd_proxy_mix, 1) AS contractual_discounts,
        discretionary_discounts  * COALESCE(gd_proxy_mix, 1) AS discretionary_discounts,
        net_revenue * COALESCE(gd_proxy_mix, 1) AS net_revenue,
        warranty * COALESCE(gd_proxy_mix, 1) AS warranty,
        other_cos  * COALESCE(gd_proxy_mix, 1) AS other_cos,
        total_cos  * COALESCE(gd_proxy_mix, 1) AS total_cos,
        gross_profit  * COALESCE(gd_proxy_mix, 1) AS gross_profit,
        revenue_units  * COALESCE(gd_proxy_mix, 1) AS revenue_units, -- issue here?
        version,
        load_date
    FROM restated_plgd_country_revenue gd
    LEFT JOIN plgd_doc_currency_mix mix ON
        gd.cal_date = mix.cal_date AND
        gd.region_5 = mix.region_5 AND
        gd.pl = mix.pl AND
        gd.country_alpha2 = mix.country_alpha2
"""

update_plgd_currency = spark.sql(update_plgd_currency)
update_plgd_currency.createOrReplaceTempView("update_plgd_currency")


update_plgd_currency2 = f"""
    SELECT
        cal_date,
        country_alpha2,
        CASE 
            WHEN currency is null THEN 'USD'
            ELSE currency
        END AS currency,
        pl,
        l5_description,
        sales_product_number,
        customer_engagement,
        COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
        COALESCE(SUM(net_currency), 0) AS net_currency,
        COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
        COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
        COALESCE(SUM(net_revenue), 0) AS net_revenue,
        COALESCE(SUM(warranty), 0) AS warranty,
        COALESCE(SUM(other_cos), 0) AS other_cos,
        COALESCE(SUM(total_cos), 0) AS total_cos,
        COALESCE(SUM(gross_profit),0) AS gross_profit,
        COALESCE(SUM(revenue_units), 0) AS revenue_units,
        version,
        load_date
    FROM update_plgd_currency
    GROUP BY cal_date,
        country_alpha2,
        currency,
        pl,
        l5_description,
        sales_product_number,
        customer_engagement,
        version,
        load_date
"""

update_plgd_currency2 = spark.sql(update_plgd_currency2)
update_plgd_currency2.createOrReplaceTempView("update_plgd_currency2")


data_with_plgd_currency_adjusted = f"""
    SELECT
        cal_date,
        country_alpha2,
        currency,
        pl,
        l5_description,
        sales_product_number,
        customer_engagement,
        SUM(gross_revenue) AS gross_revenue,
        SUM(net_currency) AS net_currency,
        SUM(contractual_discounts) AS contractual_discounts,
        SUM(discretionary_discounts) AS discretionary_discounts,
        SUM(net_revenue) AS net_revenue,
        SUM(warranty) AS warranty,
        SUM(other_cos) AS other_cos,
        SUM(total_cos) AS total_cos,
        SUM(gross_profit) AS gross_profit,
        SUM(revenue_units) AS revenue_units,
        version,
        load_date
    FROM update_plgd_currency2
    GROUP BY cal_date,
        country_alpha2,
        currency,
        pl,
        l5_description,
        sales_product_number,
        customer_engagement,
        version,
        load_date
        
    UNION ALL

    SELECT
        cal_date,
        country_alpha2,
        currency,
        pl,
        l5_description,
        sales_product_number,
        customer_engagement,
        SUM(gross_revenue) AS gross_revenue,
        SUM(net_currency) AS net_currency,
        SUM(contractual_discounts) AS contractual_discounts,
        SUM(discretionary_discounts) AS discretionary_discounts,
        SUM(net_revenue) AS net_revenue,
        SUM(warranty) AS warranty,
        SUM(other_cos) AS other_cos,
        SUM(total_cos) AS total_cos,
        SUM(gross_profit) AS gross_profit,
        SUM(revenue_units) AS revenue_units,
        version,
        load_date
    FROM data_without_plgd_dollars data
    GROUP BY cal_date,
        country_alpha2,
        currency,
        pl,
        l5_description,
        sales_product_number,
        customer_engagement,
        version,
        load_date
"""

data_with_plgd_currency_adjusted = spark.sql(data_with_plgd_currency_adjusted)
data_with_plgd_currency_adjusted.createOrReplaceTempView("data_with_plgd_currency_adjusted")

# COMMAND ----------

salesprod_financials = f"""
SELECT distinct 
    'actuals - supplies' AS record,
    cal_date,
    final.country_alpha2,
    market10,
    currency,
    sales_product_number,
    pl,
    l5_description,
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) as warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    CAST(1 AS BOOLEAN) AS official, 
    '{addversion_info[1]}' AS load_date,
    '{addversion_info[0]}' AS version
FROM data_with_plgd_currency_adjusted final
LEFT JOIN mdm.iso_country_code_xref iso ON final.country_alpha2 = iso.country_alpha2
GROUP BY cal_date, final.country_alpha2, pl, sales_product_number, customer_engagement, l5_description, currency, market10
"""

salesprod_financials = spark.sql(salesprod_financials)
salesprod_financials.createOrReplaceTempView("salesprod_financials")

# COMMAND ----------

#LOAD TO DB
write_df_to_redshift(configs, salesprod_financials, "fin_prod.odw_actuals_supplies_salesprod", "append", postactions = "", preactions = "truncate fin_prod.odw_actuals_supplies_salesprod")

# COMMAND ----------

#clean up directly in RS
query = f"""
UPDATE fin_prod.odw_actuals_supplies_salesprod
SET net_revenue = 0
WHERE net_revenue <.000001 and net_revenue > 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET net_revenue = 0
WHERE net_revenue >-.000001 and net_revenue < 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET gross_revenue = 0
WHERE gross_revenue <.000001 and gross_revenue > 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET gross_revenue = 0
WHERE gross_revenue >-.000001 and gross_revenue < 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET net_currency = 0
WHERE net_currency <.000001 and net_currency > 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET net_currency = 0
WHERE net_currency >-.000001 and net_currency < 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET contractual_discounts = 0
WHERE contractual_discounts <.000001 and contractual_discounts > 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET contractual_discounts = 0
WHERE contractual_discounts >-.000001 and contractual_discounts < 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET discretionary_discounts = 0
WHERE discretionary_discounts <.000001 and discretionary_discounts > 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET discretionary_discounts = 0
WHERE discretionary_discounts >-.000001 and discretionary_discounts < 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET total_cos = 0
WHERE total_cos <.000001 and total_cos > 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET total_cos = 0
WHERE total_cos >-.000001 and total_cos < 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET gross_profit = 0
WHERE gross_profit <.000001 and gross_profit > 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET gross_profit = 0
WHERE gross_profit >-.000001 and gross_profit < 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET warranty = 0
WHERE warranty <.000001 and warranty > 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET warranty = 0
WHERE warranty >-.000001 and warranty < 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET revenue_units = 0
WHERE revenue_units <.000001 and revenue_units > 0;

UPDATE fin_prod.odw_actuals_supplies_salesprod
SET revenue_units = 0
WHERE revenue_units >-.000001 and revenue_units < 0;
"""

submit_remote_query(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'], query)

# COMMAND ----------


