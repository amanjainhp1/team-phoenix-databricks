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

edw_fin_s3_bucket= f"s3://dataos-core-{stack}-team-phoenix-fin/"
edw_ships_s3_bucket= f"s3://dataos-core-{stack}-team-phoenix/product/"

# COMMAND ----------

# load parquet files to df
edw_revenue_units_sales_landing = spark.read.parquet(edw_fin_s3_bucket + "EDW/edw_revenue_units_sales_landing")

edw_revenue_document_currency_landing = spark.read.parquet(edw_fin_s3_bucket + "EDW/edw_revenue_document_currency_landing")

edw_revenue_dollars_landing = spark.read.parquet(edw_fin_s3_bucket + "EDW/edw_revenue_dollars_landing")

edw_shipment_actuals_landing = spark.read.parquet(edw_ships_s3_bucket + "EDW/edw_shipment_actuals_landing")

# COMMAND ----------

# load S3 tables to df
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
planet_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.planet_actuals") \
    .load()
supplies_finance_hier_restatements_2020_2021 = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.supplies_finance_hier_restatements_2020_2021") \
    .load()
supplies_hw_country_actuals_mapping = read_redshift_to_df(configs) \
    .option("dbtable", "stage.supplies_hw_country_actuals_mapping") \
    .load()
mps_card_revenue = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.mps_card_revenue") \
    .load()

# COMMAND ----------

# delta tables

import re

tables = [
    ['fin_stage.edw_revenue_units_sales_staging', edw_revenue_units_sales_landing],
    ['fin_stage.edw_revenue_document_currency_staging', edw_revenue_document_currency_landing],
    ['fin_stage.edw_revenue_dollars_staging', edw_revenue_dollars_landing],
    ['fin_stage.edw_shipment_actuals_staging', edw_shipment_actuals_landing],
    ['fin_stage.mps_ww_shipped_supply_staging', mps_ww_shipped_supply],
    ['mdm.iso_country_code_xref', iso_country_code_xref],
    ['mdm.profit_center_code_xref', profit_center_code_xref],
    ['fin_stage.itp_laser_landing', itp_laser_landing],
    ['fin_stage.supplies_iink_units_landing', supplies_iink_units_landing],
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
    ['fin_prod.mps_card_revenue', mps_card_revenue],
    ['fin_stage.planet_actuals', planet_actuals],
    ['fin_stage.supplies_finance_hier_restatements_2020_2021', supplies_finance_hier_restatements_2020_2021]
    ]

for table in tables:
    # Define the input and output formats and paths and the table name.
    schema = table[0].split(".")[0]
    table_name = table[0].split(".")[1]
    write_format = 'delta'

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
        .saveAsTable(table[0])

    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')

# COMMAND ----------

# delete Delta tables and underlying Delta files
tables = [
    ['stage.planet_actuals', planet_actuals],
    ['stage.supplies_finance_hier_restatements_2020_2021', supplies_finance_hier_restatements_2020_2021]
]

for table in tables:
    schema = table[0].split(".")[0]
    table_name = table[0].split(".")[1]

    spark.sql(f"DROP TABLE IF EXISTS {schema}.{table_name}")

# COMMAND ----------

actuals_supplies_salesprod.createOrReplaceTempView("actuals_supplies_salesprod")

# COMMAND ----------

supplies_hw_country_actuals_mapping.createOrReplaceTempView("supplies_hw_country_actuals_mapping")

# COMMAND ----------

addversion_info = call_redshift_addversion_sproc(configs, "ACTUALS - EDW SUPPLIES SALES PRODUCT FINANCIALS", "ACTUALS - EDW SUPPLIES SALES PRODUCT FINANCIALS")

# COMMAND ----------

# ETL EDW DATA PER STEERING / FINANCE GUIDELINES

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM fin_stage.edw_revenue_dollars_staging
# MAGIC WHERE transaction_detail_us_dollar_amount = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM fin_stage.edw_revenue_dollars_staging
# MAGIC WHERE business_area_code NOT IN 
# MAGIC (
# MAGIC SELECT DISTINCT plxx 
# MAGIC FROM product_line_xref 
# MAGIC WHERE technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
# MAGIC     AND pl_category IN ('SUP', 'LLC')
# MAGIC     OR plxx IN ('GO00', 'GN00', 'IE00', 'AU00') -- discontinued product lines that have records in EDW, plus Media (AU)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET 
# MAGIC   functional_area_level_11_name = 'GROSS_REVENUE'
# MAGIC WHERE 
# MAGIC   functional_area_level_11_name = 'TRADE SALES LEM'
# MAGIC OR functional_area_level_11_name = 'ROYALTY INCOME'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET 
# MAGIC   functional_area_level_11_name = 'NET_CURRENCY'
# MAGIC WHERE 
# MAGIC   functional_area_level_11_name = 'NET CURRENCY'

# COMMAND ----------

# MAGIC %sql    
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET 
# MAGIC     functional_area_level_11_name = 'CONTRACTUAL_DISCOUNTS'
# MAGIC WHERE 
# MAGIC     functional_area_level_11_name = 'CONTRA REVENUE' 
# MAGIC AND group_account_identifier IN ('3108', '3115', '3116', '3143', '3155')

# COMMAND ----------

# MAGIC %sql        
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET 
# MAGIC     functional_area_level_11_name = 'DISCRETIONARY_DISCOUNTS'
# MAGIC WHERE 
# MAGIC     functional_area_level_11_name = 'CONTRA REVENUE' 
# MAGIC AND group_account_identifier NOT IN ('3108', '3115', '3116', '3143', '3155')

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET 
# MAGIC     functional_area_level_11_name = 'WARRANTY'
# MAGIC WHERE
# MAGIC     functional_area_level_11_name LIKE 'WARRANTY%'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET 
# MAGIC     functional_area_level_11_name = 'OTHER_COS'
# MAGIC WHERE
# MAGIC     functional_area_level_11_name NOT IN ('GROSS_REVENUE', 'NET_CURRENCY', 'CONTRACTUAL_DISCOUNTS', 'DISCRETIONARY_DISCOUNTS', 'WARRANTY')

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET
# MAGIC     ipg_product_base_product_number = 'NOT_PROVIDED'
# MAGIC WHERE ipg_product_base_product_number = ''
# MAGIC OR ipg_product_base_product_number is null

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET
# MAGIC     product_base_identifier = SUBSTRING(manufacturing_product_identifier, 1, 7)
# MAGIC WHERE product_base_identifier = '?' AND manufacturing_product_identifier != '?'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET
# MAGIC     product_base_identifier = SUBSTRING(product_base_identifier, 1, 7)
# MAGIC WHERE product_base_identifier LIKE '%#%'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET
# MAGIC     product_base_identifier = SUBSTRING(product_base_identifier, 1, 6)
# MAGIC WHERE product_base_identifier LIKE '%#%'

# COMMAND ----------

# MAGIC %sql    
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET
# MAGIC     manufacturing_product_identifier = product_base_identifier
# MAGIC WHERE manufacturing_product_identifier = '?' AND product_base_identifier != '?'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET
# MAGIC     ipg_product_base_product_number = product_base_identifier
# MAGIC WHERE ipg_product_base_product_number = 'NOT_PROVIDED' AND product_base_identifier != '?'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET
# MAGIC   ipg_product_base_product_number = product_base_identifier
# MAGIC WHERE ipg_product_base_product_number = 'NA' AND product_base_identifier != '?'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- label '?' profit center code to a WW value ('WW IPG')
# MAGIC UPDATE fin_stage.edw_revenue_dollars_staging
# MAGIC SET 
# MAGIC     profit_center_code = 'P1755' 
# MAGIC WHERE profit_center_code = '?' 

# COMMAND ----------

# EDW REVENUE UNITS

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM fin_stage.edw_revenue_units_sales_staging
# MAGIC WHERE working_PandL_summary_extended_quantity = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM fin_stage.edw_revenue_units_sales_staging
# MAGIC WHERE business_area_code NOT IN 
# MAGIC (
# MAGIC SELECT DISTINCT plxx 
# MAGIC FROM product_line_xref 
# MAGIC WHERE technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
# MAGIC     AND pl_category IN ('SUP', 'LLC')
# MAGIC     OR plxx IN ('GO00', 'GN00', 'IE00', 'AU00') -- discontinued product lines that have records in EDW, plus Media (AU)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_units_sales_staging
# MAGIC SET
# MAGIC     ipg_product_base_product_number = 'not_provided'
# MAGIC WHERE ipg_product_base_product_number = ''
# MAGIC OR ipg_product_base_product_number is null

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_units_sales_staging
# MAGIC SET
# MAGIC     ipg_product_base_product_number = product_base_identifier
# MAGIC WHERE ipg_product_base_product_number = 'not_provided' AND product_base_identifier != '?'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_stage.edw_revenue_units_sales_staging
# MAGIC SET
# MAGIC     ipg_product_base_product_number = product_base_identifier
# MAGIC WHERE ipg_product_base_product_number = 'NA' AND product_base_identifier != '?'

# COMMAND ----------

# MPS SUPPLIES SHIPMENTS

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

#COMBINE FINANCIAL AND UNIT DATA, CLEAN IT UP, FORMAT COMPATABLE WITH IE2/PHOENIX

# COMMAND ----------

#all financial data

edw_pldomain_data = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    ipg_product_base_product_number AS base_product_number,
    product_base_identifier AS sales_product_number,
    manufacturing_product_identifier AS sales_product_option,
    functional_area_level_11_name AS finance11,
    SUM(transaction_detail_us_dollar_amount) AS edw_dollars
FROM fin_stage.edw_revenue_dollars_staging
WHERE business_area_code NOT IN ('AU00')
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, ipg_product_base_product_number,
product_base_identifier, manufacturing_product_identifier, functional_area_level_11_name
"""

edw_pldomain_data = spark.sql(edw_pldomain_data)
edw_pldomain_data.createOrReplaceTempView("edw_pldomain_data")


#select only gross revenue
edw_gross_rev = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(edw_dollars) AS gross_revenue
FROM edw_pldomain_data
WHERE finance11 = 'GROSS_REVENUE'
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
sales_product_option
"""

edw_gross_rev = spark.sql(edw_gross_rev)
edw_gross_rev.createOrReplaceTempView("edw_gross_rev")


#select only net currency
edw_net_currency = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(edw_dollars) AS net_currency
FROM edw_pldomain_data 
WHERE finance11 = 'NET_CURRENCY' 
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
sales_product_option
"""
                
edw_net_currency = spark.sql(edw_net_currency)
edw_net_currency.createOrReplaceTempView("edw_net_currency")


#select only contractual discounts
edw_contractual_discounts = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(edw_dollars) AS contractual_discounts
FROM edw_pldomain_data
WHERE finance11 = 'CONTRACTUAL_DISCOUNTS' 
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
sales_product_option
"""

edw_contractual_discounts = spark.sql(edw_contractual_discounts)
edw_contractual_discounts.createOrReplaceTempView("edw_contractual_discounts")


#select only discretionary discounts
edw_discretionary_discounts = f"""        
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(edw_dollars) AS discretionary_discounts
FROM edw_pldomain_data
WHERE finance11 = 'DISCRETIONARY_DISCOUNTS' 
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
sales_product_option
"""
                
edw_discretionary_discounts = spark.sql(edw_discretionary_discounts)
edw_discretionary_discounts.createOrReplaceTempView("edw_discretionary_discounts")


#select only other cost of sales (cos) -- note: original script didn't carve out warranty, so total_cos will = cos less warranty
edw_other_cos = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(edw_dollars) AS other_cos
FROM edw_pldomain_data
WHERE finance11 = 'OTHER_COS' 
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
sales_product_option
"""

edw_other_cos = spark.sql(edw_other_cos)
edw_other_cos.createOrReplaceTempView("edw_other_cos")


#select only warranty
edw_warranty = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(edw_dollars) AS warranty
FROM edw_pldomain_data
WHERE finance11 = 'WARRANTY' 
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
sales_product_option
"""
edw_warranty = spark.sql(edw_warranty)
edw_warranty.createOrReplaceTempView("edw_warranty")


#start joining each measure with each other, reverse joining to capture any dropping items
edw_gross_bind_currency = f"""    
SELECT g.revenue_recognition_fiscal_year_month_code,
    g.profit_center_code,
    g.business_area_code,
    g.base_product_number,
    g.sales_product_number,
    g.sales_product_option,
    COALESCE(SUM(gross_revenue),0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency
FROM edw_gross_rev AS g
LEFT JOIN edw_net_currency AS n 
    ON (
    g.revenue_recognition_fiscal_year_month_code = n.revenue_recognition_fiscal_year_month_code
    AND g.profit_center_code = n.profit_center_code
    AND g.business_area_code = n.business_area_code
    AND g.base_product_number = n.base_product_number
    AND g.sales_product_number = n.sales_product_number
    AND g.sales_product_option = n.sales_product_option
    )
GROUP BY g.revenue_recognition_fiscal_year_month_code, g.profit_center_code, g.business_area_code, g.base_product_number, g.sales_product_number,
g.sales_product_option
"""
                
edw_gross_bind_currency = spark.sql(edw_gross_bind_currency)
edw_gross_bind_currency.createOrReplaceTempView("edw_gross_bind_currency")

missing_currency_rows = f"""
SELECT c.revenue_recognition_fiscal_year_month_code,
    c.profit_center_code,
    c.business_area_code,
    c.base_product_number,
    c.sales_product_number,
    c.sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(c.net_currency) AS net_currency
FROM edw_net_currency AS c
LEFT JOIN edw_gross_bind_currency AS edw ON
    (c.revenue_recognition_fiscal_year_month_code = edw.revenue_recognition_fiscal_year_month_code
    AND c.profit_center_code = edw.profit_center_code
    AND c.business_area_code = edw.business_area_code
    AND c.base_product_number = edw.base_product_number
    AND c.sales_product_number = edw.sales_product_number
    AND c.sales_product_option = edw.sales_product_option
    )
WHERE gross_revenue IS NULL
GROUP BY c.revenue_recognition_fiscal_year_month_code, c.profit_center_code, c.business_area_code, c.base_product_number, c.sales_product_number,
c.sales_product_option        
"""
                
missing_currency_rows = spark.sql(missing_currency_rows)
missing_currency_rows.createOrReplaceTempView("missing_currency_rows")

missing_currency_final = f"""
            SELECT revenue_recognition_fiscal_year_month_code,
                profit_center_code,
                business_area_code,
                base_product_number,
                sales_product_number,
                sales_product_option,
                0 AS gross_revenue,
                SUM(net_currency) AS net_currency,
                0 AS contractual_discounts,
                0 AS discretionary_discounts,
                0 AS other_cos,
                0 AS warranty,
                0 AS revenue_units
            FROM missing_currency_rows
        GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
                sales_product_option
"""
missing_currency_final = spark.sql(missing_currency_final)
missing_currency_final.createOrReplaceTempView("missing_currency_final")

edw_bind_contractual_disc = f"""
SELECT g.revenue_recognition_fiscal_year_month_code,
    g.profit_center_code,
    g.business_area_code,
    g.base_product_number,
    g.sales_product_number,
    g.sales_product_option,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts
FROM edw_gross_bind_currency AS g
LEFT JOIN edw_contractual_discounts AS c
    ON (
    g.revenue_recognition_fiscal_year_month_code = c.revenue_recognition_fiscal_year_month_code
    AND g.profit_center_code = c.profit_center_code
    AND g.business_area_code = c.business_area_code
    AND g.base_product_number = c.base_product_number
    AND g.sales_product_number = c.sales_product_number
    AND g.sales_product_option = c.sales_product_option
    )
GROUP BY g.revenue_recognition_fiscal_year_month_code, g.profit_center_code, g.business_area_code, g.base_product_number, g.sales_product_number,
g.sales_product_option
"""    

edw_bind_contractual_disc = spark.sql(edw_bind_contractual_disc)
edw_bind_contractual_disc.createOrReplaceTempView("edw_bind_contractual_disc")


missing_contractual_discounts_rows = f"""
SELECT c.revenue_recognition_fiscal_year_month_code,
    c.profit_center_code,
    c.business_area_code,
    c.base_product_number,
    c.sales_product_number,
    c.sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    --SUM(net_currency) AS net_currency,
    SUM(c.contractual_discounts) AS contractual_discounts
FROM edw_contractual_discounts AS c
LEFT JOIN edw_bind_contractual_disc AS edw ON
    (c.revenue_recognition_fiscal_year_month_code = edw.revenue_recognition_fiscal_year_month_code
    AND c.profit_center_code = edw.profit_center_code
    AND c.business_area_code = edw.business_area_code
    AND c.base_product_number = edw.base_product_number
    AND c.sales_product_number = edw.sales_product_number
    AND c.sales_product_option = edw.sales_product_option
    )
WHERE gross_revenue IS NULL
GROUP BY c.revenue_recognition_fiscal_year_month_code, c.profit_center_code, c.business_area_code, c.base_product_number, c.sales_product_number,
c.sales_product_option        
"""

missing_contractual_discounts_rows = spark.sql(missing_contractual_discounts_rows)
missing_contractual_discounts_rows.createOrReplaceTempView("missing_contractual_discounts_rows")


missing_contractual_discounts_final = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    0 AS gross_revenue,
    0 AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS other_cos,
    0 AS warranty,
    0 AS revenue_units
FROM missing_contractual_discounts_rows
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
sales_product_option
"""
 
missing_contractual_discounts_final = spark.sql(missing_contractual_discounts_final)
missing_contractual_discounts_final.createOrReplaceTempView("missing_contractual_discounts_final")


edw_bind_discretionary_disc = f"""
SELECT g.revenue_recognition_fiscal_year_month_code,
    g.profit_center_code,
    g.business_area_code,
    g.base_product_number,
    g.sales_product_number,
    g.sales_product_option,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts
FROM edw_bind_contractual_disc AS g
LEFT JOIN edw_discretionary_discounts AS d
    ON (
    g.revenue_recognition_fiscal_year_month_code = d.revenue_recognition_fiscal_year_month_code
    AND g.profit_center_code = d.profit_center_code
    AND g.business_area_code = d.business_area_code
    AND g.base_product_number = d.base_product_number
    AND g.sales_product_number = d.sales_product_number
    AND g.sales_product_option = d.sales_product_option
    )
GROUP BY g.revenue_recognition_fiscal_year_month_code, g.profit_center_code, g.business_area_code, g.base_product_number, g.sales_product_number,
g.sales_product_option
"""        

edw_bind_discretionary_disc = spark.sql(edw_bind_discretionary_disc)
edw_bind_discretionary_disc.createOrReplaceTempView("edw_bind_discretionary_disc")


missing_discretionary_discounts_rows = f"""
SELECT d.revenue_recognition_fiscal_year_month_code,
    d.profit_center_code,
    d.business_area_code,
    d.base_product_number,
    d.sales_product_number,
    d.sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    --SUM(net_currency) AS net_currency,
    --SUM(contractual_discounts) AS contractual_discounts,
    SUM(d.discretionary_discounts) AS discretionary_discounts
FROM edw_discretionary_discounts AS d
LEFT JOIN edw_bind_discretionary_disc AS edw ON
    (d.revenue_recognition_fiscal_year_month_code = edw.revenue_recognition_fiscal_year_month_code
    AND d.profit_center_code = edw.profit_center_code
    AND d.business_area_code = edw.business_area_code
    AND d.base_product_number = edw.base_product_number
    AND d.sales_product_number = edw.sales_product_number
    AND d.sales_product_option = edw.sales_product_option
    )
WHERE gross_revenue IS NULL
GROUP BY d.revenue_recognition_fiscal_year_month_code, d.profit_center_code, d.business_area_code, d.base_product_number, d.sales_product_number,
d.sales_product_option        
"""

missing_discretionary_discounts_rows = spark.sql(missing_discretionary_discounts_rows)
missing_discretionary_discounts_rows.createOrReplaceTempView("missing_discretionary_discounts_rows")

missing_discretionary_discounts_final = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    0 AS gross_revenue,
    0 AS net_currency,
    0 AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    0 AS other_cos,
    0 AS warranty,
    0 AS revenue_units
FROM missing_discretionary_discounts_rows
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
sales_product_option
"""
        
missing_discretionary_discounts_final = spark.sql(missing_discretionary_discounts_final)
missing_discretionary_discounts_final.createOrReplaceTempView("missing_discretionary_discounts_final")

edw_bind_tcos = f"""
SELECT g.revenue_recognition_fiscal_year_month_code,
    g.profit_center_code,
    g.business_area_code,
    g.base_product_number,
    g.sales_product_number,
    g.sales_product_option,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(other_cos), 0) AS other_cos
FROM edw_bind_discretionary_disc AS g
LEFT JOIN edw_other_cos AS t
    ON (
    g.revenue_recognition_fiscal_year_month_code = t.revenue_recognition_fiscal_year_month_code
    AND g.profit_center_code = t.profit_center_code
    AND g.business_area_code = t.business_area_code
    AND g.base_product_number = t.base_product_number
    AND g.sales_product_number = t.sales_product_number
    AND g.sales_product_option = t.sales_product_option
    )
GROUP BY g.revenue_recognition_fiscal_year_month_code, g.profit_center_code, g.business_area_code, g.base_product_number, g.sales_product_number,
g.sales_product_option        
"""

edw_bind_tcos = spark.sql(edw_bind_tcos)
edw_bind_tcos.createOrReplaceTempView("edw_bind_tcos")

missing_tcos_rows = f"""
SELECT t.revenue_recognition_fiscal_year_month_code,
    t.profit_center_code,
    t.business_area_code,
    t.base_product_number,
    t.sales_product_number,
    t.sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    --SUM(net_currency) AS net_currency,
    --SUM(contractual_discounts) AS contractual_discounts,
    --SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(t.other_cos) AS other_cos
FROM edw_other_cos AS t
LEFT JOIN edw_bind_tcos AS edw ON
    (t.revenue_recognition_fiscal_year_month_code = edw.revenue_recognition_fiscal_year_month_code
    AND t.profit_center_code = edw.profit_center_code
    AND t.business_area_code = edw.business_area_code
    AND t.base_product_number = edw.base_product_number
    AND t.sales_product_number = edw.sales_product_number
    AND t.sales_product_option = edw.sales_product_option
    )
WHERE gross_revenue IS NULL
GROUP BY t.revenue_recognition_fiscal_year_month_code, t.profit_center_code, t.business_area_code, t.base_product_number, t.sales_product_number,
t.sales_product_option        
"""

missing_tcos_rows = spark.sql(missing_tcos_rows)
missing_tcos_rows.createOrReplaceTempView("missing_tcos_rows")

missing_tcos_final = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    0 AS gross_revenue,
    0 AS net_currency,
    0 AS contractual_discounts,
    0 AS discretionary_discounts,
    SUM(other_cos) AS other_cos,
    0 AS warranty,
    0 AS revenue_units
FROM missing_tcos_rows
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
sales_product_option
"""    

missing_tcos_final = spark.sql(missing_tcos_final)
missing_tcos_final.createOrReplaceTempView("missing_tcos_final")

edw_bind_warranty = f"""
SELECT g.revenue_recognition_fiscal_year_month_code,
    g.profit_center_code,
    g.business_area_code,
    g.base_product_number,
    g.sales_product_number,
    g.sales_product_option,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(warranty), 0) AS warranty
FROM edw_bind_tcos AS g
LEFT JOIN edw_warranty AS t
    ON (
    g.revenue_recognition_fiscal_year_month_code = t.revenue_recognition_fiscal_year_month_code
    AND g.profit_center_code = t.profit_center_code
    AND g.business_area_code = t.business_area_code
    AND g.base_product_number = t.base_product_number
    AND g.sales_product_number = t.sales_product_number
    AND g.sales_product_option = t.sales_product_option
    )
GROUP BY g.revenue_recognition_fiscal_year_month_code, g.profit_center_code, g.business_area_code, g.base_product_number, g.sales_product_number,
g.sales_product_option        
"""

edw_bind_warranty = spark.sql(edw_bind_warranty)
edw_bind_warranty.createOrReplaceTempView("edw_bind_warranty")

missing_warranty_rows = f"""
SELECT t.revenue_recognition_fiscal_year_month_code,
    t.profit_center_code,
    t.business_area_code,
    t.base_product_number,
    t.sales_product_number,
    t.sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    --SUM(net_currency) AS net_currency,
    --SUM(contractual_discounts) AS contractual_discounts,
    --SUM(discretionary_discounts) AS discretionary_discounts,
    --SUM(t.other) AS other_cos,
    SUM(t.warranty) AS warranty
FROM edw_warranty AS t
LEFT JOIN edw_bind_warranty AS edw ON
    (t.revenue_recognition_fiscal_year_month_code = edw.revenue_recognition_fiscal_year_month_code
    AND t.profit_center_code = edw.profit_center_code
    AND t.business_area_code = edw.business_area_code
    AND t.base_product_number = edw.base_product_number
    AND t.sales_product_number = edw.sales_product_number
    AND t.sales_product_option = edw.sales_product_option
    )
WHERE gross_revenue IS NULL
GROUP BY t.revenue_recognition_fiscal_year_month_code, t.profit_center_code, t.business_area_code, t.base_product_number, t.sales_product_number,
t.sales_product_option        
"""
 
missing_warranty_rows = spark.sql(missing_warranty_rows)
missing_warranty_rows.createOrReplaceTempView("missing_warranty_rows")


missing_warranty_final = f"""
            SELECT revenue_recognition_fiscal_year_month_code,
                profit_center_code,
                business_area_code,
                base_product_number,
                sales_product_number,
                sales_product_option,
                0 AS gross_revenue,
                0 AS net_currency,
                0 AS contractual_discounts,
                0 AS discretionary_discounts,
                0 AS other_cos,
                SUM(warranty) AS warranty,
                0 AS revenue_units
            FROM missing_warranty_rows
        GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
                sales_product_option
"""

missing_warranty_final = spark.sql(missing_warranty_final)
missing_warranty_final.createOrReplaceTempView("missing_warranty_final")

        
#extract unit data from staging and continue 

edw_unit_data = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    ipg_product_base_product_number AS base_product_number,
    product_base_identifier AS sales_product_number,
    manufacturing_product_identifier AS sales_product_option,
    SUM(working_PandL_summary_base_quantity) AS base_quantity,
    SUM(working_PandL_summary_extended_quantity) AS extended_quantity,
    SUM(working_PandL_summary_sales_quantity) AS sales_quantity
FROM edw_revenue_units_sales_staging
WHERE working_PandL_summary_extended_quantity != 0
    AND manufacturing_product_identifier != '?'
    AND business_area_code NOT IN ('AU00') -- again, no media here; added in base product staging
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, ipg_product_base_product_number,
product_base_identifier, manufacturing_product_identifier
"""

edw_unit_data = spark.sql(edw_unit_data)
edw_unit_data.createOrReplaceTempView("edw_unit_data")


#select extended quantity only because Gary Etchemendy said so; we re-created the inkjet process per requirements
        
edw_unit_data_selected = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(extended_quantity) AS revenue_units
FROM edw_unit_data
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
sales_product_option
"""

edw_unit_data_selected = spark.sql(edw_unit_data_selected)
edw_unit_data_selected.createOrReplaceTempView("edw_unit_data_selected")


#join edw dollars data and unit data together

edw_dollars_join_units = f"""
SELECT e.revenue_recognition_fiscal_year_month_code,
    e.profit_center_code,
    e.business_area_code,
    e.base_product_number,
    e.sales_product_number,
    e.sales_product_option,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(other_cos), 0) AS other_cos,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM edw_bind_warranty AS e
LEFT JOIN edw_unit_data_selected AS u ON 
    (e.revenue_recognition_fiscal_year_month_code = u.revenue_recognition_fiscal_year_month_code
    AND e.profit_center_code = u.profit_center_code
    AND e.business_area_code = u.business_area_code
    AND e.base_product_number = u.base_product_number
    AND e.sales_product_number = u.sales_product_number
    AND e.sales_product_option = u.sales_product_option)
GROUP BY e.revenue_recognition_fiscal_year_month_code, e.profit_center_code, e.business_area_code, e.base_product_number, e.sales_product_number, e.sales_product_option
"""

edw_dollars_join_units = spark.sql(edw_dollars_join_units)
edw_dollars_join_units.createOrReplaceTempView("edw_dollars_join_units")

missing_units = f"""
SELECT u.revenue_recognition_fiscal_year_month_code,
    u.profit_center_code,
    u.business_area_code,
    u.base_product_number,
    u.sales_product_number,
    u.sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(other_cos) AS other_cos,
    SUM(warranty) AS warranty,
    SUM(u.revenue_units) AS revenue_units
FROM edw_unit_data_selected AS u
LEFT JOIN edw_dollars_join_units AS m ON (
    u.revenue_recognition_fiscal_year_month_code = m.revenue_recognition_fiscal_year_month_code
    AND u.profit_center_code = m.profit_center_code
    AND u.business_area_code = m.business_area_code
    AND u.base_product_number = m.base_product_number
    AND u.sales_product_number = m.sales_product_number
    AND u.sales_product_option = m.sales_product_option)
WHERE gross_revenue IS NULL OR net_currency IS NULL
    OR contractual_discounts IS NULL OR discretionary_discounts IS NULL OR other_cos IS NULL OR warranty IS NULL
GROUP BY u.revenue_recognition_fiscal_year_month_code, u.profit_center_code, u.business_area_code, u.base_product_number, u.sales_product_number,
u.sales_product_option
"""

missing_units = spark.sql(missing_units)
missing_units.createOrReplaceTempView("missing_units")

edw_dollars_join_units2 = f"""
SELECT *
FROM edw_dollars_join_units

UNION ALL

SELECT *
FROM missing_currency_final

UNION ALL

SELECT * 
FROM missing_contractual_discounts_final

UNION ALL

SELECT * 
FROM missing_discretionary_discounts_final

UNION ALL

SELECT * 
FROM missing_tcos_final

UNION ALL

SELECT * 
FROM missing_warranty_final

UNION ALL

SELECT * 
FROM missing_units
"""

edw_dollars_join_units2 = spark.sql(edw_dollars_join_units2)
edw_dollars_join_units2.createOrReplaceTempView("edw_dollars_join_units2")

edw_findata = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(other_cos), 0) AS total_cos, -- "total_cos_excluding_warranty"
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM edw_dollars_join_units2
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number, sales_product_option
"""

edw_findata = spark.sql(edw_findata)
edw_findata.createOrReplaceTempView("edw_findata")

# convert edw data to ie2 format

edw_findata_time = f"""
SELECT 
    cal.Date AS cal_date,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_findata AS edw
LEFT JOIN calendar AS cal ON (edw.revenue_recognition_fiscal_year_month_code = cal.Edw_fiscal_yr_mo)
WHERE Day_of_Month = 1
GROUP BY cal.Date, profit_center_code, business_area_code, base_product_number, sales_product_number, sales_product_option
"""

edw_findata_time = spark.sql(edw_findata_time)
edw_findata_time.createOrReplaceTempView("edw_findata_time")


sales_office_geography = f"""
SELECT 
    DISTINCT(geo.country_alpha2),
    pcx.profit_center_code,
    geo.region_3
FROM profit_center_code_xref AS pcx
LEFT JOIN iso_country_code_xref AS geo ON (pcx.country_alpha2 = geo.country_alpha2)
"""

sales_office_geography = spark.sql(sales_office_geography)
sales_office_geography.createOrReplaceTempView("sales_office_geography")


edw_findata_geo = f"""
SELECT 
    cal_date,
    geo.country_alpha2,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_findata_time AS edw
LEFT JOIN sales_office_geography AS geo ON (edw.profit_center_code = geo.profit_center_code)
GROUP BY cal_date, geo.country_alpha2, business_area_code, base_product_number, sales_product_number, sales_product_option
"""

edw_findata_geo = spark.sql(edw_findata_geo)
edw_findata_geo.createOrReplaceTempView("edw_findata_geo")


edw_findata_edw_pl = f"""
SELECT 
    cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_findata_geo AS edw
LEFT JOIN product_line_xref AS pl ON business_area_code = plxx
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""            
            
edw_findata_edw_pl = spark.sql(edw_findata_edw_pl)
edw_findata_edw_pl.createOrReplaceTempView("edw_findata_edw_pl")


# pld doesn't restate; catching any latent old pl mappings and updating just in case
edw_findata_edw_pl2 = f"""
SELECT 
    cal_date,
    country_alpha2,
    CASE
        WHEN pl = 'GN' THEN 'GJ'
        WHEN pl = 'GO' THEN '5T'
        ELSE pl
    END AS pl,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_findata_edw_pl 
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""        

edw_findata_edw_pl2 = spark.sql(edw_findata_edw_pl2)
edw_findata_edw_pl2.createOrReplaceTempView("edw_findata_edw_pl2")


findata_clean_zeros = f"""
SELECT 
    cal_date,
    country_alpha2,
    pl, 
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units,
        SUM(gross_revenue) + SUM(net_currency) + SUM(contractual_discounts) + 
        SUM(discretionary_discounts) + SUM(total_cos) + sum(revenue_units) + SUM(warranty) AS total_sum            
FROM edw_findata_edw_pl2 AS edw
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option 
"""        

findata_clean_zeros = spark.sql(findata_clean_zeros)
findata_clean_zeros.createOrReplaceTempView("findata_clean_zeros")


final_findata = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,            
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM findata_clean_zeros
WHERE total_sum != 0
    AND pl IN (
        SELECT DISTINCT pl 
        FROM product_line_xref 
        WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
        AND PL_category IN ('SUP', 'LLC')
        OR PL IN ('IE') 
        )
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""        

final_findata = spark.sql(final_findata)
final_findata.createOrReplaceTempView("final_findata")


final_union_edw_data = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM final_findata edw
WHERE cal_date < '2021-11-01'  -- edw is system of record thru FY21; odw is system of record thereafter
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

final_union_edw_data = spark.sql(final_union_edw_data)
final_union_edw_data.createOrReplaceTempView("final_union_edw_data")

# COMMAND ----------

# Write out final_union_edw_data to its delta table target.
final_union_edw_data.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/final_union_edw_data")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.final_union_edw_data USING DELTA LOCATION '/tmp/delta/fin_stage/final_union_edw_data'")
spark.table("fin_stage.final_union_edw_data").createOrReplaceTempView("final_union_edw_data")


# COMMAND ----------

#MAKE CORRECTIONS TO DATA PER INPUTS FROM BUSINESS (business assertions of wrong-ness)
# 1. EDW includes cross-allocations
# 2. drop Brazil data for OPS for FY2020 forward (units) [HP acquired Simpress so Brazil OPS sales become 'hp direct']
# 3. prepare to fix missing unit data in PLIU [bad load records; IT cannot fix; attempting to calc placeholder values]
# 4. assign LU products flowing to 1N back to LU [per conversation with Finance person]

# COMMAND ----------

sprint_china_adjustments = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,
    CASE
        WHEN sales_product_number = '?' AND country_alpha2 = 'CN' AND pl IN ('GL', 'GY', 'E5', 'EO', 'G0') THEN 0
        ELSE SUM(gross_revenue) 
    END AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    CASE
        WHEN sales_product_number = '?' AND country_alpha2 = 'CN' AND pl IN ('GL', 'GY', 'E5', 'EO', 'G0') THEN 0
        ELSE SUM(warranty) 
    END AS warranty,
    CASE
        WHEN sales_product_number = '?' AND country_alpha2 = 'CN' AND pl IN ('GL', 'GY', 'E5', 'EO', 'G0') THEN 0
        ELSE SUM(total_cos) 
    END AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM fin_stage.final_union_edw_data
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option, gross_revenue, total_cos
"""

sprint_china_adjustments = spark.sql(sprint_china_adjustments)
sprint_china_adjustments.createOrReplaceTempView("sprint_china_adjustments")


corrected_edw = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM sprint_china_adjustments a
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

corrected_edw = spark.sql(corrected_edw)
corrected_edw.createOrReplaceTempView("corrected_edw")


supplies_pls_update_brazil = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    CASE 
        WHEN country_alpha2 = 'BR' 
            AND cal_date > '2019-10-01' 
            AND pl IN ('5T', 'GJ', 'GK', 'GL', 'IU', 'E5', 'EO'    )
        THEN NULL
        ELSE SUM(revenue_units)
    END AS revenue_units
FROM corrected_edw
WHERE pl IN 
    (
    SELECT DISTINCT pl 
    FROM product_line_xref 
    WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
        AND PL_category IN ('SUP', 'LLC')
        OR PL IN ('IE') 
    )
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

supplies_pls_update_brazil = spark.sql(supplies_pls_update_brazil)
supplies_pls_update_brazil.createOrReplaceTempView("supplies_pls_update_brazil")


supplies_shipment_fix1 = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    CASE 
        WHEN country_alpha2 = 'XA' AND cal_date = '2020-07-01' AND sales_product_number = 'CF279A'    THEN '9009'
        WHEN country_alpha2 = 'XA' AND cal_date = '2020-11-01' AND sales_product_number = 'CF279A'    THEN '9639'
        WHEN country_alpha2 = 'XA' AND cal_date = '2020-12-01' AND sales_product_number = 'CF279A'    THEN '6132'
        WHEN country_alpha2 = 'XA' AND cal_date = '2021-03-01' AND sales_product_number = 'CF279A'    THEN '2026'
        ELSE SUM(revenue_units)
    END AS revenue_units
FROM supplies_pls_update_brazil
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

supplies_shipment_fix1 = spark.sql(supplies_shipment_fix1)
supplies_shipment_fix1.createOrReplaceTempView("supplies_shipment_fix1")


supplies_final_findata = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM supplies_shipment_fix1
WHERE pl IN 
    (
    SELECT DISTINCT pl 
    FROM product_line_xref 
    WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
        AND PL_category IN ('SUP', 'LLC')
        OR pl IN ('IE')
    )
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

supplies_final_findata = spark.sql(supplies_final_findata)
supplies_final_findata.createOrReplaceTempView("supplies_final_findata")


supplies_data_before_fy21 = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_final_findata
WHERE cal_date < '2020-11-01'
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

supplies_data_before_fy21 = spark.sql(supplies_data_before_fy21)
supplies_data_before_fy21.createOrReplaceTempView("supplies_data_before_fy21")


supplies_data_fy21_plus = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_final_findata
WHERE cal_date >= '2020-11-01'
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

supplies_data_fy21_plus = spark.sql(supplies_data_fy21_plus)
supplies_data_fy21_plus.createOrReplaceTempView("supplies_data_fy21_plus")


supplies_final_findata_OK_data = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_data_before_fy21
WHERE pl != 'IU'
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

supplies_final_findata_OK_data = spark.sql(supplies_final_findata_OK_data)
supplies_final_findata_OK_data.createOrReplaceTempView("supplies_final_findata_OK_data")


pliu_financial_data = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM supplies_data_before_fy21
WHERE pl = 'IU'
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

pliu_financial_data = spark.sql(pliu_financial_data)
pliu_financial_data.createOrReplaceTempView("pliu_financial_data")


pliu_shipment_data = f"""
SELECT cal.Date as cal_date
    , iso.country_alpha2
    , pl
    , ipg_product_base_product_number as base_product_number
    , Manufacturing_Product_Identifier as sales_product_number    
    , Manufacturing_Product_Identifier as sales_product_option
    , 0 AS gross_revenue
    , 0 AS net_currency
    , 0 AS contractual_discounts
    , 0 AS discretionary_discounts
    , 0 AS warranty
    , 0 AS total_cos
    , SUM(shipment_base_quantity) as revenue_units
 FROM  edw_shipment_actuals_staging edw
 LEFT JOIN calendar cal ON financial_close_fiscal_year_month_code = Edw_fiscal_yr_mo
 LEFT JOIN product_line_xref plx ON business_area_code = plxx
 LEFT JOIN profit_center_code_xref pcx ON pcx.profit_center_code = edw.profit_center_code
 LEFT JOIN iso_country_code_xref iso ON iso.country_alpha2 = pcx.country_alpha2
 WHERE 1=1
 AND Day_of_Month = 1
 AND business_area_code = 'IU00'
 AND financial_close_fiscal_year_month_code < '202101'
 GROUP BY cal.Date
, iso.country_alpha2
, pl
, ipg_product_base_product_number
, Manufacturing_Product_Identifier 
"""
    
pliu_shipment_data = spark.sql(pliu_shipment_data)
pliu_shipment_data.createOrReplaceTempView("pliu_shipment_data")

supplies_findata_join_pliu_fix = f"""
SELECT 
    cal_date, 
    country_alpha2, 
    pl, 
    base_product_number, 
    sales_product_number, 
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_final_findata_OK_data
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option

UNION ALL

SELECT cal_date, 
    country_alpha2, 
    pl, 
    base_product_number, 
    sales_product_number, 
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_data_FY21_plus
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option

UNION ALL

SELECT cal_date, 
    country_alpha2, 
    pl, 
    base_product_number, 
    sales_product_number, 
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM pliu_financial_data
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option

UNION ALL

SELECT cal_date, 
    country_alpha2, 
    pl, 
    base_product_number, 
    sales_product_number, 
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM pliu_shipment_data
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""        

supplies_findata_join_pliu_fix = spark.sql(supplies_findata_join_pliu_fix)
supplies_findata_join_pliu_fix.createOrReplaceTempView("supplies_findata_join_pliu_fix")


edw_data_with_pliu_fix = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_findata_join_pliu_fix
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

edw_data_with_pliu_fix = spark.sql(edw_data_with_pliu_fix)
edw_data_with_pliu_fix.createOrReplaceTempView("edw_data_with_pliu_fix")


edw_data_with_lu_fix = f"""
SELECT
    cal_date,
    country_alpha2,
    CASE
        WHEN base_product_number IN (
        'M0H54A', 'M0H55A', 'M0H50A', 'M0H56A', 'X4E75A', 'M0H51A', '1VV21A', '1VU28A', 
        '1VV24A', '1VU27A', '1VU26A', '1VV22A', '1VU29A', 'M0H55AL', '1VV22AL'
        ) 
        AND cal_date > '2020-10-01'
        AND pl = '1N'
        THEN 'LU'
        ELSE pl
    END AS pl,
    base_product_number,
    sales_product_number,
    sales_product_option,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_data_with_pliu_fix
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""


edw_data_with_lu_fix = spark.sql(edw_data_with_lu_fix)
edw_data_with_lu_fix.createOrReplaceTempView("edw_data_with_lu_fix")


# COMMAND ----------

#SYSTEM GOAL = drillable financials, meaning PL-NON-SKU ITEMS SHOULD BE SPREAD TO SKU DATA; IDENTIFY PL-NON-SKU ITEMS HERE

# COMMAND ----------

supplies_final_findata2 = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    base_product_number,
    sales_product_number,
    sales_product_option,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_data_with_lu_fix
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

supplies_final_findata2 = spark.sql(supplies_final_findata2)
supplies_final_findata2.createOrReplaceTempView("supplies_final_findata2")


#mps only
exclusion_list1 = f""" 
SELECT item_number
FROM exclusion
WHERE 1=1
-- We will come back to the MPS / clicks later -- these next six are associated with PPU/MPS
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
    base_product_number,
    sales_product_number,
    sales_product_option,        
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_final_findata2
WHERE sales_product_number NOT IN (SELECT DISTINCT(item_number) FROM exclusion_list1) 
    OR base_product_number NOT IN (SELECT DISTINCT(item_number) FROM exclusion_list1)
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option
"""

supplies_findata_post_exclusions = spark.sql(supplies_findata_post_exclusions)
supplies_findata_post_exclusions.createOrReplaceTempView("supplies_findata_post_exclusions")


# The following item numbers that show up in the sales product field in edw appear to be charges to the PL, e.g. discretionary discount dollars charged to a PL,
# but which cannot be tied back to a SKU level.  These will need to be spread across skus (x sku, x month) later. 

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
    base_product_number,
    sales_product_number,
    sales_product_option,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_findata_post_exclusions 
WHERE 1=1
    AND sales_product_number NOT IN (SELECT sales_product_number FROM allocation_items)
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option, revenue_units

UNION ALL

SELECT cal_date,
    country_alpha2,
    pl,
    base_product_number,
    'PL-CHARGE' AS sales_product_number,
    sales_product_option,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_findata_post_exclusions 
WHERE 1=1
    AND sales_product_number IN (SELECT sales_product_number FROM allocation_items)
    AND revenue_units = 0
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option, revenue_units

UNION ALL

SELECT cal_date,
    country_alpha2,
    pl,
    base_product_number,
    CONCAT('UNKN', pl) AS sales_product_number,
    sales_product_option,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_findata_post_exclusions 
WHERE 1=1
    AND sales_product_number IN (SELECT sales_product_number FROM allocation_items)
    AND revenue_units <> 0
GROUP BY cal_date, country_alpha2, pl, base_product_number, sales_product_number, sales_product_option, revenue_units
"""

supplies_findata_with_allocations = spark.sql(supplies_findata_with_allocations)
supplies_findata_with_allocations.createOrReplaceTempView("supplies_findata_with_allocations")


#drop base product from edw and continue to clean up the 5x5 sales products

edw_supplies_combined_findata_including_iink = f"""
SELECT cal_date,
    country_alpha2,
    pl,
    CASE
        WHEN LENGTH(sales_product_number) = 11 THEN CONCAT(SUBSTRING(sales_product_number, 1, 5), 'A')  -- 5x5's?
        ELSE sales_product_number
    END AS sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_findata_with_allocations
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

edw_supplies_combined_findata_including_iink = spark.sql(edw_supplies_combined_findata_including_iink)
edw_supplies_combined_findata_including_iink.createOrReplaceTempView("edw_supplies_combined_findata_including_iink")


edw_supplies_combined_findata = f"""
SELECT cal_date,
                country_alpha2,
                pl,
                sales_product_number,    
                SUM(gross_revenue) AS gross_revenue,
                SUM(net_currency) AS net_currency,
                SUM(contractual_discounts) AS contractual_discounts,
                SUM(discretionary_discounts) AS discretionary_discounts,
                SUM(warranty) AS warranty,
                SUM(total_cos) AS total_cos,
                SUM(revenue_units) AS revenue_units
            FROM edw_supplies_combined_findata_including_iink
            WHERE pl != 'GD'
            GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

edw_supplies_combined_findata = spark.sql(edw_supplies_combined_findata)
edw_supplies_combined_findata.createOrReplaceTempView("edw_supplies_combined_findata")

# COMMAND ----------

# Write out edw_supplies_combined_findata to its delta table target.
edw_supplies_combined_findata.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/edw_supplies_combined_findata")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.edw_supplies_combined_findata USING DELTA LOCATION '/tmp/delta/fin_stage/edw_supplies_combined_findata'")

#spark.table("fin_stage.edw_supplies_combined_findata").createOrReplaceTempView("edw_supplies_combined_findata")

# COMMAND ----------

spark.table("fin_stage.edw_supplies_combined_findata").createOrReplaceTempView("edw_supplies_combined_findata")

# COMMAND ----------

# PUSH EMEA SUPPLIES FINANCIALS TO COUNTRY USING SELL-THRU DATA (HP FINANCIALS DO NOT PROVIDE COUNTRY SALES IN EMEA)

# COMMAND ----------

finance_sys_recorded_pl = f"""
SELECT distinct cal_date,
    sales_product_number,
    pl as fin_pl
FROM fin_stage.edw_supplies_combined_findata
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM fin_stage.edw_supplies_combined_findata AS sup
JOIN iso_country_code_xref AS geo ON sup.country_alpha2 = geo.country_alpha2
WHERE region_3 = 'EMEA' 
GROUP BY cal_date, sup.country_alpha2, pl, sales_product_number
"""

salesprod_emea_supplies = spark.sql(salesprod_emea_supplies)
salesprod_emea_supplies.createOrReplaceTempView("salesprod_emea_supplies")


salesprod_emea_supplies_lz_gy = f"""
SELECT cal_date,
    pl,
    country_alpha2,
    sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_emea_supplies
WHERE pl IN ('LZ', 'GY')
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

salesprod_emea_supplies_lz_gy = spark.sql(salesprod_emea_supplies_lz_gy)
salesprod_emea_supplies_lz_gy.createOrReplaceTempView("salesprod_emea_supplies_lz_gy")

salesprod_emea_remove_edw_country = f"""
SELECT cal_date,
    pl,
    sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_emea_supplies
WHERE pl NOT IN ('LZ', 'GY')
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
        WHEN Country_Code IN ('0M', '0B', '0C') THEN 'XH'
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
WHERE (cal_date BETWEEN (SELECT MIN(cal_date) FROM edw_supplies_combined_findata)
    AND (SELECT MAX(cal_date) FROM edw_supplies_combined_findata))
    AND pl IN 
    (
        SELECT DISTINCT (pl) 
        FROM product_line_xref 
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


channel_inventory_pl_restated_drivers = f"""
SELECT distinct
    ci2.cal_date,
    ci2.country_alpha2,
    ci2.sales_product_number,
    fin_pl as pl,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty
FROM channel_inventory2 ci2
JOIN finance_sys_recorded_pl fpl
    ON fpl.cal_date = ci2.cal_date
    AND fpl.sales_product_number = ci2.sales_product_number
JOIN mdm.rdma_base_to_sales_product_map rdma
    ON rdma.sales_product_number = ci2.sales_product_number
JOIN supplies_hw_country_actuals_mapping shcam
    ON rdma.base_product_number = shcam.base_product_number
    AND ci2.cal_date = shcam.cal_date
    AND ci2.country_alpha2 = shcam.country_alpha2
WHERE 1=1
AND hp_pages > 0
AND fin_pl IN (select pl from mdm.product_line_xref where pl_category ='SUP' and technology in ('LASER', 'PWA', 'INK'))
GROUP BY ci2.cal_date, ci2.country_alpha2, ci2.sales_product_number, fin_pl
"""

channel_inventory_pl_restated_drivers = spark.sql(channel_inventory_pl_restated_drivers)
channel_inventory_pl_restated_drivers.createOrReplaceTempView("channel_inventory_pl_restated_drivers")


channel_inventory_pl_restated_not_drivers = f"""
SELECT distinct
    ci2.cal_date,
    ci2.country_alpha2,
    ci2.sales_product_number,
    fin_pl as pl,
    SUM(sell_thru_usd) AS sell_thru_usd,
    SUM(sell_thru_qty) AS sell_thru_qty
FROM channel_inventory2 ci2
JOIN finance_sys_recorded_pl fpl
    ON fpl.cal_date = ci2.cal_date
    AND fpl.sales_product_number = ci2.sales_product_number
WHERE 1=1
AND fin_pl NOT IN (select pl from mdm.product_line_xref where pl_category ='SUP' and technology in ('LASER', 'PWA', 'INK'))
GROUP BY ci2.cal_date, ci2.country_alpha2, ci2.sales_product_number, fin_pl
"""

channel_inventory_pl_restated_not_drivers = spark.sql(channel_inventory_pl_restated_not_drivers)
channel_inventory_pl_restated_not_drivers.createOrReplaceTempView("channel_inventory_pl_restated_not_drivers")


channel_inventory_pl_restated = f"""
SELECT cal_date,
    country_alpha2,
    sales_product_number,
    pl,
    sell_thru_usd,
    sell_thru_qty
FROM channel_inventory_pl_restated_drivers

UNION ALL

SELECT cal_date,
    country_alpha2,
    sales_product_number,
    pl,
    sell_thru_usd,
    sell_thru_qty
FROM channel_inventory_pl_restated_not_drivers
"""

channel_inventory_pl_restated = spark.sql(channel_inventory_pl_restated)
channel_inventory_pl_restated.createOrReplaceTempView("channel_inventory_pl_restated")


document_currency_countries = f"""
SELECT DISTINCT    iso.country_alpha2
FROM edw_revenue_document_currency_staging doc
LEFT JOIN profit_center_code_xref pcx ON pcx.profit_center_code = doc.profit_center_code
LEFT JOIN iso_country_code_xref iso ON pcx.country_alpha2 = iso.country_alpha2
WHERE 1=1
    AND doc.net_k_dollars <> 0
    AND iso.country_alpha2 <> 'XW'
    AND doc.document_currency_code <> '?'
    AND iso.region_5 = 'EU'
"""

document_currency_countries = spark.sql(document_currency_countries)
document_currency_countries.createOrReplaceTempView("document_currency_countries")

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
JOIN iso_country_code_xref AS geo ON st.country_alpha2 = geo.country_alpha2
WHERE region_3 = 'EMEA'
  AND sell_thru_usd > 0
  AND sell_thru_qty > 0
  AND st.country_alpha2 IN 
    (
        SELECT country_alpha2
        FROM document_currency_countries
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
INNER JOIN rdma_base_to_sales_product_map rdma ON edw.sales_product_number = rdma.sales_product_number
WHERE pl IN (
        SELECT DISTINCT pl 
        FROM product_line_xref 
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
emea_st_product_mix = f"""
SELECT distinct cal_date,
    country_alpha2,
    sales_product_number,
    CASE
        WHEN SUM(sell_thru_usd) OVER (PARTITION BY cal_date, sales_product_number) = 0 THEN NULL
        ELSE sell_thru_usd / SUM(sell_thru_usd) OVER (PARTITION BY cal_date, sales_product_number)
    END AS product_country_mix,
    CASE
        WHEN SUM(sell_thru_qty) OVER (PARTITION BY cal_date, sales_product_number) = 0 THEN NULL
        ELSE sell_thru_qty / SUM(sell_thru_qty) OVER (PARTITION BY cal_date, sales_product_number)
    END AS unit_country_mix
FROM emea_st est
WHERE 1=1
GROUP BY cal_date, country_alpha2, sales_product_number, sell_thru_usd, sell_thru_qty
"""

emea_st_product_mix = spark.sql(emea_st_product_mix)
emea_st_product_mix.createOrReplaceTempView("emea_st_product_mix")


salesprod_emea_product_mix_country = f"""
SELECT edw.cal_date,
    edw.pl,
    st.country_alpha2,
    edw.sales_product_number,    
    COALESCE(SUM(gross_revenue * product_country_mix), 0) AS gross_revenue,
    COALESCE(SUM(net_currency * product_country_mix), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts * product_country_mix), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts * product_country_mix), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty * product_country_mix), 0) AS warranty,
    COALESCE(SUM(total_cos * product_country_mix), 0) AS total_cos,
    COALESCE(SUM(revenue_units * unit_country_mix), 0) AS revenue_units
FROM salesprod_emea_remove_edw_country edw
JOIN emea_st_product_mix st
    ON edw.cal_date = st.cal_date
    AND edw.sales_product_number = st.sales_product_number
GROUP BY edw.cal_date, edw.pl, edw.sales_product_number, st.country_alpha2
"""

salesprod_emea_product_mix_country = spark.sql(salesprod_emea_product_mix_country)
salesprod_emea_product_mix_country.createOrReplaceTempView("salesprod_emea_product_mix_country")


salesprod_emea_product_mix_country_no_match = f"""
SELECT edw.cal_date,
    edw.pl,
    edw.sales_product_number,    
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM salesprod_emea_remove_edw_country edw
LEFT JOIN emea_st_product_mix st
    ON edw.cal_date = st.cal_date
    AND edw.sales_product_number = st.sales_product_number
WHERE st.country_alpha2 is null
GROUP BY edw.cal_date, edw.pl, edw.sales_product_number
"""

salesprod_emea_product_mix_country_no_match = spark.sql(salesprod_emea_product_mix_country_no_match)
salesprod_emea_product_mix_country_no_match.createOrReplaceTempView("salesprod_emea_product_mix_country_no_match")


emea_st_product_line_mix = f"""
SELECT distinct cal_date,
    country_alpha2,
    pl,
    CASE
        WHEN SUM(sell_thru_usd) OVER (PARTITION BY cal_date, pl) = 0 THEN NULL
        ELSE sell_thru_usd / SUM(sell_thru_usd) OVER (PARTITION BY cal_date, pl)
    END AS product_country_mix,
    CASE
        WHEN SUM(sell_thru_qty) OVER (PARTITION BY cal_date, pl) = 0 THEN NULL
        ELSE sell_thru_qty / SUM(sell_thru_qty) OVER (PARTITION BY cal_date, pl)
    END AS unit_country_mix
FROM emea_st est
WHERE 1=1
GROUP BY cal_date, country_alpha2, pl, sell_thru_usd, sell_thru_qty
"""

emea_st_product_line_mix = spark.sql(emea_st_product_line_mix)
emea_st_product_line_mix.createOrReplaceTempView("emea_st_product_line_mix")


salesprod_emea_product_line_mix_country = f"""
SELECT edw.cal_date,
    edw.pl,
    st.country_alpha2,
    edw.sales_product_number,    
    COALESCE(SUM(gross_revenue * product_country_mix), 0) AS gross_revenue,
    COALESCE(SUM(net_currency * product_country_mix), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts * product_country_mix), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts * product_country_mix), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty * product_country_mix), 0) AS warranty,
    COALESCE(SUM(total_cos * product_country_mix), 0) AS total_cos,
    COALESCE(SUM(revenue_units * unit_country_mix), 0) AS revenue_units
FROM salesprod_emea_product_mix_country_no_match edw
JOIN emea_st_product_line_mix st
    ON edw.cal_date = st.cal_date
    AND edw.pl = st.pl
GROUP BY edw.cal_date, edw.pl, edw.sales_product_number, st.country_alpha2
"""

salesprod_emea_product_line_mix_country = spark.sql(salesprod_emea_product_line_mix_country)
salesprod_emea_product_line_mix_country.createOrReplaceTempView("salesprod_emea_product_line_mix_country")


all_emea_salesprod_country = f"""
SELECT * FROM salesprod_emea_product_mix_country

UNION ALL

SELECT * FROM salesprod_emea_product_line_mix_country

UNION ALL

SELECT * FROM salesprod_emea_supplies_lz_gy
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
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM edw_supplies_combined_findata
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

supplies_salesprod2 = spark.sql(supplies_salesprod2)
supplies_salesprod2.createOrReplaceTempView("supplies_salesprod2")


salesprod_row = f""" 
SELECT cal_date,
    pl,
    sup.country_alpha2,
    sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM supplies_salesprod2 AS sup
JOIN iso_country_code_xref AS geo ON sup.country_alpha2 = geo.country_alpha2
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM all_salesprod_country
GROUP BY cal_date, country_alpha2, pl, sales_product_number
"""

supplies_findata_emea_adjusted = spark.sql(supplies_findata_emea_adjusted)
supplies_findata_emea_adjusted.createOrReplaceTempView("supplies_findata_emea_adjusted")

# COMMAND ----------

#additional clean up the edw data after pushing emea to country; in preparation for adding the additional info
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
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units),0) AS revenue_units
FROM supplies_findata_emea_adjusted AS sp 
JOIN iso_country_code_xref AS geo ON (sp.country_alpha2 = geo.country_alpha2)
WHERE region_5 IN ('AP', 'EU', 'JP', 'LA', 'NA', 'XW')
GROUP BY cal_date, sp.country_alpha2, pl, sales_product_number, region_5
"""

all_salesprod_region5 = spark.sql(all_salesprod_region5)
all_salesprod_region5.createOrReplaceTempView("all_salesprod_region5")


salesprod_all_cleaned = f"""
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM all_salesprod_region5
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number
"""

salesprod_all_cleaned = spark.sql(salesprod_all_cleaned)
salesprod_all_cleaned.createOrReplaceTempView("salesprod_all_cleaned")

# COMMAND ----------

# additional work required on PL IE; PL IE MDM appears to be so problematic, we're removing it, and making a second run at it using the other 3 LF PL's country mix

lf_worldwide_data = f"""
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_all_cleaned
WHERE 1=1
    AND pl IN (SELECT pl FROM product_line_xref where pl_category = 'SUP' AND technology = 'LF')
    OR pl = 'IE'
GROUP BY cal_date, country_alpha2, pl, sales_product_number, region_5
"""

lf_worldwide_data = spark.sql(lf_worldwide_data)
lf_worldwide_data.createOrReplaceTempView("lf_worldwide_data")


lf_worldwide_data_excluding_emea_IE = f"""
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM lf_worldwide_data
WHERE (pl <> 'IE'
    OR region_5 <> 'EU')
GROUP BY cal_date, country_alpha2, pl, sales_product_number, region_5
"""

lf_worldwide_data_excluding_emea_IE = spark.sql(lf_worldwide_data_excluding_emea_IE)
lf_worldwide_data_excluding_emea_IE.createOrReplaceTempView("lf_worldwide_data_excluding_emea_IE")


lf_worldwide_data_emea_IE = f"""
SELECT 
    cal_date,
    region_5,
    pl,
    sales_product_number,    
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_supplies_combined_findata lf
LEFT JOIN iso_country_code_xref iso ON iso.country_alpha2 = lf.country_alpha2
WHERE pl = 'IE'
    AND region_5 = 'EU'
GROUP BY cal_date, pl, sales_product_number, region_5
"""        

lf_worldwide_data_emea_IE = spark.sql(lf_worldwide_data_emea_IE)
lf_worldwide_data_emea_IE.createOrReplaceTempView("lf_worldwide_data_emea_IE")


lf_country_mix_by_month = f"""
SELECT 
    cal_date,
    country_alpha2,
    region_5,
    CASE
        WHEN SUM(gross_revenue) OVER(PARTITION BY cal_date, region_5) = 0 THEN NULL
        ELSE gross_revenue / SUM(gross_revenue) OVER(PARTITION BY cal_date, region_5) 
    END AS dollar_country_mix,
    CASE
        WHEN SUM(revenue_units) OVER(PARTITION BY cal_date, region_5) = 0 THEN NULL
        ELSE revenue_units / SUM(revenue_units) OVER(PARTITION BY cal_date, region_5) 
    END AS unit_country_mix
FROM lf_worldwide_data_excluding_emea_IE
WHERE region_5 = 'EU'
"""    

lf_country_mix_by_month = spark.sql(lf_country_mix_by_month)
lf_country_mix_by_month.createOrReplaceTempView("lf_country_mix_by_month")


ie_emea_country = f"""
SELECT 
    ie.cal_date,
    country_alpha2,
    ie.region_5,
    pl,
    sales_product_number,    
    SUM(gross_revenue * dollar_country_mix) AS gross_revenue,
    SUM(net_currency * dollar_country_mix) AS net_currency,
    SUM(contractual_discounts * dollar_country_mix) AS contractual_discounts,
    SUM(discretionary_discounts * dollar_country_mix) AS discretionary_discounts,
    SUM(warranty * dollar_country_mix) AS warranty,
    SUM(total_cos * dollar_country_mix) AS total_cos,
    SUM(revenue_units * unit_country_mix) AS revenue_units
FROM lf_worldwide_data_emea_IE ie
LEFT JOIN lf_country_mix_by_month lf ON ie.region_5 = lf.region_5 AND ie.cal_date = lf.cal_date
GROUP BY ie.cal_date, pl, sales_product_number, ie.region_5, country_alpha2
"""

ie_emea_country = spark.sql(ie_emea_country)
ie_emea_country.createOrReplaceTempView("ie_emea_country")


lf_worldwide_with_ie_too = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM ie_emea_country
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number
        
UNION ALL

SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM lf_worldwide_data_excluding_emea_IE
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number
"""

lf_worldwide_with_ie_too = spark.sql(lf_worldwide_with_ie_too)
lf_worldwide_with_ie_too.createOrReplaceTempView("lf_worldwide_with_ie_too")


salesprod_all_cleaned2 = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM lf_worldwide_with_ie_too
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number
        
UNION ALL

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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_all_cleaned
WHERE 1=1
    AND pl NOT IN (SELECT pl FROM product_line_xref where pl_category = 'SUP' AND technology = 'LF')
    AND pl <> 'IE'
GROUP BY cal_date, country_alpha2, pl, sales_product_number, region_5
"""

salesprod_all_cleaned2 = spark.sql(salesprod_all_cleaned2)
salesprod_all_cleaned2.createOrReplaceTempView("salesprod_all_cleaned2")

# COMMAND ----------

#write out salesprod_all_cleaned2 to a delta table target.
salesprod_all_cleaned2.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/salesprod_all_cleaned2")

#create the table
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.salesprod_all_cleaned2 USING DELTA LOCATION '/tmp/delta/fin_stage/salesprod_all_cleaned2'")

# COMMAND ----------

spark.table("fin_stage.salesprod_all_cleaned2").createOrReplaceTempView("salesprod_all_cleaned2")

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
FROM mps_ww_shipped_supply_staging AS mps
JOIN calendar AS cal ON mps.Month = cal.Date
WHERE Prod_Line IN 
    (
    SELECT DISTINCT pl 
    FROM product_line_xref 
    WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
        AND PL_category IN ('SUP', 'LLC') 
    -- excludes GD in case there are any; GD has a different business model; would not expect GD volumes from mps
        AND pl NOT IN ('GD') -- would we expect there to be GY or LZ units for mps?
        OR PL IN ('GO', 'GN', 'IE')
    ) 
    AND Fiscal_Yr > '2015' AND Day_of_Month = 1
    AND cal.Date < '2021-11-01'
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
JOIN iso_country_code_xref AS geo ON mps.country = geo.country
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_all_cleaned2
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_all_cleaned2
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
FROM calendar
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
    total_cos,
    revenue_units,
    indirect_units,
    direct_units,
        (select min(cal_date) from salesprod_all_cleaned2) as min_cal_date,
        (select max(cal_date) from salesprod_all_cleaned2) as max_cal_date
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
    0 AS total_cos,
    SUM(sales_quantity) AS revenue_units
FROM supplies_iink_units_landing AS iink
LEFT JOIN iso_country_code_xref AS c ON iink.country = c.country
LEFT JOIN calendar AS cal ON cal_Date = cal.Date
WHERE Fiscal_Yr > 2015 AND sales_quantity != 0 AND Day_of_Month = 1
AND cal_date < '2021-11-01'
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
    sum(total_cos) as total_cos,
    sum(revenue_units) as revenue_units
FROM iink_units iink
LEFT JOIN iso_country_code_xref iso ON iink.country_alpha2 = iso.country_alpha2
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
FROM ib ib
LEFT JOIN iso_country_code_xref iso ON iso.country_alpha2 = ib.country_alpha2
WHERE 1=1
    AND ib.version = (select max(version) from ib)
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
LEFT JOIN rdma_base_to_sales_product_map rdma ON iink.sales_product_number = rdma.sales_product_number
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
LEFT JOIN supplies_hw_mapping map ON iink.base_product_number = map.base_product_number
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
    coalesce(sum(total_cos), 0) as total_cos,
    coalesce(sum(revenue_units), 0) as revenue_units
FROM iink_country_join
WHERE 1=1
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

iink_country = spark.sql(iink_country)
iink_country.createOrReplaceTempView("iink_country")

# COMMAND ----------

#itp actuals: ibp switched systems, losing market10 integrity in 9-21.  Code below retains market10 integrity from IBP (although country is now known); starting in 9-21, country determined by IB mix
# 1. data from ibp for itp

itp_units_ibp = f"""    
SELECT 
    cal_date,
    market10,
    sales_product as sales_product_number,
    sum(units) as revenue_units
FROM itp_laser_landing
WHERE 1=1
AND units <> 0
AND cal_date < '2021-11-01'
GROUP BY cal_date, market10, sales_product
"""

itp_units_ibp = spark.sql(itp_units_ibp)
itp_units_ibp.createOrReplaceTempView("itp_units_ibp")


itp_units_with_valid_mkt10 = f"""    
SELECT 
    cal_date,
    market10,
    sales_product_number,
    sum(revenue_units) as revenue_units
FROM itp_units_ibp
WHERE 1=1
AND cal_date < '2021-09-01'
GROUP BY cal_date, market10, sales_product_number
"""

itp_units_with_valid_mkt10 = spark.sql(itp_units_with_valid_mkt10)
itp_units_with_valid_mkt10.createOrReplaceTempView("itp_units_with_valid_mkt10")


simplified_country_list = f"""
SELECT distinct market10, country_alpha2, country 
FROM iso_country_code_xref
WHERE country_alpha2 IN ('DE', 'NL', 'MX', 'AU', 'IT', 'TW', 'IN', 'US', 'GB', 'TR') 
"""

simplified_country_list = spark.sql(simplified_country_list)
simplified_country_list.createOrReplaceTempView("simplified_country_list")


itp_valid_mkt_10_plus_country = f"""
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
FROM itp_units_with_valid_mkt10 itp
LEFT JOIN simplified_country_list scl ON scl.market10 = itp.market10
GROUP BY cal_date, country_alpha2, sales_product_number
"""

itp_valid_mkt_10_plus_country = spark.sql(itp_valid_mkt_10_plus_country)
itp_valid_mkt_10_plus_country.createOrReplaceTempView("itp_valid_mkt_10_plus_country")


itp_units_ibp_bad_mkt10 = f"""    
SELECT 
    cal_date,
    market10,
    sales_product_number,
    sum(revenue_units) as revenue_units
FROM itp_units_ibp
WHERE 1=1
AND cal_date > '2021-08-01'
GROUP BY cal_date, market10, sales_product_number
"""

itp_units_ibp_bad_mkt10 = spark.sql(itp_units_ibp_bad_mkt10)
itp_units_ibp_bad_mkt10.createOrReplaceTempView("itp_units_ibp_bad_mkt10")


# add region_5
itp_mkt10_reg5 = f"""
SELECT
	distinct region_5, market10	
FROM iso_country_code_xref
WHERE 1=1 
AND market10 IN (SELECT distinct market10 FROM itp_units_ibp_bad_mkt10)
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
FROM itp_units_ibp_bad_mkt10 ibp
LEFT JOIN itp_mkt10_reg5 map
ON ibp.market10 = map.market10
GROUP BY cal_date, ibp.market10, region_5, sales_product_number
"""    

itp_units_ibp2 = spark.sql(itp_units_ibp2)
itp_units_ibp2.createOrReplaceTempView("itp_units_ibp2")
    

itp_units_ibp3 = f"""
SELECT
	cal_date,
	region_5,
	sales_product_number,
	sum(revenue_units) as revenue_units
FROM itp_units_ibp2 ibp
GROUP BY cal_date, region_5, sales_product_number
"""    

itp_units_ibp3 = spark.sql(itp_units_ibp3)
itp_units_ibp3.createOrReplaceTempView("itp_units_ibp3")   
    

# 2. create a market10 to country alpha 2 map for itp
# what are the top ib countries by market10?
# first, map salesprod to baseprod for itp skus

itp_salesprod_to_baseprod = f"""
SELECT cal_date,
    region_5,
    itp.sales_product_number,
    rdma.base_product_number
FROM itp_units_ibp2 itp
LEFT JOIN rdma_base_to_sales_product_map rdma ON itp.sales_product_number = rdma.sales_product_number
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
LEFT JOIN supplies_hw_mapping map 
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
FROM ib ib
LEFT JOIN iso_country_code_xref iso ON iso.country_alpha2 = ib.country_alpha2
WHERE 1=1
    AND ib.version = (select max(version) from ib)
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
WHERE ib is not null
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
    sum(revenue_units) as original_units,
	sum(revenue_units * COALESCE(ib_country_mix, 1)) as revenue_units
FROM itp_units_ibp3 itp
LEFT JOIN itp_ib_country_mix c 
	ON c.region_5 = itp.region_5
	AND c.cal_date = itp.cal_date
	AND c.sales_product_number = itp.sales_product_number
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
    CASE
        WHEN region_5 = 'EU' AND country_alpha2 is null THEN 'DE'
        WHEN region_5 = 'AP' AND country_alpha2 is null THEN 'NZ'
        WHEN region_5 = 'NA' AND country_alpha2 is null THEN 'US'
        ELSE country_alpha2
    END AS country_alpha2,
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
GROUP BY cal_date, country_alpha2, sales_product_number, region_5

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,
    ce_split,
    sum(gross_revenue) AS gross_revenue,
    sum(net_currency) AS net_currency,
    sum(contractual_discounts) AS contractual_discounts,
    sum(discretionary_discounts) AS discretionary_discounts,
    sum(warranty) AS warranty,
    sum(other_cos) as other_cos,
    sum(total_cos) AS total_cos,
    sum(revenue_units) as revenue_units
FROM itp_valid_mkt_10_plus_country
GROUP BY cal_date, country_alpha2, sales_product_number, ce_split, pl
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
    SUM(total_COS) AS total_cos
FROM supplies_manual_mcode_jv_detail_landing AS jv
JOIN product_line_xref AS pl ON jv.pl = pl.plxx
JOIN calendar AS cal ON jv.yearmon = cal.Calendar_Yr_Mo
LEFT JOIN iso_country_code_xref AS iso ON jv.country = iso.country
WHERE 1=1 
    AND Day_of_Month = 1
    AND cal.Date < '2021-11-01'
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
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM designated_mcodes
GROUP BY cal_date, pl, country_alpha2, sales_product_number        
"""

format_mcodes = spark.sql(format_mcodes)
format_mcodes.createOrReplaceTempView("format_mcodes")


mcodes_offset = f"""
SELECT
    cal_date,
    pl,
    country_alpha2,
    'PL-CHARGE' AS sales_product_number,
    'TRAD' AS ce_split,                
    SUM(gross_revenue) * -1 AS gross_revenue,
    0 AS net_currency,
    SUM(contractual_discounts) * -1 AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS warranty,
    SUM(total_cos) * -1 AS total_cos,
    0 AS revenue_units
FROM format_mcodes
GROUP BY cal_date, pl, country_alpha2, sales_product_number        
"""

mcodes_offset = spark.sql(mcodes_offset)
mcodes_offset.createOrReplaceTempView("mcodes_offset")

# leaving old code here for now as reference; updated 5-22-2023
mps_revenue_from_edw = f"""
SELECT 
    Date AS cal_date,
    geo.country_alpha2,
    pl,
    'EST_MPS_REVENUE_JV' AS sales_product_number,
    'TRAD' AS ce_split,
    SUM(transaction_detail_us_dollar_amount) AS gross_revenue
FROM edw_revenue_dollars_staging edw
JOIN calendar AS cal ON revenue_recognition_fiscal_year_month_code = Edw_fiscal_yr_mo
JOIN profit_center_code_xref AS pcx ON pcx.profit_center_code = edw.profit_center_code
JOIN iso_country_code_xref AS geo ON pcx.country_alpha2 = geo.country_alpha2
JOIN product_line_xref AS pl ON business_area_code = plxx
WHERE Day_of_Month = 1
    AND product_base_identifier IN ('H7503A', 'H7509A', 'U1001AC', 'H7523A', 'U07LCA', 'UE266_001') -- from RDMA and confirmed by MPS
    AND functional_area_level_11_name = 'GROSS_REVENUE'
    AND Date < '2021-11-01'
GROUP BY Date, geo.country_alpha2, pl 
"""

mps_revenue_from_edw = spark.sql(mps_revenue_from_edw)
mps_revenue_from_edw.createOrReplaceTempView("mps_revenue_from_edw")


mps_revenue_from_card = f"""
SELECT
    cal_date,
    country_alpha2,
    pl,
    'EST_MPS_REVENUE_JV' AS sales_product_number,
    'TRAD' AS ce_split,
    SUM(usd_amount) as gross_revenue
FROM fin_prod.mps_card_revenue
WHERE 1=1
    AND usd_amount <> 0
    AND product_number IN ('H7503A','H7509A','H7523A','U07LCA','U1001AC','UE266_001')
    AND cal_date < '2021-11-01'
    AND pl = '5T'
GROUP BY cal_date, country_alpha2, pl
"""

mps_revenue_from_card = spark.sql(mps_revenue_from_card)
mps_revenue_from_card.createOrReplaceTempView("mps_revenue_from_card")


estimated_mps_revenue = f"""
SELECT 
    cal_date,
    country_alpha2,
    pl,
    sales_product_number,                
    ce_split,    
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    0 AS net_currency,
    0 AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS warranty,
    0 AS total_cos,
    0 AS revenue_units
FROM mps_revenue_from_card
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
    CASE
        WHEN country_alpha2 = 'XW' THEN 'US'
        ELSE country_alpha2
    END AS country_alpha2,
    pl,
    sales_product_number,
    ce_split,    
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts),0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM salesprod_add_mcodes
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split
"""    

salesprod_final_before_charges_spread = spark.sql(salesprod_final_before_charges_spread)
salesprod_final_before_charges_spread.createOrReplaceTempView("salesprod_final_before_charges_spread")


# COMMAND ----------

# SPREAD CHARGES

# COMMAND ----------

salesprod_before_plcharges_temp = f"""
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_final_before_charges_spread edw
LEFT JOIN calendar cal 
    ON cal.Date = edw.cal_date
LEFT JOIN iso_country_code_xref iso
    ON edw.country_alpha2 = iso.country_alpha2
WHERE 1=1
AND day_of_month = 1
GROUP BY cal_date, pl, edw.country_alpha2, sales_product_number, ce_split, Fiscal_Month, region_5
"""    

salesprod_before_plcharges_temp = spark.sql(salesprod_before_plcharges_temp)
salesprod_before_plcharges_temp.createOrReplaceTempView("salesprod_before_plcharges_temp")

# COMMAND ----------

# Write out salesprod_before_plcharges_temp to its delta table target.
salesprod_before_plcharges_temp.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/salesprod_before_plcharges_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.salesprod_before_plcharges_temp USING DELTA LOCATION '/tmp/delta/fin_stage/salesprod_before_plcharges_temp'")

#spark.table("fin_stage.salesprod_before_plcharges_temp").createOrReplaceTempView("salesprod_before_plcharges_temp")

# COMMAND ----------

spark.table("fin_stage.salesprod_before_plcharges_temp").createOrReplaceTempView("salesprod_before_plcharges_temp2")

# COMMAND ----------

salesprod_pre_spread_unadjusted = f"""
SELECT 
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    Fiscal_month,
    gross_revenue,
    net_currency,
    contractual_discounts,
    discretionary_discounts,
    warranty,
    total_cos,
    revenue_units
FROM salesprod_before_plcharges_temp2
WHERE 1=1
AND pl != 'LU'
"""

salesprod_pre_spread_unadjusted = spark.sql(salesprod_pre_spread_unadjusted)
salesprod_pre_spread_unadjusted.createOrReplaceTempView("salesprod_pre_spread_unadjusted")


salesprod_pre_spread_lu_to_adjust = f"""
SELECT 
    cal_date,
    country_alpha2,
    region_5,
    pl,
    CASE
        WHEN (sales_product_number = 'PL-CHARGE' 
        AND (Fiscal_Month = 3 OR Fiscal_Month = 6 OR Fiscal_Month = 9 OR Fiscal_Month = 12)
        AND cal_date > '2020-10-01')
        THEN 'UNKNLU'
        ELSE sales_product_number
    END AS sales_product_number,
    ce_split,
    Fiscal_month,
    gross_revenue,
    net_currency,
    contractual_discounts,
    discretionary_discounts,
    warranty,
    total_cos,
    revenue_units
FROM salesprod_before_plcharges_temp2
WHERE 1=1
AND pl = 'LU'
"""

salesprod_pre_spread_lu_to_adjust = spark.sql(salesprod_pre_spread_lu_to_adjust)
salesprod_pre_spread_lu_to_adjust.createOrReplaceTempView("salesprod_pre_spread_lu_to_adjust")


salesprod_pre_spread_join_back = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    Fiscal_month,
    gross_revenue,
    net_currency,
    contractual_discounts,
    discretionary_discounts,
    warranty,
    total_cos,
    revenue_units
FROM salesprod_pre_spread_lu_to_adjust

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    Fiscal_month,
    gross_revenue,
    net_currency,
    contractual_discounts,
    discretionary_discounts,
    warranty,
    total_cos,
    revenue_units
FROM salesprod_pre_spread_unadjusted

"""

salesprod_pre_spread_join_back = spark.sql(salesprod_pre_spread_join_back)
salesprod_pre_spread_join_back.createOrReplaceTempView("salesprod_pre_spread_join_back")


salesprod_prepre_spread = f"""
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_pre_spread_join_back
WHERE 1=1
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

salesprod_prepre_spread = spark.sql(salesprod_prepre_spread)
salesprod_prepre_spread.createOrReplaceTempView("salesprod_prepre_spread")


salesprod_pre_spread_noISS = f"""
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_prepre_spread
WHERE 1=1
    AND pl <> 'LU' -- add G0 later for CTSS
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

salesprod_pre_spread_noISS = spark.sql(salesprod_pre_spread_noISS)
salesprod_pre_spread_noISS.createOrReplaceTempView("salesprod_pre_spread_noISS")


salesprod_with_ISS = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    0 AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    0 AS discretionary_discounts,
    0 as warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_prepre_spread
WHERE 1=1
    AND pl = 'LU' -- add G0 later for CTSS
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split        
"""

salesprod_with_ISS = spark.sql(salesprod_with_ISS)
salesprod_with_ISS.createOrReplaceTempView("salesprod_with_ISS")


salesprod_with_ISS_currency = f"""
SELECT
    cal_date,
    country_alpha2,
    region_5,
    pl,
    'PL-CHARGE' AS sales_product_number,
    ce_split,
    0 AS gross_revenue,
    SUM(net_currency) AS net_currency,
    0 AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    0 AS total_cos,
    0 AS revenue_units
FROM salesprod_prepre_spread
WHERE 1=1
    AND pl IN ('LU') -- add G0 later for CTSS
GROUP BY cal_date, country_alpha2, region_5, pl, ce_split        
"""

salesprod_with_ISS_currency = spark.sql(salesprod_with_ISS_currency)
salesprod_with_ISS_currency.createOrReplaceTempView("salesprod_with_ISS_currency")


post_ISS_adjustment = f"""
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_pre_spread_noISS
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, revenue_units

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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_with_ISS
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, revenue_units

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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_with_ISS_currency
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, revenue_units
"""

post_ISS_adjustment = spark.sql(post_ISS_adjustment)
post_ISS_adjustment.createOrReplaceTempView("post_ISS_adjustment")


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
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units),0) AS revenue_units
FROM post_ISS_adjustment
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
    SUM(total_cos * COALESCE(edw_gross_rev_mix, 1)) AS total_cos
FROM salesprod_spread_able_with_charges2 p
LEFT JOIN gross_rev_product_mix_by_country mix ON
    p.cal_date = mix.cal_date AND
    p.country_alpha2 = mix.country_alpha2 AND
    p.pl = mix.pl AND
    p.ce_split = mix.ce_split
WHERE sales_product_number is not null
GROUP BY p.cal_date, p.country_alpha2, region_5, p.pl, p.ce_split, sales_product_number
"""

pl_charges_spread_to_SKU1 = spark.sql(pl_charges_spread_to_SKU1)
pl_charges_spread_to_SKU1.createOrReplaceTempView("pl_charges_spread_to_SKU1")


pl_charges_spread_to_SKU2 = f"""
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
    SUM(total_cos * COALESCE(edw_gross_rev_mix, 1)) AS total_cos
FROM salesprod_spread_able_with_charges2 p
LEFT JOIN gross_rev_product_mix_by_country mix ON
    p.cal_date = mix.cal_date AND
    p.country_alpha2 = mix.country_alpha2 AND
    p.pl = mix.pl AND
    p.ce_split = mix.ce_split
WHERE sales_product_number is null
GROUP BY p.cal_date, p.country_alpha2, region_5, p.pl, p.ce_split, sales_product_number
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
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM pl_charges_spread_to_SKU1
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
FROM rdma_base_to_sales_product_map
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_edw_spread edw
LEFT JOIN rdma_updated_sku_PLs rdma ON edw.sales_product_number = rdma.sales_product_number
WHERE edw.pl <> 'GD' AND rdma.sales_product_line_code <> 'GD'
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_product_line_restated edw
WHERE 1=1 
    AND sales_product_number <> 'PL-CHARGE' -- why does these exist; values are all zero
    AND pl NOT IN ('IE', 'IX') -- inactive PLs for LF; at this point, any sales products in IE/TX would be unmapped or UNKN and can be dropped
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_edw_spread
WHERE pl = 'GD'
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_edw_spread
WHERE sales_product_number IN ('CISS', 'CTSS', 'BIRDS', 'EST_MPS_REVENUE_JV', 'LFMPS')
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
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM updated_pl_plus_gd
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split
"""

edw_data_with_updated_rdma_pl2 = spark.sql(edw_data_with_updated_rdma_pl2)
edw_data_with_updated_rdma_pl2.createOrReplaceTempView("edw_data_with_updated_rdma_pl2")

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
FROM list_price_eu_country_list
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
FROM country_currency_map_landing cmap
LEFT JOIN iso_country_code_xref iso ON cmap.country_alpha2 = iso.country_alpha2 AND cmap.country = iso.country
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
    CASE
        WHEN country_alpha2 = 'XW' THEN 'US'
        ELSE country_alpha2
    END AS country_alpha2,
    CASE
        WHEN country_alpha2 = 'XW' THEN 'NA'
        ELSE region_5
    END AS region_5,
    pl,
    sales_product_number,
    ce_split,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM edw_data_with_updated_rdma_pl2 redw
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
                                FROM iso_country_code_xref
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
                                FROM iso_country_code_xref
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
                                FROM iso_country_code_xref
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
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(warranty), 0) AS warranty,
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_data_normalish_items  AS act
LEFT JOIN calendar AS cal ON cal_date = cal.Date
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_currency_emea2
WHERE country_alpha2 NOT IN (
                                SELECT country_alpha2
                                FROM iso_country_code_xref
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_currency_emea2
WHERE country_alpha2 IN (
                                SELECT country_alpha2
                                FROM iso_country_code_xref
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
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM emea_salesprod_xcode_adjusted
GROUP BY cal_date, country_alpha2, region_5, pl, sales_product_number, ce_split, currency
"""

emea_salesprod_xcode_adjusted2 = spark.sql(emea_salesprod_xcode_adjusted2)
emea_salesprod_xcode_adjusted2.createOrReplaceTempView("emea_salesprod_xcode_adjusted2")    

# COMMAND ----------

edw_document_currency_landing = f"""
SELECT
    cal.Date as cal_date,
    Fiscal_Year_Qtr,
    Fiscal_Yr,
    pl,
    iso.country_alpha2,
    iso.region_5,
    document_currency_code,
    SUM(net_k_dollars) * 1000 AS revenue -- at net revenue level but sources does not have hedge, so equivalent to revenue before hedge
FROM fin_stage.edw_revenue_document_currency_staging doc
LEFT JOIN calendar cal ON fiscal_year_month_code = Edw_fiscal_yr_mo
LEFT JOIN profit_center_code_xref pcx ON pcx.profit_center_code = doc.profit_center_code
LEFT JOIN iso_country_code_xref iso ON pcx.country_alpha2 = iso.country_alpha2
LEFT JOIN product_line_xref plx ON plxx = business_area_code
WHERE 1=1
    AND Day_of_month = 1
    AND  net_k_dollars <> 0
    AND iso.country_alpha2 <> 'XW'
    AND document_currency_code <> '?'
    AND cal.Date < '2021-11-01'
GROUP BY cal.Date, iso.country_alpha2, document_currency_code, pl, iso.region_5, Fiscal_Year_Qtr, Fiscal_Yr
"""

edw_document_currency_landing = spark.sql(edw_document_currency_landing)
edw_document_currency_landing.createOrReplaceTempView("edw_document_currency_landing")


edw_document_currency_2023_restatements = f"""
SELECT
    cal_date,
    Fiscal_Year_Qtr,
    Fiscal_Yr,
    CASE
        WHEN pl = '65' THEN 'UD'
        WHEN pl = 'EO' THEN 'GL'
        WHEN pl = 'GM' THEN 'K6'
        ELSE pl
    END AS pl,
    country_alpha2,
    region_5,
    document_currency_code,
    SUM(revenue) AS revenue 
FROM edw_document_currency_landing
WHERE 1=1
GROUP BY cal_date,
    Fiscal_Year_Qtr,
    Fiscal_Yr,
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
    Fiscal_Year_Qtr,
    Fiscal_Yr,
    pl,
    country_alpha2,
    region_5,
    document_currency_code,
    SUM(revenue) AS revenue
FROM edw_document_currency_2023_restatements
WHERE 1=1
GROUP BY cal_date,
    Fiscal_Year_Qtr,
    Fiscal_Yr,
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
    END AS document_currency_mix1
FROM edw_document_currency1
GROUP BY cal_date, document_currency_code, pl, region_5, Fiscal_Year_Qtr, Fiscal_Yr, revenue
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
    ABS(CAST(document_currency_mix1 as decimal (11, 5))) AS document_currency_mix1
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
WHERE document_currency_mix1 > 0.01
GROUP BY cal_date, document_currency_code, pl, region_5, Fiscal_Year_Qtr, Fiscal_Yr, revenue, doc_date
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


document_currency_time_availability = f"""
-- build out history from edw for IE2 history
SELECT distinct cal_date
FROM country_currency_mix2
"""

document_currency_time_availability = spark.sql(document_currency_time_availability)
document_currency_time_availability.createOrReplaceTempView("document_currency_time_availability")


currency_history_needed = f"""
SELECT distinct sales.cal_date
FROM actuals_supplies_salesprod sales
LEFT JOIN document_currency_time_availability doc ON doc.cal_date = sales.cal_date
WHERE doc.cal_date is null
"""

currency_history_needed = spark.sql(currency_history_needed)
currency_history_needed.createOrReplaceTempView("currency_history_needed")


placeholder_mix = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    SUM(country_currency_mix) AS country_currency_mix
FROM country_currency_mix2
WHERE cal_date = (SELECT MIN(cal_date) FROM country_currency_mix2)
GROUP BY cal_date, country_alpha2, currency, region_5, pl
"""

placeholder_mix = spark.sql(placeholder_mix)
placeholder_mix.createOrReplaceTempView("placeholder_mix")


currency_history_needed2 = f"""
SELECT history.cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    country_currency_mix
FROM placeholder_mix pm
CROSS JOIN currency_history_needed history
"""

currency_history_needed2 = spark.sql(currency_history_needed2)
currency_history_needed2.createOrReplaceTempView("currency_history_needed2")


country_currency_mix3 = f"""
SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    country_currency_mix
FROM country_currency_mix2 c

UNION ALL

SELECT
    cal_date,
    country_alpha2,
    currency,
    region_5,
    pl,
    country_currency_mix
FROM currency_history_needed2
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_with_country_detail_ams_ap
WHERE 1=1
GROUP BY cal_date, country_alpha2, currency, region_5, pl, sales_product_number, ce_split
"""        
        
salesprod_with_country_detail_exclude_emea = spark.sql(salesprod_with_country_detail_exclude_emea)
salesprod_with_country_detail_exclude_emea.createOrReplaceTempView("salesprod_with_country_detail_exclude_emea")


salesprod_with_country_detail3 = f"""
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
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units),0) AS revenue_units
FROM salesprod_with_country_detail_exclude_emea
GROUP BY cal_date, country_alpha2, currency, region_5, pl, sales_product_number, ce_split
"""

salesprod_with_country_detail3 = spark.sql(salesprod_with_country_detail3)
salesprod_with_country_detail3.createOrReplaceTempView("salesprod_with_country_detail3")

# COMMAND ----------

# Write out salesprod_with_country_detail3 to its delta table target.
salesprod_with_country_detail3.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/salesprod_with_country_detail3")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.salesprod_with_country_detail3 USING DELTA LOCATION '/tmp/delta/fin_stage/salesprod_with_country_detail3'")

spark.table("fin_stage.salesprod_with_country_detail3").createOrReplaceTempView("salesprod_with_country_detail3")

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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_with_country_detail3  AS act
WHERE 1=1
    AND country_alpha2 NOT IN (
                                SELECT country_alpha2
                                FROM iso_country_code_xref
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_with_country_detail3
WHERE 1=1
    AND country_alpha2 IN (
                                SELECT country_alpha2
                                FROM iso_country_code_xref
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
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM data_set_reunioned
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, currency, region_5
"""

salesprod_normal_with_currency2 = spark.sql(salesprod_normal_with_currency2)
salesprod_normal_with_currency2.createOrReplaceTempView("salesprod_normal_with_currency2")


xcode_adjusted_data = f"""
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_normal_with_currency2
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, currency, region_5
"""

xcode_adjusted_data = spark.sql(xcode_adjusted_data)
xcode_adjusted_data.createOrReplaceTempView("xcode_adjusted_data")

# COMMAND ----------

# Write out salesprod_with_country_detail3 to its delta table target. 
xcode_adjusted_data.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/xcode_adjusted_data")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.xcode_adjusted_data USING DELTA LOCATION '/tmp/delta/fin_stage/xcode_adjusted_data'")

spark.table("fin_stage.xcode_adjusted_data").createOrReplaceTempView("xcode_adjusted_data")

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
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(revenue_units), 0) AS revenue_units
FROM xcode_adjusted_data
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
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM xcode_adjusted_data2
WHERE 1=1
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, currency, region_5
"""

salesprod_preplanet_with_currency_map1 = spark.sql(salesprod_preplanet_with_currency_map1)
salesprod_preplanet_with_currency_map1.createOrReplaceTempView("salesprod_preplanet_with_currency_map1")

# COMMAND ----------

# TIE OUT TO OFFICIAL FINANCIALS

# COMMAND ----------

planet_extract = f"""
SELECT 
    cal_date,
    Fiscal_Yr,
    p.country_alpha2,
    region_5,
    pl,
    SUM(l2fa_gross_trade_rev * 1000) AS p_gross_revenue,
    SUM(l2fa_net_currency * 1000) AS p_net_currency,
    SUM(l2fa_contra_rev_contractual * -1000) AS p_contractual_discounts, -- flipping signs to make compatible with EDW data
    SUM(l2fa_contra_rev_discretionary * -1000) AS p_discretionary_discounts,
    SUM(l2fa_warranty * -1000) AS p_warranty,
    SUM(l2fa_total_cos * -1000) AS p_total_cos
FROM planet_actuals AS p
JOIN iso_country_code_xref AS iso ON p.country_alpha2 = iso.country_alpha2
JOIN calendar AS cal ON cal.Date = p.cal_date
WHERE pl IN 
    (
    SELECT DISTINCT (pl) 
    FROM product_line_xref 
    WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
        AND PL_category IN ('SUP', 'LLC')
    )
    AND Fiscal_Yr > '2016'
    AND Day_of_Month = 1 
    AND cal_date < '2021-11-01'
    AND l2fa_gross_trade_rev + l2fa_net_currency + l2fa_contra_rev_contractual + l2fa_contra_rev_discretionary + l2fa_warranty + l2fa_total_cos != 0
GROUP BY cal_date, p.country_alpha2, pl, region_5, Fiscal_Yr
"""

planet_extract = spark.sql(planet_extract)
planet_extract.createOrReplaceTempView("planet_extract")


planet_cleaned = f"""
SELECT
    cal_date,
    Fiscal_Yr,
    country_alpha2,
    region_5,
    CASE
        WHEN pl = 'IX' THEN 'TX' -- restating all history:: does finance want this or does it want only 1-2 years restated?
        ELSE pl
    END AS pl,
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,            
    SUM(p_total_cos) - SUM(p_warranty) AS p_total_cos_excluding_warranty -- this is really other cost of sales
FROM planet_extract
GROUP BY cal_date, country_alpha2, region_5, pl, Fiscal_Yr
"""
        
planet_cleaned = spark.sql(planet_cleaned)
planet_cleaned.createOrReplaceTempView("planet_cleaned")


planet_targets_excluding_toner_sacp_restatements = f"""
SELECT cal_date,
    Fiscal_Yr,
    region_5,
    pl,
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,
    SUM(p_total_cos_excluding_warranty) AS p_total_cos
FROM planet_cleaned
WHERE 1=1

-- the profit centers for which there will be restatements provided by Finance

AND pl NOT IN 

(
SELECT DISTINCT (pl) 
FROM product_line_xref 
WHERE Technology IN ('LASER')
AND PL_category IN ('SUP')
AND pl NOT IN ('LZ', 'GY', 'N4', 'N5')
)

OR Fiscal_Yr NOT IN ('2020', '2021')
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets_excluding_toner_sacp_restatements = spark.sql(planet_targets_excluding_toner_sacp_restatements)
planet_targets_excluding_toner_sacp_restatements.createOrReplaceTempView("planet_targets_excluding_toner_sacp_restatements")


calendar_table = f"""
SELECT date_key, 
    Date as cal_date, 
    fiscal_yr, 
    month_abbrv 
FROM calendar
where day_of_month = 1
"""

calendar_table = spark.sql(calendar_table)
calendar_table.createOrReplaceTempView("calendar_table")


calendar_table2 = f"""
select date_key, 
    cal_date, 
    fiscal_yr,
    CONCAT(Month_abbrv,'FY',SUBSTRING(fiscal_yr,3,4)) as sacp_fiscal_period
FROM calendar_table
"""

calendar_table2 = spark.sql(calendar_table2)
calendar_table2.createOrReplaceTempView("calendar_table2")


# add incremental dollars to PL5T because of hierarchy restatements in FY2022.  PL G9 and K8 (mps related services) were forced into PL5T by external factors; it wasn't local finance's decision
incremental_data_raw = f"""
SELECT 
    pl,
    l6_description,
    CASE    
        WHEN market = 'AMERICAS HQ' THEN 'NORTH AMERICA'
        WHEN market = 'APJ HQ L2' THEN 'GREATER ASIA'
        WHEN market = 'EMEA' THEN 'CENTRAL EUROPE'
        WHEN market = 'INDIA B SL' THEN 'INDIA SL & BL'
        WHEN market LIKE 'WW%' THEN 'WORLD WIDE'
        ELSE market
    END AS market10,
    CONCAT(
        LEFT(sacp_date_period, 3),
        'FY',
        SUBSTRING(sacp_date_period, 8, 2)
        ) as sacp_fiscal_period,
    SUM(COALESCE(gross_trade_revenues, 0)) as gross_revenue,
    SUM(COALESCE(net_currency, 0))  as net_currency,
    SUM(COALESCE(contractual_discounts, 0) * -1) as contractual_discounts,
    SUM((COALESCE(trade_discounts,0) - COALESCE(contractual_discounts,0))  * -1) as discretionary_discounts,
    SUM(COALESCE(warranty,0)  * -1) as warranty,
    SUM((COALESCE(total_cost_of_sales,0) - COALESCE(warranty, 0))  * -1) as total_cos
FROM supplies_finance_hier_restatements_2020_2021
GROUP BY market, l6_description, sacp_date_period, pl
"""

incremental_data_raw = spark.sql(incremental_data_raw)
incremental_data_raw.createOrReplaceTempView("incremental_data_raw")


incremental_hierarchy_data = f"""
SELECT
    cal_date,
    fiscal_yr,
    CASE    
        WHEN market10 = 'CENTRAL EUROPE' THEN 'EU'
        WHEN market10 = 'LATIN AMERICA' THEN 'LA'
        WHEN market10 = 'NORTH AMERICA' THEN 'NA'
        WHEN market10 IN ('INDIA SL & BL', 'GREATER ASIA', 'GREATER CHINA') THEN 'AP'
        ELSE 'XW'
    END AS region_5,
    pl,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos
FROM incremental_data_raw r 
left join calendar_table2 cal ON r.sacp_fiscal_period = cal.sacp_fiscal_period
WHERE 1=1
GROUP BY cal_date, market10, fiscal_yr, pl
"""

incremental_hierarchy_data = spark.sql(incremental_hierarchy_data)
incremental_hierarchy_data.createOrReplaceTempView("incremental_hierarchy_data")


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
    SUM(p_total_cos) AS p_total_cos
FROM planet_targets_excluding_toner_sacp_restatements
GROUP BY cal_date, region_5, pl, Fiscal_Yr

UNION ALL

SELECT cal_date,
    Fiscal_Yr,
    region_5,
    pl,
    SUM(gross_revenue) AS p_gross_revenue,
    SUM(net_currency) AS p_net_currency,
    SUM(contractual_discounts) AS p_contractual_discounts,
    SUM(discretionary_discounts) AS p_discretionary_discounts,
    SUM(warranty) AS p_warranty,
    SUM(total_cos) AS p_total_cos
FROM incremental_hierarchy_data
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets = spark.sql(planet_targets)
planet_targets.createOrReplaceTempView("planet_targets")


planet_targets_2020_2021_restated = f"""
SELECT cal_date,
    Fiscal_Yr,
    region_5,
    pl,
    COALESCE(SUM(p_gross_revenue), 0) AS p_gross_revenue,
    COALESCE(SUM(p_net_currency), 0) AS p_net_currency,
    COALESCE(SUM(p_contractual_discounts), 0) AS p_contractual_discounts,
    COALESCE(SUM(p_discretionary_discounts), 0) AS p_discretionary_discounts,
    COALESCE(SUM(p_warranty), 0) AS p_warranty,
    COALESCE(SUM(p_total_cos), 0) AS p_total_cos
FROM planet_targets            
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets_2020_2021_restated = spark.sql(planet_targets_2020_2021_restated)
planet_targets_2020_2021_restated.createOrReplaceTempView("planet_targets_2020_2021_restated")


planet_targets_2023_restatements = f"""
-- 2023 finance hierarchy restatements -- have to do here because EO to GL was impacted by the FY22 restatements (impacting FY20 and FY21)
SELECT cal_date,
    Fiscal_Yr,
    region_5,
    CASE
      WHEN cal_date > '2020-10-01' AND pl = 'EO' THEN 'GL'
      WHEN cal_date > '2020-10-01' AND pl = 'GM' THEN 'K6'
      WHEN cal_date > '2020-10-01' AND pl = '65' THEN 'UD'
      ELSE pl
    END AS pl,    
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,
    SUM(p_total_cos) AS p_total_cos
FROM planet_targets_2020_2021_restated            
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets_2023_restatements = spark.sql(planet_targets_2023_restatements)
planet_targets_2023_restatements.createOrReplaceTempView("planet_targets_2023_restatements")


planet_targets_post_all_restatements = f"""
SELECT cal_date,
    Fiscal_Yr,
    region_5,
    pl,    
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,
    SUM(p_total_cos) AS p_total_cos
FROM planet_targets_2023_restatements            
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets_post_all_restatements = spark.sql(planet_targets_post_all_restatements)
planet_targets_post_all_restatements.createOrReplaceTempView("planet_targets_post_all_restatements")


salesprod_prep_for_planet_targets = f"""
SELECT
    sp.cal_date,
    Fiscal_Yr,
    region_5,
    sp.pl,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos
FROM salesprod_preplanet_with_currency_map1 AS sp
JOIN calendar AS cal ON sp.cal_date = cal.Date
WHERE Fiscal_Yr > '2016'
    AND Day_of_Month = 1
GROUP BY sp.cal_date, sp.pl, region_5, Fiscal_Yr
"""

salesprod_prep_for_planet_targets = spark.sql(salesprod_prep_for_planet_targets)
salesprod_prep_for_planet_targets.createOrReplaceTempView("salesprod_prep_for_planet_targets")


salesprod_add_planet = f"""
SELECT
    sp.cal_date,
    sp.Fiscal_Yr,
    sp.region_5,
    sp.pl,                
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_currency), 0) AS net_currency,
    COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(warranty), 0) AS warranty,
    COALESCE(SUM(total_cos), 0) AS total_cos,
    COALESCE(SUM(p_gross_revenue), 0) AS p_gross_revenue,
    COALESCE(SUM(p_net_currency), 0) AS p_net_currency,
    COALESCE(SUM(p_contractual_discounts), 0) AS p_contractual_discounts,
    COALESCE(SUM(p_discretionary_discounts), 0) AS p_discretionary_discounts,
    COALESCE(SUM(p_warranty), 0) AS p_warranty,
    COALESCE(SUM(p_total_cos), 0) AS p_total_cos
FROM salesprod_prep_for_planet_targets AS sp 
LEFT JOIN planet_targets_post_all_restatements AS p ON (sp.cal_date = p.cal_date AND sp.region_5 = p.region_5 AND sp.pl = p.pl AND sp.Fiscal_Yr = p.Fiscal_Yr)
GROUP BY sp.cal_date, sp.region_5, sp.pl, sp.Fiscal_Yr
"""

salesprod_add_planet = spark.sql(salesprod_add_planet)
salesprod_add_planet.createOrReplaceTempView("salesprod_add_planet")


any_planet_without_salesprod = f"""
SELECT
    p.cal_date,
    p.Fiscal_Yr,
    p.region_5,
    p.pl,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_warranty) AS p_warranty,
    SUM(p_total_cos) AS p_total_cos
FROM planet_targets_2023_restatements AS p
LEFT JOIN salesprod_prep_for_planet_targets AS sp ON (sp.cal_date = p.cal_date AND sp.region_5 = p.region_5 AND sp.pl = p.pl AND sp.Fiscal_Yr = p.Fiscal_Yr)
WHERE gross_revenue IS NULL OR net_currency IS NULL OR contractual_discounts IS NULL OR discretionary_discounts IS NULL OR total_cos IS NULL
GROUP BY p.cal_date, p.Fiscal_Yr, p.region_5, p.pl
"""

any_planet_without_salesprod = spark.sql(any_planet_without_salesprod)
any_planet_without_salesprod.createOrReplaceTempView("any_planet_without_salesprod")


planet_adjust1 = f"""
SELECT
    cal_date,
    CASE
        WHEN region_5 = 'JP' THEN 'JP'
        WHEN region_5 = 'AP' THEN 'XI'
        WHEN region_5 = 'EU' THEN 'XA'
        WHEN region_5 = 'LA' THEN 'XH'
        WHEN region_5 = 'NA' THEN 'XG'
        ELSE 'XW'
    END AS country_alpha2,
    pl,
    'EDW_TIE_TO_PLANET' AS sales_product_number,
    'TRAD' AS ce_split,
    SUM(p_gross_revenue) AS gross_revenue,
    SUM(p_net_currency) AS net_currency,
    SUM(p_contractual_discounts) AS contractual_discounts,
    SUM(p_discretionary_discounts) AS discretionary_discounts,
    SUM(p_warranty) AS warranty,
    SUM(p_total_cos) AS total_cos,
    0 AS revenue_units
FROM any_planet_without_salesprod
GROUP BY cal_date, region_5, pl
"""

planet_adjust1 = spark.sql(planet_adjust1)
planet_adjust1.createOrReplaceTempView("planet_adjust1")


salesprod_calc_difference = f"""
SELECT
    cal_date,
    Fiscal_Yr,
    region_5,
    pl,
    COALESCE(SUM(p_gross_revenue) - SUM(gross_revenue), 0) AS plug_gross_revenue,
    COALESCE(SUM(p_net_currency) - SUM(net_currency), 0) AS plug_net_currency,
    COALESCE(SUM(p_contractual_discounts) - SUM(contractual_discounts), 0) AS plug_contractual_discounts,
    COALESCE(SUM(p_discretionary_discounts) - SUM(discretionary_discounts), 0) AS plug_discretionary_discounts,
    COALESCE(SUM(p_warranty) - SUM(warranty), 0) AS plug_warranty,
    COALESCE(SUM(p_total_cos) - SUM(total_cos), 0) AS plug_total_cos
FROM salesprod_add_planet
GROUP BY cal_date, Fiscal_Yr, region_5, pl
"""

salesprod_calc_difference = spark.sql(salesprod_calc_difference)
salesprod_calc_difference.createOrReplaceTempView("salesprod_calc_difference")


planet_tieout = f"""
SELECT
    cal_date,
    region_5,
    pl,
    COALESCE(SUM(plug_gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(plug_net_currency), 0) AS net_currency,
    COALESCE(SUM(plug_contractual_discounts), 0) AS contractual_discounts,
    COALESCE(SUM(plug_discretionary_discounts), 0) AS discretionary_discounts,
    COALESCE(SUM(plug_warranty), 0) AS warranty,
    COALESCE(SUM(plug_total_cos), 0) AS total_cos
FROM salesprod_calc_difference 
GROUP BY cal_date, region_5, pl
"""

planet_tieout = spark.sql(planet_tieout)
planet_tieout.createOrReplaceTempView("planet_tieout")


planet_tieout2 = f"""
SELECT
    cal_date,
    region_5,
    CASE
        WHEN region_5 = 'JP' THEN 'JP'
        WHEN region_5 = 'AP' THEN 'XI'
        WHEN region_5 = 'EU' THEN 'XA'
        WHEN region_5 = 'LA' THEN 'XH'
        WHEN region_5 = 'NA' THEN 'XG'
        ELSE 'XW'
    END AS country_alpha2,
    pl,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos
FROM planet_tieout 
GROUP BY cal_date, pl, region_5
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
    SUM(warranty) AS warranty,
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM planet_adjust1
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split

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
    SUM(total_cos) AS total_cos,
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
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM planet_adjusts
WHERE pl IN 
    (
    SELECT DISTINCT (pl) 
    FROM product_line_xref 
    WHERE Technology IN ('INK', 'LASER', 'PWA')
        AND PL_category IN ('SUP')
    )
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split
"""

planet_adjusts_supplies = spark.sql(planet_adjusts_supplies)
planet_adjusts_supplies.createOrReplaceTempView("planet_adjusts_supplies")


planet_adjusts_large_format = f"""
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
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM planet_adjusts p
LEFT JOIN calendar cal ON p.cal_date = cal.date
WHERE 1=1
AND day_of_month = 1
AND pl IN 
    (
    SELECT DISTINCT (pl) 
    FROM product_line_xref 
    WHERE Technology IN ('LF')
    AND PL_category IN ('SUP')
    )
AND Fiscal_Yr NOT IN ('2016', '2017', '2018')
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split
"""

planet_adjusts_large_format = spark.sql(planet_adjusts_large_format)
planet_adjusts_large_format.createOrReplaceTempView("planet_adjusts_large_format")


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
    SUM(total_cos) AS total_cos,
    0 AS revenue_units
FROM planet_adjusts pa
JOIN calendar cal ON pa.cal_date = cal.Date
WHERE Day_of_month = 1
    AND pl IN 
                (
                SELECT DISTINCT (pl) 
                FROM product_line_xref 
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM planet_adjusts_large_format
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM salesprod_preplanet_with_currency_map1
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, currency
"""

all_salesprod_final_pre = spark.sql(all_salesprod_final_pre)
all_salesprod_final_pre.createOrReplaceTempView("all_salesprod_final_pre")


planet_w_pls = f"""
SELECT 
    cal_date,
    Fiscal_Yr,
    planet.country_alpha2,
    region_5,
    planet.pl,
    l5_description,
    SUM(l2fa_total_cos * -1000) AS p_total_cos -- flip signs to make compatible with EDW data
FROM planet_actuals planet
JOIN iso_country_code_xref AS iso ON planet.country_alpha2 = iso.country_alpha2
JOIN product_line_xref plx ON planet.pl = plx.pl
JOIN calendar cal ON cal.Date = planet.cal_date
WHERE planet.pl LIKE 'W%'
AND PL_category IN ('ALLOC', 'SUP', 'LLC')
AND Technology IN ('LASER', 'INK', 'PWA', 'LLCS', 'LF')
AND Day_of_Month = 1
AND cal_date < '2021-11-01'
AND l2fa_total_cos != 0
AND Fiscal_Yr > '2016'            
OR planet.pl IN ('W002', 'W086', 'W183')
GROUP BY l5_description, planet.pl, planet.country_alpha2, region_5, Fiscal_Yr, cal_date
"""

planet_w_pls = spark.sql(planet_w_pls)
planet_w_pls.createOrReplaceTempView("planet_w_pls")


planet_w_adjusts = f"""
SELECT
    cal_date,
    country_alpha2,
    'USD' AS currency,
    pl,
    'EDW_TIE_TO_PLANET' AS sales_product_number,
    'TRAD' AS ce_split,
    0 AS gross_revenue,
    0 AS net_currency,
    0 AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS warranty,
    SUM(p_total_cos) AS total_cos,
    0 AS revenue_units
FROM planet_w_pls
GROUP BY cal_date, country_alpha2, pl
"""

planet_w_adjusts = spark.sql(planet_w_adjusts)
planet_w_adjusts.createOrReplaceTempView("planet_w_adjusts")


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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM planet_adjusts_final            
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
    SUM(total_cos) AS total_cos,
    SUM(revenue_units) AS revenue_units
FROM planet_w_adjusts            
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
    SUM(contractual_discounts) * -1 AS contractual_discounts,
    SUM(discretionary_discounts) * -1 AS discretionary_discounts,
    SUM(warranty) * -1 AS warranty,
    SUM(total_cos) * -1 AS total_cos,
    SUM(revenue_units) AS revenue_units,
                -- row clean up
                SUM(gross_revenue) + SUM(net_currency) + SUM(contractual_discounts) + SUM(discretionary_discounts) + SUM(total_cos) + SUM(warranty) + SUM(revenue_units) AS total_rows
FROM ALL_salesprod
GROUP BY cal_date, country_alpha2, pl, sales_product_number, ce_split, currency
"""

all_salesprod2 = spark.sql(all_salesprod2)
all_salesprod2.createOrReplaceTempView("all_salesprod2")



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
    SUM(total_cos) AS other_cos,
    SUM(total_cos) + SUM(warranty) AS total_cos,
    SUM(gross_revenue) + SUM(net_currency) - SUM(contractual_discounts) - SUM(discretionary_discounts) - SUM(total_cos) - SUM(warranty) AS gross_profit,
    SUM(revenue_units) AS revenue_units,    
    '{addversion_info[1]}' AS load_date,
    '{addversion_info[0]}' AS version
FROM ALL_salesprod2 AS sp
JOIN product_line_xref AS plx ON sp.pl = plx.pl 
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
        FROM product_line_xref 
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
    SUM(revenue_units) AS revenue_units, -- is data showing up here?
    pre.version,
    pre.load_date
FROM salesprod_planet_precurrency pre
LEFT JOIN iso_country_code_xref iso ON pre.country_alpha2 = iso.country_alpha2
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
LEFT JOIN iso_country_code_xref iso on iink.country_alpha2 = iso.country_alpha2
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
    cal.Date as cal_date,
    pl,
    iso.country_alpha2,
    iso.region_5,
    document_currency_code,
    SUM(net_k_dollars) * 1000 AS revenue -- at net revenue level but sources does not have hedge, so equivalent to revenue before hedge
FROM edw_revenue_document_currency_staging doc
LEFT JOIN calendar cal ON fiscal_year_month_code = Edw_fiscal_yr_mo
LEFT JOIN profit_center_code_xref pcx ON pcx.profit_center_code = doc.profit_center_code
LEFT JOIN iso_country_code_xref iso ON pcx.country_alpha2 = iso.country_alpha2
LEFT JOIN product_line_xref plx ON plxx = business_area_code
WHERE 1=1
    AND Day_of_month = 1
    AND net_k_dollars <> 0
    AND business_area_code = 'GD00'
    AND iso.country_alpha2 <> 'XW'
    AND document_currency_code <> '?'
    AND iso.country_alpha2 NOT LIKE 'X%'
    AND cal.Date < '2021-11-01'
GROUP BY cal.Date, document_currency_code, pl, iso.region_5, iso.country_alpha2
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
LEFT JOIN iso_country_code_xref iso ON final.country_alpha2 = iso.country_alpha2
GROUP BY cal_date, final.country_alpha2, pl, sales_product_number, customer_engagement, l5_description, currency, market10
"""

salesprod_financials = spark.sql(salesprod_financials)
salesprod_financials.createOrReplaceTempView("salesprod_financials")

# COMMAND ----------

#LOAD TO DB
write_df_to_redshift(configs, salesprod_financials, "fin_prod.edw_actuals_supplies_salesprod", "append", postactions = "", preactions = "truncate fin_prod.edw_actuals_supplies_salesprod")

# COMMAND ----------

#clean up directly in RS
query = f"""
UPDATE fin_prod.edw_actuals_supplies_salesprod
SET net_revenue = 0
WHERE net_revenue <.000001 and net_revenue > 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET net_revenue = 0
WHERE net_revenue >-.000001 and net_revenue < 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET gross_revenue = 0
WHERE gross_revenue <.000001 and gross_revenue > 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET gross_revenue = 0
WHERE gross_revenue >-.000001 and gross_revenue < 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET net_currency = 0
WHERE net_currency <.000001 and net_currency > 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET net_currency = 0
WHERE net_currency >-.000001 and net_currency < 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET contractual_discounts = 0
WHERE contractual_discounts <.000001 and contractual_discounts > 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET contractual_discounts = 0
WHERE contractual_discounts >-.000001 and contractual_discounts < 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET discretionary_discounts = 0
WHERE discretionary_discounts <.000001 and discretionary_discounts > 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET discretionary_discounts = 0
WHERE discretionary_discounts >-.000001 and discretionary_discounts < 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET total_cos = 0
WHERE total_cos <.000001 and total_cos > 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET total_cos = 0
WHERE total_cos >-.000001 and total_cos < 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET gross_profit = 0
WHERE gross_profit <.000001 and gross_profit > 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET gross_profit = 0
WHERE gross_profit >-.000001 and gross_profit < 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET warranty = 0
WHERE warranty <.000001 and warranty > 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET warranty = 0
WHERE warranty >-.000001 and warranty < 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET revenue_units = 0
WHERE revenue_units <.000001 and revenue_units > 0;

UPDATE fin_prod.edw_actuals_supplies_salesprod
SET revenue_units = 0
WHERE revenue_units >-.000001 and revenue_units < 0;
"""

submit_remote_query(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'], query)
