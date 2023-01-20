# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

edw_fin_s3_bucket= f"s3://dataos-core-{stack}-team-phoenix-fin/"
edw_ships_s3_bucket= f"s3://dataos-core-{stack}-team-phoenix/product/"

# COMMAND ----------

# load parquet files to df
edw_revenue_units_sales_landing_media = spark.read.parquet(edw_fin_s3_bucket + "EDW/edw_revenue_units_sales_landing")


# COMMAND ----------

# create parquet data as df
edw_revenue_units_sales_landing_media.createOrReplaceTempView("edw_revenue_units_sales_landing_media")

# COMMAND ----------

# load S3 tables to df
edw_actuals_supplies_salesprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.edw_actuals_supplies_salesprod") \
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
supplies_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.supplies_xref") \
    .load()
rdma_base_to_sales_product_map = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.rdma_base_to_sales_product_map") \
    .load()
yields = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.yield") \
    .load()

# COMMAND ----------

import re

tables = [
    ['fin_prod.edw_actuals_supplies_salesprod', edw_actuals_supplies_salesprod],
    ['mdm.iso_country_code_xref', iso_country_code_xref],
    ['mdm.calendar', calendar],
    ['mdm.profit_center_code_xref', profit_center_code_xref],
    ['mdm.product_line_xref', product_line_xref],
    ['mdm.rdma_base_to_sales_product_map', rdma_base_to_sales_product_map],
    ['mdm.supplies_xref', supplies_xref],
    ['mdm.yields', yields]
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
         renamed_column = re.sub('\$', '_dollars', re.sub(' ', '_', column[0])).lower()
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

# call version sproc // replace when add to edw actuals supplies baseprod //keep GY below
addversion_info = call_redshift_addversion_sproc(configs, "ACTUALS EDW SUP BASE PROD FIN - TEST", "ACTUALS - EDW SUP BASE PROD FIN - TEST")

# COMMAND ----------

# EDW actuals supplies salesprod -- remove GY filter after migration
actuals_supplies_salesprod = f"""
SELECT record,
    cal_date,
    country_alpha2,
    market10,
    sales_product_number,
    pl, 
    customer_engagement, 
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractuaL_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    official,
    version
FROM edw_actuals_supplies_salesprod
WHERE pl != 'GY'
GROUP BY cal_date, country_alpha2, sales_product_number, pl, customer_engagement, market10, record, official, version
"""

actuals_supplies_salesprod = spark.sql(actuals_supplies_salesprod)
actuals_supplies_salesprod.createOrReplaceTempView("actuals_supplies_salesprod")

# COMMAND ----------

#rdma base to sales map
rdma_salesprod_to_baseprod_map_abridged = f"""
SELECT 
    sales_product_number,
    sales_product_line_code,
    base_product_number,
    base_product_line_code,
    base_prod_per_sales_prod_qty,
    base_product_amount_percent
FROM rdma_base_to_sales_product_map
"""

rdma_salesprod_to_baseprod_map_abridged = spark.sql(rdma_salesprod_to_baseprod_map_abridged)
rdma_salesprod_to_baseprod_map_abridged.createOrReplaceTempView("rdma_salesprod_to_baseprod_map_abridged")


rdma_salesprod_to_baseprod_map_2023_hierarchy_corrections = f"""
SELECT 
    sales_product_number,
    CASE
        WHEN sales_product_line_code = '65' THEN 'UD'
        WHEN sales_product_line_code = 'EO' THEN 'GL'
        WHEN sales_product_line_code = 'GM' THEN 'K6'
        ELSE sales_product_line_code
    END AS sales_product_line_code,
    base_product_number,
    CASE
        WHEN base_product_line_code = '65' THEN 'UD'
        WHEN base_product_line_code = 'EO' THEN 'GL'
        WHEN base_product_line_code = 'GM' THEN 'K6'
    END AS base_product_line_code,
    base_prod_per_sales_prod_qty,
    base_product_amount_percent
FROM rdma_salesprod_to_baseprod_map_abridged
"""

rdma_salesprod_to_baseprod_map_2023_hierarchy_corrections = spark.sql(rdma_salesprod_to_baseprod_map_2023_hierarchy_corrections)
rdma_salesprod_to_baseprod_map_2023_hierarchy_corrections.createOrReplaceTempView("rdma_salesprod_to_baseprod_map_2023_hierarchy_corrections")


rdma_salesprod_to_baseprod_map_correction1 = f"""
SELECT 
    sales_product_number,
    sales_product_line_code,
    base_product_number,
    base_product_line_code,
    base_prod_per_sales_prod_qty,
    CASE    
        WHEN base_product_number = 'L0S55A' AND sales_product_number = 'X4E09AN' THEN '5'
        WHEN base_product_number = 'L0S52A' AND sales_product_number = 'X4E09AN' THEN '5'
        WHEN base_product_number = 'L0S49A' AND sales_product_number = 'X4E09AN' THEN '5'
        WHEN base_product_number = 'F6U19A' AND sales_product_number = 'X4E09AN' THEN '10'
        WHEN base_product_number = 'T6L90A' AND sales_product_number = 'X4E09AN' THEN '5'
        WHEN base_product_number = 'T6L86A' AND sales_product_number = 'X4E09AN' THEN '5'
        WHEN base_product_number = 'T6M14A' AND sales_product_number = 'X4E09AN' THEN '10'
        WHEN base_product_number = 'CN684W' AND sales_product_number = 'X4E09AN' THEN '5'
        WHEN base_product_number = 'CN052A' AND sales_product_number = 'X4E09AN' THEN '10'
        WHEN base_product_number = 'CN051A' AND sales_product_number = 'X4E09AN' THEN '10'
        WHEN base_product_number = 'CN050A' AND sales_product_number = 'X4E09AN' THEN '10'
        WHEN base_product_number = 'CN683W' AND sales_product_number = 'X4E09AN' THEN '5'
        WHEN base_product_number = 'CN682W' AND sales_product_number = 'X4E09AN' THEN '5'
        WHEN base_product_number = 'CN681W' AND sales_product_number = 'X4E09AN' THEN '5'
        WHEN base_product_number = 'T6L94A' AND sales_product_number = 'X4E09AN' THEN '5'
        WHEN base_product_number = 'L0S55A' AND sales_product_number = 'X4E11AN' THEN '6'
        WHEN base_product_number = 'L0S52A' AND sales_product_number = 'X4E11AN' THEN '6'
        WHEN base_product_number = 'L0S49A' AND sales_product_number = 'X4E11AN' THEN '6'
        WHEN base_product_number = 'F6U19A' AND sales_product_number = 'X4E11AN' THEN '9'
        WHEN base_product_number = 'T6L90A' AND sales_product_number = 'X4E11AN' THEN '3'
        WHEN base_product_number = 'T6L86A' AND sales_product_number = 'X4E11AN' THEN '3'
        WHEN base_product_number = 'T6M14A' AND sales_product_number = 'X4E11AN' THEN '6'
        WHEN base_product_number = 'CN684W' AND sales_product_number = 'X4E11AN' THEN '5'
        WHEN base_product_number = 'CN045A' AND sales_product_number = 'X4E11AN' THEN '11'
        WHEN base_product_number = 'CN052A' AND sales_product_number = 'X4E11AN' THEN '8'
        WHEN base_product_number = 'CN050A' AND sales_product_number = 'X4E11AN' THEN '8'
        WHEN base_product_number = 'CN683W' AND sales_product_number = 'X4E11AN' THEN '6'
        WHEN base_product_number = 'CN682W' AND sales_product_number = 'X4E11AN' THEN '6'
        WHEN base_product_number = 'CN681W' AND sales_product_number = 'X4E11AN' THEN '6'
        WHEN base_product_number = 'CN051A' AND sales_product_number = 'X4E11AN' THEN '8'
        WHEN base_product_number = 'T6L94A' AND sales_product_number = 'X4E11AN' THEN '3'
        WHEN base_product_number = 'CB435A' AND sales_product_number = 'CB435AE' THEN '100'
        WHEN base_product_number = 'CB434AF' AND sales_product_number = 'CB435AF' THEN '100'
        ELSE base_product_amount_percent
    END AS base_product_amount_percent
FROM rdma_salesprod_to_baseprod_map_2023_hierarchy_corrections
"""

rdma_salesprod_to_baseprod_map_correction1 = spark.sql(rdma_salesprod_to_baseprod_map_correction1)
rdma_salesprod_to_baseprod_map_correction1.createOrReplaceTempView("rdma_salesprod_to_baseprod_map_correction1")


rdma_salesprod_to_baseprod_map_correction2 = f"""
SELECT 
     CASE
        WHEN sales_product_number = '1VW01AN' THEN '1VV62A' -- NOT '1VV61A'
        WHEN sales_product_number = '1VV88AN' AND base_product_number <> 'C2P08A' THEN 'C2P09A' -- NOT 'E5Z01A'
        WHEN sales_product_number = '1VV83AN' AND base_product_number <> 'N9K10A' THEN 'N9K09A' -- NOT 'E5Z01A'
        WHEN sales_product_number = '1VV83AC' AND base_product_number <> 'N9K10A' THEN 'N9K09A' -- NOT 'E5Z01A'
        WHEN sales_product_number = '1VV88AC' AND base_product_number <> 'C2P08A' THEN 'C2P09A' -- NOT 'E5Z02A'
        WHEN sales_product_number =  'M0J33AN' AND base_product_number <> 'F6U21A' THEN 'F6U20A' -- NOT 'F6U22A' 
        WHEN sales_product_number = '3YQ09AE' THEN '3YM82A' -- NOT '3YM83A'
        WHEN sales_product_number = '3YQ08AE'  THEN '3YM83A' -- NOT '3YM82A'         
        ELSE base_product_number
    END AS base_product_number,
    base_product_line_code,
    sales_product_number,
    sales_product_line_code,
    CASE
        WHEN sales_product_number = '3YN51AN' THEN '2'
        ELSE base_prod_per_sales_prod_qty
    END AS base_prod_per_sales_prod_qty,
    base_product_amount_percent
FROM rdma_salesprod_to_baseprod_map_correction1
"""

rdma_salesprod_to_baseprod_map_correction2 = spark.sql(rdma_salesprod_to_baseprod_map_correction2)
rdma_salesprod_to_baseprod_map_correction2.createOrReplaceTempView("rdma_salesprod_to_baseprod_map_correction2")

# COMMAND ----------

#media sales units
edw_media_units = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    ipg_product_base_product_number AS base_product_number,
    product_base_identifier AS sales_product_number,
    manufacturing_product_identifier AS sales_product_option,
    SUM(working_PandL_summary_base_quantity) AS base_quantity,
    SUM(working_PandL_summary_extended_quantity) AS extended_quantity,
    SUM(working_PandL_summary_sales_quantity) AS sales_quantity
FROM edw_revenue_units_sales_landing_media 
WHERE working_PandL_summary_extended_quantity != 0
    AND manufacturing_product_identifier != '?'
    AND business_area_code IN ('AU00', 'UR00') -- media and SPS
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, ipg_product_base_product_number, product_base_identifier, manufacturing_product_identifier
"""

edw_media_units = spark.sql(edw_media_units)
edw_media_units.createOrReplaceTempView("edw_media_units")

# COMMAND ----------

#media base product units processing
edw_unit_data_selected = f"""
SELECT revenue_recognition_fiscal_year_month_code,
    profit_center_code,
    business_area_code,
    base_product_number,
    sales_product_number,
    sales_product_option,
COALESCE(CAST(SUM(extended_quantity) AS decimal(13,2) ), 0) AS revenue_units
FROM edw_media_units
GROUP BY revenue_recognition_fiscal_year_month_code, profit_center_code, business_area_code, base_product_number, sales_product_number,
    sales_product_option
"""

edw_unit_data_selected = spark.sql(edw_unit_data_selected)
edw_unit_data_selected.createOrReplaceTempView("edw_unit_data_selected")


# eliminate X-codes
media_formatted = f"""
SELECT            
    cal.Date AS cal_date,
    geo.country_alpha2,
    market10,
    sales_product_number,
    pl,
    'TRAD' AS customer_engagement,
    SUM(revenue_units) AS revenue_units
FROM edw_unit_data_selected media
JOIN calendar cal ON Edw_fiscal_yr_mo = revenue_recognition_fiscal_year_month_code
JOIN product_line_xref pl ON business_area_code = plxx
JOIN profit_center_code_xref pcx ON media.profit_center_code = pcx.profit_center_code
JOIN iso_country_code_xref geo ON pcx.country_alpha2 = geo.country_alpha2
WHERE Day_of_Month = 1
AND revenue_units <> 0
AND cal.Date < '2021-11-01'
GROUP BY cal.Date, geo.country_alpha2, market10, sales_product_number, pl
"""

media_formatted = spark.sql(media_formatted)
media_formatted.createOrReplaceTempView("media_formatted")


media_with_explicit_country_detail = f"""
SELECT            
    cal_date,
    country_alpha2,
    market10,
    sales_product_number,
    pl,
    customer_engagement,
    SUM(revenue_units) AS revenue_units
FROM media_formatted
WHERE country_alpha2 NOT IN 
        (
            SELECT country_alpha2
            FROM iso_country_code_xref
            WHERE country_alpha2 LIKE 'X%'
            AND country_alpha2 != 'XK'
        )
GROUP BY cal_date, country_alpha2, market10, sales_product_number, pl, customer_engagement
"""

media_with_explicit_country_detail = spark.sql(media_with_explicit_country_detail)
media_with_explicit_country_detail.createOrReplaceTempView("media_with_explicit_country_detail")


media_with_xcodes = f"""
SELECT            
    cal_date,
    market10,
    sales_product_number,
    pl,
    customer_engagement,
    SUM(revenue_units) AS revenue_units
FROM media_formatted
WHERE country_alpha2 IN 
        (
            SELECT country_alpha2
            FROM iso_country_code_xref
            WHERE country_alpha2 LIKE 'X%'
            AND country_alpha2 != 'XK'
        )
GROUP BY cal_date, market10, sales_product_number, pl, customer_engagement
"""

media_with_xcodes = spark.sql(media_with_xcodes)
media_with_xcodes.createOrReplaceTempView("media_with_xcodes")


country_mix_media = f"""
SELECT cal_date,
    country_alpha2,
    market10,
    pl,
    CASE
        WHEN SUM(revenue_units) OVER (PARTITION BY cal_date, pl, market10) = 0 THEN NULL
        ELSE revenue_units / SUM(revenue_units) OVER (PARTITION BY cal_date, pl, market10)
    END AS country_unit_mix
FROM media_with_explicit_country_detail
GROUP BY cal_date, country_alpha2, market10, revenue_units, pl
"""

country_mix_media = spark.sql(country_mix_media)
country_mix_media.createOrReplaceTempView("country_mix_media")


adjusted_media_xcodes = f"""
SELECT
    m.cal_date,
    country_alpha2,
    m.market10,
    sales_product_number,
    m.pl,
    customer_engagement,
    SUM(revenue_units * COALESCE(country_unit_mix, 1)) AS revenue_units
FROM media_with_xcodes m
JOIN calendar cal ON cal_date = cal.Date
JOIN country_mix_media mix ON m.market10 = mix.market10 AND m.pl = mix.pl AND m.cal_date = mix.cal_date
WHERE day_of_month = 1
GROUP BY m.cal_date, country_alpha2, m.market10, sales_product_number, m.pl, customer_engagement
"""

adjusted_media_xcodes = spark.sql(adjusted_media_xcodes)
adjusted_media_xcodes.createOrReplaceTempView("adjusted_media_xcodes")


media_with_country = f"""
SELECT            
    cal_date,
    country_alpha2,
    market10,
    sales_product_number,
    pl,
    customer_engagement,
    SUM(revenue_units) AS revenue_units
FROM media_with_explicit_country_detail
GROUP BY cal_date, country_alpha2, market10, sales_product_number, pl, customer_engagement

UNION ALL

SELECT            
    cal_date,
    country_alpha2,
    market10,
    sales_product_number,
    pl,
    customer_engagement,
    SUM(revenue_units) AS revenue_units
FROM adjusted_media_xcodes
GROUP BY cal_date, country_alpha2, market10, sales_product_number, pl, customer_engagement
"""

media_with_country = spark.sql(media_with_country)
media_with_country.createOrReplaceTempView("media_with_country")

media_formatted2 = f"""
SELECT            
    cal_date,
    country_alpha2,
    market10,
    sales_product_number,
    pl,
    customer_engagement, 
    0 AS gross_revenue,
    0 AS net_currency,
    0 AS contractual_discounts,
    0 AS discretionary_discounts,
    0 AS net_revenue,
    0 AS warranty,
    0 AS other_cos,
    0 AS total_cos,
    0 AS gross_profit,
SUM(revenue_units) AS revenue_units
FROM media_with_country
GROUP BY cal_date, country_alpha2, market10, sales_product_number, pl, customer_engagement
"""

media_formatted2 = spark.sql(media_formatted2)
media_formatted2.createOrReplaceTempView("media_formatted2")


edw_media_insights_ready = f""" 
SELECT 
    (SELECT distinct record FROM edw_actuals_supplies_salesprod) AS record,
    cal_date,
    country_alpha2,
    market10,
    sales_product_number,
    pl, 
    customer_engagement, 
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractuaL_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    (SELECT distinct official FROM edw_actuals_supplies_salesprod) AS official,
    (SELECT distinct version FROM edw_actuals_supplies_salesprod) AS version
FROM media_formatted2
GROUP BY cal_date, country_alpha2, sales_product_number, pl, customer_engagement, market10
"""
        
edw_media_insights_ready = spark.sql(edw_media_insights_ready)
edw_media_insights_ready.createOrReplaceTempView("edw_media_insights_ready")


media_only_rdma_map = f"""
SELECT 
    sales_product_number,
    sales_product_line_code,
    base_product_number,
    base_product_line_code,
    base_prod_per_sales_prod_qty,
    base_product_amount_percent
FROM rdma_salesprod_to_baseprod_map_correction2
WHERE sales_product_line_code IN ('AU', 'UR')
AND base_product_line_code NOT IN ('AU', 'UR')
"""        

media_only_rdma_map = spark.sql(media_only_rdma_map)
media_only_rdma_map.createOrReplaceTempView("media_only_rdma_map")


media_salesprod_convert_to_baseprod = f"""
SELECT 
    record,
    cal_date,
    country_alpha2,
    market10,
    sp.sales_product_number,
    sp.pl AS sales_product_line_code,
    base_product_number,
    base_product_line_code,
    customer_engagement,
    COALESCE(SUM(gross_revenue * base_product_amount_percent/100), SUM(gross_revenue)) AS gross_revenue,
    COALESCE(SUM(net_currency * base_product_amount_percent/100), SUM(net_currency)) AS net_currency,
    COALESCE(SUM(contractual_discounts * base_product_amount_percent/100), SUM(contractual_discounts)) AS contractuaL_discounts,
    COALESCE(SUM(discretionary_discounts * base_product_amount_percent/100), SUM(discretionary_discounts)) AS discretionary_discounts,
    COALESCE(SUM(net_revenue * base_product_amount_percent/100), SUM(net_revenue)) AS net_revenue,
    COALESCE(SUM(warranty * base_product_amount_percent/100), SUM(warranty)) AS warranty,
    COALESCE(SUM(other_cos * base_product_amount_percent/100), SUM(other_cos)) AS other_cos,
    COALESCE(SUM(total_cos * base_product_amount_percent/100), SUM(total_COS)) AS total_cos,
    COALESCE(SUM(gross_profit * base_product_amount_percent/100), SUM(gross_profit)) AS gross_profit,
    COALESCE(SUM(revenue_units * base_prod_per_sales_prod_qty), SUM(revenue_units)) AS revenue_units,
    official,
    version 
FROM edw_media_insights_ready AS sp
JOIN media_only_rdma_map AS r ON (sp.sales_product_number = r.sales_product_number)
GROUP BY record, cal_date, country_alpha2, sp.sales_product_number, sp.pl, base_product_number, base_product_line_code,
    customer_engagement, official, version, market10
"""

media_salesprod_convert_to_baseprod = spark.sql(media_salesprod_convert_to_baseprod)
media_salesprod_convert_to_baseprod.createOrReplaceTempView("media_salesprod_convert_to_baseprod")

supplies_units_from_media = f"""
SELECT
    record,
    cal_date,
    country_alpha2,
    market10,
    base_product_number,
    base_product_line_code AS pl,
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractuaL_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    (SELECT distinct official from actuals_supplies_salesprod) AS official,
    version
FROM media_salesprod_convert_to_baseprod
GROUP BY record, cal_date, country_alpha2, base_product_number, base_product_line_code,
    customer_engagement, official, version, market10
"""

supplies_units_from_media = spark.sql(supplies_units_from_media)
supplies_units_from_media.createOrReplaceTempView("supplies_units_from_media")

# COMMAND ----------

#convert salesprod to baseprod
salesprod_data_convert_to_baseprod_data = f"""
SELECT 
    record,
    cal_date,
    country_alpha2,
    market10,
    sp.sales_product_number,
    sp.pl AS sales_product_line_code,
    base_product_number,
    base_product_line_code,
    customer_engagement,
    COALESCE(SUM(gross_revenue * base_product_amount_percent/100), SUM(gross_revenue)) AS gross_revenue,
    COALESCE(SUM(net_currency * base_product_amount_percent/100), SUM(net_currency)) AS net_currency,
    COALESCE(SUM(contractual_discounts * base_product_amount_percent/100), SUM(contractual_discounts)) AS contractuaL_discounts,
    COALESCE(SUM(discretionary_discounts * base_product_amount_percent/100), SUM(discretionary_discounts)) AS discretionary_discounts,
    COALESCE(SUM(net_revenue * base_product_amount_percent/100), SUM(net_revenue)) AS net_revenue,
    COALESCE(SUM(warranty * base_product_amount_percent/100), SUM(warranty)) AS warranty,
    COALESCE(SUM(other_cos * base_product_amount_percent/100), SUM(other_cos)) AS other_cos,
    COALESCE(SUM(total_cos * base_product_amount_percent/100), SUM(total_COS)) AS total_cos,
    COALESCE(SUM(gross_profit * base_product_amount_percent/100), SUM(gross_profit)) AS gross_profit,
    COALESCE(SUM(revenue_units * base_prod_per_sales_prod_qty), SUM(revenue_units)) AS revenue_units,
    official,
    version 
FROM actuals_supplies_salesprod AS sp
JOIN rdma_salesprod_to_baseprod_map_correction2 AS r ON (sp.sales_product_number = r.sales_product_number)
WHERE sp.sales_product_number NOT IN ('BIRDS', 'CISS', 'CTSS', 'EST_MPS_REVENUE_JV', 'EDW_TIE_TO_PLANET', 'LFMPS')
GROUP BY record, cal_date, country_alpha2, sp.sales_product_number, sp.pl, base_product_number, base_product_line_code,
    customer_engagement, official, version, market10
"""

salesprod_data_convert_to_baseprod_data = spark.sql(salesprod_data_convert_to_baseprod_data)
salesprod_data_convert_to_baseprod_data.createOrReplaceTempView("salesprod_data_convert_to_baseprod_data")

# COMMAND ----------

#convert non-rdma items
mcodes_other_manual_additions = f"""
SELECT 
    record,
    cal_date,
    country_alpha2,
    market10,
    sales_product_number AS base_product_number,
    pl, 
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractuaL_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    official,
    version
FROM edw_actuals_supplies_salesprod
-- the below sales products will not be in the base product map, but their sales product = base product
WHERE sales_product_number IN ('BIRDS', 'CISS', 'CTSS', 'EST_MPS_REVENUE_JV', 'EDW_TIE_TO_PLANET', 'LFMPS')
GROUP BY record, cal_date, country_alpha2, sales_product_number, pl, customer_engagement, official, version, market10                                
"""

mcodes_other_manual_additions = spark.sql(mcodes_other_manual_additions)
mcodes_other_manual_additions.createOrReplaceTempView("mcodes_other_manual_additions")


sp_missing_bp = f"""
SELECT 
    record,
    cal_date,
    country_alpha2,
    market10,
    sp.sales_product_number,
    sp.pl,
    base_product_number,
    base_product_line_code,
    customer_engagement,
    COALESCE(SUM(gross_revenue * base_product_amount_percent/100), SUM(gross_revenue)) AS gross_revenue,
    COALESCE(SUM(net_currency * base_product_amount_percent/100), SUM(net_currency)) AS net_currency,
    COALESCE(SUM(contractual_discounts * base_product_amount_percent/100), SUM(contractual_discounts)) AS contractuaL_discounts,
    COALESCE(SUM(discretionary_discounts * base_product_amount_percent/100), SUM(discretionary_discounts)) AS discretionary_discounts,
    COALESCE(SUM(net_revenue * base_product_amount_percent/100), SUM(net_revenue)) AS net_revenue,
    COALESCE(SUM(warranty * base_product_amount_percent/100), SUM(warranty)) AS warranty,
    COALESCE(SUM(other_cos * base_product_amount_percent/100), SUM(other_cos)) AS other_cos,
    COALESCE(SUM(total_cos * base_product_amount_percent/100), SUM(total_cos)) AS total_cos,
    COALESCE(SUM(gross_profit * base_product_amount_percent/100), SUM(gross_profit)) AS gross_profit,
    COALESCE(SUM(revenue_units * base_prod_per_sales_prod_qty), SUM(revenue_units)) AS revenue_units,
    official,
    version 
FROM actuals_supplies_salesprod AS sp
LEFT JOIN rdma_salesprod_to_baseprod_map_correction2 AS r ON (sp.sales_product_number = r.sales_product_number)    
WHERE base_product_number IS NULL
    AND sp.sales_product_number NOT IN ('BIRDS', 'CISS', 'CTSS', 'EST_MPS_REVENUE_JV', 'EDW_TIE_TO_PLANET', 'LFMPS')
GROUP BY record, cal_date, country_alpha2, sp.sales_product_number, sp.pl, base_product_number, base_product_line_code,
    customer_engagement, official, version, market10
"""

sp_missing_bp = spark.sql(sp_missing_bp)
sp_missing_bp.createOrReplaceTempView("sp_missing_bp")


supplies_baseprod_data_join_mcodes =f"""
SELECT 
    record,
    cal_date,
    country_alpha2,
    market10,
    base_product_number,
    base_product_line_code AS pl,
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractuaL_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    official,
    version 
FROM salesprod_data_convert_to_baseprod_data
GROUP BY record, cal_date, country_alpha2, base_product_number, base_product_line_code, customer_engagement, official, version, market10, sales_product_line_code

UNION ALL

SELECT 
    record,
    cal_date,
    country_alpha2,
    market10,
    base_product_number,
    pl, 
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractuaL_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    official,
    version
    FROM mcodes_other_manual_additions
    GROUP BY record, cal_date, country_alpha2, base_product_number, pl, customer_engagement, official, version, market10
    
UNION ALL

SELECT
    record,
    cal_date,
    country_alpha2,
    market10,
    base_product_number,
    pl, 
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractuaL_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    official,
    version
FROM supplies_units_from_media                    
GROUP BY record, cal_date, country_alpha2, base_product_number, pl, customer_engagement, official, version, market10
"""

supplies_baseprod_data_join_mcodes = spark.sql(supplies_baseprod_data_join_mcodes)
supplies_baseprod_data_join_mcodes.createOrReplaceTempView("supplies_baseprod_data_join_mcodes")


baseprod_unknown = f"""
SELECT 
    record,
    cal_date,
    country_alpha2,
    market10,
    sales_product_number,
    ('UNKN' + pl) AS base_product_number,
    pl,
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractuaL_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    official,
    version 
FROM sp_missing_bp
GROUP BY record, cal_date, country_alpha2, sales_product_number, ('UNKN' + pl), pl, customer_engagement, official, version, market10
"""
    
baseprod_unknown = spark.sql(baseprod_unknown)
baseprod_unknown.createOrReplaceTempView("baseprod_unknown")


supplies_baseprod_data = f"""
SELECT 
    record,
    cal_date,
    country_alpha2,
    market10,
    base_product_number,
    pl, 
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractuaL_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    official,
    version
FROM supplies_baseprod_data_join_mcodes
GROUP BY record, cal_date, country_alpha2, base_product_number, pl, customer_engagement, official, version, market10

UNION ALL

SELECT 
    record,
    cal_date,
    country_alpha2,
    market10,
    base_product_number,
    pl, 
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractuaL_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    official,
    version
FROM baseprod_unknown
GROUP BY record, cal_date, country_alpha2, base_product_number, pl, customer_engagement, official, version, market10
"""

supplies_baseprod_data = spark.sql(supplies_baseprod_data)
supplies_baseprod_data.createOrReplaceTempView("supplies_baseprod_data")


supplies_baseprod_data2 = f"""
SELECT 
    record,
    cal_date,
    country_alpha2,
    market10,
    base_product_number,
    pl, 
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractuaL_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    official,
    version
FROM supplies_baseprod_data
WHERE pl IN 
    (
        SELECT distinct pl
        FROM product_line_xref
        WHERE PL_category IN ( 'SUP', 'LLC', 'ALLOC')
        AND Technology IN ('PWA', 'LASER', 'INK', 'LLCS', 'LF')
        AND pl <> 'IX'
    )
GROUP BY record, cal_date, country_alpha2, base_product_number, pl, customer_engagement, official, version, market10
"""

supplies_baseprod_data2 = spark.sql(supplies_baseprod_data2)
supplies_baseprod_data2.createOrReplaceTempView("supplies_baseprod_data2")


# COMMAND ----------

# add equivalent units
add_equivalents_units = f"""
SELECT 
    base_product_number,
    COALESCE(equivalents_multiplier, 1) AS equivalents_multiplier
FROM supplies_xref
"""

add_equivalents_units = spark.sql(add_equivalents_units)
add_equivalents_units.createOrReplaceTempView("add_equivalents_units")


supplies_equivalents = f"""
SELECT
    record,
    cal_date,
    country_alpha2,
    market10,
    bp.base_product_number,
    pl,
    customer_engagement,                
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractuaL_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    SUM(revenue_units) * SUM(equivalents_multiplier) AS equivalent_units,
    (SELECT distinct official FROM actuals_supplies_salesprod) AS official,
    version
FROM supplies_baseprod_data2 AS bp
LEFT JOIN add_equivalents_units AS eq ON (bp.base_product_number = eq.base_product_number)
GROUP BY record, cal_date, country_alpha2, bp.base_product_number, pl, customer_engagement, equivalents_multiplier, version, market10
"""
            
supplies_equivalents = spark.sql(supplies_equivalents)
supplies_equivalents.createOrReplaceTempView("supplies_equivalents")                    

# COMMAND ----------

supplies_equivalents.count()

# COMMAND ----------

# add yields for ccs and pages

sub_months = f"""
SELECT Date AS cal_date                    
FROM calendar
WHERE day_of_month = 1
"""

sub_months = spark.sql(sub_months)
sub_months.createOrReplaceTempView("sub_months")


yields_table = f""" 
SELECT 
    base_product_number,
    geography AS region_5,
    -- NOTE: assumes effective_date is in YYYYMM format. Multiplying by 100 and adding 1 to get to YYYYMMDD
    effective_date,
    COALESCE(LEAD(effective_date) OVER (PARTITION BY base_product_number, geography ORDER BY effective_date), 
        CAST('2119-08-30' AS DATE)) AS next_effective_date,
    value AS yield
FROM yields
WHERE official = 1    
AND geography_grain = 'REGION_5'
"""

yields_table = spark.sql(yields_table)
yields_table.createOrReplaceTempView("yields_table")


sub_yields = f"""
SELECT 
    base_product_number,
    sub_months.cal_date,
    region_5,
    yield
FROM yields_table yields
JOIN sub_months sub_months
ON yields.effective_date <= sub_months.cal_date
AND yields.next_effective_date > sub_months.cal_date
"""

sub_yields = spark.sql(sub_yields)
sub_yields.createOrReplaceTempView("sub_yields")


baseprod_actuals_with_yields = f"""
SELECT 
    bp.record,
    bp.cal_date,
    bp.country_alpha2,
    bp.market10,
    geo.region_5,
    bp.base_product_number,
    bp.pl,
    customer_engagement,
    k_color,
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
    SUM(equivalent_units) AS equivalent_units,
    SUM(yield) AS yield,
    SUM(yield * revenue_units) AS yield_x_units,
    CASE    
        WHEN k_color != 'BLACK' THEN NULL
        ELSE SUM(revenue_units * yield) 
    END AS yield_x_units_black_only,
    (SELECT distinct official from actuals_supplies_salesprod) AS official,
    bp.version
FROM supplies_equivalents bp
LEFT JOIN iso_country_code_xref geo ON bp.country_alpha2 = geo.country_alpha2 
LEFT JOIN sub_yields yield ON bp.base_product_number = yield.base_product_number AND bp.cal_date = yield.cal_date AND geo.region_5 = yield.region_5
LEFT JOIN supplies_xref supply ON bp.base_product_number = supply.base_product_number
GROUP BY bp.record, bp.cal_date, bp.country_alpha2, geo.region_5, bp.base_product_number, bp.pl, customer_engagement, k_color, bp.version, bp.market10
"""

baseprod_actuals_with_yields = spark.sql(baseprod_actuals_with_yields)
baseprod_actuals_with_yields.createOrReplaceTempView("baseprod_actuals_with_yields")


baseprod_actuals_yields =f"""
SELECT
    cal_date,
    country_alpha2,
    market10,
    base_product_number,
    bp.pl,
    l5_description,
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(total_cos ) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    SUM(equivalent_units) AS equivalent_units,
    SUM(yield_x_units) AS yield_x_units,
    SUM(yield_x_units_black_only) AS yield_x_units_black_only,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos
FROM baseprod_actuals_with_yields AS bp
JOIN product_line_xref AS plx ON bp.pl = plx.pl
GROUP BY cal_date, country_alpha2,base_product_number, bp.pl, customer_engagement, market10, l5_description
"""
                
baseprod_actuals_yields = spark.sql(baseprod_actuals_yields)
baseprod_actuals_yields.createOrReplaceTempView("baseprod_actuals_yields")

# COMMAND ----------

write_df_to_redshift(configs, baseprod_actuals_yields, "fin_stage.edw_actuals_supplies_baseprod_staging_interim_supplies_only", "append", postactions = "", preactions = "truncate fin_stage.edw_actuals_supplies_baseprod_staging_interim_supplies_only")
