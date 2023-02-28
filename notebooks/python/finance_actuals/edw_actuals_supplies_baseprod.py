# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# load S3 tables to df
edw_actuals_supplies_baseprod_staging_interim_supplies_only = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.edw_actuals_supplies_baseprod_staging_interim_supplies_only") \
    .load()
planet_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.planet_actuals") \
    .load()
supplies_finance_hier_restatements_2020_2021 = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.supplies_finance_hier_restatements_2020_2021") \
    .load()
iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load()
iso_cc_rollup_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_cc_rollup_xref") \
    .load()
supplies_hw_country_actuals_mapping = read_redshift_to_df(configs) \
    .option("dbtable", "stage.supplies_hw_country_actuals_mapping") \
    .load()
supplies_hw_mapping = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.supplies_hw_mapping") \
    .load()
calendar = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.calendar") \
    .load()
product_line_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.product_line_xref") \
    .load()
edw_actuals_supplies_salesprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.edw_actuals_supplies_salesprod") \
    .load()
hardware_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.hardware_xref") \
    .load()

# COMMAND ----------

import re

tables = [
    ['fin_stage.edw_actuals_supplies_baseprod_staging_interim_supplies_only', edw_actuals_supplies_baseprod_staging_interim_supplies_only],
    ['fin_prod.planet_actuals', planet_actuals],
    #['stage.supplies_hw_country_actuals_mapping', supplies_hw_country_actuals_mapping],
    ['fin_prod.supplies_finance_hier_restatements_2020_2021', supplies_finance_hier_restatements_2020_2021],
    ['mdm.iso_country_code_xref', iso_country_code_xref],
    ['mdm.iso_cc_rollup_xref', iso_cc_rollup_xref],
    ['mdm.supplies_hw_mapping', supplies_hw_mapping],
    ['mdm.calendar', calendar],
    ['mdm.product_line_xref', product_line_xref],
    ['fin_prod.edw_actuals_supplies_salesprod', edw_actuals_supplies_salesprod],
    ['mdm.hardware_xref', hardware_xref]
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

# call version sproc
addversion_info = call_redshift_addversion_sproc(configs, "ACTUALS - EDW SUPPLIES BASE PRODUCT FINANCIALS", "ACTUALS - EDW SUPPLIES BASE PRODUCT FINANCIALS")

# COMMAND ----------

# EDW actuals supplies baseprod
actuals_supplies_baseprod = f"""
SELECT             
    cal_date,
    country_alpha2,
    market10,
    base_product_number,
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
    SUM(equivalent_units) AS equivalent_units,
    SUM(yield_x_units) AS yield_x_units,
    SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM fin_stage.edw_actuals_supplies_baseprod_staging_interim_supplies_only
WHERE 1=1
GROUP BY cal_date, country_alpha2, base_product_number, pl, customer_engagement, market10
"""

actuals_supplies_baseprod = spark.sql(actuals_supplies_baseprod)
actuals_supplies_baseprod.createOrReplaceTempView("actuals_supplies_baseprod")

# COMMAND ----------

#platform subset by cartridge mix
usage_share_country_hp_pages_mix = f"""
SELECT 
    cal_date,
    country_alpha2,
    platform_subset,
    base_product_number,
    customer_engagement,
    page_mix as platform_mix,
    version
FROM stage.supplies_hw_country_actuals_mapping
WHERE version = (select max(version) from stage.supplies_hw_country_actuals_mapping)
    AND cal_date BETWEEN (SELECT MIN(cal_date) FROM fin_prod.edw_actuals_supplies_salesprod) 
                    AND (SELECT MAX(cal_date) FROM fin_prod.edw_actuals_supplies_salesprod)
    AND page_mix <> 0
GROUP BY 
    cal_date,
    country_alpha2,
    platform_subset,
    base_product_number,
    customer_engagement,
    page_mix,
    version
"""

usage_share_country_hp_pages_mix = spark.sql(usage_share_country_hp_pages_mix)
usage_share_country_hp_pages_mix.createOrReplaceTempView("usage_share_country_hp_pages_mix")

# COMMAND ----------

#accounting items // addback 1
accounting_items_addback = f"""
SELECT
    cal_date,
    country_alpha2,
    market10,
    'NA' AS platform_subset,
    base_product_number,
    pl,
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
    SUM(equivalent_units) AS equivalent_units,
    SUM(yield_x_units) AS yield_x_units,
    SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM actuals_supplies_baseprod
WHERE base_product_number LIKE 'UNK%'
    OR base_product_number IN ('EDW_TIE_TO_PLANET', 'BIRDS', 'CISS', 'CTSS', 'EST_MPS_REVENUE_JV', 'LFMPS')
GROUP BY cal_date, country_alpha2, base_product_number, pl, customer_engagement, market10
"""

accounting_items_addback = spark.sql(accounting_items_addback)
accounting_items_addback.createOrReplaceTempView("accounting_items_addback") 

# COMMAND ----------

#non-accounting items // data to map printers to 
baseprod_without_acct_items = f"""
SELECT
    cal_date,
    country_alpha2,
    market10,
    base_product_number,
    pl,
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
    SUM(equivalent_units) AS equivalent_units,
    SUM(yield_x_units) AS yield_x_units,
    SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM actuals_supplies_baseprod
WHERE base_product_number NOT LIKE 'UNK%'
    AND base_product_number NOT IN ('EDW_TIE_TO_PLANET', 'BIRDS', 'CISS', 'CTSS', 'EST_MPS_REVENUE_JV', 'LFMPS')
GROUP BY cal_date, country_alpha2, base_product_number, pl, customer_engagement, market10
"""

baseprod_without_acct_items = spark.sql(baseprod_without_acct_items)
baseprod_without_acct_items.createOrReplaceTempView("baseprod_without_acct_items") 

# COMMAND ----------

#base product with assigned printers based upon country usage share // addback 2
baseprod_printer_from_usc = f"""
SELECT 
    act.cal_date,            
    act.country_alpha2,
    market10,
    platform_subset,
    act.base_product_number,
    pl,
    act.customer_engagement,
    SUM(gross_revenue * platform_mix) AS gross_revenue,
    SUM(net_currency * platform_mix) AS net_currency,
    SUM(contractual_discounts * platform_mix) AS contractual_discounts,
    SUM(discretionary_discounts * platform_mix) AS discretionary_discounts,
    SUM(net_revenue * platform_mix) AS net_revenue,    
    SUM(warranty * platform_mix) AS warranty,    
    SUM(other_cos * platform_mix) AS other_cos,    
    SUM(total_cos * platform_mix) AS total_cos,
    SUM(gross_profit * platform_mix) AS gross_profit,
    SUM(revenue_units * platform_mix) AS revenue_units,
    SUM(equivalent_units * platform_mix) AS equivalent_units,
    SUM(yield_x_units * platform_mix) AS yield_x_units,
    SUM(yield_x_units_black_only * platform_mix) AS yield_x_units_black_only
FROM baseprod_without_acct_items act
JOIN usage_share_country_hp_pages_mix mix 
    ON mix.cal_date = act.cal_date 
    AND mix.country_alpha2 = act.country_alpha2 
    AND mix.base_product_number = act.base_product_number
    AND mix.customer_engagement = act.customer_engagement
GROUP BY act.cal_date, act.country_alpha2, act.base_product_number, pl, act.customer_engagement, platform_subset, market10
"""

baseprod_printer_from_usc = spark.sql(baseprod_printer_from_usc)
baseprod_printer_from_usc.createOrReplaceTempView("baseprod_printer_from_usc") 


# COMMAND ----------

# set to NA failed match via usc  // addback 3
baseprod_printer_from_usc2 = f"""
SELECT 
    act.cal_date,            
    act.country_alpha2,
    market10,
    'NA' AS platform_subset,
    act.base_product_number,
    pl,
    act.customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue ) AS net_revenue,    
    SUM(warranty) AS warranty,    
    SUM(other_cos) AS other_cos,    
    SUM(total_cos) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    SUM(equivalent_units) AS equivalent_units,
    SUM(yield_x_units) AS yield_x_units,
    SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM baseprod_without_acct_items act
LEFT JOIN usage_share_country_hp_pages_mix mix 
    ON mix.cal_date = act.cal_date 
    AND mix.country_alpha2 = act.country_alpha2 
    AND mix.base_product_number = act.base_product_number
    AND mix.customer_engagement = act.customer_engagement
WHERE platform_subset is null
GROUP BY act.cal_date, act.country_alpha2, act.base_product_number, pl, act.customer_engagement, platform_subset, market10
"""

baseprod_printer_from_usc2 = spark.sql(baseprod_printer_from_usc2)
baseprod_printer_from_usc2.createOrReplaceTempView("baseprod_printer_from_usc2")  

# COMMAND ----------

#final base product with platform subset allocation
all_baseprod_with_platform_subsets = f"""
SELECT * 
FROM accounting_items_addback
UNION ALL
SELECT * 
FROM baseprod_printer_from_usc
UNION ALL
SELECT *
FROM baseprod_printer_from_usc2
"""

all_baseprod_with_platform_subsets = spark.sql(all_baseprod_with_platform_subsets)
all_baseprod_with_platform_subsets.createOrReplaceTempView("all_baseprod_with_platform_subsets")


baseprod_financials_preplanet_table = f"""
SELECT
    cal_date,
    country_alpha2,
    market10,
    platform_subset,
    base_product_number,
    bp.pl,
    l5_description,
    customer_engagement,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_currency) AS net_currency,
    SUM(contractual_discounts) AS contractual_discounts,
    SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
    SUM(warranty) AS warranty,
    SUM(other_cos) AS other_cos,
    SUM(total_cos ) AS total_cos,
    SUM(gross_profit) AS gross_profit,
    SUM(revenue_units) AS revenue_units,
    SUM(equivalent_units) AS equivalent_units,
    SUM(yield_x_units) AS yield_x_units,
    SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM all_baseprod_with_platform_subsets AS bp
JOIN mdm.product_line_xref AS plx ON bp.pl = plx.pl
GROUP BY cal_date, country_alpha2, platform_subset, base_product_number, bp.pl, customer_engagement, market10, l5_description
"""

baseprod_financials_preplanet_table = spark.sql(baseprod_financials_preplanet_table)
baseprod_financials_preplanet_table.createOrReplaceTempView("baseprod_financials_preplanet_table")

# COMMAND ----------

#planet target totals
planet_data = f"""
SELECT 
    cal_date,
    Fiscal_Yr,    
    p.country_alpha2,
    region_5,
    pl,
    SUM(l2fa_gross_trade_rev * 1000) AS p_gross_revenue,
    SUM(l2fa_net_currency * 1000) AS p_net_currency,
    SUM(l2fa_contra_rev_contractual * 1000) AS p_contractual_discounts, 
    SUM(l2fa_contra_rev_discretionary * 1000) AS p_discretionary_discounts,
    SUM(l2fa_net_revenues * 1000) AS p_net_revenue,
    SUM(l2fa_warranty * 1000) AS p_warranty,
    SUM(l2fa_total_cos * 1000) AS p_total_cos,
    SUM(l2fa_gross_profit * 1000) AS p_gross_profit
FROM fin_prod.planet_actuals AS p
JOIN mdm.iso_country_code_xref AS iso ON p.country_alpha2 = iso.country_alpha2
JOIN mdm.calendar AS cal ON cal.Date = p.cal_date
WHERE pl IN 
    (
        SELECT DISTINCT (pl) 
        FROM mdm.product_line_xref 
        WHERE Technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
            AND PL_category IN ('SUP', 'LLC')
            OR pl = 'IX'
            )
AND Fiscal_Yr > '2016'
AND Day_of_Month = 1
AND cal_date < '2021-11-01'
GROUP BY cal_date, p.country_alpha2, pl, region_5, Fiscal_Yr
"""

planet_data = spark.sql(planet_data)
planet_data.createOrReplaceTempView("planet_data")


planet_system_targets = f"""
SELECT cal_date,
    Fiscal_Yr,
    region_5,
    CASE
        WHEN pl = 'IX' THEN 'TX'
        ELSE pl
    END AS pl,
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
    SUM(p_contractual_discounts) AS p_contractual_discounts,
    SUM(p_discretionary_discounts) AS p_discretionary_discounts,
    SUM(p_net_revenue) AS p_net_revenue,
    SUM(p_warranty) AS p_warranty,
    SUM(p_total_cos) - SUM(p_warranty) AS p_other_cos,
    SUM(p_total_cos) AS p_total_cos,
    SUM(p_gross_profit) AS p_gross_profit
FROM planet_data
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_system_targets = spark.sql(planet_system_targets)
planet_system_targets.createOrReplaceTempView("planet_system_targets")

# COMMAND ----------

#restated planet totals
planet_targets_excluding_toner_sacp_restatements = f"""
SELECT cal_date,
    Fiscal_Yr,
    region_5,
    pl,
    SUM(p_gross_revenue) AS p_gross_revenue,
    SUM(p_net_currency) AS p_net_currency,
	SUM(p_contractual_discounts) AS p_contractual_discounts,
	SUM(p_discretionary_discounts) AS p_discretionary_discounts,
	SUM(p_net_revenue) AS p_net_revenue,
	SUM(p_warranty) AS p_warranty,
	SUM(p_other_cos) AS p_other_cos,
	SUM(p_total_cos) AS p_total_cos,
	SUM(p_gross_profit) AS p_gross_profit
FROM planet_system_targets
WHERE 1=1
AND pl NOT IN 
	(
	SELECT DISTINCT (pl) 
	FROM mdm.product_line_xref 
	WHERE Technology = 'LASER'
		AND PL_category = 'SUP'
		AND pl NOT IN ('LZ', 'GY', 'N4', 'N5')
	)
	OR Fiscal_Yr NOT IN ('2020', '2021')
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets_excluding_toner_sacp_restatements = spark.sql(planet_targets_excluding_toner_sacp_restatements)
planet_targets_excluding_toner_sacp_restatements.createOrReplaceTempView("planet_targets_excluding_toner_sacp_restatements")


calendar_table = f"""
select date_key, Date as cal_date, fiscal_yr, month_abbrv 
from mdm.calendar
where day_of_month = 1
"""

calendar_table = spark.sql(calendar_table)
calendar_table.createOrReplaceTempView("calendar_table")


calendar_table2 = f"""
select date_key, cal_date, fiscal_yr,
	CONCAT(Month_abbrv,'FY',SUBSTRING(fiscal_yr,3,4)) as sacp_fiscal_period
FROM calendar_table
"""

calendar_table2 = spark.sql(calendar_table2)
calendar_table2.createOrReplaceTempView("calendar_table2")

#2022 restatements
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
	SUM(COALESCE(contractual_discounts, 0)) as contractual_discounts,
	SUM(COALESCE(trade_discounts,0) - COALESCE(contractual_discounts,0)) as discretionary_discounts,
	SUM(COALESCE(net_revenues, 0)) as net_revenue,
	SUM(COALESCE(warranty,0)) as warranty,
	SUM((COALESCE(total_cost_of_sales,0) - COALESCE(warranty,0))) as other_cos,
	SUM(COALESCE(total_cost_of_sales,0)) as total_cos,
	SUM(COALESCE(net_revenues, 0) - COALESCE(total_cost_of_sales, 0)) as gross_profit
FROM fin_prod.supplies_finance_hier_restatements_2020_2021
GROUP BY market, l6_description, sacp_date_period, pl
"""

incremental_data_raw = spark.sql(incremental_data_raw)
incremental_data_raw.createOrReplaceTempView("incremental_data_raw")


incremental_data = f"""
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
	SUM(gross_revenue) AS p_gross_revenue,
	SUM(net_currency) AS p_net_currency,
	SUM(contractual_discounts) AS p_contractual_discounts,
	SUM(discretionary_discounts) AS p_discretionary_discounts,
	SUM(net_revenue) AS p_net_revenue,
	SUM(warranty) AS p_warranty,
	SUM(other_cos) AS p_other_cos,
	SUM(total_cos ) AS p_total_cos,
	SUM(gross_profit) AS p_gross_profit
FROM incremental_data_raw r 
left join calendar_table2 cal ON r.sacp_fiscal_period = cal.sacp_fiscal_period
WHERE 1=1
GROUP BY cal_date, market10, fiscal_yr, pl
"""

incremental_data = spark.sql(incremental_data)
incremental_data.createOrReplaceTempView("incremental_data")


#restated targets
planet_targets_2022_restatements = f"""
SELECT cal_date,
	Fiscal_Yr,
	region_5,
	pl,
	SUM(p_gross_revenue) AS p_gross_revenue,
	SUM(p_net_currency) AS p_net_currency,
	SUM(p_contractual_discounts) AS p_contractual_discounts,
	SUM(p_discretionary_discounts) AS p_discretionary_discounts,
	SUM(p_net_revenue) AS p_net_revenue,
	SUM(p_warranty) AS p_warranty,
	SUM(p_other_cos) AS p_other_cos,
	SUM(p_total_cos) AS p_total_cos,
    SUM(p_gross_profit) AS p_gross_profit
FROM planet_targets_excluding_toner_sacp_restatements
GROUP BY cal_date, region_5, pl, Fiscal_Yr

UNION ALL

SELECT cal_date,
	Fiscal_Yr,
	region_5,
	pl,
	SUM(p_gross_revenue) AS p_gross_revenue,
	SUM(p_net_currency) AS p_net_currency,
	SUM(p_contractual_discounts) AS p_contractual_discounts,
	SUM(p_discretionary_discounts) AS p_discretionary_discounts,
	SUM(p_net_revenue) AS p_net_revenue,
	SUM(p_warranty) AS p_warranty,
	SUM(p_other_cos) AS p_other_cos,
	SUM(p_total_cos) AS p_total_cos,
	SUM(p_gross_profit) AS p_gross_profit
FROM incremental_data
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets_2022_restatements = spark.sql(planet_targets_2022_restatements)
planet_targets_2022_restatements.createOrReplaceTempView("planet_targets_2022_restatements")


planet_targets = f"""
SELECT cal_date,
	Fiscal_Yr,
	region_5,
	pl,
	COALESCE(SUM(p_gross_revenue), 0) AS p_gross_revenue,
	COALESCE(SUM(p_net_currency), 0) AS p_net_currency,
	COALESCE(SUM(p_contractual_discounts), 0) AS p_contractual_discounts,
	COALESCE(SUM(p_discretionary_discounts), 0) AS p_discretionary_discounts,
	COALESCE(SUM(p_net_revenue), 0) AS p_net_revenue,
	COALESCE(SUM(p_warranty), 0) AS p_warranty,
	COALESCE(SUM(p_other_cos), 0) AS p_other_cos,
	COALESCE(SUM(p_total_cos), 0) AS p_total_cos,
	COALESCE(SUM(p_gross_profit), 0) AS p_gross_profit
FROM planet_targets_2022_restatements
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets = spark.sql(planet_targets)
planet_targets.createOrReplaceTempView("planet_targets")


planet_targets_2023_restatements = f"""
-- 2023 finance hierarchy restatements  -- have to do here because EO to GL was impacted by the FY22 restatements (impacting FY20 and FY21)
SELECT cal_date,
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
	SUM(p_net_revenue) AS p_net_revenue,
	SUM(p_warranty) AS p_warranty,
	SUM(p_other_cos) AS p_other_cos,
	SUM(p_total_cos) AS p_total_cos,
	SUM(p_gross_profit) AS p_gross_profit
FROM planet_targets            
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets_2023_restatements = spark.sql(planet_targets_2023_restatements)
planet_targets_2023_restatements.createOrReplaceTempView("planet_targets_2023_restatements")


planet_targets_post_restatements = f"""
SELECT cal_date,
    Fiscal_Yr,
    region_5,
    pl,    
    SUM(p_gross_revenue) AS p_gross_revenue,
	SUM(p_net_currency) AS p_net_currency,
	SUM(p_contractual_discounts) AS p_contractual_discounts,
	SUM(p_discretionary_discounts) AS p_discretionary_discounts,
	SUM(p_net_revenue) AS p_net_revenue,
	SUM(p_warranty) AS p_warranty,
	SUM(p_other_cos) AS p_other_cos,
	SUM(p_total_cos) AS p_total_cos,
	SUM(p_gross_profit) AS p_gross_profit
FROM planet_targets_2023_restatements            
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets_post_restatements = spark.sql(planet_targets_post_restatements)
planet_targets_post_restatements.createOrReplaceTempView("planet_targets_post_restatements")

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
FROM fin_stage.final_union_edw_data fued
LEFT JOIN mdm.iso_country_code_xref iso
    ON fued.country_alpha2 = iso.country_alpha2
WHERE fued.country_alpha2 NOT LIKE "%X%"
AND region_5 NOT IN ('EU', 'XW', 'XU')
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

# create country level planet targets; multiple options
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
    SUM(p_total_cos * country_mix) AS p_total_cos
FROM planet_targets_post_restatements p
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
    SUM(p_total_cos) AS p_total_cos
FROM planet_targets_post_restatements p
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

#add targets to base product dataset
baseprod_prep_for_planet_targets = f"""
SELECT
	bp.cal_date,
	Fiscal_Yr,
    bp.country_alpha2,
	region_5,
	bp.pl,
	SUM(gross_revenue) AS gross_revenue,
	SUM(net_currency) AS net_currency,
	SUM(contractual_discounts) AS contractual_discounts,
	SUM(discretionary_discounts) AS discretionary_discounts,
	SUM(net_revenue) AS net_revenue,
	SUM(warranty) AS warranty,
	SUM(other_cos) AS other_cos,
	SUM(total_cos) AS total_cos,
	SUM(gross_profit) AS gross_profit
FROM baseprod_financials_preplanet_table AS bp
LEFT JOIN mdm.iso_country_code_xref AS iso ON bp.country_alpha2 = iso.country_alpha2 AND bp.market10 = iso.market10
JOIN mdm.calendar AS cal ON bp.cal_date = cal.Date
WHERE Fiscal_Yr > '2016'
AND Day_of_Month = 1
GROUP BY bp.cal_date, bp.pl, region_5, Fiscal_Yr, bp.country_alpha2
"""

baseprod_prep_for_planet_targets = spark.sql(baseprod_prep_for_planet_targets)
baseprod_prep_for_planet_targets.createOrReplaceTempView("baseprod_prep_for_planet_targets")

baseprod_add_planet = f"""
SELECT
	bp.cal_date,
	bp.Fiscal_Yr,
    bp.country_alpha2,
	bp.region_5,
	bp.pl,
	COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
	COALESCE(SUM(net_currency), 0) AS net_currency,
	COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
	COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
	COALESCE(SUM(net_revenue), 0) AS net_revenue,
	COALESCE(SUM(warranty), 0) AS warranty,
	COALESCE(SUM(other_cos), 0) AS other_cos,
	COALESCE(SUM(total_cos), 0) AS total_cos,
	COALESCE(SUM(gross_profit), 0) AS gross_profit,
	COALESCE(SUM(p_gross_revenue), 0) AS p_gross_revenue,
	COALESCE(SUM(p_net_currency), 0) AS p_net_currency,
	COALESCE(SUM(p_contractual_discounts), 0) AS p_contractual_discounts,
	COALESCE(SUM(p_discretionary_discounts), 0) AS p_discretionary_discounts,
	COALESCE(SUM(p_net_revenue), 0) AS p_net_revenue,
	COALESCE(SUM(p_warranty), 0) AS p_warranty,
	COALESCE(SUM(p_other_cos), 0) AS p_other_cos,
	COALESCE(SUM(p_total_cos), 0) AS p_total_cos,
	COALESCE(SUM(p_gross_profit), 0) AS p_gross_profit
FROM baseprod_prep_for_planet_targets AS bp 
LEFT JOIN planet_targets_fully_restated_to_country AS p ON (bp.cal_date = p.cal_date AND bp.region_5 = p.region_5 AND bp.pl = p.pl AND bp.Fiscal_Yr = p.Fiscal_Yr AND p.country_alpha2 = bp.country_alpha2)
GROUP BY bp.cal_date, bp.region_5, bp.pl, bp.Fiscal_Yr, bp.country_alpha2
"""

baseprod_add_planet = spark.sql(baseprod_add_planet)
baseprod_add_planet.createOrReplaceTempView("baseprod_add_planet")


baseprod_calc_difference = f"""
SELECT
	cal_date,
	Fiscal_Yr,
    country_alpha2,
	region_5,
	pl,
	COALESCE(SUM(p_gross_revenue) - SUM(gross_revenue), 0) AS plug_gross_revenue,
	COALESCE(SUM(p_net_currency) - SUM(net_currency), 0) AS plug_net_currency,
	COALESCE(SUM(p_contractual_discounts) - SUM(contractual_discounts), 0) AS plug_contractual_discounts,
	COALESCE(SUM(p_discretionary_discounts) - SUM(discretionary_discounts), 0) AS plug_discretionary_discounts,
	COALESCE(SUM(p_net_revenue) - SUM(net_revenue), 0) AS plug_net_revenue,
	COALESCE(SUM(p_warranty) - SUM(warranty), 0) AS plug_warranty,
	COALESCE(SUM(p_other_cos) - SUM(other_cos), 0) AS plug_other_cos,
	COALESCE(SUM(p_total_cos) - SUM(total_cos), 0) AS plug_total_cos,
	COALESCE(SUM(p_gross_profit) - SUM(gross_profit), 0) AS plug_gross_profit
FROM baseprod_add_planet
GROUP BY cal_date, Fiscal_Yr, region_5, pl, country_alpha2
"""

baseprod_calc_difference = spark.sql(baseprod_calc_difference)
baseprod_calc_difference.createOrReplaceTempView("baseprod_calc_difference")

# start here 2-28-2023
baseprod_planet_tieout = f"""
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
	'NA' AS platform_subset,
	'EDW_TIE2_SALES_TO_BASE_ROUNDING_ERROR' AS base_product_number,
	pl,
	'TRAD' AS customer_engagement,
	SUM(plug_gross_revenue) AS gross_revenue,
	SUM(plug_net_currency) AS net_currency,
	SUM(plug_contractual_discounts) AS contractual_discounts,
	SUM(plug_discretionary_discounts) AS discretionary_discounts,
	SUM(plug_net_revenue) AS net_revenue,
	SUM(plug_warranty) AS warranty,
	SUM(plug_other_cos) AS other_cos,
	SUM(plug_total_cos) AS total_cos,
	SUM(plug_gross_profit) AS gross_profit,
	0 AS revenue_units,
	0 AS equivalent_units,
	0 AS yield_x_units,
	0 as yield_x_units_black_only
FROM baseprod_calc_difference
GROUP BY cal_date, region_5, pl
"""

baseprod_planet_tieout = spark.sql(baseprod_planet_tieout)
baseprod_planet_tieout.createOrReplaceTempView("baseprod_planet_tieout")


planet_adjusts = f"""
SELECT
	cal_date,
    p.country_alpha2,
	market10,
	platform_subset,
	base_product_number,
	p.pl,
	l5_description,
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
	SUM(equivalent_units) AS equivalent_units,
	SUM(yield_x_units) AS yield_x_units,
	SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM baseprod_planet_tieout AS p
JOIN mdm.iso_country_code_xref AS iso ON p.country_alpha2 = iso.country_alpha2
JOIN mdm.product_line_xref AS plx ON p.pl = plx.pl
GROUP BY cal_date, p.country_alpha2, market10, platform_subset, base_product_number, p.pl, l5_description, customer_engagement
"""

planet_adjusts = spark.sql(planet_adjusts)
planet_adjusts.createOrReplaceTempView("planet_adjusts")


final_planet_adjust_to_baseprod_supplies = f"""
SELECT
	cal_date,
	country_alpha2,
	market10,
	platform_subset,
	base_product_number,
	pl,
	l5_description,
	CASE	
		WHEN pl = 'GD' THEN 'I-INK'
		ELSE 'TRAD'
	END AS customer_engagement,
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
	SUM(yield_x_units) AS yield_x_units,
	SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM planet_adjusts
WHERE pl IN 
    (
		SELECT DISTINCT (pl) 
		FROM mdm.product_line_xref 
		WHERE Technology IN ('INK', 'LASER', 'PWA') 
			AND PL_category IN ('SUP')
	)
GROUP BY cal_date, country_alpha2, market10, platform_subset, base_product_number, pl, l5_description, customer_engagement, market10, l5_description
"""
final_planet_adjust_to_baseprod_supplies = spark.sql(final_planet_adjust_to_baseprod_supplies)
final_planet_adjust_to_baseprod_supplies.createOrReplaceTempView("final_planet_adjust_to_baseprod_supplies")


final_planet_adjust_to_baseprod_largeformat = f"""
SELECT
	cal_date,
	country_alpha2,
	market10,
	platform_subset,
	base_product_number,
	pl,
	l5_description,
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
	SUM(equivalent_units) AS equivalent_units,
	SUM(yield_x_units) AS yield_x_units,
	SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM planet_adjusts p
left join mdm.calendar cal ON cal.date = cal_date
WHERE pl IN 
	(
		SELECT pl
		FROM mdm.product_line_xref 
		WHERE Technology = 'LF' 
			AND PL_category = 'SUP'
    )
and Fiscal_Yr NOT IN ('2016', '2017', '2018')
and day_of_month = 1
GROUP BY cal_date, country_alpha2, market10, platform_subset, base_product_number, pl, l5_description, customer_engagement, market10, l5_description
"""

final_planet_adjust_to_baseprod_largeformat = spark.sql(final_planet_adjust_to_baseprod_largeformat)
final_planet_adjust_to_baseprod_largeformat.createOrReplaceTempView("final_planet_adjust_to_baseprod_largeformat")


final_planet_adjust_to_baseprod_llcs = f"""
SELECT
	cal_date,
	country_alpha2,
	market10,
	platform_subset,
	base_product_number,
	pl,
	l5_description,
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
	SUM(equivalent_units) AS equivalent_units,
	SUM(yield_x_units) AS yield_x_units,
	SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM planet_adjusts p
JOIN mdm.calendar cal ON cal.Date = p.cal_date
WHERE Day_of_Month = 1 
and pl IN 
	(
	SELECT DISTINCT (pl) 
	FROM mdm.product_line_xref 
	WHERE Technology IN ('LLCS')
		AND PL_category IN ('LLC')
	)
and Fiscal_Yr NOT IN ('2016', '2017', '2018')
GROUP BY cal_date, country_alpha2, market10, platform_subset, base_product_number, pl, l5_description, customer_engagement, market10, l5_description
"""

final_planet_adjust_to_baseprod_llcs = spark.sql(final_planet_adjust_to_baseprod_llcs)
final_planet_adjust_to_baseprod_llcs.createOrReplaceTempView("final_planet_adjust_to_baseprod_llcs")



# COMMAND ----------

final_planet_adjust_to_baseprod = f"""
SELECT
	cal_date,
	country_alpha2,
	market10,
	platform_subset,
	base_product_number,
	pl,
	l5_description,
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
	SUM(equivalent_units) AS equivalent_units,
	SUM(yield_x_units) AS yield_x_units,
	SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM final_planet_adjust_to_baseprod_supplies
GROUP BY cal_date, country_alpha2, market10, platform_subset, base_product_number, pl, l5_description, customer_engagement, market10, l5_description
		
UNION ALL

SELECT
	cal_date,
	country_alpha2,
	market10,
	platform_subset,
	base_product_number,
	pl,
	l5_description,
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
	SUM(equivalent_units) AS equivalent_units,
	SUM(yield_x_units) AS yield_x_units,
	SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM final_planet_adjust_to_baseprod_largeformat
GROUP BY cal_date, country_alpha2, market10, platform_subset, base_product_number, pl, l5_description, customer_engagement, market10, l5_description
					
UNION ALL
			
SELECT
	cal_date,
    country_alpha2,
	market10,
	platform_subset,
	base_product_number,
	pl,
	l5_description,
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
	SUM(equivalent_units) AS equivalent_units,
	SUM(yield_x_units) AS yield_x_units,
	SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM final_planet_adjust_to_baseprod_llcs
GROUP BY cal_date, country_alpha2, market10, platform_subset, base_product_number, pl, l5_description, customer_engagement, market10, l5_description
"""

final_planet_adjust_to_baseprod = spark.sql(final_planet_adjust_to_baseprod)
final_planet_adjust_to_baseprod.createOrReplaceTempView("final_planet_adjust_to_baseprod")

# COMMAND ----------

baseprod_add_planet_adjusts = f"""
SELECT
	cal_date,
	country_alpha2,
	market10,
	platform_subset,
	base_product_number,
	pl,
	l5_description,
	customer_engagement,
	SUM(gross_revenue) AS gross_revenue,
	SUM(net_currency) AS net_currency,
	SUM(contractual_discounts) AS contractual_discounts,
	SUM(discretionary_discounts) AS discretionary_discounts,
    SUM(net_revenue) AS net_revenue,
	SUM(warranty) AS warranty,
	SUM(other_cos) AS other_cos,
	SUM(total_cos ) AS total_cos,
	SUM(gross_profit) AS gross_profit,
	SUM(revenue_units) AS revenue_units,
	SUM(equivalent_units) AS equivalent_units,
	SUM(yield_x_units) AS yield_x_units,
	SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM baseprod_financials_preplanet_table
GROUP BY cal_date, country_alpha2, market10, platform_subset, base_product_number, pl, l5_description, customer_engagement
			
UNION ALL
			
SELECT
	cal_date,
	country_alpha2,
	market10,
	platform_subset,
	base_product_number,
	pl,
	l5_description,
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
	SUM(equivalent_units) AS equivalent_units,
	SUM(yield_x_units) AS yield_x_units,
	SUM(yield_x_units_black_only) AS yield_x_units_black_only
FROM final_planet_adjust_to_baseprod
GROUP BY cal_date, country_alpha2, market10, platform_subset, base_product_number, pl, l5_description, customer_engagement, market10, l5_description
"""

baseprod_add_planet_adjusts = spark.sql(baseprod_add_planet_adjusts)
baseprod_add_planet_adjusts.createOrReplaceTempView("baseprod_add_planet_adjusts")

# COMMAND ----------

baseprod_load_financials = f"""    
SELECT 'actuals - edw supplies base product financials' AS record,
	cal_date,
	country_alpha2,
	market10,
	platform_subset,
	base_product_number,
	pl,
	l5_description,
	customer_engagement,
	COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
	COALESCE(SUM(net_currency), 0) AS net_currency,
	COALESCE(SUM(contractual_discounts), 0) AS contractual_discounts,
	COALESCE(SUM(discretionary_discounts), 0) AS discretionary_discounts,
	COALESCE(SUM(net_revenue), 0) AS net_revenue,
	COALESCE(SUM(warranty), 0) AS warranty,
	COALESCE(SUM(other_cos), 0) AS other_cos,
	COALESCE(SUM(total_cos), 0) AS total_cos,
	COALESCE(SUM(gross_profit), 0) AS gross_profit,
	COALESCE(SUM(revenue_units), 0) AS revenue_units,
	COALESCE(SUM(equivalent_units), 0) AS equivalent_units,
	COALESCE(SUM(yield_x_units), 0) AS yield_x_units,
	COALESCE(SUM(yield_x_units_black_only), 0) AS yield_x_units_black_only,
    CAST(1 AS BOOLEAN) AS official,
    '{addversion_info[1]}' AS load_date,
	'{addversion_info[0]}' AS version
FROM baseprod_add_planet_adjusts
GROUP BY cal_date, country_alpha2, market10, platform_subset, base_product_number, pl, l5_description, customer_engagement
"""

baseprod_load_financials = spark.sql(baseprod_load_financials)
baseprod_load_financials.createOrReplaceTempView("baseprod_load_financials")

# COMMAND ----------

#write_df_to_redshift(configs, baseprod_load_financials, "fin_prod.edw_actuals_supplies_baseprod", "append", postactions = "", preactions = "truncate fin_prod.edw_actuals_supplies_baseprod")

# COMMAND ----------

#clean up directly in RS
query = f"""
UPDATE fin_prod.edw_actuals_supplies_baseprod
SET net_revenue = 0
WHERE net_revenue <.000001 and net_revenue > 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET net_revenue = 0
WHERE net_revenue >-.000001 and net_revenue < 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET gross_revenue = 0
WHERE gross_revenue <.000001 and gross_revenue > 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET gross_revenue = 0
WHERE gross_revenue >-.000001 and gross_revenue < 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET net_currency = 0
WHERE net_currency <.000001 and net_currency > 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET net_currency = 0
WHERE net_currency >-.000001 and net_currency < 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET contractual_discounts = 0
WHERE contractual_discounts <.000001 and contractual_discounts > 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET contractual_discounts = 0
WHERE contractual_discounts >-.000001 and contractual_discounts < 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET discretionary_discounts = 0
WHERE discretionary_discounts <.000001 and discretionary_discounts > 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET discretionary_discounts = 0
WHERE discretionary_discounts >-.000001 and discretionary_discounts < 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET total_cos = 0
WHERE total_cos <.000001 and total_cos > 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET total_cos = 0
WHERE total_cos >-.000001 and total_cos < 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET gross_profit = 0
WHERE gross_profit <.000001 and gross_profit > 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET gross_profit = 0
WHERE gross_profit >-.000001 and gross_profit < 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET revenue_units = 0
WHERE revenue_units <.000001 and revenue_units > 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET revenue_units = 0
WHERE revenue_units >-.000001 and revenue_units < 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET equivalent_units = 0
WHERE equivalent_units <.000001 and equivalent_units > 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET equivalent_units = 0
WHERE equivalent_units >-.000001 and equivalent_units < 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET yield_x_units_black_only = 0
WHERE yield_x_units_black_only <.000001 and yield_x_units_black_only > 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET yield_x_units_black_only = 0
WHERE yield_x_units_black_only >-.000001 and yield_x_units_black_only < 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET yield_x_units = 0
WHERE yield_x_units <.000001 and yield_x_units > 0;

UPDATE fin_prod.edw_actuals_supplies_baseprod
SET yield_x_units = 0
WHERE yield_x_units >-.000001 and yield_x_units < 0;
"""

submit_remote_query(configs, query)
