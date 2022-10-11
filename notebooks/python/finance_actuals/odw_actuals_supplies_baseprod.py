# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# load S3 tables to df
odw_actuals_supplies_baseprod_staging_interim_supplies_only = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.odw_actuals_supplies_baseprod_staging_interim_supplies_only") \
    .load()
sacp_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_sacp_actuals") \
    .load()
iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load()
iso_cc_rollup_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_cc_rollup_xref") \
    .load()
working_forecast = read_redshift_to_df(configs) \
    .option("query", "SELECT * FROM prod.working_forecast WHERE version = (SELECT max(version) from prod.working_forecast)") \
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
ib = read_redshift_to_df(configs) \
    .option("query", "SELECT * FROM prod.ib WHERE version = (SELECT MAX(version) FROM prod.ib WHERE record = 'IB' AND official = 1)") \
    .load()
odw_actuals_supplies_salesprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_actuals_supplies_salesprod") \
    .load()
hardware_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.hardware_xref") \
    .load()

# COMMAND ----------

tables = [
    ['fin_prod.odw_actuals_supplies_baseprod_staging_interim_supplies_only', odw_actuals_supplies_baseprod_staging_interim_supplies_only],
    ['fin_prod.odw_sacp_actuals', sacp_actuals],
    ['mdm.iso_country_code_xref', iso_country_code_xref],
    ['mdm.iso_cc_rollup_xref', iso_cc_rollup_xref],
    ['prod.working_forecast', working_forecast],
    ['mdm.supplies_hw_mapping', supplies_hw_mapping],
    ['mdm.calendar', calendar],
    ['mdm.product_line_xref', product_line_xref],
    #['prod.ib', ib],
    ['fin_prod.odw_actuals_supplies_salesprod', odw_actuals_supplies_salesprod],
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
      .mode("overwrite") \
      .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Create the table.
    spark.sql("CREATE TABLE IF NOT EXISTS " + table[0] + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')

# COMMAND ----------

ib.createOrReplaceTempView("ib")

# COMMAND ----------

# call version sproc
addversion_info = call_redshift_addversion_sproc(configs, "ACTUALS - ODW SUPPLIES BASE PRODUCT FINANCIALS", "ACTUALS - ODW SUPPLIES BASE PRODUCT FINANCIALS")

# COMMAND ----------

# ODW actuals supplies baseprod
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
FROM odw_actuals_supplies_baseprod_staging_interim_supplies_only
WHERE 1=1
GROUP BY cal_date, country_alpha2, base_product_number, pl, customer_engagement, market10
"""

actuals_supplies_baseprod = spark.sql(actuals_supplies_baseprod)
actuals_supplies_baseprod.createOrReplaceTempView("actuals_supplies_baseprod")

# COMMAND ----------

#platform subset by cartridge demand mix
cartridge_demand = f"""
SELECT 
    cal_date,
    geography AS market10,
    platform_subset,
    base_product_number,
    SUM(adjusted_cartridges) AS units
FROM working_forecast
WHERE version = (select max(version) from working_forecast)
    AND cal_date BETWEEN (SELECT MIN(cal_date) FROM odw_actuals_supplies_salesprod) 
                    AND (SELECT MAX(cal_date) FROM odw_actuals_supplies_salesprod)
    AND adjusted_cartridges <> 0
    AND geography_grain = 'MARKET10'
GROUP BY 
    cal_date,
    geography,
    platform_subset,
    base_product_number
"""

cartridge_demand = spark.sql(cartridge_demand)
cartridge_demand.createOrReplaceTempView("cartridge_demand")

cartridge_demand_ptr_mix = f"""
SELECT distinct    cal_date,
    market10,
    platform_subset,
    base_product_number,
    CASE
        WHEN SUM(units) OVER (PARTITION BY cal_date, market10, base_product_number) = 0 THEN NULL
        ELSE units / SUM(units) OVER (PARTITION BY cal_date, market10, base_product_number)
    END AS platform_mix
FROM cartridge_demand
GROUP BY cal_date, 
    market10, 
    platform_subset, 
    base_product_number,
    units
"""

cartridge_demand_ptr_mix = spark.sql(cartridge_demand_ptr_mix)
cartridge_demand_ptr_mix.createOrReplaceTempView("cartridge_demand_ptr_mix")

# COMMAND ----------

# platform subset by ib mix, get ib -- if no data comes back, comment out official = 1
installed_base_history = f"""
SELECT cal_date,
    platform_subset,
    ib.country_alpha2,
    market10,
    sum(units) as units,
    ib.version
FROM ib ib
LEFT JOIN iso_country_code_xref iso
    ON ib.country_alpha2 = iso.country_alpha2
WHERE 1=1
--AND ib.version = (select max(version) from ib where record = 'IB' AND official = 1)
AND units <> 0
AND units IS NOT NULL
AND cal_date <= (SELECT MAX(cal_date) FROM odw_actuals_supplies_salesprod)
GROUP BY
    cal_date,
    platform_subset,
    market10,
    ib.country_alpha2,
    ib.version
"""

installed_base_history = spark.sql(installed_base_history)
installed_base_history.createOrReplaceTempView("installed_base_history")

# COMMAND ----------

#assign cartridge to printer ib using supplies_hw_mapping // build out the supplies hw mapping table using code leveraged from working forecast
shm_01_iso = f"""
SELECT DISTINCT market10
    , region_5
FROM iso_country_code_xref
where 1=1
    AND NOT market10 IS NULL
    AND region_5 NOT IN ('XU','XW')
"""

shm_01_iso = spark.sql(shm_01_iso)
shm_01_iso.createOrReplaceTempView("shm_01_iso")


shm_02_geo_1 = f"""
SELECT DISTINCT shm.platform_subset
    , shm.base_product_number
    , shm.geography
    , CASE WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%STND%' THEN 'STD'
           WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%YET2%' THEN 'HP+'
           WHEN hw.technology = 'LASER' THEN 'TRAD'
           ELSE shm.customer_engagement END AS customer_engagement
FROM supplies_hw_mapping AS shm
JOIN hardware_xref AS hw
    ON hw.platform_subset = shm.platform_subset
WHERE 1=1
    AND hw.official = 1
    AND shm.official = 1
    and geography_grain = 'MARKET10'
"""

shm_02_geo_1 = spark.sql(shm_02_geo_1)
shm_02_geo_1.createOrReplaceTempView("shm_02_geo_1")


shm_03_geo_2 = f"""
SELECT DISTINCT shm.platform_subset
    , shm.base_product_number
    , iso.market10 AS geography
    , CASE WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%STND%' THEN 'STD'
           WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%YET2%' THEN 'HP+'
           WHEN hw.technology = 'LASER' THEN 'TRAD'
           ELSE shm.customer_engagement END AS customer_engagement
FROM supplies_hw_mapping AS shm
JOIN hardware_xref AS hw
    ON hw.platform_subset = shm.platform_subset
JOIN shm_01_iso AS iso
    ON iso.region_5 = shm.geography
WHERE 1=1
    AND shm.official = 1
    AND hw.official = 1
    AND shm.geography_grain = 'REGION_5'
"""

shm_03_geo_2 = spark.sql(shm_03_geo_2)
shm_03_geo_2.createOrReplaceTempView("shm_03_geo_2")

shm_03b_geo_3 = f"""
SELECT DISTINCT shm.platform_subset
    , shm.base_product_number
    , iso.market10 AS geography
    , CASE WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%STND%' THEN 'STD'
           WHEN hw.technology = 'LASER' AND shm.platform_subset LIKE '%YET2%' THEN 'HP+'
           WHEN hw.technology = 'LASER' THEN 'TRAD'
           ELSE shm.customer_engagement END AS customer_engagement
FROM supplies_hw_mapping AS shm
JOIN hardware_xref AS hw
    ON hw.platform_subset = shm.platform_subset
JOIN iso_cc_rollup_xref  AS cc
    ON cc.country_level_1 = shm.geography  -- gives us cc.country_alpha2
JOIN iso_country_code_xref AS iso
    ON iso.country_alpha2 = cc.country_alpha2     -- changed geography_grain to geography
WHERE 1=1
    AND shm.official = 1
    AND hw.official = 1
    AND shm.geography_grain = 'REGION_8'
    AND cc.country_scenario = 'HOST_REGION_8'
"""

shm_03b_geo_3 = spark.sql(shm_03b_geo_3)
shm_03b_geo_3.createOrReplaceTempView("shm_03b_geo_3")

shm_04_combined = f"""
SELECT platform_subset
    , base_product_number
    , geography
    , customer_engagement
FROM shm_02_geo_1

UNION ALL

SELECT platform_subset
    , base_product_number
    , geography
    , customer_engagement
FROM shm_03_geo_2

UNION ALL

SELECT platform_subset
    , base_product_number
    , geography
    , customer_engagement
FROM shm_03b_geo_3
"""

shm_04_combined = spark.sql(shm_04_combined)
shm_04_combined.createOrReplaceTempView("shm_04_combined")


shm_05_remove_dupes = f"""
SELECT DISTINCT platform_subset
    , base_product_number
    , geography
    , customer_engagement
FROM shm_04_combined
"""

shm_05_remove_dupes = spark.sql(shm_05_remove_dupes)
shm_05_remove_dupes.createOrReplaceTempView("shm_05_remove_dupes")

shm_06_map_geo = f"""
SELECT platform_subset
    , base_product_number
    , geography
    , customer_engagement
    , platform_subset + ' ' + base_product_number + ' ' +
        geography + ' ' + customer_engagement AS composite_key
FROM shm_05_remove_dupes
"""

shm_06_map_geo = spark.sql(shm_06_map_geo)
shm_06_map_geo.createOrReplaceTempView("shm_06_map_geo")


#modify shm_06_map_geo for different customer engagements in actuals supplies then forecast supplies:
shm_07_collapse_ce_type = f"""
            SELECT distinct platform_subset
                , base_product_number
                , geography
                , CASE
                    WHEN customer_engagement = 'I-INK' THEN 'I-INK'
                    ELSE 'TRAD'
                END AS customer_engagement
            FROM shm_06_map_geo
"""
shm_07_collapse_ce_type = spark.sql(shm_07_collapse_ce_type)
shm_07_collapse_ce_type.createOrReplaceTempView("shm_07_collapse_ce_type")


supplies_ce_supplies_hw_map = f"""
SELECT distinct platform_subset
    , base_product_number
    , geography
    , customer_engagement
FROM shm_07_collapse_ce_type
WHERE customer_engagement = 'I-INK'

UNION ALL

SELECT distinct platform_subset
    , base_product_number
    , geography
    , 'EST_DIRECT_FULFILLMENT' AS customer_engagement
FROM shm_07_collapse_ce_type
WHERE customer_engagement = 'TRAD'

UNION ALL

SELECT distinct platform_subset
    , base_product_number
    , geography
    , 'EST_INDIRECT_FULFILLMENT' AS customer_engagement
FROM shm_07_collapse_ce_type
WHERE customer_engagement = 'TRAD'

UNION ALL

SELECT distinct platform_subset
    , base_product_number
    , geography
    , customer_engagement
FROM shm_07_collapse_ce_type
WHERE customer_engagement = 'TRAD'
"""
supplies_ce_supplies_hw_map = spark.sql(supplies_ce_supplies_hw_map)
supplies_ce_supplies_hw_map.createOrReplaceTempView("supplies_ce_supplies_hw_map")


#end supplies_hw_mapping build out
map_crtg_to_printers = f"""
SELECT
    distinct platform_subset,
    base_product_number,
    geography as market10
FROM supplies_ce_supplies_hw_map
WHERE 1=1
    AND platform_subset IN (select distinct platform_subset from installed_base_history)
    AND base_product_number IN (select distinct base_product_number from actuals_supplies_baseprod)
"""

map_crtg_to_printers = spark.sql(map_crtg_to_printers)
map_crtg_to_printers.createOrReplaceTempView("map_crtg_to_printers")  


# COMMAND ----------

#platform subset by ib mix
installed_base_with_crtg = f"""
SELECT
    cal_date,
    country_alpha2,
    ib.market10,
    ib.platform_subset,
    base_product_number,
    sum(units) as units
FROM installed_base_history ib
INNER JOIN map_crtg_to_printers map ON
    ib.market10 = map.market10 AND
    ib.platform_subset = map.platform_subset
GROUP BY cal_date,
    country_alpha2,
    ib.market10,
    ib.platform_subset,
    base_product_number
"""

installed_base_with_crtg = spark.sql(installed_base_with_crtg)
installed_base_with_crtg.createOrReplaceTempView("installed_base_with_crtg") 


ib_printer_mix = f"""
SELECT distinct    cal_date,
    country_alpha2,
    market10,
    platform_subset,
    base_product_number,
    CASE
        WHEN SUM(units) OVER (PARTITION BY cal_date, country_alpha2, market10, base_product_number) = 0 THEN NULL
        ELSE units / SUM(units) OVER (PARTITION BY cal_date, country_alpha2, market10, base_product_number)
    END AS ib_mix
FROM installed_base_with_crtg
GROUP BY cal_date,
    country_alpha2,
    market10,
    platform_subset,
    base_product_number,
    units
"""

ib_printer_mix = spark.sql(ib_printer_mix)
ib_printer_mix.createOrReplaceTempView("ib_printer_mix") 

# COMMAND ----------

#accounting items
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

#non-accounting items // data to map printers to // addback 1
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

#base product with assigned printers based upon cartridge demand // addback 2
baseprod_printer_from_crtg_demand = f"""
SELECT 
    act.cal_date,            
    country_alpha2,
    act.market10,
    platform_subset,
    act.base_product_number,
    pl,
    customer_engagement,
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
JOIN cartridge_demand_ptr_mix mix ON mix.cal_date = act.cal_date AND mix.market10 = act.market10 AND mix.base_product_number = act.base_product_number
GROUP BY act.cal_date, country_alpha2, act.base_product_number, pl, customer_engagement, platform_subset, act.market10
"""

baseprod_printer_from_crtg_demand = spark.sql(baseprod_printer_from_crtg_demand)
baseprod_printer_from_crtg_demand.createOrReplaceTempView("baseprod_printer_from_crtg_demand") 


# COMMAND ----------

# failed match via cartridge demand
baseprod_without_crtg_demand_connect = f"""
SELECT
    bp.cal_date,
    bp.country_alpha2,
    bp.market10,
    platform_subset,
    bp.base_product_number,
    bp.pl,
    bp.customer_engagement,
    SUM(bp.gross_revenue) AS gross_revenue,
    SUM(bp.net_currency) AS net_currency,
    SUM(bp.contractual_discounts) AS contractual_discounts,
    SUM(bp.discretionary_discounts) AS discretionary_discounts,
    SUM(bp.net_revenue) AS net_revenue,
    SUM(bp.warranty) AS warranty,
    SUM(bp.other_cos) AS other_cos,
    SUM(bp.total_cos) AS total_cos,
    SUM(bp.gross_profit) AS gross_profit,
    SUM(bp.revenue_units) AS revenue_units,
    SUM(bp.equivalent_units) AS equivalent_units,
    SUM(bp.yield_x_units) AS yield_x_units,
    SUM(bp.yield_x_units_black_only) AS yield_x_units_black_only
FROM baseprod_without_acct_items bp
LEFT JOIN baseprod_printer_from_crtg_demand sub 
    ON bp.cal_date = sub.cal_date 
    AND bp.country_alpha2 = sub.country_alpha2 
    AND bp.base_product_number = sub.base_product_number 
    AND bp.pl = sub.pl 
    AND bp.customer_engagement = sub.customer_engagement 
    AND bp.market10 = sub.market10 
WHERE platform_subset IS NULL
GROUP BY bp.cal_date, bp.country_alpha2, bp.base_product_number, bp.pl, bp.customer_engagement, bp.market10, platform_subset
"""

baseprod_without_crtg_demand_connect = spark.sql(baseprod_without_crtg_demand_connect)
baseprod_without_crtg_demand_connect.createOrReplaceTempView("baseprod_without_crtg_demand_connect") 

# COMMAND ----------

#base product with assigned printers based upon ib // addback 3
baseprod_printer_by_ib_mix = f"""
SELECT
    act.cal_date,
    act.country_alpha2,
    act.market10,
    mix.platform_subset,
    act.base_product_number,
    pl,
    customer_engagement,
    SUM(gross_revenue * ib_mix) AS gross_revenue,
    SUM(net_currency * ib_mix) AS net_currency,
    SUM(contractual_discounts * ib_mix) AS contractual_discounts,
    SUM(discretionary_discounts * ib_mix) AS discretionary_discounts,
    SUM(net_revenue * ib_mix) AS net_revenue,
    SUM(warranty * ib_mix) AS warranty,
    SUM(other_cos * ib_mix) AS other_cos,
    SUM(total_cos * ib_mix) AS total_cos,
    SUM(gross_profit * ib_mix) AS gross_profit,
    SUM(revenue_units * ib_mix) AS revenue_units,
    SUM(equivalent_units * ib_mix) AS equivalent_units,
    SUM(yield_x_units * ib_mix) AS yield_x_units,
    SUM(yield_x_units_black_only * ib_mix) AS yield_x_units_black_only
FROM baseprod_without_crtg_demand_connect act
JOIN ib_printer_mix AS mix ON mix.cal_date = act.cal_date AND mix.base_product_number = act.base_product_number AND act.country_alpha2 = mix.country_alpha2 AND act.market10 = mix.market10
GROUP BY act.cal_date, act.country_alpha2, act.base_product_number, pl, customer_engagement, mix.platform_subset, act.market10
"""            

baseprod_printer_by_ib_mix = spark.sql(baseprod_printer_by_ib_mix)
baseprod_printer_by_ib_mix.createOrReplaceTempView("baseprod_printer_by_ib_mix") 

# COMMAND ----------

# failed match via ib
baseprod_without_printer_map_from_crtg_demand_or_ib = f"""
SELECT
    act.cal_date,
    act.country_alpha2,
    act.market10,
    mix.platform_subset,
    act.base_product_number,
    act.pl,
    act.customer_engagement,
    SUM(act.gross_revenue) AS gross_revenue,
    SUM(act.net_currency) AS net_currency,
    SUM(act.contractual_discounts) AS contractual_discounts,
    SUM(act.discretionary_discounts) AS discretionary_discounts,
    SUM(act.net_revenue) AS net_revenue,
    SUM(act.warranty) AS warranty,
    SUM(act.other_cos) AS other_cos,
    SUM(act.total_cos) AS total_cos,
    SUM(act.gross_profit) AS gross_profit,
    SUM(act.revenue_units) AS revenue_units,
    SUM(act.equivalent_units) AS equivalent_units,
    SUM(act.yield_x_units) AS yield_x_units,
    SUM(act.yield_x_units_black_only) AS yield_x_units_black_only
FROM baseprod_without_crtg_demand_connect act
LEFT JOIN baseprod_printer_by_ib_mix AS mix 
    ON mix.cal_date = act.cal_date
    AND mix.country_alpha2 = act.country_alpha2
    AND act.pl = mix.pl
    AND act.customer_engagement = mix.customer_engagement
    AND mix.base_product_number = act.base_product_number 
    AND act.market10 = mix.market10
WHERE mix.platform_subset IS NULL
GROUP BY act.cal_date, act.country_alpha2, act.base_product_number, act.pl, act.customer_engagement, mix.platform_subset, act.market10
"""

baseprod_without_printer_map_from_crtg_demand_or_ib = spark.sql(baseprod_without_printer_map_from_crtg_demand_or_ib)
baseprod_without_printer_map_from_crtg_demand_or_ib.createOrReplaceTempView("baseprod_without_printer_map_from_crtg_demand_or_ib") 

# COMMAND ----------

#map printer to crtg based upon supplies hw mapping
map_ptr_to_crtg = f"""
SELECT distinct    platform_subset,
    base_product_number,
    market10,
    CASE
        WHEN COUNT(platform_subset) OVER (PARTITION BY base_product_number, market10) = 0 THEN NULL
        ELSE COUNT(platform_subset) OVER (PARTITION BY base_product_number, market10)
    END AS printers_per_baseprod
FROM map_crtg_to_printers 
GROUP BY platform_subset, base_product_number, market10
"""                

map_ptr_to_crtg = spark.sql(map_ptr_to_crtg)
map_ptr_to_crtg.createOrReplaceTempView("map_ptr_to_crtg") 


map_ptr_to_crtg_no_nulls = f"""
SELECT distinct    platform_subset,
    base_product_number,
    market10,
    coalesce(sum(printers_per_baseprod), 0) AS printers_per_baseprod
FROM map_ptr_to_crtg 
GROUP BY platform_subset, base_product_number, market10
"""

map_ptr_to_crtg_no_nulls = spark.sql(map_ptr_to_crtg_no_nulls)
map_ptr_to_crtg_no_nulls.createOrReplaceTempView("map_ptr_to_crtg_no_nulls")     


map_ptr_to_crtg2 = f"""
SELECT
    platform_subset,
    base_product_number,
    m.market10,
    CAST(printers_per_baseprod AS decimal(10,8)) AS printers_per_baseprod
FROM map_ptr_to_crtg_no_nulls m
"""

map_ptr_to_crtg2 = spark.sql(map_ptr_to_crtg2)
map_ptr_to_crtg2.createOrReplaceTempView("map_ptr_to_crtg2")

# COMMAND ----------

#supplies hardware map mix
date_helper = f"""
SELECT
    date_key
    , Date AS cal_date
FROM calendar
WHERE day_of_month = 1
"""

date_helper = spark.sql(date_helper)
date_helper.createOrReplaceTempView("date_helper")


hw_supplies_map3 = f"""
SELECT 
    cal_date,
    platform_subset,
    base_product_number,
    printers_per_baseprod,
    market10
FROM date_helper
CROSS JOIN map_ptr_to_crtg2
WHERE cal_date BETWEEN 
    (SELECT MIN(cal_date) FROM odw_actuals_supplies_salesprod) 
    AND 
    (SELECT MAX(cal_date) FROM odw_actuals_supplies_salesprod)
"""

hw_supplies_map3 = spark.sql(hw_supplies_map3)
hw_supplies_map3.createOrReplaceTempView("hw_supplies_map3")


supplies_hw_map_mix = f"""
SELECT distinct cal_date,
    platform_subset,
    base_product_number,
    m.market10,
    SUM(printers_per_baseprod) AS printers_per_baseprod,
    1 / SUM(printers_per_baseprod) AS hw_mix
FROM hw_supplies_map3 m
LEFT JOIN iso_country_code_xref iso ON m.market10 = iso.market10
GROUP BY platform_subset, base_product_number, m.market10, cal_date
"""

supplies_hw_map_mix = spark.sql(supplies_hw_map_mix)
supplies_hw_map_mix.createOrReplaceTempView("supplies_hw_map_mix")

# COMMAND ----------

##base product with assigned printers based upon supplies hw map // addback 4
actuals_join_hw_mix = f"""
SELECT
    sup.cal_date,
    country_alpha2,
    sup.market10,
    map.platform_subset,
    sup.base_product_number,
    pl,
    sup.customer_engagement,
    SUM(gross_revenue * COALESCE(hw_mix, 1)) AS gross_revenue,
    SUM(net_currency * COALESCE(hw_mix, 1)) AS net_currency,
    SUM(contractual_discounts * COALESCE(hw_mix, 1)) AS contractual_discounts,
    SUM(discretionary_discounts * COALESCE(hw_mix, 1)) AS discretionary_discounts,
    SUM(net_revenue * COALESCE(hw_mix, 1)) AS net_revenue,
    SUM(warranty * COALESCE(hw_mix, 1)) AS warranty,
    SUM(other_cos * COALESCE(hw_mix, 1)) AS other_cos,
    SUM(total_cos * COALESCE(hw_mix, 1)) AS total_cos,
    SUM(gross_profit * COALESCE(hw_mix, 1)) AS gross_profit,
    SUM(revenue_units * COALESCE(hw_mix, 1)) AS revenue_units,
    SUM(equivalent_units * COALESCE(hw_mix, 1)) AS equivalent_units,
    SUM(yield_x_units * COALESCE(hw_mix, 1)) AS yield_x_units,
    SUM(yield_x_units_black_only * COALESCE(hw_mix, 1)) AS yield_x_units_black_only
FROM baseprod_without_printer_map_from_crtg_demand_or_ib  sup
LEFT JOIN supplies_hw_map_mix AS map ON map.base_product_number = sup.base_product_number AND map.cal_date = sup.cal_date AND 
    map.market10 = sup.market10
GROUP BY sup.cal_date, country_alpha2, sup.market10, sup.base_product_number, pl, sup.customer_engagement, map.platform_subset
"""

actuals_join_hw_mix = spark.sql(actuals_join_hw_mix)
actuals_join_hw_mix.createOrReplaceTempView("actuals_join_hw_mix")


#platform subset has to be a primary key, so it cannot be null
baseprod_map_printer_using_shm = f"""
SELECT
    cal_date,
    country_alpha2,
    market10,
    CASE
        WHEN platform_subset IS NULL THEN 'NA'
        ELSE platform_subset
    END AS platform_subset,
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
FROM actuals_join_hw_mix
GROUP BY cal_date, country_alpha2, market10, base_product_number, pl, customer_engagement, platform_subset
"""

baseprod_map_printer_using_shm = spark.sql(baseprod_map_printer_using_shm)
baseprod_map_printer_using_shm.createOrReplaceTempView("baseprod_map_printer_using_shm")

# COMMAND ----------

#final base product with platform subset allocation
all_baseprod_with_platform_subsets = f"""
SELECT * 
FROM accounting_items_addback
UNION ALL
SELECT * 
FROM baseprod_printer_from_crtg_demand
UNION ALL
SELECT *
FROM baseprod_printer_by_ib_mix
UNION ALL
SELECT *
FROM baseprod_map_printer_using_shm
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
JOIN product_line_xref AS plx ON bp.pl = plx.pl
GROUP BY cal_date, country_alpha2, platform_subset, base_product_number, bp.pl, customer_engagement, market10, l5_description
"""

baseprod_financials_preplanet_table = spark.sql(baseprod_financials_preplanet_table)
baseprod_financials_preplanet_table.createOrReplaceTempView("baseprod_financials_preplanet_table")

# COMMAND ----------

#sacp, planet replacement, target totals
planet_data = f"""
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
FROM odw_sacp_actuals AS p
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
    AND gross_revenue + net_currency + contractual_discounts + discretionary_discounts + other_cos + warranty + total_cos != 0
    --AND cal_date = (SELECT MAX(cal_date) FROM odw_sacp_actuals)
    AND cal_date > '2021-10-01'
GROUP BY cal_date, pl, region_5, Fiscal_Yr
"""

planet_data = spark.sql(planet_data)
planet_data.createOrReplaceTempView("planet_data")


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
FROM planet_data
GROUP BY cal_date, region_5, pl, Fiscal_Yr
"""

planet_targets = spark.sql(planet_targets)
planet_targets.createOrReplaceTempView("planet_targets")

# COMMAND ----------

#add targets to base product dataset
baseprod_prep_for_planet_targets = f"""
SELECT
    bp.cal_date,
    Fiscal_Yr,
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
LEFT JOIN iso_country_code_xref AS iso ON bp.country_alpha2 = iso.country_alpha2 AND bp.market10 = iso.market10
JOIN calendar AS cal ON bp.cal_date = cal.Date
WHERE Fiscal_Yr > '2016'
AND Day_of_Month = 1
GROUP BY bp.cal_date, bp.pl, region_5, Fiscal_Yr
"""

baseprod_prep_for_planet_targets = spark.sql(baseprod_prep_for_planet_targets)
baseprod_prep_for_planet_targets.createOrReplaceTempView("baseprod_prep_for_planet_targets")

baseprod_add_planet = f"""
SELECT
    bp.cal_date,
    bp.Fiscal_Yr,
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
    COALESCE(SUM(p_gross_revenue), 0) + COALESCE(SUM(p_net_currency), 0) - COALESCE(SUM(p_contractual_discounts), 0) - COALESCE(SUM(p_discretionary_discounts), 0) AS p_net_revenue,    
    COALESCE(SUM(p_warranty), 0) AS p_warranty,
    COALESCE(SUM(p_other_cos), 0) AS p_other_cos,
    COALESCE(SUM(p_total_cos), 0) AS p_total_cos,
    COALESCE(SUM(p_gross_revenue), 0) + COALESCE(SUM(p_net_currency), 0) - COALESCE(SUM(p_contractual_discounts), 0) - COALESCE(SUM(p_discretionary_discounts), 0) - COALESCE(SUM(p_total_cos), 0) AS p_gross_profit
FROM baseprod_prep_for_planet_targets AS bp 
LEFT JOIN planet_targets AS p ON (bp.cal_date = p.cal_date AND bp.region_5 = p.region_5 AND bp.pl = p.pl AND bp.Fiscal_Yr = p.Fiscal_Yr)
GROUP BY bp.cal_date, bp.region_5, bp.pl, bp.Fiscal_Yr
"""

baseprod_add_planet = spark.sql(baseprod_add_planet)
baseprod_add_planet.createOrReplaceTempView("baseprod_add_planet")


baseprod_calc_difference = f"""
SELECT
    cal_date,
    Fiscal_Yr,
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
GROUP BY cal_date, Fiscal_Yr, region_5, pl
"""

baseprod_calc_difference = spark.sql(baseprod_calc_difference)
baseprod_calc_difference.createOrReplaceTempView("baseprod_calc_difference")


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
JOIN iso_country_code_xref AS iso ON p.country_alpha2 = iso.country_alpha2
JOIN product_line_xref AS plx ON p.pl = plx.pl
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
        FROM product_line_xref 
        WHERE Technology IN ('INK', 'LASER', 'PWA', 'LF') 
            AND PL_category IN ('SUP')
    )
GROUP BY cal_date, country_alpha2, market10, platform_subset, base_product_number, pl, l5_description, customer_engagement, market10, l5_description
"""
final_planet_adjust_to_baseprod_supplies = spark.sql(final_planet_adjust_to_baseprod_supplies)
final_planet_adjust_to_baseprod_supplies.createOrReplaceTempView("final_planet_adjust_to_baseprod_supplies")


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
JOIN calendar cal ON cal.Date = p.cal_date
WHERE Day_of_Month = 1 
and pl IN 
    (
    SELECT DISTINCT (pl) 
    FROM product_line_xref 
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

write_df_to_redshift(configs, baseprod_load_financials, "fin_prod.odw_actuals_supplies_baseprod", "append", postactions = "", preactions = "truncate fin_prod.odw_actuals_supplies_baseprod")

# COMMAND ----------

#clean up directly in RS
query = f"""
UPDATE fin_prod.odw_actuals_supplies_baseprod
SET net_revenue = 0
WHERE net_revenue <.000001 and net_revenue > 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET net_revenue = 0
WHERE net_revenue >-.000001 and net_revenue < 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET gross_revenue = 0
WHERE gross_revenue <.000001 and gross_revenue > 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET gross_revenue = 0
WHERE gross_revenue >-.000001 and gross_revenue < 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET net_currency = 0
WHERE net_currency <.000001 and net_currency > 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET net_currency = 0
WHERE net_currency >-.000001 and net_currency < 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET contractual_discounts = 0
WHERE contractual_discounts <.000001 and contractual_discounts > 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET contractual_discounts = 0
WHERE contractual_discounts >-.000001 and contractual_discounts < 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET discretionary_discounts = 0
WHERE discretionary_discounts <.000001 and discretionary_discounts > 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET discretionary_discounts = 0
WHERE discretionary_discounts >-.000001 and discretionary_discounts < 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET total_cos = 0
WHERE total_cos <.000001 and total_cos > 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET total_cos = 0
WHERE total_cos >-.000001 and total_cos < 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET gross_profit = 0
WHERE gross_profit <.000001 and gross_profit > 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET gross_profit = 0
WHERE gross_profit >-.000001 and gross_profit < 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET revenue_units = 0
WHERE revenue_units <.000001 and revenue_units > 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET revenue_units = 0
WHERE revenue_units >-.000001 and revenue_units < 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET equivalent_units = 0
WHERE equivalent_units <.000001 and equivalent_units > 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET equivalent_units = 0
WHERE equivalent_units >-.000001 and equivalent_units < 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET yield_x_units_black_only = 0
WHERE yield_x_units_black_only <.000001 and yield_x_units_black_only > 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET yield_x_units_black_only = 0
WHERE yield_x_units_black_only >-.000001 and yield_x_units_black_only < 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET yield_x_units = 0
WHERE yield_x_units <.000001 and yield_x_units > 0;

UPDATE fin_prod.odw_actuals_supplies_baseprod
SET yield_x_units = 0
WHERE yield_x_units >-.000001 and yield_x_units < 0;
"""

submit_remote_query(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'], query)
