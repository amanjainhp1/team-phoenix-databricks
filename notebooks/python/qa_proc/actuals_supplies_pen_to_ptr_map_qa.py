# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# load S3 tables to df
actuals_supplies_baseprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_supplies_baseprod") \
    .load()
iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load()
iso_cc_rollup_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_cc_rollup_xref") \
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
hardware_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.hardware_xref") \
    .load()
supplies_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.supplies_xref") \
    .load()
supplies_hw_country_actuals_mapping = read_redshift_to_df(configs) \
    .option("dbtable", "stage.supplies_hw_country_actuals_mapping") \
    .load()
ib = read_redshift_to_df(configs) \
    .option("query", "SELECT * FROM prod.ib WHERE version = (SELECT MAX(version) FROM prod.ib WHERE record = 'IB' AND official = 1)") \
    .load()

# COMMAND ----------

import re

tables = [
    ['fin_prod.actuals_supplies_baseprod', actuals_supplies_baseprod],
    ['mdm.iso_country_code_xref', iso_country_code_xref],
    ['mdm.iso_cc_rollup_xref', iso_cc_rollup_xref],
    ['mdm.supplies_hw_mapping', supplies_hw_mapping],
    ['mdm.calendar', calendar],
    ['mdm.product_line_xref', product_line_xref],
    #['stage.supplies_hw_country_actuals_mapping', supplies_hw_country_actuals_mapping],
    ['mdm.hardware_xref', hardware_xref]
]

write_df_to_delta(tables)

# COMMAND ----------

supplies_hw_country_actuals_mapping.createOrReplaceTempView("supplies_hw_country_actuals_mapping")

# COMMAND ----------

ib.createOrReplaceTempView("ib")

# COMMAND ----------

# call version sproc
addversion_info = call_redshift_addversion_sproc(configs, "TEST - ACTUALS SUPPLIES HARDWARE MAPPING DROPOUT", "TEST - ACTUALS SUPPLIES HARDWARE MAPPING DROPOUT")

# COMMAND ----------

# actuals supplies baseprod NA platform subset
actuals_supplies_baseprod_na_printer = f"""
SELECT             
    cal_date,
    country_alpha2,
    market10,
    base_product_number,
    pl,
    customer_engagement,  
    SUM(net_revenue) AS net_revenue,
    SUM(revenue_units) AS revenue_units
FROM fin_prod.actuals_supplies_baseprod
WHERE 1=1
AND platform_subset = 'NA'
and pl in (select pl from mdm.product_line_xref where pl_category = 'SUP' and technology in ('PWA', 'INK', 'LASER') and pl <> 'LZ' and pl <> 'GY')
and base_product_number NOT IN ('BIRDS', 'EDW_TIE_TO_PLANET', 'EDW_TIE2_SALES_TO_BASE_ROUNDING_ERROR', 'EST_MPS_REVENUE_JV', 'CISS', 'CTSS')
and base_product_number NOT LIKE 'UNKN%'
GROUP BY cal_date, country_alpha2, base_product_number, pl, customer_engagement, market10
"""

actuals_supplies_baseprod_na_printer = spark.sql(actuals_supplies_baseprod_na_printer)
actuals_supplies_baseprod_na_printer.createOrReplaceTempView("actuals_supplies_baseprod_na_printer")

# COMMAND ----------

actuals_supplies_baseprod_na_printer.count()

# COMMAND ----------

# hp pages (complete ib, usage, share)
shca_mapping_actuals_period_only = f"""
SELECT cal_date
    , country_alpha2
    , platform_subset
    , base_product_number
    , customer_engagement
    , SUM(hp_pages) as hp_pages
FROM supplies_hw_country_actuals_mapping
WHERE 1=1
AND cal_date <= (SELECT MAX(cal_date) FROM actuals_supplies_baseprod_na_printer)
GROUP BY cal_date, country_alpha2, platform_subset, base_product_number, customer_engagement
"""
 
shca_mapping_actuals_period_only = spark.sql(shca_mapping_actuals_period_only)
shca_mapping_actuals_period_only.createOrReplaceTempView("shca_mapping_actuals_period_only")

# COMMAND ----------

# ib
ib_data = f"""
SELECT cal_date,
    platform_subset,
    ib.country_alpha2,
    market10,
    sum(units) as units,
    ib.version
FROM ib ib
LEFT JOIN mdm.iso_country_code_xref iso
    ON ib.country_alpha2 = iso.country_alpha2
WHERE 1=1
AND units > 0
AND units IS NOT NULL
AND cal_date <= (SELECT MAX(cal_date) FROM actuals_supplies_baseprod_na_printer)
GROUP BY
    cal_date,
    platform_subset,
    market10,
    ib.country_alpha2,
    ib.version
"""

ib_data = spark.sql(ib_data)
ib_data.createOrReplaceTempView("ib_data")

# COMMAND ----------

#assign cartridge to printer in usage_share_country using supplies_hw_mapping // build out the supplies hw mapping table 
shm_01_iso = f"""
SELECT DISTINCT market10
    , country_alpha2
FROM mdm.iso_country_code_xref
WHERE 1=1
    AND NOT market10 IS NULL
    AND region_5 NOT IN ('XU','XW')
    AND country_alpha2 NOT LIKE 'X%'
    OR country_alpha2 LIKE 'XK'
"""

shm_01_iso = spark.sql(shm_01_iso)
shm_01_iso.createOrReplaceTempView("shm_01_iso")


shm_02_iso = f"""
SELECT DISTINCT region_5
    , country_alpha2
FROM mdm.iso_country_code_xref
WHERE 1=1
    AND NOT market8 IS NULL
    AND region_5 NOT IN ('XU','XW')
    AND country_alpha2 NOT LIKE 'X%'
    OR country_alpha2 LIKE 'XK'
"""

shm_02_iso = spark.sql(shm_02_iso)
shm_02_iso.createOrReplaceTempView("shm_02_iso")


shm_03_iso = f"""
SELECT DISTINCT market8
    , country_alpha2
FROM mdm.iso_country_code_xref
WHERE 1=1
    AND NOT market8 IS NULL
    AND region_5 NOT IN ('XU','XW')
    AND country_alpha2 NOT LIKE 'X%'
    OR country_alpha2 LIKE 'XK'
"""

shm_03_iso = spark.sql(shm_03_iso)
shm_03_iso.createOrReplaceTempView("shm_03_iso")


# COMMAND ----------

# build out country to geography and consolidate customer engagement
shm_02_geo_1 = f"""
SELECT DISTINCT shm.platform_subset
    , shm.base_product_number
    , iso.country_alpha2
    , CASE
          WHEN shm.customer_engagement = 'I-INK' THEN 'I-INK'
          ELSE 'TRAD'
     END AS customer_engagement
FROM mdm.supplies_hw_mapping shm
LEFT JOIN shm_01_iso iso
    ON shm.geography = iso.market10
WHERE 1=1
    AND shm.official = 1
    and shm.geography_grain = 'MARKET10'
"""

shm_02_geo_1 = spark.sql(shm_02_geo_1)
shm_02_geo_1.createOrReplaceTempView("shm_02_geo_1")


shm_03_geo_2 = f"""
SELECT DISTINCT shm.platform_subset
    , shm.base_product_number
    , iso.country_alpha2
    , CASE
          WHEN shm.customer_engagement = 'I-INK' THEN 'I-INK'
          ELSE 'TRAD'
     END AS customer_engagement
FROM mdm.supplies_hw_mapping shm
LEFT JOIN shm_02_iso iso
    ON shm.geography = iso.region_5
WHERE 1=1
    AND shm.official = 1
    and shm.geography_grain = 'REGION_5'
"""

shm_03_geo_2 = spark.sql(shm_03_geo_2)
shm_03_geo_2.createOrReplaceTempView("shm_03_geo_2")


shm_04_geo_3 = f"""
SELECT DISTINCT shm.platform_subset
    , shm.base_product_number
    , iso.country_alpha2
    , CASE
          WHEN shm.customer_engagement = 'I-INK' THEN 'I-INK'
          ELSE 'TRAD'
     END AS customer_engagement
FROM mdm.supplies_hw_mapping shm
LEFT JOIN shm_03_iso iso
    ON shm.geography = iso.market8
WHERE 1=1
    AND shm.official = 1
    and shm.geography_grain = 'REGION_8'
"""

shm_04_geo_3 = spark.sql(shm_04_geo_3)
shm_04_geo_3.createOrReplaceTempView("shm_04_geo_3")


shm_05_combined = f"""
SELECT platform_subset
    , base_product_number
    , country_alpha2
    , customer_engagement
FROM shm_02_geo_1
UNION ALL
SELECT platform_subset
    , base_product_number
    , country_alpha2
    , customer_engagement
FROM shm_03_geo_2
UNION ALL
SELECT platform_subset
    , base_product_number
    , country_alpha2
    , customer_engagement
FROM shm_04_geo_3
"""

shm_05_combined = spark.sql(shm_05_combined)
shm_05_combined.createOrReplaceTempView("shm_05_combined")


shm_06_remove_dupes_01 = f"""
SELECT DISTINCT platform_subset
    , base_product_number
    , country_alpha2
    , customer_engagement
FROM shm_05_combined
"""

shm_06_remove_dupes_01 = spark.sql(shm_06_remove_dupes_01)
shm_06_remove_dupes_01.createOrReplaceTempView("shm_06_remove_dupes_01")


shm_07_map_geo_1 = f"""
SELECT DISTINCT platform_subset
    , base_product_number
    , country_alpha2
    , customer_engagement
FROM shm_06_remove_dupes_01
WHERE customer_engagement = 'I-INK'
"""

shm_07_map_geo_1 = spark.sql(shm_07_map_geo_1)
shm_07_map_geo_1.createOrReplaceTempView("shm_07_map_geo_1")


shm_08_map_geo_2 = f"""
SELECT DISTINCT platform_subset
    , base_product_number
    , country_alpha2
    , customer_engagement
FROM shm_06_remove_dupes_01
WHERE customer_engagement = 'TRAD'
"""

shm_08_map_geo_2 = spark.sql(shm_08_map_geo_2)
shm_08_map_geo_2.createOrReplaceTempView("shm_08_map_geo_2")


shm_09_map_geo_3 = f"""
SELECT DISTINCT platform_subset
    , base_product_number
    , country_alpha2
    , 'EST_INDIRECT_FULFILLMENT' AS customer_engagement
FROM shm_08_map_geo_2
"""

shm_09_map_geo_3 = spark.sql(shm_09_map_geo_3)
shm_09_map_geo_3.createOrReplaceTempView("shm_09_map_geo_3")


shm_10_map_geo_4 = f"""
SELECT DISTINCT platform_subset
    , base_product_number
    , country_alpha2
    , 'EST_DIRECT_FULFILLMENT' AS customer_engagement
FROM shm_08_map_geo_2
"""

shm_10_map_geo_4 = spark.sql(shm_10_map_geo_4)
shm_10_map_geo_4.createOrReplaceTempView("shm_10_map_geo_4")


shm_11_map_geo_5 = f"""
SELECT distinct platform_subset
    , base_product_number
    , country_alpha2
    , customer_engagement
FROM shm_07_map_geo_1
UNION ALL
SELECT distinct platform_subset
    , base_product_number
    , country_alpha2
    , customer_engagement
FROM shm_08_map_geo_2
UNION ALL
SELECT distinct platform_subset
    , base_product_number
    , country_alpha2
    , customer_engagement
FROM shm_09_map_geo_3
UNION ALL
SELECT distinct platform_subset
    , base_product_number
    , country_alpha2
    , customer_engagement
FROM shm_10_map_geo_4
"""
shm_11_map_geo_5 = spark.sql(shm_11_map_geo_5)
shm_11_map_geo_5.createOrReplaceTempView("shm_11_map_geo_5")


shm_12_map_geo_6 = f"""
SELECT distinct platform_subset
    , base_product_number
    , country_alpha2
    , customer_engagement
    , CONCAT(platform_subset,' ',base_product_number,' ',country_alpha2,' ',customer_engagement) AS composite_key
FROM shm_11_map_geo_5
"""

shm_12_map_geo_6 = spark.sql(shm_12_map_geo_6)
shm_12_map_geo_6.createOrReplaceTempView("shm_12_map_geo_6")

# COMMAND ----------

# baseprod NA platform subset map back to shm, data
baseprod_na_printer_join_shm = f"""
SELECT             
    bnp.cal_date,
    bnp.country_alpha2,
    bnp.market10,
    shm.platform_subset,
    hx.hw_product_family,
    bnp.base_product_number,
    bnp.pl,
    sx.technology,
    bnp.customer_engagement,  
    SUM(bnp.net_revenue) AS net_revenue,
    SUM(bnp.revenue_units) AS revenue_units,
    SUM(ib.units) as ib_units,
    SUM(hp_pages) as hp_pages
FROM actuals_supplies_baseprod_na_printer bnp
LEFT JOIN shm_12_map_geo_6 shm
    ON bnp.base_product_number = shm.base_product_number
    AND bnp.country_alpha2 = shm.country_alpha2
    AND bnp.customer_engagement = shm.customer_engagement
LEFT JOIN ib_data ib 
    ON ib.cal_date = bnp.cal_date
    AND ib.platform_subset = shm.platform_subset
    AND ib.country_alpha2 = bnp.country_alpha2
LEFT JOIN shca_mapping_actuals_period_only shca
    ON shca.cal_date = bnp.cal_date
    AND shca.platform_subset = shm.platform_subset
    AND shca.country_alpha2 = bnp.country_alpha2
LEFT JOIN mdm.supplies_xref sx
    ON sx.base_product_number = bnp.base_product_number
LEFT JOIN mdm.hardware_xref hx
    ON hx.platform_subset = shm.platform_subset
WHERE 1=1
GROUP BY             
    bnp.cal_date,
    bnp.country_alpha2,
    bnp.market10,
    shm.platform_subset,
    bnp.base_product_number,
    bnp.pl,
    bnp.customer_engagement,
    sx.technology,
    hx.hw_product_family
"""

baseprod_na_printer_join_shm = spark.sql(baseprod_na_printer_join_shm)
baseprod_na_printer_join_shm.createOrReplaceTempView("baseprod_na_printer_join_shm")

# COMMAND ----------

baseprod_na_printer_join_shm.count()

# COMMAND ----------

write_df_to_redshift(configs, baseprod_na_printer_join_shm, "scen.supplies_pen_to_ptr_mapping_dropout", "append", postactions = "", preactions = "truncate scen.supplies_pen_to_ptr_mapping_dropout")
