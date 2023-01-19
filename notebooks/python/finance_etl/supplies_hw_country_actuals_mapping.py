# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# load S3 tables to df
calendar = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.calendar") \
    .load()
iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load()
product_line_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.product_line_xref") \
    .load()
supplies_hw_mapping = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.supplies_hw_mapping") \
    .load()
odw_actuals_supplies_salesprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_actuals_supplies_salesprod") \
    .load()

# COMMAND ----------

odw_actuals_supplies_salesprod.createOrReplaceTempView("odw_actuals_supplies_salesprod")

# COMMAND ----------

# load from spectrum table
usage_share_country = read_redshift_to_df(configs) \
    .option("query", "SELECT * FROM phoenix_spectrum_prod.usage_share_country WHERE version = (SELECT MAX(version) FROM phoenix_spectrum_prod.usage_share_country) AND measure = 'HP_PAGES'") \
    .load()

# COMMAND ----------

usage_share_country.createOrReplaceTempView("usage_share_country")

# COMMAND ----------

tables = [
    ['mdm.iso_country_code_xref', iso_country_code_xref],
    ['mdm.supplies_hw_mapping', supplies_hw_mapping],
    ['mdm.calendar', calendar],
    ['mdm.product_line_xref', product_line_xref]
    #['prod.ib', ib]
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

# call version sproc
#addversion_info = call_redshift_addversion_sproc(configs, "ACTUALS SUPPLIES HW ALLOCATIONS - SUPPLIES HW COUNTRY MAPPING", "ACTUALS SUPPLIES HW ALLOCATIONS - SUPPLIES HW COUNTRY MAPPING")

# COMMAND ----------

# usage_share_country_abridged (hp pages only)
usage_share_country01 = f"""
SELECT cal_date
    , geography as country_alpha2
    , platform_subset
    , customer_engagement
    , sum(units) as hp_pages
FROM usage_share_country
WHERE 1=1
GROUP BY cal_date
    , geography
    , platform_subset
    , customer_engagement
"""

usage_share_country01 = spark.sql(usage_share_country01)
usage_share_country01.createOrReplaceTempView("usage_share_country01")

# COMMAND ----------

# usage_share_country consolidate customer engagement to align with actuals customer engagements
usage_share_country02 = f"""
SELECT distinct cal_date
    , country_alpha2
    , platform_subset
    , CASE
          WHEN customer_engagement = 'I-INK' THEN 'I-INK'
          ELSE 'TRAD'
      END AS customer_engagement
    , sum(hp_pages) as hp_pages
FROM usage_share_country01
GROUP BY cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
"""

usage_share_country02 = spark.sql(usage_share_country02)
usage_share_country02.createOrReplaceTempView("usage_share_country02")

# COMMAND ----------

# add indirect and direct fulfillment, leveraging shm 'trad'
usage_share_country03 = f"""
SELECT cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
    , sum(hp_pages) as hp_pages
FROM usage_share_country02
WHERE 1=1
    AND customer_engagement = 'I-INK'
GROUP BY cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
"""

usage_share_country03 = spark.sql(usage_share_country03)
usage_share_country03.createOrReplaceTempView("usage_share_country03")


usage_share_country04 = f"""
SELECT cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
    , sum(hp_pages) as hp_pages
FROM usage_share_country02
WHERE 1=1
    AND customer_engagement = 'TRAD'
GROUP BY cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
"""

usage_share_country04 = spark.sql(usage_share_country04)
usage_share_country04.createOrReplaceTempView("usage_share_country04")


usage_share_country05 = f"""
SELECT cal_date
    , country_alpha2
    , platform_subset
    , 'EST_DIRECT_FULFILLMENT' AS customer_engagement
    , sum(hp_pages) as hp_pages
FROM usage_share_country04
WHERE 1=1
GROUP BY cal_date
    , country_alpha2
    , platform_subset
"""

usage_share_country05 = spark.sql(usage_share_country05)
usage_share_country05.createOrReplaceTempView("usage_share_country05")


usage_share_country06 = f"""
SELECT cal_date
    , country_alpha2
    , platform_subset
    , 'EST_INDIRECT_FULFILLMENT' AS customer_engagement
    , sum(hp_pages) as hp_pages
FROM usage_share_country04
WHERE 1=1
GROUP BY cal_date
    , country_alpha2
    , platform_subset
"""

usage_share_country06 = spark.sql(usage_share_country06)
usage_share_country06.createOrReplaceTempView("usage_share_country06")


usage_share_country07 = f"""
SELECT cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
    , sum(hp_pages) as hp_pages
FROM usage_share_country03
WHERE 1=1
GROUP BY cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement

UNION ALL

SELECT cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
    , sum(hp_pages) as hp_pages
FROM usage_share_country04
WHERE 1=1
GROUP BY cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement

UNION ALL

SELECT cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
    , sum(hp_pages) as hp_pages
FROM usage_share_country05
WHERE 1=1
GROUP BY cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement

UNION ALL

SELECT cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
    , sum(hp_pages) as hp_pages
FROM usage_share_country06
WHERE 1=1
GROUP BY cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
"""
usage_share_country07 = spark.sql(usage_share_country07)
usage_share_country07.createOrReplaceTempView("usage_share_country07")



usage_share_country08 = f"""
SELECT distinct cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
    , sum(hp_pages) as hp_pages
FROM usage_share_country07
WHERE 1=1
GROUP BY cal_date
    , country_alpha2
    , platform_subset
    , customer_engagement
"""

usage_share_country08 = spark.sql(usage_share_country08)
usage_share_country08.createOrReplaceTempView("usage_share_country08")

# COMMAND ----------

#assign cartridge to printer in usage_share_country using supplies_hw_mapping // build out the supplies hw mapping table using code leveraged from working forecast
shm_01_iso = f"""
SELECT DISTINCT market10
    , country_alpha2
FROM iso_country_code_xref
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
FROM iso_country_code_xref
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
FROM iso_country_code_xref
WHERE 1=1
    AND NOT market8 IS NULL
    AND region_5 NOT IN ('XU','XW')
    AND country_alpha2 NOT LIKE 'X%'
    OR country_alpha2 LIKE 'XK'
"""

shm_03_iso = spark.sql(shm_03_iso)
shm_03_iso.createOrReplaceTempView("shm_03_iso")

# COMMAND ----------

shm_02_geo_1 = f"""
SELECT DISTINCT shm.platform_subset
    , shm.base_product_number
    , iso.country_alpha2
    , CASE
          WHEN shm.customer_engagement = 'I-INK' THEN 'I-INK'
          ELSE 'TRAD'
     END AS customer_engagement
FROM supplies_hw_mapping shm
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
FROM supplies_hw_mapping shm
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
FROM supplies_hw_mapping shm
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

# MAGIC %sql
# MAGIC select *
# MAGIC from usage_share_country04

# COMMAND ----------

# add base product number to usage share country
usage_share_baseprod_01 = f"""
SELECT usc.cal_date
    , usc.country_alpha2
    , usc.platform_subset
    , shm.base_product_number
    , usc.customer_engagement
    , sum(hp_pages) as hp_pages
FROM usage_share_country08 usc
JOIN shm_12_map_geo_6 shm
    ON usc.country_alpha2 = shm.country_alpha2
    AND usc.platform_subset = shm.platform_subset
    AND usc.customer_engagement = shm.customer_engagement
GROUP BY usc.cal_date
    , usc.country_alpha2
    , usc.platform_subset
    , shm.base_product_number
    , usc.customer_engagement
"""

usage_share_baseprod_01 = spark.sql(usage_share_baseprod_01)
usage_share_baseprod_01.createOrReplaceTempView("usage_share_baseprod_01")


# calc mix of printers attached to a particular base product in a particular month etc.
usage_share_baseprod_02 = f"""
SELECT cal_date
    , country_alpha2
    , platform_subset
    , base_product_number
    , customer_engagement
    , sum(hp_pages) as hp_pages
    , CASE
        WHEN SUM(hp_pages) OVER (PARTITION BY cal_date, country_alpha2, customer_engagement, base_product_number) = 0 THEN NULL
        ELSE hp_pages / SUM(hp_pages) OVER (PARTITION BY cal_date, country_alpha2, customer_engagement, base_product_number)
    END AS page_mix
FROM usage_share_baseprod_01
GROUP BY cal_date
    , country_alpha2
    , platform_subset
    , base_product_number
    , customer_engagement
    , hp_pages
"""

usage_share_baseprod_02 = spark.sql(usage_share_baseprod_02)
usage_share_baseprod_02.createOrReplaceTempView("usage_share_baseprod_02")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from usage_share_baseprod_02
# MAGIC where customer_engagement = 'EST_INDIRECT_FULFILLMENT'
# MAGIC limit 5;
