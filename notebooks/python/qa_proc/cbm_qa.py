# Databricks notebook source
import re

# COMMAND ----------

# MAGIC %run "../common/configs"

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

# load df from redshift
calendar = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.calendar") \
    .load()
iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load()
product_line_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.product_line_xref") \
    .load()

# COMMAND ----------

tables = [
  ['fin_stage.cbm_st_data', cbm_st_data],
  ['mdm.iso_country_code_xref', iso_country_code_xref],
  ['mdm.calendar', calendar],
  ['mdm.product_line_xref', product_line_xref]
]

write_df_to_delta(tables, True)

# COMMAND ----------

cbm_database = """
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

cbm_database = spark.sql(cbm_database)
cbm_database.createOrReplaceTempView("cbm_database")

# COMMAND ----------

cbm_database.count()

# COMMAND ----------

cbm_test_01_pl = """
SELECT distinct pl, count(*) as row_count
FROM cbm_database
WHERE 1=1
GROUP BY pl
ORDER BY 1;
"""

cbm_test_01_pl = spark.sql(cbm_test_01_pl)
cbm_test_01_pl.createOrReplaceTempView("cbm_test_01_pl")

# COMMAND ----------

cbm_test_01_pl.show()

# COMMAND ----------

cbm_test_02_ci = """
SELECT 'CBM_TEST_02_CI' AS record
  , cal_date
  , fiscal_year_qtr
  , fiscal_yr
  , pl
  , market8
  , market10
  , region_5
  , sum(channel_inventory_usd) as channel_inventory_usd
  , sum(channel_inventory_qty) as channel_inventory_qty
  , current_timestamp() as load_date
FROM cbm_database cbm
LEFT JOIN mdm.calendar c
  ON c.Date = cbm.cal_date
LEFT JOIN mdm.iso_country_code_xref iso
  ON iso.country_alpha2 = cbm.country_alpha2
WHERE 1=1
AND day_of_month = 1
AND month_abbrv IN ('JAN', 'APR', 'JUL', 'OCT')
AND pl IN (SELECT pl FROM mdm.product_line_xref WHERE pl_category = 'SUP' AND technology IN ('PWA', 'INK', 'LASER', 'LF'))
GROUP BY cal_date
  , fiscal_year_qtr
  , fiscal_yr
  , pl
  , market8
  , market10
  , region_5
"""

cbm_test_02_ci = spark.sql(cbm_test_02_ci)
cbm_test_02_ci.createOrReplaceTempView("cbm_test_02_ci")

# COMMAND ----------

cbm_test_02_ci.show()
