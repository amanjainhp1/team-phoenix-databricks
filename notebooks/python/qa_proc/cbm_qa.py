# Databricks notebook source
#import libraries
import re
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# COMMAND ----------

#Global Variables
query_list = []

# COMMAND ----------

# MAGIC %run "../common/configs"

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

#load cbm data from sfai into DF
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

# update delta tables
tables = [
  ['fin_stage.cbm_st_data', cbm_st_data],
  ['mdm.iso_country_code_xref', iso_country_code_xref],
  ['mdm.calendar', calendar],
  ['mdm.product_line_xref', product_line_xref]
]

write_df_to_delta(tables, True)

# COMMAND ----------

#select and format cbm supplies data 
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

# list supplies pls in cbm
cbm_01_pl_list = """
SELECT distinct pl, count(*) as row_count
FROM cbm_database
WHERE 1=1
GROUP BY pl
ORDER BY 1;
"""

cbm_01_pl_list = spark.sql(cbm_01_pl_list)
cbm_01_pl_list.createOrReplaceTempView("cbm_01_pl_list")

# COMMAND ----------

cbm_01_pl_list.show()

# COMMAND ----------

cbm_02_current_ci = """
SELECT 'CBM_02_CURRENT_CI' AS record
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

cbm_02_current_ci = spark.sql(cbm_02_current_ci)
cbm_02_current_ci.createOrReplaceTempView("cbm_02_current_ci")

# COMMAND ----------

cbm_02_current_ci.show()

# COMMAND ----------

# This test compares cbm channel inventory extracts to previous channel inventory balances and identifies where the CI has changed by 5% or more by fiscal_quarter x product line x market
cbm_02_ci_coc = """
SELECT *
FROM
(
    SELECT previous_cbm.previous_fiscal_year_qtr as fiscal_year_qtr
        , previous_cbm.previous_pl as pl
        , previous_cbm.previous_market8 as market8
        , (current_cbm.current_ci_usd - previous_cbm.previous_ci_usd) *1.0 / NULLIF(previous_cbm.previous_ci_usd, 0) as ci_usd_rate_change
        , (current_cbm.current_ci_qty - previous_cbm.previous_ci_qty) *1.0 / NULLIF(previous_cbm.previous_ci_qty, 0) as ci_qty_rate_change
    FROM
        (SELECT cbm.fiscal_year_qtr as previous_fiscal_year_qtr
           , cbm.pl as previous_pl
           , cbm.market8 as previous_market8
           , sum(cbm.channel_inventory_usd) as previous_ci_usd
           , sum(cbm.channel_inventory_qty) as previous_ci_qty
           , cbm.load_date as previous_load_date
        FROM fin_stage.cbm_test_02_ci_summary cbm
        WHERE 1=1
        AND load_date = (SELECT MAX(load_date) from fin_stage.cbm_test_02_ci_summary)
        GROUP BY cbm.fiscal_year_qtr
            , cbm.pl
            , cbm.market8
            , cbm.load_date
        ) as previous_cbm
        JOIN
        (
        SELECT new.fiscal_year_qtr as current_fiscal_year_qtr
            , new.pl as current_pl
            , new.market8 as current_market8
            , sum(new.channel_inventory_usd) as current_ci_usd
            , sum(new.channel_inventory_qty) as current_ci_qty
            , new.load_date as current_load_date
        FROM cbm_02_current_ci new
        WHERE 1=1
        GROUP BY new.fiscal_year_qtr
            , new.pl
            , new.market8
            , new.load_date
        ) as current_cbm
        ON UPPER(current_cbm.current_fiscal_year_qtr) = UPPER(previous_cbm.previous_fiscal_year_qtr)
        AND UPPER(current_cbm.current_pl) = UPPER(previous_cbm.previous_pl)
        AND UPPER(current_cbm.current_market8) = UPPER(previous_cbm.previous_market8)
) as test
WHERE ROUND (ABS(test.ci_usd_rate_change), 4) > 0.05
"""

cbm_02_ci_coc = spark.sql(cbm_02_ci_coc)
cbm_02_ci_coc.createOrReplaceTempView("cbm_02_ci_coc")

# COMMAND ----------

#print(cbm_02_ci_coc)

# COMMAND ----------

cbm_02_ci_coc.show()

# COMMAND ----------

# add latest cbm extract to delta tables
tables = [
  ['fin_stage.cbm_test_02_ci_summary', cbm_02_current_ci]
]

for table in tables:
        # Define the input and output formats and paths and the table name.
        schema_name = table[0].split(".")[0]
        table_name = table[0].split(".")[1]
        write_format = 'delta'
        save_path = f'/tmp/delta/{schema_name}/{table_name}'
        
        # Load the data from its source.
        df = table[1]
        print(f'loading {table[0]}...')

        # Write the data to its target.
        df.write \
            .format(write_format) \
            .mode("append") \
            .option('overwriteSchema', 'true') \
            .save(save_path)

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        
        # Create the table.
        spark.sql("CREATE TABLE IF NOT EXISTS " + table[0] + " USING DELTA LOCATION '" + save_path + "'")
        
        spark.table(table[0]).createOrReplaceTempView(table_name)
        
        print(f'{table[0]} loaded')

# COMMAND ----------

# delete Delta tables and underlying Delta files
tables = [
  #  ['stage.planet_actuals', planet_actuals],
  #  ['stage.supplies_finance_hier_restatements_2020_2021', supplies_finance_hier_restatements_2020_2021]
]

for table in tables:
    schema = table[0].split(".")[0]
    table_name = table[0].split(".")[1]
    save_path = f'/tmp/delta/{schema}/{table_name}'
    
    spark.sql(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    dbutils.fs.rm(save_path, True)
