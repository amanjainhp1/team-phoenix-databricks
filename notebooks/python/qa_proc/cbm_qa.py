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
#driver_check_ci_balances = read_redshift_to_df(configs) \
#    .option("dbtable", "scen.driver_check_ci_balances") \
#    .load()    

# COMMAND ----------

# update delta tables
tables = [
  ['fin_stage.cbm_st_data', cbm_st_data],
  ['mdm.iso_country_code_xref', iso_country_code_xref],
  ['mdm.calendar', calendar],
  ['mdm.product_line_xref', product_line_xref],
  ['scen.driver_check_ci_balances', driver_check_ci_balances]
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

#cbm_01_pl_list.show()

# COMMAND ----------

cbm_01_pl_list.count()

# COMMAND ----------

phoenix_01_pl_list = """
SELECT distinct pl
FROM mdm.product_line_xref
WHERE 1=1
    AND pl_category IN ('SUP', 'LLC')
    AND technology IN ('INK', 'LASER', 'PWA', 'LLCS', 'LF')
ORDER BY 1;
"""

phoenix_01_pl_list = spark.sql(phoenix_01_pl_list)
phoenix_01_pl_list.createOrReplaceTempView("phoenix_01_pl_list")

# COMMAND ----------

#phoenix_01_pl_list.show()

# COMMAND ----------

phoenix_01_pl_list.count()

# COMMAND ----------

cbm_v_phoenix_pl = """
SELECT p.pl as phoenix_pl
    , cbm.pl as cbm_pl
FROM phoenix_01_pl_list p
LEFT JOIN cbm_01_pl_list cbm
    ON p.pl = cbm.pl
WHERE cbm.pl is null
ORDER BY p.pl
"""

cbm_v_phoenix_pl = spark.sql(cbm_v_phoenix_pl)
cbm_v_phoenix_pl.createOrReplaceTempView("cbm_v_phoenix_pl")

# COMMAND ----------

cbm_v_phoenix_pl.show()

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
  , (SELECT MAX(version) FROM prod.version WHERE record = 'ACTUALS - ADJUSTED_REVENUE - SALES PRODUCT') AS adj_rev_sp_version
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

#DELETE ME AFTER INITIAL TABLE SET UP
#write_df_to_redshift(configs, cbm_02_current_ci, "scen.driver_check_ci_balances", "append")

# COMMAND ----------

#grant team access
query_access_grant = """
GRANT ALL ON TABLE scen.driver_check_ci_balances TO GROUP auto_glue;
"""

submit_remote_query(configs, query_access_grant)

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
        FROM driver_check_ci_balances cbm
        WHERE 1=1
        AND load_date = (SELECT MAX(load_date) from driver_check_ci_balances)
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

#write_df_to_redshift(configs, cbm_02_current_ci, "scen.driver_check_ci_balances", "append")

# COMMAND ----------

ci_balances_current_prep = driver_check_ci_balances.toPandas()
ci_agg = ci_balances_current_prep.reindex(['fiscal_year_qtr', 'variable', 'channel_inventory_usd'], axis=1)

# COMMAND ----------

# https://plotly.com/python-api-reference/generated/plotly.express.line

fig = px.line(data_frame=ci_agg,
              x='cal_date',
              y='channel_inventory_usd',
              line_group='variable',
              color='variable',
              title='CBM CI$ v2v compare')

fig.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all")
        ])
    )
)

fig.update_layout(
    autosize=False,
    width=1400,
    height=500,
    margin=dict(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4
    ),
)

fig.show()
