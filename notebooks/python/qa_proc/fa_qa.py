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

# load df from redshift
#driver_check_fa_balances = read_redshift_to_df(configs) \
  #  .option("dbtable", "scen.driver_check_ci_balances") \
  #  .load()

# COMMAND ----------

# update delta tables
tables = [
  #['fin_stage.cbm_st_data', cbm_st_data], # with cbm_qa at end of adjusted revenue salesprod job, this will pick up the cbm_st_data used for that adjusted revenue salesprod run, as intended
  #['mdm.iso_country_code_xref', iso_country_code_xref],
  #['mdm.calendar', calendar],
  #['mdm.product_line_xref', product_line_xref],
  #['scen.driver_check_fa_balances', driver_check_fa_balances]
]

write_df_to_delta(tables, True)

# COMMAND ----------

#select edw data 
edw_data = """
SELECT 
  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
  , SUM(gross_revenue) + SUM(net_currency) + SUM(contractual_discounts) + SUM(discretionary_discounts) AS net_revenue
  , SUM(revenue_units) as revenue_units  
FROM fin_stage.final_union_edw_data edw
LEFT JOIN mdm.calendar c
  ON edw.cal_date = c.Date
WHERE 1=1
AND day_of_month = 1
AND cal_date > '2016-10-01'
GROUP BY  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
"""

edw_data = spark.sql(edw_data)
edw_data.createOrReplaceTempView("edw_data")

# COMMAND ----------

edw_data.count()

# COMMAND ----------

#select odw data 
odw_data = """
SELECT 
  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
  , SUM(gross_revenue) + SUM(net_currency) + SUM(contractual_discounts) + SUM(discretionary_discounts) AS net_revenue
  , SUM(revenue_units) as revenue_units  
FROM fin_stage.final_union_odw_data odw
LEFT JOIN mdm.calendar c
  ON odw.cal_date = c.Date
WHERE 1=1
AND day_of_month = 1
GROUP BY  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
"""

odw_data = spark.sql(odw_data)
odw_data.createOrReplaceTempView("odw_data")

# COMMAND ----------

odw_data.count()

# COMMAND ----------

# union edw with odw data
fa_input_data = """
SELECT 
  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
  , net_revenue
  , revenue_units  
FROM edw_data
WHERE 1=1

UNION ALL

SELECT 
  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
  , net_revenue
  , revenue_units  
FROM odw_data
WHERE 1=1
"""

fa_input_data = spark.sql(fa_input_data)
fa_input_data.createOrReplaceTempView("fa_input_data")


fa_input_data2 = """
SELECT 
  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
  , COALESCE(net_revenue, 0) as net_revenue
  , COALESCE(revenue_units, 0) as revenue_units  
FROM fa_input_data
"""

fa_input_data2 = spark.sql(fa_input_data2)
fa_input_data2.createOrReplaceTempView("fa_input_data2")


# COMMAND ----------

fa_input_data.count()

# COMMAND ----------

#select edw processed data prior to planet
edw_pre_planet_data = """
SELECT 
  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
  , SUM(gross_revenue) + SUM(net_currency) + SUM(contractual_discounts) + SUM(discretionary_discounts) AS net_revenue
  , SUM(revenue_units) as revenue_units  
FROM fin_stage.xcode_adjusted_data edw
LEFT JOIN mdm.calendar c
  ON edw.cal_date = c.Date
WHERE 1=1
AND day_of_month = 1
AND cal_date > '2016-10-01'
GROUP BY  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
"""

edw_pre_planet_data = spark.sql(edw_pre_planet_data)
edw_pre_planet_data.createOrReplaceTempView("edw_pre_planet_data")

# COMMAND ----------

#select odw processed data prior to sacp
odw_pre_sacp_data = """
SELECT 
  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
  , SUM(gross_revenue) + SUM(net_currency) + SUM(contractual_discounts) + SUM(discretionary_discounts) AS net_revenue
  , SUM(revenue_units) as revenue_units  
FROM fin_stage.odw_xcode_adjusted_data odw
LEFT JOIN mdm.calendar c
  ON odw.cal_date = c.Date
WHERE 1=1
AND day_of_month = 1
AND cal_date > '2016-10-01'
GROUP BY  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
"""

odw_pre_sacp_data = spark.sql(odw_pre_sacp_data)
odw_pre_sacp_data.createOrReplaceTempView("odw_pre_sacp_data")

# COMMAND ----------

# union edw with odw data
fa_output_data_pre_planet = """
SELECT 
  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
  , net_revenue
  , revenue_units  
FROM edw_pre_planet_data
WHERE 1=1

UNION ALL

SELECT 
  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
  , net_revenue
  , revenue_units  
FROM odw_pre_sacp_data
WHERE 1=1
"""

fa_output_data_pre_planet = spark.sql(fa_output_data_pre_planet)
fa_output_data_pre_planet.createOrReplaceTempView("fa_output_data_pre_planet")


fa_output_data_pre_planet2 = """
SELECT 
  cal_date
  , fiscal_year_qtr
  , pl
  , country_alpha2
  , COALESCE(net_revenue, 0) as net_revenue
  , COALESCE(revenue_units, 0) as revenue_units  
FROM fa_output_data_pre_planet
"""

fa_output_data_pre_planet2 = spark.sql(fa_output_data_pre_planet2)
fa_output_data_pre_planet2.createOrReplaceTempView("fa_output_data_pre_planet2")

# COMMAND ----------

#print(cbm_02_ci_coc)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fiscal_year_qtr
# MAGIC   , pl
# MAGIC   , region_5
# MAGIC   , SUM(net_revenue) as net_revenue
# MAGIC   , SUM(revenue_units) as revenue_units
# MAGIC   , 'fa_input' as step
# MAGIC FROM fa_input_data2 fa
# MAGIC LEFT JOIN mdm.iso_country_code_xref iso
# MAGIC   ON iso.country_alpha2 = fa.country_alpha2
# MAGIC WHERE 1=1
# MAGIC AND pl IN (SELECT pl FROM mdm.product_line_xref WHERE pl_category = 'SUP' AND technology = 'LASER' AND pl NOT IN ('LZ', 'GY'))
# MAGIC AND fiscal_year_qtr = '2022Q4'
# MAGIC GROUP BY fiscal_year_qtr
# MAGIC   , pl
# MAGIC   , region_5

# COMMAND ----------

odw_pre_planet_data.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fiscal_year_qtr
# MAGIC   , pl
# MAGIC   , region_5
# MAGIC   , SUM(net_revenue) as net_revenue
# MAGIC   , SUM(revenue_units) as revenue_units
# MAGIC   , 'fa_output_pre_planet' as step
# MAGIC FROM fa_output_data_pre_planet2 fa
# MAGIC LEFT JOIN mdm.iso_country_code_xref iso
# MAGIC   ON iso.country_alpha2 = fa.country_alpha2
# MAGIC WHERE 1=1
# MAGIC AND pl IN (SELECT pl FROM mdm.product_line_xref WHERE pl_category = 'SUP' AND technology = 'LASER'AND pl NOT IN ('LZ', 'GY'))
# MAGIC AND fiscal_year_qtr = '2022Q4'
# MAGIC GROUP BY fiscal_year_qtr
# MAGIC   , pl
# MAGIC   , region_5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fiscal_year_qtr
# MAGIC   , pl
# MAGIC   , region_5
# MAGIC   , SUM(net_revenue) as net_revenue
# MAGIC   , SUM(revenue_units) as revenue_units
# MAGIC   , 'actuals_supplies_salesprod' as step
# MAGIC FROM fin_prod.actuals_supplies_salesprod fa
# MAGIC LEFT JOIN mdm.calendar c
# MAGIC   ON c.Date = fa.cal_date
# MAGIC LEFT JOIN mdm.iso_country_code_xref iso
# MAGIC   ON iso.country_alpha2 = fa.country_alpha2
# MAGIC WHERE 1=1
# MAGIC AND day_of_month = 1
# MAGIC AND pl IN (SELECT pl FROM mdm.product_line_xref WHERE pl_category = 'SUP' AND technology = 'LASER'AND pl NOT IN ('LZ', 'GY'))
# MAGIC AND fiscal_year_qtr = '2022Q4'
# MAGIC GROUP BY fiscal_year_qtr
# MAGIC   , pl
# MAGIC   , region_5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fiscal_year_qtr
# MAGIC   , pl
# MAGIC   , region_5
# MAGIC   , SUM(net_revenue) as net_revenue
# MAGIC   , SUM(revenue_units) as revenue_units
# MAGIC   , 'sales_to_base' as step
# MAGIC FROM fin_prod.actuals_supplies_baseprod fa
# MAGIC LEFT JOIN mdm.calendar c
# MAGIC   ON c.Date = fa.cal_date
# MAGIC LEFT JOIN mdm.iso_country_code_xref iso
# MAGIC   ON iso.country_alpha2 = fa.country_alpha2
# MAGIC WHERE 1=1
# MAGIC AND day_of_month = 1
# MAGIC AND fa.base_product_number NOT LIKE 'EDW%'
# MAGIC AND pl IN (SELECT pl FROM mdm.product_line_xref WHERE pl_category = 'SUP' AND technology = 'LASER'AND pl NOT IN ('LZ', 'GY'))
# MAGIC AND fiscal_year_qtr = '2022Q4'
# MAGIC GROUP BY fiscal_year_qtr
# MAGIC   , pl
# MAGIC   , region_5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fiscal_year_qtr
# MAGIC   , pl
# MAGIC   , region_5
# MAGIC   , SUM(net_revenue) as net_revenue
# MAGIC   , SUM(revenue_units) as revenue_units
# MAGIC   , 'actuals_supplies_baseprod' as step
# MAGIC FROM fin_prod.actuals_supplies_baseprod fa
# MAGIC LEFT JOIN mdm.calendar c
# MAGIC   ON c.Date = fa.cal_date
# MAGIC LEFT JOIN mdm.iso_country_code_xref iso
# MAGIC   ON iso.country_alpha2 = fa.country_alpha2
# MAGIC WHERE 1=1
# MAGIC AND day_of_month = 1
# MAGIC AND pl IN (SELECT pl FROM mdm.product_line_xref WHERE pl_category = 'SUP' AND technology = 'LASER'AND pl NOT IN ('LZ', 'GY'))
# MAGIC AND fiscal_year_qtr = '2022Q4'
# MAGIC GROUP BY fiscal_year_qtr
# MAGIC   , pl
# MAGIC   , region_5

# COMMAND ----------

#write_df_to_redshift(configs, cbm_02_current_ci, "scen.driver_check_fa_balances", "append")

# COMMAND ----------

# https://plotly.com/python-api-reference/generated/plotly.express.line

fig = px.line(data_frame=ci_agg,
              x='fiscal_year_qtr',
              y='channel_inventory_usd',
              line_group='version',
              color='version',
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
