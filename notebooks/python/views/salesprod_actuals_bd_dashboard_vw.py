# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import col, current_date, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType, DecimalType

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

query = """
CREATE OR REPLACE VIEW supplies_fcst.salesprod_actuals_bd_dashboard_vw AS 

WITH
             supplies_llc_data AS
             (
                    SELECT
                           Calendar_Yr_Mo AS yearmon,
                           pl,
                           sales_product_number AS salesProdNbr,
                           Region_3,
                           Region_4,
                           Country,
                           customer_engagement AS Route_to_market,
                           SUM(gross_revenue) AS gross_revenue,
                           SUM(net_currency) AS net_currency,
                           SUM(contractual_discounts) AS contractual_discounts,
                           SUM(discretionary_discounts) AS discretionary_discounts,
                           SUM(total_cos) AS total_cos,
                           SUM(revenue_units) AS revenue_units
                    FROM fin_prod.actuals_supplies_salesprod AS salesprod
                    JOIN mdm.calendar AS cal ON salesprod.cal_date = cal.Date
                    JOIN mdm.iso_country_code_xref geo ON geo.country_alpha2 = salesprod.country_alpha2
                    WHERE Day_of_Month = 1
                    -- exclude LF PL's for now (in development)
                    and pl NOT IN ('IE', 'IX', 'UK', 'TX')
                    GROUP BY
                           Calendar_Yr_Mo,
                           pl,
                           sales_product_number,
                           region_3,
                           region_4,
                           country,
                           customer_engagement
             ),
             media AS
             (
                    SELECT
                           yearmon,
                           pl,
                           salesProdNbr,
                           Region_3,
                           Region_4,
                           Country,
                           Route_to_market,
                           SUM(gross_revenue) AS gross_revenue,
                           SUM(net_currency) AS net_currency,
                           SUM(contractual_discounts) AS contractual_discounts,
                           SUM(discretionary_discounts) AS discretionary_discounts,
                           SUM(total_cos) AS total_cos,
                           SUM(revenue_units) AS revenue_units
                    FROM IE2_Landing.dbo.actuals_plau_bigdata_dashboard_landing
                    GROUP BY yearmon, pl, salesProdNbr, Region_3, Region_4, Country, Route_to_market
             ),
             supplies_llcs_media AS
             (
                    SELECT * FROM supplies_llc_data
                    UNION ALL
                    SELECT * FROM media
             )
             
                    SELECT
                           yearmon,
                           pl,
                           salesProdNbr,
                           Region_3,
                           Region_4,
                           Country,
                           Route_to_market,
                           SUM(gross_revenue) AS Gross_revenue,
                           SUM(net_currency) AS Net_currency,
                           SUM(contractual_discounts) AS Contractual_discounts,
                           SUM(discretionary_discounts) AS Discretionary_discounts,
                           SUM(total_cos) AS Total_COS,
                           SUM(revenue_units) AS Revenue_units,
                           CONVERT(Date, GETDATE(), 9) AS version, -- what to do about version if anything?
                           GETDATE() AS load_date
                    FROM supplies_llcs_media
                    GROUP BY yearmon, pl, salesProdNbr, Region_3, Region_4, Country, Route_to_market

      
GRANT ALL ON TABLE supplies_fcst.salesprod_actuals_bd_dashboard_vw TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query)

# COMMAND ----------

result = read_redshift_to_df(configs) \
    .option("query", f"select [year_mon] year_month, [pl], [sales_Prod_Nbr] sales_product_number, [region_3], [region_4], [country], [route_to_market], [gross_revenue], [net_currency], [contractual_discounts], [discretionary_discounts], [total_cos], [revenue_units] from financials.salesprod_actuals_bd_dashboard_vw") \
    .load()

# COMMAND ----------

result.display()

# COMMAND ----------


