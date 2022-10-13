# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

forecast_gru_override = """
SELECT *
FROM ie2_financials.dbo.forecast_gru_override
"""

forecast_gru_override = read_sql_server_to_df(configs) \
    .option("query", forecast_gru_override) \
    .load()

write_df_to_redshift(configs, forecast_gru_override, "fin_prod.forecast_gru_override", "overwrite")

# COMMAND ----------

forecast_fixed_cost_input = """
SELECT record
    , fixedcost_desc as fixed_cost_desc
    , pl
    , region_3
    , country_code
    , fiscal_yr_qtr
    , fixedcost_k_qtr as fixed_cost_k_qtr
    , official
    , version
    , load_date
FROM ie2_financials.dbo.forecast_fixedcost_input
"""

forecast_fixed_cost_input = read_sql_server_to_df(configs) \
    .option("query", forecast_fixed_cost_input) \
    .load()

write_df_to_redshift(configs, forecast_fixed_cost_input, "fin_prod.forecast_fixed_cost_input", "overwrite")

# COMMAND ----------

forecast_variablecost_ink = """
SELECT *
FROM ie2_financials.dbo.forecast_variablecost_ink
"""

f_report_units = read_sql_server_to_df(configs) \
    .option("query", forecast_variablecost_ink) \
    .load()

write_df_to_redshift(configs, f_report_units, "fin_prod.forecast_variable_cost_ink", "overwrite")

# COMMAND ----------

currency_hedge = """
SELECT *
FROM ie2_prod.dbo.currency_hedge
"""

currency_hedge = read_sql_server_to_df(configs) \
    .option("query", currency_hedge) \
    .load()

write_df_to_redshift(configs, currency_hedge, "prod.currency_hedge", "overwrite")

# COMMAND ----------

forecast_sales_gru = """
SELECT "record"
        , build_type
        , sales_product_number
        , region_5
        , country_alpha2
        , price_term_code
        , price_start_effective_date
        , qb_sequence_number
        , list_price
        , sales_product_line_code
        , accountingrate as "accounting_rate"
        , "list_price_usd" as list_price_usd
      ,"ListPriceAdder_LC" as list_price_adder_lc
      ,"currencycode_adder" as "currency_code_adder"
      , "listpriceadder_usd" as "list_price_adder_usd"
      ,"eoq_discount"
      ,"salesproduct_gru" as "sales_product_gru"
      ,"load_date"
      ,"version"
FROM ie2_financials.dbo.forecast_sales_gru
"""

forecast_sales_gru = read_sql_server_to_df(configs) \
    .option("query", forecast_sales_gru) \
    .load()

write_df_to_redshift(configs, forecast_sales_gru, "fin_prod.forecast_sales_gru", "overwrite")

# COMMAND ----------

rdma = """
SELECT *
FROM ie2_prod.dbo.rdma
"""

rdma = read_sql_server_to_df(configs) \
    .option("query", rdma) \
    .load()

write_df_to_redshift(configs, rdma, "mdm.rdma", "overwrite")

# COMMAND ----------

working_forecast_country = """
SELECT *
FROM ie2_prod.dbo.working_forecast_country
where version = '2022.10.06.1'
"""

f_report_units = read_sql_server_to_df(configs) \
    .option("query", working_forecast_country) \
    .load()

write_df_to_redshift(configs, f_report_units, "stage.working_forecast_country_temp", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC for list_price

# COMMAND ----------

rdma_base_to_sales_product_map = """
SELECT *
FROM ie2_prod.dbo.rdma_base_to_sales_product_map
"""

rdma_base_to_sales_product_map = read_sql_server_to_df(configs) \
    .option("query", rdma_base_to_sales_product_map) \
    .load()

write_df_to_redshift(configs, rdma_base_to_sales_product_map, "mdm.rdma_base_to_sales_product_map", "overwrite")
