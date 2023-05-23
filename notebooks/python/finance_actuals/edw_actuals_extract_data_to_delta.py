# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

dbutils.widgets.text('extract_data_to_delta', '')

# COMMAND ----------

if dbutils.widgets.get('extract_data_to_delta').lower().strip() != 'true':
    dbutils.notebook.exit(f"EXIT: extract_data_to_delta parameter is not set to 'true'")

# COMMAND ----------

#load cbm data from sfai
cbm_st_data = read_sql_server_to_df(configs) \
    .option("dbtable", "CBM.dbo.cbm_st_data") \
    .load()

# COMMAND ----------

# # load parquet files to df
edw_fin_s3_bucket= f"s3://dataos-core-{stack}-team-phoenix-fin/"
edw_ships_s3_bucket= f"s3://dataos-core-{stack}-team-phoenix/product/"

edw_revenue_units_sales_landing = spark.read.parquet(edw_fin_s3_bucket + "EDW/edw_revenue_units_sales_landing")
edw_revenue_document_currency_landing = spark.read.parquet(edw_fin_s3_bucket + "EDW/edw_revenue_document_currency_landing")
edw_revenue_dollars_landing = spark.read.parquet(edw_fin_s3_bucket + "EDW/edw_revenue_dollars_landing")
edw_shipment_actuals_landing = spark.read.parquet(edw_ships_s3_bucket + "EDW/edw_shipment_actuals_landing")

# COMMAND ----------

# load redshift data to df
mps_ww_shipped_supply = read_redshift_to_df(configs) \
    .option("dbtable", "prod.mps_ww_shipped_supply") \
    .load()
iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load()
calendar = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.calendar") \
    .load()
profit_center_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.profit_center_code_xref") \
    .load()
product_line_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.product_line_xref") \
    .load()
itp_laser_landing = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.itp_laser_landing") \
    .load()
supplies_iink_units_landing = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.supplies_iink_units_landing") \
    .load()
supplies_manual_mcode_jv_detail_landing = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.supplies_manual_mcode_jv_detail_landing") \
    .load()
country_currency_map_landing = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.country_currency_map") \
    .load()
list_price_eu_country_list = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.list_price_eu_country_list") \
    .load()
exclusion = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.exclusion") \
    .load()
rdma_base_to_sales_product_map = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.rdma_base_to_sales_product_map") \
    .load()
supplies_hw_mapping = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.supplies_hw_mapping") \
    .load()
ib = read_redshift_to_df(configs) \
    .option("dbtable", "prod.ib") \
    .load()
actuals_supplies_salesprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_supplies_salesprod") \
    .load()
planet_actuals = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.planet_actuals") \
    .load()
supplies_finance_hier_restatements_2020_2021 = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.supplies_finance_hier_restatements_2020_2021") \
    .load()
supplies_hw_country_actuals_mapping = read_redshift_to_df(configs) \
    .option("dbtable", "stage.supplies_hw_country_actuals_mapping") \
    .load()

# COMMAND ----------

tables = [
    ['fin_stage.edw_revenue_units_sales_staging', edw_revenue_units_sales_landing],
    ['fin_stage.edw_revenue_document_currency_staging', edw_revenue_document_currency_landing],
    ['fin_stage.edw_revenue_dollars_staging', edw_revenue_dollars_landing],
    ['fin_stage.edw_shipment_actuals_staging', edw_shipment_actuals_landing],
    ['fin_stage.mps_ww_shipped_supply_staging', mps_ww_shipped_supply],
    ['mdm.iso_country_code_xref', iso_country_code_xref],
    ['mdm.profit_center_code_xref', profit_center_code_xref],
    ['fin_stage.itp_laser_landing', itp_laser_landing],
    ['fin_stage.supplies_iink_units_landing', supplies_iink_units_landing],
    ['mdm.calendar', calendar],
    ['mdm.product_line_xref', product_line_xref],
    ['fin_stage.supplies_manual_mcode_jv_detail_landing', supplies_manual_mcode_jv_detail_landing],
    ['fin_stage.country_currency_map_landing', country_currency_map_landing],
    ['mdm.list_price_eu_country_list', list_price_eu_country_list],
    ['fin_stage.cbm_st_data', cbm_st_data],
    ['mdm.exclusion', exclusion],
    ['mdm.rdma_base_to_sales_product_map', rdma_base_to_sales_product_map],
    ['mdm.supplies_hw_mapping', supplies_hw_mapping],
    ['stage.ib', ib],
    ['fin_stage.planet_actuals', planet_actuals],
    ['fin_stage.supplies_finance_hier_restatements_2020_2021', supplies_finance_hier_restatements_2020_2021],
    ['stage.supplies_hw_country_actuals_mapping', supplies_hw_country_actuals_mapping],
    ['fin_prod.actuals_supplies_salesprod', actuals_supplies_salesprod]
]

# COMMAND ----------

# delta tables
write_df_to_delta(tables=tables, rename_cols=True, threads=4)
