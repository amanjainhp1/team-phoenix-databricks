# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Create Delta Lake Tables from Redshift

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Call Libraries

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ./database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Table Instantiation

# COMMAND ----------

#----------- cbm ------------
cbm_st_data = read_sql_server_to_df(configs) \
    .option("dbtable", "CBM.dbo.cbm_st_data") \
    .load()

#--------- fin_prod ---------
actuals_supplies_salesprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_supplies_salesprod") \
    .load()

odw_document_currency = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.odw_document_currency") \
    .load()

#----------- mdm ------------
calendar = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.calendar") \
    .load()

country_currency_map = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.country_currency_map") \
    .load()

iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load()

list_price_eu_countrylist = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.list_price_eu_countrylist") \
    .load()

product_line_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.product_line_xref") \
    .load()

profit_center_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.profit_center_code_xref") \
    .load()

rdma_base_to_sales_product_map = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.rdma_base_to_sales_product_map") \
    .load()

#---------- prod ------------
acct_rates = read_redshift_to_df(configs) \
    .option("dbtable", "prod.acct_rates") \
    .load()

version = read_redshift_to_df(configs) \
    .option("dbtable", "prod.version") \
    .load()

#----------- parquet ------------
edw_revenue_document_currency_landing = spark.read.parquet("s3://dataos-core-itg-team-phoenix-fin/landing/EDW/edw_revenue_document_currency_landing")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Tables

# COMMAND ----------




tables = [
    ['mdm.calendar', calendar]
    ['CBM.dbo.cbm_st_data', cbm_st_data],
    ['fin_prod.actuals_supplies_salesprod', actuals_supplies_salesprod],
    ['fin_prod.odw_document_currency', odw_document_currency]
    ['fin_stage.edw_revenue_document_currency_landing', edw_revenue_document_currency_landing],
    ['mdm.country_currency_map', country_currency_map],
    ['mdm.iso_country_code_xref', iso_country_code_xref],
    ['mdm.list_price_eu_countrylist', list_price_eu_countrylist],
    ['mdm.product_line_xref', product_line_xref],
    ['mdm.profit_center_code_xref', profit_center_code_xref]
    ['mdm.rdma_base_to_sales_product_map', rdma_base_to_sales_product_map],
    ['prod.acct_rates', acct_rates]
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
