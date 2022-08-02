# Databricks notebook source
import re

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

configs["source_system"] =  dbutils.widgets.get("source_system")
configs["source_database"] = dbutils.widgets.get("source_database")
configs["source_schema"] = dbutils.widgets.get("source_schema")
configs["source_table"] = dbutils.widgets.get("source_table")

configs["destination_system"] = dbutils.widgets.get("destination_system")
configs["destination_database"] = dbutils.widgets.get("destination_database")
configs["destination_schema"] = dbutils.widgets.get("destination_schema")
configs["destination_table"] = dbutils.widgets.get("destination_table")

configs["datestamp"] = dbutils.widgets.get("datestamp")
configs["timestamp"] = dbutils.widgets.get("timestamp")

configs["load_large_tables"] = dbutils.widgets.get("load_large_tables") # for controlling loading of large tables (e.g. 11 GB)

# COMMAND ----------

# set source and destination table vars
source = "{}.{}".format(configs["source_schema"], configs["source_table"])
if configs["source_system"] == "sqlserver":
    source = configs["source_database"] + "." + source

destination = "{}.{}".format(configs["destination_schema"], configs["destination_table"])
if configs["destination_system"] == "sqlserver":
    destination = configs["destination_database"] + destination

# COMMAND ----------

# retrieve data from source system
if configs["source_system"] == "redshift":
    table_df = read_redshift_to_df(configs) \
        .option("dbTable", source) \
        .load()
else:
    table_df = read_sql_server_to_df(configs) \
        .option("dbTable", source) \
        .load()

table_df.createOrReplaceTempView("table_df")

# COMMAND ----------

# rearrange incoming dataframes to destination table schema

input_table_cols = [c.lower() for c in table_df.columns]

if configs["destination_system"] == "redshift":
    output_table_cols = read_redshift_to_df(configs) \
        .option("dbtable", destination) \
        .load() \
        .columns
else:
    output_table_cols = read_sql_server_to_df(configs) \
        .option("dbtable", destination) \
        .load() \
        .columns

query = "SELECT \n"

for col in output_table_cols:
        if "_id" not in col: # omit if identity column
            if not(col == "id" and configs["destination_table"] == "hardware_xref"):
                if input_table_cols.__contains__(col):
                    query += col
                else: # special column conditions
                    if re.sub('[\\^_]', '-', col) in input_table_cols:
                        query += '`' + re.sub('[\\^_]', '-', col) + '`' + ' AS ' + col
                    if re.sub('[\\^_]', ' ', col) in input_table_cols:
                        query += '`' + re.sub('[\\^_]', ' ', col) + '`' + ' AS ' + col
                    if col == "official":
                        query += "1 AS official"
                    if col == "last_modified_date":
                        query += "load_date AS last_modified_date"
                    if col == "profit_center_code" and configs["destination_table"] == "product_line_xref":
                        query += "profit_center AS profit_center_code"
                    if col == "load_date":
                        query += """CAST("{}" AS TIMESTAMP) AS load_date""".format(configs["timestamp"])
                    if col == "record" and configs["destination_table"] == "instant_ink_enrollees_ltf":
                        query += "'iink_enrollees_ltf' AS record"
                    if col == 'eoq_discount_pct':
                        query += "`EOQ Discount %` AS eoq_discount_pct"
                    # forecast_fixed_cost_input
                    if col == 'fixed_cost_desc':
                        query += "FixedCost_Desc AS fixed_cost_desc"
                    if col == 'fixed_cost_k_qtr':
                        query += "FixedCost_K_Qtr AS fixed_cost_k_qtr"
                    # forecast_supplies_baseprod_region & forecast_supplies_base_prod_region_stf 
                    if col == 'base_prod_gru':
                        query += 'BaseProd_GRU AS base_prod_gru'
                    if col == 'base_prod_contra_per_unit':
                        query += 'BaseProd_Contra_perUnit AS base_prod_contra_per_unit'
                    if col == 'base_prod_revenue_currency_hedge_unit':
                        query += 'BaseProd_RevenueCurrencyHedge_Unit AS base_prod_revenue_currency_hedge_unit'
                    if col == 'base_prod_aru':
                        query += 'BaseProd_ARU AS base_prod_aru'
                    if col == 'base_prod_variable_cost_per_unit':
                        query += 'BaseProd_VariableCost_perUnit AS base_prod_variable_cost_per_unit'
                    if col == 'base_prod_contribution_margin_unit':
                        query += 'BaseProd_ContributionMargin_Unit AS base_prod_contribution_margin_unit'
                    if col == 'base_prod_fixed_cost_per_unit':
                        query += 'BaseProd_FixedCost_perUnit AS base_prod_fixed_cost_per_unit'
                    if col == 'base_prod_gross_margin_unit':
                        query += 'BaseProd_GrossMargin_Unit AS base_prod_gross_margin_unit'
                    # current_stf_dollarization & stf_dollarization
                    if col == 'gross_revenue':
                        query += 'GrossRevenue AS gross_revenue'
                    if col == 'net_revenue':
                        query += 'NetRevenue AS net_revenue'

                if col in output_table_cols[0:len(output_table_cols)-1]:
                    query = query + ","
                
                query += "\n"

final_table_df = spark.sql(query + "FROM table_df")

# COMMAND ----------

# for large tables, we need to filter records and in order to retrieve many smaller datasets rather than one large dataset
large_tables = ['working_forecast_country', 'working_forecast', 'forecast_supplies_baseprod', 'forecast_supplies_base_prod_region', 'adjusted_revenue', 'trade_forecast']

if configs["destination_table"] in large_tables and load_large_tables.lower() == 'true':
    filtervals = []
    filtercol = ''
    if configs["destination_table"] == 'working_forecast_country':
        filtercol = 'country' 
    elif configs["destination_table"] == 'working_forecast':
        filtercol= 'geography'
    elif configs['destination_table'] == 'forecast_supples_base_prod_region5':
        filtercol = 'region_5'
    elif configs['destination_table'] == 'forecast_supplies_baseprod' or configs['destination_table' == 'adjusted_revenue']:
        filtercol = 'country_alpha2'
    elif configs['destination_table'] == 'trade_forecast':
        filtercol = 'region_5'

    filtervals = read_sql_server_to_df(configs) \
        .option('query', f'SELECT DISTINCT {filtercol} from {source}') \
        .load() \
        .select(f'{filtercol}') \
        .rdd.flatMap(lambda x: x).collect()
    filtervals.sort()
    
    for filterval in filtervals:
        print(filterval + " data loaded")
        write_df_to_redshift(configs, final_table_df.filter(f"{filtercol} = '{filterval}'"), destination, "append")

# COMMAND ----------

# load all other tables
if configs["destination_table"] not in large_tables:
    # write data to S3
    write_df_to_s3(final_table_df, "{}{}/{}/{}/".format(constants['S3_BASE_BUCKET'][stack], configs["destination_table"], configs["datestamp"], configs["timestamp"]), "csv", "overwrite")

    # truncate existing redshift data and write data to redshift
    write_df_to_redshift(configs=configs, df=final_table_df, destination=destination, mode="append", preactions="TRUNCATE " + destination)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
