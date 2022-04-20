# Databricks notebook source
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
                    query = query + col
                else: # special column conditions
                    if col == "official":
                        query = query + "1 AS official"
                    if col == "last_modified_date":
                        query = query + "load_date AS last_modified_date"
                    if col == "profit_center_code" and configs["destination_table"] == "product_line_xref":
                        query = query + "profit_center AS profit_center_code"
                    if col == "load_date":
                        query = query + """CAST("{}" AS TIMESTAMP) AS load_date""".format(configs["timestamp"])
                    if col == "record" and configs["destination_table"] == "instant_ink_enrollees_ltf":
                        query = query + "'iink_enrollees_ltf' AS record"

                if col in output_table_cols[0:len(output_table_cols)-1]:
                    query = query + ","
                
                query = query + "\n"

final_table_df = spark.sql(query + "FROM table_df")

# COMMAND ----------

# write data to S3
write_df_to_s3(final_table_df, "{}{}/{}/{}/".format(constants['S3_BASE_BUCKET'][stack], configs["destination_table"], configs["datestamp"], configs["timestamp"]), "csv", "overwrite")

# truncate existing redshift data and
# write data to redshift
submit_remote_query(stack, configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], "TRUNCATE " + destination)

write_df_to_redshift(configs, final_table_df, destination, "append")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")