# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

currency_hedge_source_query = """

SELECT * FROM prod.currency_hedge
"""

# COMMAND ----------

currency_hedge_records = read_redshift_to_df(configs) \
    .option("query", currency_hedge_source_query) \
    .load()

# COMMAND ----------

# currency_hedge_records.display()

# COMMAND ----------

write_df_to_redshift(configs, currency_hedge_records, "fin_prod.currency_hedge", "overwrite")

# COMMAND ----------

RS_source_code = """

drop table prod.currency_hedge

"""

# COMMAND ----------

conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'" \
    .format(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"])
con = psycopg2.connect(conn_string)
cur = con.cursor()

cur.execute(RS_source_code)
con.commit()

cur.close()

