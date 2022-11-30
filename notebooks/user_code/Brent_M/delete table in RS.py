# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

RS_source_code = """

drop table stage.norm_shipments

"""

# COMMAND ----------

conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'" \
    .format(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"])
con = psycopg2.connect(conn_string)
cur = con.cursor()

cur.execute(RS_source_code)
con.commit()

cur.close()

