# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

RS_source_code = """
ALTER TABLE fin_prod.adjusted_revenue_flash ALTER COLUMN official TYPE bool
"""

# COMMAND ----------

import psycopg2

def submit_spectrum_query(dbname, port, user, password, host, sql_query):  
    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
        .format(dbname, port, user, password, host)
    con = psycopg2.connect(conn_string)
    con.autocommit = True
    cur = con.cursor()
    cur.execute(sql_query)
    con.commit()
    cur.close()

# COMMAND ----------

# for reporting cluster

submit_spectrum_query("prod", configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], RS_source_code)
