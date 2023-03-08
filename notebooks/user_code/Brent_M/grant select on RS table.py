# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# grant select on mdm.hw_product_family_ink_business_segments to group int_analyst;
# grant select on all tables in schema prod to group int_analyst;
# grant select on prod.working_forecast to group int_analyst;
prod_query = """
grant select on all tables in schema mdm to group int_analyst;
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

submit_spectrum_query("prod", configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], prod_query)
