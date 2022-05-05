# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

query = """

create external schema phoenix_spectrum_test
from data catalog 
database 'team-phoenix-test'
iam_role 'arn:aws:iam::740156627385:role/team-phoenix-role'
create external database if not exists;

"""


# COMMAND ----------

# submit_remote_query("prod", configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], query)

import psycopg2

def submit_spectrum_query(dbname, port, user, password, host, sql_query):  
    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
        .format(dbname, port, user, password, host)
    con = psycopg2.connect(conn_string)
    con.autocommit = True
    cur = con.cursor()
    cur.execute(query)
    con.commit()
    cur.close()

submit_spectrum_query("dev", configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], query)


# COMMAND ----------


