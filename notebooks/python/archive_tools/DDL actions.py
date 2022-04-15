# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

query = """

create external table phoenix_spectrum_prod.ib 
(
record varchar(255),
cal_date date,
country_alpha2 varchar(255),
platform_subset varchar(255),
customer_engagement varchar(255),
measure varchar(255),
units float,
official bool,
load_date timestamp,
version varchar(255)
)
stored as parquet 
location 's3://dataos-core-prod-team-phoenix/spectrum/ib/';
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

submit_spectrum_query("prod", configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], query)


# COMMAND ----------


