# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

prod_query = """

"""




# COMMAND ----------

reporting_query = """
create external table phoenix_spectrum_reporting.ib
(
record varchar(255),
cal_date date,
country_alpha2 varchar(255),
platform_subset varchar(255),
customer_engagement varchar(255),
measure varchar(255),
units DOUBLE PRECISION,
official bool,
load_date timestamp,
version varchar(255)
)
stored as parquet 
location 's3://dataos-core-prod-team-phoenix/spectrum/ib/'
"""


# COMMAND ----------

# use this block to modify an existing external table
reporting_query = """
alter table phoenix_spectrum_reporting.ib
set location 's3://dataos-core-prod-team-phoenix/spectrum/ib/'
"""

# COMMAND ----------

reporting_configs = configs.copy()
reporting_configs["redshift_username"] = secrets_get(constants["REDSHIFT_SECRET_NAME"]["reporting"], "us-west-2")["username"]
reporting_configs["redshift_password"] = secrets_get(constants["REDSHIFT_SECRET_NAME"]["reporting"], "us-west-2")["password"]
reporting_configs["redshift_url"] = constants["REDSHIFT_URL"]["reporting"]
reporting_configs["redshift_dev_group"] = 'auto_reporting'

# COMMAND ----------

# configs

# COMMAND ----------

# reporting_configs

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

# for prod cluster

submit_spectrum_query("prod", configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], prod_query)


# COMMAND ----------

# for reporting cluster

submit_spectrum_query("prod", reporting_configs["redshift_port"], reporting_configs["redshift_username"], reporting_configs["redshift_password"], reporting_configs["redshift_url"], reporting_query)

