# Databricks notebook source
# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# MAGIC %run ../../python/common/configs

# COMMAND ----------

prod_query = """
create external table phoenix_spectrum_prod.list_price_filtered_historical
(
sales_product_number varchar(255),
country_alpha2 varchar(255),
currency_code varchar(255),
price_term_code varchar(255),
price_start_effective_date varchar(255),
qb_sequence_number varchar(255),
list_price varchar(255),
product_line varchar(255),
accounting_rate varchar(255),
list_price_usd varchar(255),
load_date timestamp
)
stored as parquet 
location 's3://dataos-core-prod-team-phoenix/spectrum/list_price_filtered_historical/';
"""


# COMMAND ----------

reporting_query = """
create external table phoenix_spectrum_reporting.list_price_filtered_historical
(
sales_product_number varchar(255),
country_alpha2 varchar(255),
currency_code varchar(255),
price_term_code varchar(255),
price_start_effective_date varchar(255),
qb_sequence_number varchar(255),
list_price varchar(255),
product_line varchar(255),
accounting_rate varchar(255),
list_price_usd varchar(255),
load_date timestamp
)
stored as parquet 
location 's3://dataos-core-prod-team-phoenix/spectrum/list_price_filtered_historical/'
"""

# COMMAND ----------

reporting_configs = configs.copy()
reporting_configs["redshift_username"] = secrets_get(constants["REDSHIFT_SECRET_NAME"]["reporting"], "us-west-2")["username"]
reporting_configs["redshift_password"] = secrets_get(constants["REDSHIFT_SECRET_NAME"]["reporting"], "us-west-2")["password"]
reporting_configs["redshift_url"] = constants["REDSHIFT_URL"]["reporting"]
reporting_configs["redshift_dev_group"] = 'auto_reporting'

# COMMAND ----------

configs

# COMMAND ----------

reporting_configs

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


# COMMAND ----------

#drop external prod table
prod_drop_query = """
drop table phoenix_spectrum_prod.list_price_filtered_historical
"""


# COMMAND ----------

#drop external reporting table
reporting_drop_query = """
drop table phoenix_spectrum_reporting.list_price_filtered_historical
"""

# COMMAND ----------

#run drop code on prod

submit_spectrum_query("prod", configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], prod_drop_query)


# COMMAND ----------

#run drop code on reporting
submit_spectrum_query("prod", reporting_configs["redshift_port"], reporting_configs["redshift_username"], reporting_configs["redshift_password"], reporting_configs["redshift_url"], reporting_drop_query)
