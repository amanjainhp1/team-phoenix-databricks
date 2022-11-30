# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

#GRANT ALL ON prod.ib_datamart_source_vw to GROUP phoenix_dev;
#GRANT SELECT ON SCHEMA :: phoenix_spectrum_reporting TO phoenix_dev

#GRANT SELECT ON SCHEMA phoenix_spectrum_reporting TO GROUP phoenix_dev;
#grant ALL on all tables in schema phoenix_spectrum_reporting to group phoenix_dev;
#GRANT ALL ON SCHEMA phoenix_spectrum_reporting TO GROUP phoenix_dev;

reporting_query = """
GRANT ALL ON SCHEMA phoenix_spectrum_reporting TO GROUP phoenix_dev;
"""

# COMMAND ----------

reporting_configs = configs.copy()
reporting_configs["redshift_username"] = secrets_get(constants["REDSHIFT_SECRET_NAME"]["reporting"], "us-west-2")["username"]
reporting_configs["redshift_password"] = secrets_get(constants["REDSHIFT_SECRET_NAME"]["reporting"], "us-west-2")["password"]
reporting_configs["redshift_url"] = constants["REDSHIFT_URL"]["reporting"]
reporting_configs["redshift_dev_group"] = 'auto_reporting'

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

# for reporting cluster

submit_spectrum_query("prod", reporting_configs["redshift_port"], reporting_configs["redshift_username"], reporting_configs["redshift_password"], reporting_configs["redshift_url"], reporting_query)

