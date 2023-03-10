# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

ib_datamart_source_code = """

create or replace view financials.currency_hedge_vw as  
  
SELECT
   a.profit_center,
   a.currency,
   a.month,
   a.revenue_currency_hedge,
   b.technology,
   a.load_date,
   a.version
FROM fin_prod.currency_hedge a 
    left join mdm.product_line_xref b on a.profit_center=b.profit_center_code; 
  
ALTER TABLE financials.currency_hedge_vw owner to auto_glue;
GRANT ALL ON financials.currency_hedge_vw to GROUP phoenix_dev;

"""

# COMMAND ----------

conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'" \
    .format(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"])
con = psycopg2.connect(conn_string)
cur = con.cursor()

cur.execute(ib_datamart_source_code)
con.commit()

cur.close()


# COMMAND ----------

drop_view_code = """

drop view financials.currency_hedge_vw
"""

# COMMAND ----------

conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'" \
    .format(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"])
con = psycopg2.connect(conn_string)
cur = con.cursor()

cur.execute(drop_view_code)
con.commit()

cur.close()

# COMMAND ----------


