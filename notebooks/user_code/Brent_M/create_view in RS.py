# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

ib_datamart_source_code = """

create or replace view prod.prelim_ib_vw as  
  
SELECT  
    a.record,  
    month_begin,  
    geography_grain,  
    geography,  
    a.country_alpha2,  
    hps_ops,  
    split_name,  
    a.platform_subset,  
    printer_installs,  
    b.embargoed_sanctioned_flag,  
    c.technology,  
    ib,  
    'PRELIM' as version  
FROM scen.prelim_ib a  
    LEFT JOIN mdm.iso_country_code_xref b on a.country_alpha2=b.country_alpha2  
    LEFT JOIN mdm.hardware_xref c on a.platform_subset = c.platform_subset  
WITH NO SCHEMA BINDING;  
  
GRANT ALL ON prod.prelim_ib_vw to GROUP phoenix_dev;  
GRANT SELECT ON prod.prelim_ib_vw to GROUP int_analyst;  
GRANT SELECT ON prod.prelim_ib_vw to GROUP auto_team_phoenix_analyst;
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

drop view prod.prelim_ib_vs_latest_official_vw
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


