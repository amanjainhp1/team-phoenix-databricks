# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# PULL D & P MPS units from stage.rtm_historical_actuals table
rtm_mps_actuals_query = """

SELECT
    rtm,
    geo,
    geo_type,
    a.base_prod_number,
    b.platform_subset,
    date,
    units,
    yeti_type,
    a.load_date,
    a.version
FROM stage.rtm_historical_actuals a
    left join mdm.rdma b on a.base_prod_number=b.base_prod_number
    left join mdm.product_line_xref c on b.pl = c.pl
WHERE 1=1
    and rtm in ('DMPS','PMPS')
    and c.technology = 'LASER'
    and b.pl NOT IN ('GW','LX')

"""


rtm_mps_actuals_records = read_redshift_to_df(configs) \
    .option("query", rtm_mps_actuals_query) \
    .load()

# COMMAND ----------

rtm_mps_actuals_records.display()

# COMMAND ----------

# write_df_to_redshift(configs, rtm_ib_decayed_records, "stage.rtm_ib_decayed", "overwrite")
