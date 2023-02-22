# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

query = """

SELECT
    ns.record
    , ns.cal_date
    , ns.region_5
    , ns.country_alpha2
    , ns.platform_subset
    , ns.units
    , getdate() as load_date
    ,'PRELIM' as version
FROM stage.norm_ships ns
"""


redshift_ns_records = read_redshift_to_df(configs) \
    .option("query", query) \
    .load()

# COMMAND ----------

write_df_to_sqlserver(configs, redshift_ns_records, "[IE2_Staging].[dbo].[norm_shipments]", "append")
