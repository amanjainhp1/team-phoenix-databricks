# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------


ib_staging_query = """
SELECT *
FROM stage.norm_shipments_ce
"""

# execute query from stage table
ib_staging_records = read_redshift_to_df(configs) \
    .option("query", ib_staging_query) \
    .load()

# COMMAND ----------

write_df_to_redshift(configs=configs, df=ib_staging_records, destination="scen.prelim_norm_shipmetns_ce", mode="overwrite", preactions="")
