# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

query = """

SELECT 
    record, 
    platform_subset, 
    region_5, 
    country_alpha2, 
    em_dm, 
    business_model, 
    month_begin, 
    split_name, 
    pre_post_flag, 
    value, 
    active, 
    active_at, 
    inactive_at, 
    load_date, 
    version, 
    official
FROM prod.ce_splits
"""


redshift_ns_records = read_redshift_to_df(configs) \
    .option("query", query) \
    .load()

# COMMAND ----------

write_df_to_sqlserver(configs, redshift_ns_records, "ie2_prod.dbo.ce_splits", "overwrite")
