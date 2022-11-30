# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

query = """

select
	month_begin,
	region_5,
	country_alpha2,
	hps_ops,
	split_name,
	platform_subset,
	printer_installs,
	ib
from
	stage.ib_02_ce_splits
where 1=1
order by
	region_5,
	country_alpha2,
	month_begin
"""


redshift_ltf_records = read_redshift_to_df(configs) \
    .option("query", query) \
    .load()

# COMMAND ----------

write_df_to_sqlserver(configs, redshift_ltf_records, "ie2_staging.dbo.ib_02_ce_splits", "overwrite")

# COMMAND ----------


