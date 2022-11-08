# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

query = """

select 
	record,
	cal_date,
	region_5,
	country_alpha2,
	platform_subset,
	units,
	version,
	load_date
from prod.norm_shipments
where
	1 = 1
	and version = '2022.10.31.1'
order by
	version
"""


redshift_ns_records = read_redshift_to_df(configs) \
    .option("query", query) \
    .load()

# COMMAND ----------

write_df_to_sqlserver(configs, redshift_ns_records, "ie2_prod.dbo.norm_shipments", "append")
