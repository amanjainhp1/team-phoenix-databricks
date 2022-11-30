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
	geography_grain,
	geography,
	"year",
	split_name,
	value,
	avg_printer_life,
	load_date,
	version,
	official
FROM prod.decay_m13
"""


redshift_ns_records = read_redshift_to_df(configs) \
    .option("query", query) \
    .load()

# COMMAND ----------

write_df_to_sqlserver(configs, redshift_ns_records, "ie2_prod.dbo.decay_m13", "overwrite")
