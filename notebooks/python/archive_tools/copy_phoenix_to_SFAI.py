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
	forecast_name,
	cal_date,
	country_alpha2,
	platform_subset,
	base_product_number,
	units,
	load_date,
	official,
	version
from
	prod.hardware_ltf
where
	1 = 1
	and record = 'HW_FCST'
	and version = '2022.06.02.1'
order by
	version
"""


redshift_ltf_records = read_redshift_to_df(configs) \
    .option("query", query) \
    .load()

# COMMAND ----------

write_df_to_sqlserver(configs, redshift_ltf_records, "ie2_prod.dbo.hardware_ltf", "append")
