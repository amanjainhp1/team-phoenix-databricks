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
	country_alpha2,
	base_product_number,
	platform_subset,
	base_quantity,
	load_date,
    official,
	version,
    "source"
from prod.actuals_hw
where 1=1
	and record = 'ACTUALS - HW'
	and cal_date > '2022-03-01'
order by cal_date 
"""


redshift_actuals_hw_records = read_redshift_to_df(configs) \
    .option("query", query) \
    .load()

# COMMAND ----------

write_df_to_sqlserver(configs, redshift_actuals_hw_records, "ie2_prod.dbo.actuals_hw", "append")

# COMMAND ----------


