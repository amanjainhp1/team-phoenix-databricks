# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

query = """

SELECT [record]
      ,[version]
      ,[load_date]
      ,[cal_date]
      ,[region_5]
      ,[country_alpha2]
      ,[platform_subset]
      ,[customer_engagement]
      ,[split_value]
      ,[units]
  FROM prod.norm_shipments_ce
where
	1 = 1
	and version = '2022.10.31.1'
"""


redshift_ns_records = read_redshift_to_df(configs) \
    .option("query", query) \
    .load()

# COMMAND ----------

write_df_to_sqlserver(configs, redshift_ns_records, "ie2_prod.dbo.norm_shipments_ce", "append")
