# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

df=spark.read.parquet(constants["S3_BASE_BUCKET"][stack]+"/cupsm_inputs/ink/")
df.show()
row_count=df.count()
print(row_count)
df=df.select(['printer_platform_name','country_alpha2','printer_managed','date_month_dim_ky','hp_share']).distinct()
df.show()
row_count=df.count()
print(row_count)
read_usage_share_data = read_redshift_to_df(configs).option("query", "select platform_subset,geography,customer_engagement,calendar_yr_mo,units from prod.usage_share_country a inner join mdm.calendar c on a.cal_date =c.date where measure='HP_SHARE' and source='TELEMETRY'").load()
row_count_us=read_usage_share_data.count()
print(row_count_us)
df_result=df.exceptAll(read_usage_share_data).show()
