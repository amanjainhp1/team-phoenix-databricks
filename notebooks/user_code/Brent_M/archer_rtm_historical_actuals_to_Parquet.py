# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

## Archer_Prod.dbo.rtm_actuals_country_speedlic_vw RTM historical data

archer_rtm_historical_query = """

select *
from Archer_Prod.dbo.rtm_actuals_country_speedlic_vw
where date > '2022-12-01'

"""

archer_rtm_historical_records = read_sql_server_to_df(configs) \
    .option("query", archer_rtm_historical_query) \
    .load()

archer_rtm_historical_records.cache()

# COMMAND ----------

# write to parquet file in s3

s3_usage_share_output_bucket = constants["S3_BASE_BUCKET"][stack] + "archive/archer_rtm_historical_actuals/2023_03_23/"


# COMMAND ----------

write_df_to_s3(archer_rtm_historical_records, s3_usage_share_output_bucket, "parquet", "overwrite")

# COMMAND ----------

write_df_to_redshift(configs, archer_rtm_historical_records, "stage.rtm_historical_actuals", "append")

# COMMAND ----------

## Archer_Prod.dbo.f_report_units_rtm('TEST') --  LTF with RTM splits

archer_ltf_rtm_query = """

select *
from Archer_Prod.dbo.f_report_units_rtm('TEST')

"""

archer_ltf_rtm_records = read_sql_server_to_df(configs) \
    .option("query", archer_ltf_rtm_query) \
    .load()

archer_ltf_rtm_records.cache()

# COMMAND ----------

# write to parquet file in s3

s3_usage_share_output_bucket = constants["S3_BASE_BUCKET"][stack] + "landing/archer_ltf_rtm/2023_01_27/"


# COMMAND ----------

write_df_to_s3(archer_ltf_rtm_records, s3_usage_share_output_bucket, "parquet", "overwrite")

# COMMAND ----------

write_df_to_redshift(configs, archer_ltf_rtm_records, "stage.archer_ltf_rtm", "overwrite")
