# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

## Archer_Prod.dbo.rtm_actuals_country_speedlic_vw RTM historical data
## get the min_date

archer_rtm_historical_min_date_query = """

SELECT MIN(date) AS min_date
FROM Archer_Prod.dbo.rtm_actuals_country_speedlic_vw
WHERE 1=1
    AND UPPER(RTM) IN ('CMPS','DMPS')

"""

min_date = read_sql_server_to_df(configs) \
    .option("query", archer_rtm_historical_min_date_query) \
    .load() \
    .select("min_date").head()[0]

print(min_date)

# COMMAND ----------

query = """
      
DELETE
FROM stage.rtm_historical_actuals
where 1=1
	AND date >= '{}'
""".format(min_date)

submit_remote_query(configs, query)



# COMMAND ----------

archer_rtm_historical_actuals_query = """

select
    UPPER(RTM) as rtm,
    geo,
    UPPER(geo_type) as geo_type,
    base_prod_number,
    date,
    units,
    UPPER(yeti_type) as yeti_type,
    getdate() as load_date,
    version
from Archer_Prod.dbo.rtm_actuals_country_speedlic_vw
WHERE UPPER(RTM) in ('CMPS','DMPS')

"""

archer_rtm_historical_actuals_records = read_sql_server_to_df(configs) \
    .option("query", archer_rtm_historical_actuals_query) \
    .load()


# archer_rtm_historical_actuals_records.display()

# COMMAND ----------

write_df_to_redshift(configs, archer_rtm_historical_actuals_records, "stage.rtm_historical_actuals", "append")
