# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Forecasting Engine Variables

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Global - Entire Forecast

# COMMAND ----------

ib_version = '2023.03.03.1'
us_version = '2023.03.04.1'

toner_wf_version = '2023.03.09.1'
ink_wf_version = '2023.02.09.1'

# COMMAND ----------

wf_country_version_df = spark.sql("""select max(version) as version from prod.working_forecast_country""")
wf_country_version = wf_country_version_df.first()['version']

# COMMAND ----------

print('ib_version: ' + ib_version)
print('us_version: ' + us_version)

print('toner_wf_version: ' + toner_wf_version)
print('ink_wf_version: ' + ink_wf_version)

print('working_forecast_country: ' + wf_country_version)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Base Forecast

# COMMAND ----------

iink_query = spark.sql("""
select max(year_month) as iink_stf_month_end
from prod.instant_ink_enrollees 
where official = 1 
    and data_source = 'FCST'
""")

iink_stf_month_end = iink_query.first()[0]

# COMMAND ----------

print('iink_stf_month_end: ' + iink_stf_month_end)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Toner Pivots

# COMMAND ----------

pivots_start = '2018-01-01'
pivots_end = '2027-12-01'

# COMMAND ----------

print('pivots_start: ' + pivots_start)
print('pivots_end: ' + pivots_end)
