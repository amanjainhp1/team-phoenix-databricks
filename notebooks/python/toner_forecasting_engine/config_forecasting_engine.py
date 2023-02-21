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

ib_version = '2023.01.26.1'
us_version = '2023.01.30.2'

toner_ib_version = ''
ink_ib_version = ''

# COMMAND ----------

print('ib_version: ' + ib_version)
print('us_version: ' + us_version)

print('toner_ib_version: ' + toner_ib_version)
print('ink_ib_version: ' + ink_ib_version)

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

pivots_start = '2017-11-01'
pivots_end = '2027-10-01'

# COMMAND ----------

print('pivots_start: ' + pivots_start)
print('pivots_end: ' + pivots_end)
