# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Forecasting Engine Variables

# COMMAND ----------

# MAGIC %run ./common/configs

# COMMAND ----------

# MAGIC %run ./common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Global - Entire Forecast

# COMMAND ----------

# create empty widgets for interactive sessions
dbutils.widgets.text('technology', '')
dbutils.widgets.text('installed_base_version', '')  # installed base version
dbutils.widgets.text('usage_share_version', '')  # usage-share version
dbutils.widgets.text('working_forecast_source_name', '')
dbutils.widgets.text('pivots_start', '')
dbutils.widgets.text('pivots_end', '')
dbutils.widgets.text('cycle_date', '')

# COMMAND ----------

technology = dbutils.widgets.get('technology')

ib_version = dbutils.widgets.get('installed_base_version')
us_version = dbutils.widgets.get('usage_share_version')

working_forecast_source_name = dbutils.widgets.get('working_forecast_source_name')

# COMMAND ----------

# for labelling tables, laser/toner = toner, ink = ink
# for filtering technologies, laser/toner = 'LASER', ink = 'INK/PWA'
technology_label = ''
technologies_list = ''
if technology == 'ink':
    technology_label = 'ink'
    technologies_list = "'INK', 'PWA'"
elif technology in ('laser', 'toner'):
    technology_label = 'toner'
    technologies_list = "'LASER'"

# COMMAND ----------

print('technology: ' + technology)
print('technology_label: ' + technology_label)
print('technologies_list: ' + technologies_list)

print('ib_version: ' + ib_version)
print('us_version: ' + us_version)

print('working_forecast_source_name: ' + wf_version)

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

pivots_start = dbutils.widgets.get('pivots_start')
pivots_end = dbutils.widgets.get('pivots_end')

# COMMAND ----------

print('pivots_start: ' + pivots_start)
print('pivots_end: ' + pivots_end)

# COMMAND ----------
cycle_date = dbutils.widgets.get('cycle_date')
print('cycle_date: ' + cycle_date)
