# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import functions as func
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# --Get LTF data from Archer into stage.hardware_ltf_stage

f_report_units_query = """
SELECT *
FROM ie2_financials.dbo.forecast_supplies_baseprod
where version = '2022.11.08.1'

"""

f_report_units = read_sql_server_to_df(configs) \
    .option("query", f_report_units_query) \
    .load()

write_df_to_redshift(configs, f_report_units, "ifs2.forecast_supplies_baseprod", "overwrite")

# f_report_units.createOrReplaceTempView("f_report_units")

# COMMAND ----------


