# Databricks notebook source
import pyspark.sql.functions as f
import time
from pyspark.sql import Window
import pandas as pd
from IPython.display import HTML

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../qa_framework/execute_testcases_testresults
