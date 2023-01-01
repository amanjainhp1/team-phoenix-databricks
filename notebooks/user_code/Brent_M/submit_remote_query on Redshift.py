# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

submit_remote_query(configs, """select pg_terminate_backend(1073987800);""")
