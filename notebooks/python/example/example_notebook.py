# Databricks notebook source
task = dbutils.widgets.get("task")

# COMMAND ----------

print("this task is: " + task)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS: " + task)
