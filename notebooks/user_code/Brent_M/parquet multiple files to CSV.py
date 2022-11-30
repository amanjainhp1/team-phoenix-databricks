# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# load all datasets
list_price_gpsy_1 = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/list_price_gpsy_historical/2022.08.03.1/")
list_price_gpsy_2 = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/list_price_gpsy_historical/2022.09.02.1/")
list_price_gpsy_3 = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/list_price_gpsy_historical/2022.10.04.1/")
list_price_gpsy_4 = spark.read.parquet("s3://dataos-core-prod-team-phoenix/spectrum/list_price_gpsy_historical/2022.11.02.1/")

# COMMAND ----------

# Union the datasets and then filter
list_price_gpsy_final = list_price_gpsy_1.union(list_price_gpsy_2).union(list_price_gpsy_3).union(list_price_gpsy_4)
list_price_gpsy_final_filtered = list_price_gpsy_final[list_price_gpsy_final['product_line'].isin(['5T','GJ','GK','GP','IU','LS','N4','N5','GL','E5','EO','G0'])]


# COMMAND ----------

# get a count
list_price_gpsy_final_filtered.count()

# COMMAND ----------

# output to CSV
list_price_gpsy_final_filtered.coalesce(1).write.csv("s3://dataos-core-prod-team-phoenix/landing/test/gpsy_output.csv", header=True)

# COMMAND ----------

# this section does save a CSV, but it is not named correctly.  Revisit and fix code
save_location= "s3://dataos-core-prod-team-phoenix/landing/test/"
csv_location = save_location
file_location = save_location+'gpsy_output.csv'

list_price_gpsy_final_filtered.repartition(1).write.csv(path=csv_location, mode="append", header="true")
