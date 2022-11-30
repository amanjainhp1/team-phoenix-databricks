# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# IE2_Landing.dbo.edw_revenue_units_sales_landing
# load parquet file
edw_s3 = spark.read.parquet("s3://dataos-core-itg-team-phoenix-fin/landing/EDW/edw_revenue_units_sales_landing/part-00000-tid-3689856782391625736-5511b3d9-c4a5-4228-be00-374f544ef370-0-1-c000.snappy.parquet")
edw_s3.count()
edw_s3.display()


# COMMAND ----------

# IE2_Landing.dbo.edw_revenue_document_currency_landing
# load parquet file
edw_s3 = spark.read.parquet("s3://dataos-core-itg-team-phoenix-fin/landing/EDW/edw_revenue_document_currency_landing/part-00000-tid-8602196773490778707-eb800b3d-3327-40df-b7c7-c45da87aec22-1-1-c000.snappy.parquet")
edw_s3.count()
edw_s3.display()

# COMMAND ----------

# IE2_Landing.dbo.edw_revenue_dollars_landing
# load parquet file
edw_s3 = spark.read.parquet("s3://dataos-core-itg-team-phoenix-fin/landing/EDW/edw_revenue_dollars_landing/part-00000-tid-1619046364515447834-4862d1d3-41eb-4b63-992c-8faff270d26c-2-1-c000.snappy.parquet")
edw_s3.count()
edw_s3.display()

# COMMAND ----------

# IE2_Landing.dbo.edw_revenue_units_base_landing
# load parquet file
edw_s3 = spark.read.parquet("s3://dataos-core-itg-team-phoenix-fin/landing/EDW/edw_revenue_units_base_landing/part-00000-tid-4837404354099801983-97e9cce3-e6e6-4b9b-9aa6-048444ecc610-3-1-c000.snappy.parquet")
edw_s3.count()
edw_s3.display()

# COMMAND ----------

# IE2_Landing.dbo.edw_shipment_actuals_landing
# load parquet file
edw_s3 = spark.read.parquet("s3://dataos-core-itg-team-phoenix-fin/landing/EDW/edw_shipment_actuals_landing/part-00000-tid-4833141427447557539-cf8242d8-7522-4dfb-b9fa-2b4d3ad21b9b-4-1-c000.snappy.parquet")
edw_s3.count()
edw_s3.display()
