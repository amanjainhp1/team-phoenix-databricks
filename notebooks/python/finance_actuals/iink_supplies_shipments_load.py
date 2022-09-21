# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %r
# MAGIC options(java.parameters = "-Xmx18g" )
# MAGIC library(readxl)
# MAGIC library(tidyverse)
# MAGIC library(DBI)
# MAGIC 
# MAGIC options(scipen=999)

# COMMAND ----------

for key, val in configs.items():
    spark.conf.set(key, val)
    
spark.conf.set('aws_bucket_name', constants['S3_BASE_BUCKET'][stack])

# COMMAND ----------

tables = ['iink_salesprod_units']

for table in tables:
  spark.read.csv(f'{constants["S3_BASE_BUCKET"][stack]}/product/supplies_iink/{table}/').createOrReplaceTempView('iink_units_raw')

# COMMAND ----------

# MAGIC %r
# MAGIC path <- "s3://dataos-core-itg-team-phoenix/product/supplies_iink/iink_salesprod_units.csv"
# MAGIC spark_df <- read.csv(path)

# COMMAND ----------

iink_units_r_table = SparkR::collect(SparkR::sql(
"SELECT * FROM iink_units_raw"))

# COMMAND ----------

# MAGIC %r
# MAGIC iink_select <- iink_units_r_table %>%
# MAGIC   # remove WK per Indra's instruction; overrode by ink forecasting on 7/28/2022
# MAGIC   # dplyr::filter(SKU.Type != "WK") %>%
# MAGIC   dplyr::mutate(
# MAGIC     Units = as.numeric(Units),
# MAGIC     cal_date = as.Date(Date, "%m/%d/%Y")
# MAGIC   ) %>%
# MAGIC   dplyr::group_by(
# MAGIC     SKU, Region, cal_date
# MAGIC   ) %>%
# MAGIC   summarize(
# MAGIC     Units = sum(Units)
# MAGIC   ) %>%
# MAGIC   ungroup()
# MAGIC 
# MAGIC iink_select %>% summarize(Units = sum(Units))

# COMMAND ----------

# MAGIC %r
# MAGIC iink_units <- iink_select %>%
# MAGIC   dplyr::rename(
# MAGIC     sales_product_number = SKU,
# MAGIC     sales_quantity = Units
# MAGIC   ) %>%
# MAGIC   dplyr::mutate(
# MAGIC     Region_3 = ifelse(Region == "Canada" | Region == "America", "AMS", Region),
# MAGIC     Region_5 = ifelse(Region == "Canada"| Region == "America", "N.Amer", Region_3),
# MAGIC     Region_5 = ifelse(Region == "EMEA", "EU", Region_5),
# MAGIC     Region_5 = ifelse(Region == 'APJ', 'AP', Region_5),
# MAGIC     country = ifelse(Region == "America", "United States of America",
# MAGIC                      ifelse(Region == "EMEA", "UNKNOWN EMEA",
# MAGIC                      ifelse(Region == 'APJ', "UNKNOWN AP", Region))),
# MAGIC     pl = "GD",
# MAGIC     load_date = Sys.Date()
# MAGIC   ) %>%
# MAGIC   dplyr::select(
# MAGIC     cal_date, sales_product_number, pl, country, sales_quantity, load_date
# MAGIC   ) 
# MAGIC 
# MAGIC str(iink_units)

# COMMAND ----------

#LOAD TO DB
write_df_to_redshift(configs, iink_units, "fin_prod.iink_units", "append", postactions = "", preactions = "truncate fin_prod.iink_units")
