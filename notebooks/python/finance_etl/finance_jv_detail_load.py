# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

from functools import reduce
from pyspark.sql.functions import col, current_date, regexp_extract, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType, DecimalType

# COMMAND ----------

# mount S3 bucket
bucket = f"dataos-core-{stack}-team-phoenix-fin" 
bucket_prefix = "jv_detail"
dbfs_mount = '/mnt/jv_detail/'

s3_mount(f'{bucket}/{bucket_prefix}', dbfs_mount)

# COMMAND ----------

jv_detail_latest_file = retrieve_latest_s3_object_by_prefix(bucket, bucket_prefix)

jv_detail_latest_file = jv_detail_latest_file.split("/")[len(jv_detail_latest_file.split("/"))-1]

print(jv_detail_latest_file)

# COMMAND ----------

   jv_detail_latest_file_df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{jv_detail_latest_file}")

# COMMAND ----------

jv_detail_latest_file_df = jv_detail_latest_file_df.withColumn("load_date", current_date())

# COMMAND ----------

jv_detail_latest_file_df.createOrReplaceTempView("jv_detail_latest_file_df")

# COMMAND ----------

# MAGIC %r
# MAGIC options(java.parameters = "-Xmx18g" )
# MAGIC library(readxl)
# MAGIC library(tidyverse)
# MAGIC library(DBI)
# MAGIC 
# MAGIC options(scipen=999)

# COMMAND ----------

# MAGIC %r
# MAGIC mcodes = SparkR::collect(SparkR::sql(
# MAGIC "SELECT * FROM jv_detail_latest_file_df"))

# COMMAND ----------

# MAGIC %r
# MAGIC mcode_char_cols <- c("jv_description", "jvPL", "reg3", "jvCountry", "QTR", "acct_entry_month")
# MAGIC mcodes[mcode_char_cols] <- lapply(mcodes[mcode_char_cols], as.character)
# MAGIC 
# MAGIC mcode_num_cols <- c("jvGrossRev", "jvContra", "jvNetRev", "jvCOS", "jvGM")
# MAGIC mcodes[mcode_num_cols] <- lapply(mcodes[mcode_num_cols], as.numeric)
# MAGIC 
# MAGIC mcodes$reg3[mcodes$reg3 == "Americas"] <- "AMS"
# MAGIC mcodes$reg3[mcodes$reg3 == "Asia Pacific"] <- "APJ"

# COMMAND ----------

# MAGIC %r
# MAGIC mcodes <- mcodes %>%
# MAGIC   dplyr::mutate(
# MAGIC     jvCountry = ifelse(is.na(jvCountry) & reg3 == "EMEA", "UNKNOWN EMEA", jvCountry),
# MAGIC     jvCountry = ifelse(is.na(jvCountry) & reg3 == "APJ", "UNKNOWN AP", jvCountry),
# MAGIC     jvCountry = ifelse(is.na(jvCountry) & reg3 == "AMS", "UNKNOWN LA", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Brazil Sales", "Brazil", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "EMEA HQ", "UNKNOWN EMEA", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "US Sales", "United States of America", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Other", "UNKNOWN LA", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Other Countries Sales", "UNKNOWN LA", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "CACE", "UNKNOWN LA", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "CACEVE", "UNKNOWN LA", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "CACEVE HQ", "UNKNOWN LA", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Mexico Sales", "Mexico", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Bolivia", "Bolivia (Plurinational State of)", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Argentina Sales", "Argentina", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Australia Sales", "Australia", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Bangladesh Sales", "Bangladesh", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Brunei Sales", "Brunei Darussalam", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Canada Sales", "Canada", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Chile Sales", "Chile", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "China Sales", "China", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Cambodia Sales", "Cambodia", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Colombia Sales", "Colombia", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Hong Kong Sales", "Hong Kong", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "India Sales", "India", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Indonesia Sales", "Indonesia", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Malaysia Sales", "Malaysia", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Myanmar Sales", "Myanmar", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Nepal Sales", "Nepal", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "New Zealand Sales", "New Zealand", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Outer Mongolia Sales", "Mongolia", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Pakistan Sales", "Pakistan", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Peru Sales", "Peru", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Philippines Sales", "Philippines", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Singapore Sales", "Singapore", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Sri Lanka Sales", "Sri Lanka", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Taiwan Sales", "Taiwan", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Thailand Sales", "Thailand", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "Vietnam Sales", "Vietnam", jvCountry),
# MAGIC     jvCountry = ifelse(jvCountry == "WW Carve Out", "UNKNOWN WORLDWIDE", jvCountry)
# MAGIC   )

# COMMAND ----------

# MAGIC %r
# MAGIC mcodes %>% dplyr::select(jv_description, jvPL) %>% distinct()

# COMMAND ----------

# MAGIC %r
# MAGIC mcodes_prep_to_load <- mcodes %>%
# MAGIC   dplyr::rename(
# MAGIC     yearmon = acct_entry_month,
# MAGIC     sales_product_number = jv_description,
# MAGIC     PL = jvPL,
# MAGIC     region_3 = reg3,
# MAGIC     country = jvCountry,
# MAGIC     gross_revenue = jvGrossRev,
# MAGIC     contractual_discounts = jvContra,
# MAGIC     net_revenue = jvNetRev,
# MAGIC     total_COS = jvCOS,
# MAGIC     gross_margin = jvGM
# MAGIC   ) %>%
# MAGIC   dplyr::mutate(
# MAGIC     region_5 = ifelse(region_3 == "AMS", "LA", region_3),
# MAGIC     region_5 = ifelse(country == "United States of America" | country == "Canada", "NA", region_5),
# MAGIC     region_5 = ifelse(region_5 == "APJ", "AP", region_5),
# MAGIC     region_5 = ifelse(region_5 == "EMEA", "EU", region_5),
# MAGIC     region_5 = ifelse(region_5 == "WW", "XW", region_5)
# MAGIC   ) %>%
# MAGIC   dplyr::select(yearmon, sales_product_number, PL, region_3, region_5, country,
# MAGIC                 gross_revenue, contractual_discounts, total_COS) 
# MAGIC str(mcodes_prep_to_load)

# COMMAND ----------

# MAGIC %r
# MAGIC mcodes_load <- mcodes_prep_to_load %>%
# MAGIC   dplyr::mutate(
# MAGIC     load_date = Sys.Date()
# MAGIC   )
# MAGIC 
# MAGIC str(mcodes_load)

# COMMAND ----------

# MAGIC %r
# MAGIC mcodes_load <- SparkR::createDataFrame(mcodes_load)
# MAGIC SparkR::createOrReplaceTempView(mcodes_load, "mcodes_load")

# COMMAND ----------

#LOAD TO DB
write_df_to_redshift(configs, spark.sql('SELECT * FROM mcodes_load'), "fin_stage.supplies_manual_mcode_jv_detail_landing", "append", postactions = "", preactions = "truncate fin_stage.supplies_manual_mcode_jv_detail_landing")
