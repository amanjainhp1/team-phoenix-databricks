# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

constants['S3_BASE_BUCKET'][stack]

# COMMAND ----------

yield_1 = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load('s3a://dataos-core-itg-team-phoenix-fin/landing/odw/yield.xlsx')

# COMMAND ----------

write_df_to_redshift(configs, yield_1, "stage.yield_lf", "overwrite")

# COMMAND ----------

lf_actuals = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load('s3a://dataos-core-itg-team-phoenix/landing/ODW/lf_actuals.xlsx')

# COMMAND ----------

lf_actuals.select(col("cal_date")).distinct().display()

# COMMAND ----------

lf_actuals.createOrReplaceTempView("lf_actuals_view")

# COMMAND ----------

df_iink_stf = spark.read.format('csv').options(header='true', inferSchema='true').load('{}landing/ODW/lf_actuals_subset.xlsx'.format(constants['S3_BASE_BUCKET'][stack]))

# COMMAND ----------

df_iink_stf.display()

# COMMAND ----------

lf_actuals.createOrReplaceTempView("lf_actuals")

# COMMAND ----------

write_df_to_redshift(configs, lf_actuals, "stage.lf_actuals", "overwrite")

# COMMAND ----------

final_lf_acts = """
SELECT
 record,cast(cal_date as date) cal_date,country_alpha2,base_product_number,platform_subset,base_quantity,cast(load_date as timestamptz) load_date,1 official,version,"source"
FROM stage.lf_actuals
"""

df_final_lf_acts = read_redshift_to_df(configs).option("query", final_lf_acts).load()

# COMMAND ----------

df_final_lf_acts = df_final_lf_acts.select('record','cal_date','country_alpha2','base_product_number','platform_subset','source','base_quantity','official','load_date','version')

# COMMAND ----------

df_final_lf_acts.count()

# COMMAND ----------

write_df_to_redshift(configs, df_final_lf_acts, "prod.actuals_hw", "append")
