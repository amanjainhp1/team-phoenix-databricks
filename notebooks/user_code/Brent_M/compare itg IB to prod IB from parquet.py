# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

prod_ib = spark.read.parquet('s3://insights-environment-sandbox/BrentT/ib')
# prod_ib.count()
prod_ib.createOrReplaceTempView('prod_ib')



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod_ib

# COMMAND ----------

ib_itg_query = """

SELECT *
  FROM prod.ib
  WHERE version = '2022.08.24.1'
  
"""

redshift_ib_itg_records = read_redshift_to_df(configs) \
    .option("query", ib_itg_query) \
    .load()


# COMMAND ----------

redshift_ib_itg_records.show()

# COMMAND ----------

itg_ib = redshift_ib_itg_records
itg_ib.createOrReplaceTempView('itg_ib')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'itg' as record, cal_date, country_alpha2, platform_subset, customer_engagement, measure, units from itg_ib
# MAGIC UNION ALL
# MAGIC SELECT 'prod' as record, cal_date, country_alpha2, platform_subset, customer_engagement, measure, units from prod_ib

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   a.cal_date, 
# MAGIC   a.country_alpha2, 
# MAGIC   a.platform_subset, 
# MAGIC   a.customer_engagement, 
# MAGIC   a.measure, 
# MAGIC   a.units,
# MAGIC   b.units,
# MAGIC   a.units - b.units
# MAGIC from itg_ib a 
# MAGIC left join prod_ib b on 
# MAGIC   a.cal_date = b.cal_date 
# MAGIC   and a.country_alpha2 = b.country_alpha2 
# MAGIC   and a.platform_subset = b.platform_subset 
# MAGIC   and a.customer_engagement = b.customer_engagement 
# MAGIC   and a.measure = b.measure
# MAGIC where a.units <> b.units
# MAGIC order by 8

# COMMAND ----------


