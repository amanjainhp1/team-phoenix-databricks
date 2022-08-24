# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

prod_ib = spark.read.parquet('s3://insights-environment-sandbox/BrentT/ib')
prod_ib.count()
prod_ib.createOrReplaceTempView('prod_ib')

%sql
SELECT COUNT(*) FROM prob_ib

# COMMAND ----------



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


