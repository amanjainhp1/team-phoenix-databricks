# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

## OZZY MPS historical actuals
# UserId:  Phoenix  
# pw:  KSfi3917!#1h2H

# COMMAND ----------

# build query to pull data from OZZY server

# retrieve ozzy secrets
ozzy_secret = secrets_get(constants["OZZY_SECRET_NAME"][stack], "us-west-2")

ozzy_mps_actuals_query = """
SELECT top 10 *
FROM ozzy.dbo.WOPR_dMPS_pMPS_FY13_to_15_for_Brent
"""

# Connection details
# Connection details
ozzy_mps_actuals_records = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://{ozzy_secret['host']}:{ozzy_secret['port']};") \
    .option("user", ozzy_secret["username"]) \
    .option("password", ozzy_secret["password"]) \
    .option("query", ozzy_mps_actuals_query) \
    .load()


# ozzy_mps_actuals_records = spark.read \
#   .format("jdbc") \
#   .option("url", "jdbc:sqlserver://MSPBAPROD.CORP.HPICLOUD.NET:1433;") \
#   .option("user", "Phoenix") \
#   .option("password", "KSfi3917!#1h2H") \
#   .option("query", ozzy_mps_actuals_query) \
#   .load()


# COMMAND ----------

ozzy_mps_actuals_records.display()
