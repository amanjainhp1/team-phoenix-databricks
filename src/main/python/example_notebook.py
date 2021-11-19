# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1: Setup

# COMMAND ----------

dbutils.widgets.text("REDSHIFT_SECRET_NAME", "")
dbutils.widgets.dropdown("REDSHIFT_REGION_NAME", "us-west-2", ["us-west-2", "us-east-2"])

# COMMAND ----------

# import libraries
import os

# COMMAND ----------

# MAGIC %run ./common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ./common/s3_utils

# COMMAND ----------

# set common parameters
sfai_hostname = "sfai.corp.hpicloud.net"
sfai_port = 1433
sfai_url = f"jdbc:sqlserver://{sfai_hostname}:{sfai_port};database="
sfai_username = "databricks_user" # placeholder
sfai_password = "databricksdemo123" # placeholder

# COMMAND ----------

redshift_secret_name = dbutils.widgets.get("REDSHIFT_SECRET_NAME")
redshift_region_name = dbutils.widgets.get("REDSHIFT_REGION_NAME")

redshift_username = secrets_get(redshift_secret_name, redshift_region_name)["username"]
redshift_password = secrets_get(redshift_secret_name, redshift_region_name)["password"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Extract

# COMMAND ----------

# set table specific parameters
database = "IE2_Prod"
schema = "dbo"
table = "ib"

# extract data from SFAI
ib_df = spark.read \
  .format("jdbc") \
  .option("url", sfai_url + database) \
  .option("dbTable", f"{schema}.{table}") \
  .option("user", sfai_username) \
  .option("password", sfai_password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Transform

# COMMAND ----------

ib_df.createOrReplaceTempView("ib_table")
filtered_ib_df0 = spark.sql("SELECT * FROM ib_table WHERE version = '2021.09.28.1'")

# COMMAND ----------

filtered_ib_df0.createOrReplaceTempView("filtered_ib_table0")
filtered_ib_df1 = spark.sql("SELECT * FROM filtered_ib_table0 WHERE platform_subset='THINKJET'")

# COMMAND ----------

display(filtered_ib_df1)

# COMMAND ----------

filtered_ib_df2 = filtered_ib_df1.filter("country = 'US'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Load

# COMMAND ----------

# mount s3 bucket to cluster
s3_mount("dataos-core-dev-team-phoenix/proto/example-bucket/", "example-bucket")

# COMMAND ----------

filtered_ib_df2.write \
  .format("csv") \
  .mode("overwrite") \
  .save("dbfs:/mnt/example-bucket/test/")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lh ../../dbfs/mnt/example-bucket/test/
