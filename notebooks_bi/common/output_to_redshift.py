# Databricks notebook source
# MAGIC %md
# MAGIC # Output Tables to Redshift Using SQL Queries

# COMMAND ----------

# imports
import json
import pyspark.sql.functions as func
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../../notebooks/python/common/secrets_manager_utils

# COMMAND ----------

# Global Variables
# redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")
redshift_secrets = secrets_get("arn:aws:secretsmanager:us-west-2:740156627385:secret:dev/redshift/dataos-core-dev-01/auto_glue-dj6tOj", "us-west-2")
spark.conf.set("username", redshift_secrets["username"])
spark.conf.set("password", redshift_secrets["password"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Redshift Class
# MAGIC Parameters: username, password, table name, query
# MAGIC 
# MAGIC Methods:
# MAGIC 
# MAGIC getData() Retrieves query data and returns a dataframe
# MAGIC saveTable() Uses dataframe parameter to create Redshift Table

# COMMAND ----------

class RedshiftOut(username, password, tableName, query) {
  def getData() = {
     dataDF = spark.read \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
        .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
        .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
        .option("user", username) \
        .option("password", password) \
        .option("query", query) \
        .load()
     
     return(dataDF)
  }
  
  def saveTable(dataDF) = {
      dataDF.write \
      .format("com.databricks.spark.redshift") \
      .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
      .option("dbtable", tableName) \
      .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
      .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
      .option("user", username) \
      .option("password", password) \
      .mode("overwrite")
      .save()
   }
  
  // Function from Matt Koson for granting permission to dev group
  def submitRemoteQuery(url, username, password, query) {

    conn = null
    conn = DriverManager.getConnection(url, username, password)

    if (conn != null) {
      print(s"""Connected to ${url}\n""")
    }

    statement = conn.createStatement()

    statement.executeUpdate(query)

    conn.close()
  }
}

# COMMAND ----------

redshiftUrl = "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None"

for ((table, query) <- queryList){
  redObj = RedshiftOut(username, password, table, query)
  dataDF = redObj.getData()
  redObj.saveTable(dataDF)
  redObj.submitRemoteQuery(redshiftUrl, username, password, s"GRANT ALL ON ${table} TO group dev_arch_eng")
}
