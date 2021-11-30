// Databricks notebook source
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

def submitRemoteQuery(url: String, username: String, password: String, query: String) {
  
  var conn: Connection = null
  conn = DriverManager.getConnection(url, username, password)
  if (conn != null) {
      print(s"""Connected to ${url}\n""")
  }

  val statement: Statement = conn.createStatement()
  
  statement.executeUpdate(query)
  
  conn.close()
}

// COMMAND ----------

def readSqlServerToDF(): org.apache.spark.sql.DataFrameReader = {
  val dfReader = spark.read
    .format("jdbc")
    .option("url", SFAI_URL)
    .option("user", sfaiUsername)
    .option("password", sfaiPassword)
    .option("driver", SFAI_DRIVER)
  return dfReader
}

// COMMAND ----------

def readRedshiftToDF(): org.apache.spark.sql.DataFrameReader = {
  val dfReader = spark.read
    .format("com.databricks.spark.redshift")
    .option("url", redshiftUrl)
    .option("tempdir", redshiftTempBucket)
    .option("aws_iam_role", redshiftAwsRole)
    .option("user", redshiftUsername)
    .option("password", redshiftPassword)
  return dfReader
}

// COMMAND ----------

def writeDFToRedshift(dataframe: org.apache.spark.sql.Dataset[Row], destination: String, mode: String): Unit = {
  dataframe.write
  .format("com.databricks.spark.redshift")
  .option("url", redshiftUrl)
  .option("tempdir", redshiftTempBucket)
  .option("aws_iam_role", redshiftAwsRole)
  .option("user", redshiftUsername)
  .option("password", redshiftPassword)
  .option("dbtable", destination)
  .mode(mode)
  .save()
}

// COMMAND ----------

def writeDFToS3(dataframe: org.apache.spark.sql.Dataset[Row], destination: String, format: String = "parquet", mode: String = "error"): Unit = {
  dataframe.write
  .format(format)
  .mode(mode)
  .save(destination)
}

// COMMAND ----------


