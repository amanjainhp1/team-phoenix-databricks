// Databricks notebook source
// register database driver/s
Class.forName("com.amazon.redshift.jdbc.Driver")
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

// COMMAND ----------

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

def readSqlServerToDF(configs: Map[String, String]): org.apache.spark.sql.DataFrameReader = {
  val dfReader = spark.read
    .format("jdbc")
    .option("url", configs("sfaiUrl"))
    .option("user", configs("sfaiUsername"))
    .option("password", configs("sfaiPassword"))
  return dfReader
}

// COMMAND ----------

def readRedshiftToDF(configs: Map[String, String]): org.apache.spark.sql.DataFrameReader = {
  val dfReader = spark.read
    .format("com.databricks.spark.redshift")
    .option("url", configs("redshiftUrl"))
    .option("tempdir", configs("redshiftTempBucket"))
    .option("aws_iam_role", configs("redshiftAwsRole"))
    .option("user", configs("redshiftUsername"))
    .option("password", configs("redshiftPassword"))
  return dfReader
}

// COMMAND ----------

def writeDFToRedshift(configs: Map[String, String], dataframe: org.apache.spark.sql.Dataset[Row], destination: String, mode: String, tempformat: String = "AVRO"): Unit = {
  dataframe.write
  .format("com.databricks.spark.redshift")
  .option("url", configs("redshiftUrl"))
  .option("tempdir", configs("redshiftTempBucket"))
  .option("tempformat", tempformat)
  .option("aws_iam_role", configs("redshiftAwsRole"))
  .option("user", configs("redshiftUsername"))
  .option("password", configs("redshiftPassword"))
  .option("dbtable", destination)
  .option("postactions", s"GRANT ALL ON TABLE ${destination} TO GROUP ${configs("redshiftDevGroup")}")
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


