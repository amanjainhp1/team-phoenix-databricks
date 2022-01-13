// Databricks notebook source
val sfaiUrl = dbutils.widgets.get("sfaiUrl")
val sfaiDatabase = dbutils.widgets.get("sfaiDatabase")
val sfaiUsername = dbutils.widgets.get("sfaiUsername")
val sfaiPassword = dbutils.widgets.get("sfaiPassword")
val redshiftUrl = dbutils.widgets.get("redshiftUrl")
val redshiftUsername = dbutils.widgets.get("redshiftUsername")
val redshiftPassword = dbutils.widgets.get("redshiftPassword")
val redshiftTempBucket = dbutils.widgets.get("redshiftTempBucket")
val redshiftAwsRole = dbutils.widgets.get("redshiftAwsRole")
val table = dbutils.widgets.get("table")
val datestamp = dbutils.widgets.get("datestamp")
val timestamp = dbutils.widgets.get("timestamp")

// COMMAND ----------

//retrieve data from SFAI
var tableDF = spark.read
  .format("jdbc")
  .option("url", sfaiUrl + "database=" + sfaiDatabase)
  .option("dbTable", s"""dbo.${table}""")
  .option("user", sfaiUsername)
  .option("password", sfaiPassword)
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .load()

if(table == "hardware_xref") {
  tableDF = tableDF.drop("id")
}

// write data to S3
tableDF.write
  .format("csv")
  .save(s"s3a://dataos-core-dev-team-phoenix/proto/landing/${table}/${datestamp}/${timestamp}/")

// truncate existing redshift data
submitRemoteQuery(redshiftUrl, redshiftUsername, redshiftPassword, s"TRUNCATE mdm.${table}")

// write data to redshift
tableDF.write
  .format("com.databricks.spark.redshift")
  .option("url", redshiftUrl)
  .option("dbtable", s"mdm.${table}")
  .option("tempdir", redshiftTempBucket)
  .option("aws_iam_role", redshiftAwsRole)
  .option("user", redshiftUsername)
  .option("password", redshiftPassword)
  .option("tempformat", "CSV GZIP")
  .mode("overwrite")
  .save()

// COMMAND ----------

dbutils.notebook.exit("SUCCESS")
