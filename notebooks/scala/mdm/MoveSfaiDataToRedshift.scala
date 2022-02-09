// Databricks notebook source
// MAGIC %run ../../scala/common/DatabaseUtils.scala

// COMMAND ----------

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
val redshiftTimestamp = dbutils.widgets.get("redshiftTimestamp")
val stack = dbutils.widgets.get("stack")

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

tableDF.createOrReplaceTempView("tableDF")

// COMMAND ----------

// rearrange incoming SQL server dataframes to Redshift schema

val inputTableCols = tableDF.columns.map(x => x.toLowerCase)

val schema = if(List("decay", "instant_ink_enrollees_ltf", "version").contains(table)) "prod" else "mdm"

val outputTableCols = spark.read
  .format("com.databricks.spark.redshift")
  .option("url", redshiftUrl)
  .option("dbtable", s"${schema}.${table}")
  .option("tempdir", redshiftTempBucket)
  .option("aws_iam_role", redshiftAwsRole)
  .option("user", redshiftUsername)
  .option("password", redshiftPassword)
  .load()
  .columns

var query = "SELECT \n"

for (col <- outputTableCols; if (!(List("cal_id", "geo_id", "iso_cc_id", "pl_id", "profit_center_id", "rdma_id", "sup_hw_id", "sup_xref_id", "yield_id").contains(col)))) {
  if (col == "id" && table == "hardware_xref") {} else {
    if (inputTableCols.contains(col)) {
      query = query + col
    } else {
      if (col == "official") query = query + "1 AS official"
      if (col == "last_modified_date") query = query + "load_date AS last_modified_date"
      if (col == "profit_center_code" && table == "product_line_xref") query = query + "profit_center AS profit_center_code"
      if (col == "load_date") query = query + s"""\"${redshiftTimestamp}\" AS load_date"""
    }
    
    if (outputTableCols.dropRight(1).contains(col)) query = query + ","
    
    query = query + "\n"
  }
}

val finalTableDF = spark.sql(query + "FROM tableDF")

// COMMAND ----------

// write data to S3
finalTableDF.write
  .format("csv")
  .save(s"s3a://dataos-core-${stack}-team-phoenix/proto/landing/${table}/${datestamp}/${timestamp}/")

// truncate existing redshift data
submitRemoteQuery(redshiftUrl, redshiftUsername, redshiftPassword, s"TRUNCATE ${schema}.${table}")

// write data to redshift
finalTableDF.write
  .format("com.databricks.spark.redshift")
  .option("url", redshiftUrl)
  .option("dbtable", s"${schema}.${table}")
  .option("tempdir", redshiftTempBucket)
  .option("aws_iam_role", redshiftAwsRole)
  .option("user", redshiftUsername)
  .option("password", redshiftPassword)
  .option("tempformat", "CSV GZIP")
  .mode("append")
  .save()

submitRemoteQuery(redshiftUrl, redshiftUsername, redshiftPassword, s"GRANT ALL ON ${schema}.${table} TO group dev_arch_eng")

// COMMAND ----------

dbutils.notebook.exit("SUCCESS")
