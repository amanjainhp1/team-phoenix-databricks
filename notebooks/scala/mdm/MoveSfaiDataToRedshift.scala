// Databricks notebook source
// MAGIC %run ../../scala/common/DatabaseUtils

// COMMAND ----------

var configs: Map[String, String] = Map()
configs += ("stack" -> dbutils.widgets.get("stack"),
            "sfaiUsername" -> dbutils.widgets.get("sfaiUsername"),
            "sfaiPassword" -> dbutils.widgets.get("sfaiPassword"),
            "sfaiUrl" -> dbutils.widgets.get("sfaiUrl"),
            "redshiftUsername" -> dbutils.widgets.get("redshiftUsername"),
            "redshiftPassword" -> dbutils.widgets.get("redshiftPassword"),
            "redshiftAwsRole" -> dbutils.widgets.get("redshiftAwsRole"),
            "redshiftUrl" -> dbutils.widgets.get("redshiftUrl"),
            "redshiftTempBucket" -> dbutils.widgets.get("redshiftTempBucket"),
            "sfaiDatabase" -> dbutils.widgets.get("sfaiDatabase"),
            "table" -> dbutils.widgets.get("table"),
            "datestamp" -> dbutils.widgets.get("datestamp"),
            "timestamp" -> dbutils.widgets.get("timestamp"),
            "redshiftTimestamp" -> dbutils.widgets.get("redshiftTimestamp"),
            "redshiftDevGroup" -> dbutils.widgets.get("redshiftDevGroup"))

// COMMAND ----------

//retrieve data from SFAI
var tableDF = readSqlServerToDF(configs)
  .option("dbTable", s"""${configs("sfaiDatabase")}.dbo.${configs("table")}""")
  .load()

tableDF.createOrReplaceTempView("tableDF")

// COMMAND ----------

// rearrange incoming SQL server dataframes to Redshift schema

val inputTableCols = tableDF.columns.map(x => x.toLowerCase)

val schema = if(List("decay", "hardware_ltf", "version").contains(configs("table"))) "prod" else "mdm"

val outputTableCols = readRedshiftToDF(configs)
  .option("dbtable", s"""${schema}.${configs("table")}""")
  .load()
  .columns

var query = "SELECT \n"

for (col <- outputTableCols; if (!(List("cal_id", "decay_id", "geo_id", "hw_ltf_id", "iso_cc_id", "pl_id", "profit_center_id", "rdma_id", "sup_hw_id", "sup_xref_id", "yield_id").contains(col)))) {
  if (col == "id" && configs("table") == "hardware_xref") {} else {
    if (inputTableCols.contains(col)) {
      query = query + col
    } else {
      if (col == "official") query = query + "1 AS official"
      if (col == "last_modified_date") query = query + "load_date AS last_modified_date"
      if (col == "profit_center_code" && configs("table") == "product_line_xref") query = query + "profit_center AS profit_center_code"
      if (col == "load_date") query = query + s"""CAST(\"${configs("redshiftTimestamp")}\" AS TIMESTAMP) AS load_date"""
    }
    
    if (outputTableCols.dropRight(1).contains(col)) query = query + ","
    
    query = query + "\n"
  }
}

val finalTableDF = spark.sql(query + "FROM tableDF")

// COMMAND ----------

// write data to S3
writeDFToS3(finalTableDF, s"""s3a://dataos-core-${configs("stack")}-team-phoenix/landing/${configs("table")}/${configs("datestamp")}/${configs("timestamp")}/""", "csv", "overwrite")

// truncate existing redshift data and
// write data to redshift
writeDFToRedshift(configs, finalTableDF, s"""${configs("stack")}.${configs("table")}""", "append", "CSV GZIP", "", s"""TRUNCATE ${schema}.${configs("table")}""")

// COMMAND ----------

dbutils.notebook.exit("SUCCESS")
