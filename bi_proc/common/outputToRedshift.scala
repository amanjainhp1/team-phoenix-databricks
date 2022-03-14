// Databricks notebook source
// MAGIC %md
// MAGIC # Output Tables to Redshift Using SQL Queries

// COMMAND ----------

// MAGIC %run "./get_redshift_secrets"

// COMMAND ----------

// imports
import java.io._
import org.apache.spark.sql.DataFrame
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

// COMMAND ----------

// Global Variables
val username: String = spark.conf.get("username")
val password: String = spark.conf.get("password")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Redshift Class
// MAGIC Parameters: username, password, table name, query
// MAGIC 
// MAGIC Methods:
// MAGIC 
// MAGIC getData() Retrieves query data and returns a dataframe
// MAGIC saveTable() Uses dataframe parameter to create Redshift Table

// COMMAND ----------

class RedshiftOut(username:String, password:String, tableName:String, query:String) {
  def getData() : DataFrame = {
     val dataDF = spark.read
        .format("com.databricks.spark.redshift")
        .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None")
        .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/")
        .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role")
        .option("user", username)
        .option("password", password)
        .option("query", query)
        .load()
     
     return(dataDF)
  }
  
  def saveTable(dataDF:DataFrame ) : Unit = {
      dataDF.write
      .format("com.databricks.spark.redshift")
      .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None")
      .option("dbtable", tableName)
      .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/")
      .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role")
      .option("user", username)
      .option("password", password)
      .mode("overwrite")
      .save()
   }
  
  // Function from Matt Koson for granting permission to dev group
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
}

// COMMAND ----------

var redshiftUrl: String = "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None"

for ((table, query) <- queryList){
  var redObj = new RedshiftOut(username, password, table, query)
  val dataDF = redObj.getData()
  redObj.saveTable(dataDF)
  redObj.submitRemoteQuery(redshiftUrl, username, password, s"GRANT ALL ON ${table} TO group dev_arch_eng")
}
