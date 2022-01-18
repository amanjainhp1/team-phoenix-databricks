# Databricks notebook source
import boto3
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

# COMMAND ----------

region_name = "us-west-2"
secret_name = "arn:aws:secretsmanager:us-west-2:740156627385:secret:dev/redshift/dataos-core-dev-01/auto_glue-dj6tOj"
s3_bucket = "dataos-core-dev-team-phoenix"

# COMMAND ----------

def secrets_get(secret_name):
    endpoint_url = "https://secretsmanager.us-west-2.amazonaws.com"
    client = boto3.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return eval(get_secret_value_response['SecretString'])

# COMMAND ----------

username = secrets_get(secret_name)['username']
password = secrets_get(secret_name)['password']

spark.conf.set('username',username)
spark.conf.set('password',password)

# COMMAND ----------

# MAGIC %scala
# MAGIC val username: String = spark.conf.get("username")
# MAGIC val password: String = spark.conf.get("password")

# COMMAND ----------

# MAGIC %sh
# MAGIC aws s3 ls s3://hp-bigdata-prod-enrichment/ie2_deliverables/rdma/

# COMMAND ----------

# MAGIC %scala
# MAGIC val rdmaDf = spark.read
# MAGIC   .format("com.databricks.spark.csv")
# MAGIC   .option("header","True")
# MAGIC   .option("sep", "")
# MAGIC   .load("s3a://hp-bigdata-prod-enrichment/ie2_deliverables/rdma/rdma_product-full{*}")
# MAGIC 
# MAGIC rdmaDf.createOrReplaceTempView("df")

# COMMAND ----------

df= spark.sql('select * from df')

# COMMAND ----------

from pyspark.sql.functions import datediff,col
from pyspark.sql import functions as func
def dynamic_date(col, frmts=("dd-MMM-yy","yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss:SSS", "yyyy-MM-dd HH:mm:ss.SSSSS")): ##add new
  return func.coalesce(*[func.to_timestamp(col, i) for i in frmts])

df = df.withColumn("Product_Status_Date", dynamic_date(func.col("Product_Status_Date")))

# COMMAND ----------

from pyspark.sql.types import DoubleType
df=df.withColumn("Sheets_Per_Pack_Quantity", df["Sheets_Per_Pack_Quantity"].cast(DoubleType()))
df=df.withColumn("Ink_Available_CCs_Quantity", df["Ink_Available_CCs_Quantity"].cast(DoubleType()))
df=df.withColumn("Base_Prod_AFR_Goal_Percent", df["Base_Prod_AFR_Goal_Percent"].cast(DoubleType()))
df=df.withColumn("Base_Prod_Acpt_Goal_Range_Pc", df["Base_Prod_Acpt_Goal_Range_Pc"].cast(DoubleType()))
df=df.withColumn("Base_Prod_TFR_Goal_Percent", df["Base_Prod_TFR_Goal_Percent"].cast(DoubleType()))

# COMMAND ----------

df=df.withColumn('Category',lit(None).cast(StringType()))
df=df.withColumn('Tone',lit(None).cast(StringType()))

# COMMAND ----------

df=df.withColumn('ID',lit(None).cast(IntegerType()))

# COMMAND ----------

df=df.select('Base_Prod_Number','PL','Base_Prod_Name','Base_Prod_Desc','Product_Lab_Name','Prev_Base_Prod_Number','Product_Class','Platform','Product_Platform_Desc','Product_Platform_Status_CD','Product_Platform_Group','Product_Family','Product_Family_Desc','Product_Family_Status_CD','FMC','Platform_Subset','Platform_Subset_ID','Platform_Subset_Desc','Platform_Subset_Status_CD','Product_Technology_Name','Product_Technology_ID','Media_Size','Product_Status','Product_Status_Date','Product_Type','Sheets_Per_Pack_Quantity','After_Market_Flag','CoBranded_Product_Flag','Forecast_Assumption_Flag','Base_Product_Known_Flag','OEM_Flag','Selectability_Code','Supply_Color','Ink_Available_CCs_Quantity','SPS_Type','Warranty_Reporting_Flag','Base_Prod_AFR_Goal_Percent','Refurb_Product_Flag','Base_Prod_Acpt_Goal_Range_Pc','Base_Prod_TFR_Goal_Percent','Engine_Type','Engine_Type_ID','Market_Category_Name','Finl_Market_Category','Category','Tone','Platform_Subset_Group_Name','ID','Attribute_Extension1','Leveraged_Product_Platform_Name','Platform_Subset_Group_Status_Cd','Platform_Subset_Group_Desc','Product_Segment_Name')

# COMMAND ----------

df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
  .option("dbtable", "stage.rdma_stage") \
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
  .option("user", username) \
  .option("password", password) \
  .mode("overwrite") \
  .save()

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.sql.Connection
# MAGIC import java.sql.DriverManager
# MAGIC import java.sql.ResultSet
# MAGIC import java.sql.ResultSetMetaData
# MAGIC import java.sql.SQLException
# MAGIC import java.sql.Statement
# MAGIC import java.util.StringJoiner
# MAGIC 
# MAGIC class RedshiftQuery (var username: String, var password: String, var redshiftQuery: String) {
# MAGIC   
# MAGIC   var conn: Connection = null
# MAGIC   conn = DriverManager.getConnection("jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None",username,password)
# MAGIC   if (conn != null) {
# MAGIC       print("Connected to Redshift\n")
# MAGIC   }
# MAGIC 
# MAGIC   val statement: Statement = conn.createStatement()
# MAGIC   val query: String = redshiftQuery
# MAGIC   val queryLower: String = redshiftQuery.toLowerCase()
# MAGIC   
# MAGIC   try {
# MAGIC     if (queryLower.contains("select")) {
# MAGIC      val  resultSet: ResultSet = statement.executeQuery(query)
# MAGIC       val rsmd: ResultSetMetaData = resultSet.getMetaData()
# MAGIC       val columnsNumber: Int = rsmd.getColumnCount();
# MAGIC       
# MAGIC       print("Printing result...\n")
# MAGIC 
# MAGIC       while (resultSet.next()) {
# MAGIC         val sj: StringJoiner = new StringJoiner("|")
# MAGIC         for (i <- 1 to columnsNumber) {
# MAGIC           val columnValue: String = resultSet.getString(i)
# MAGIC           sj.add(rsmd.getColumnName(i) + ": " + columnValue)
# MAGIC         }
# MAGIC         print(sj.toString() + "\n")
# MAGIC       }
# MAGIC       
# MAGIC     } else if (queryLower.contains("delete") || queryLower.contains("call") || queryLower.contains("truncate")) {
# MAGIC       statement.executeUpdate(query)
# MAGIC     }
# MAGIC     conn.close()
# MAGIC   
# MAGIC   } catch {
# MAGIC     case e: SQLException => println("SQL exception. Check your SQL query")
# MAGIC     case _: Throwable => println("Got some other kind of Throwable exception")
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC new RedshiftQuery(username,password,s"""CALL prod.p_rdma();""")

# COMMAND ----------


