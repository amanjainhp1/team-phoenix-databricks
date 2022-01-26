# Databricks notebook source
import boto3
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

# COMMAND ----------

region_name = "us-west-2"
secret_name = dbutils.widgets.get("redshift_secrets_name")
s3_bucket = f"""dataos-core-{dbutils.widgets.get("stack")}-team-phoenix"""
url = f"""jdbc:redshift://dataos-redshift-core-{dbutils.widgets.get("stack")}-01.hp8.us:5439/{dbutils.widgets.get("stack")}?ssl_verify=None"""

# COMMAND ----------

def secrets_get(secret_name):
    endpoint_url = "https://secretsmanager.us-west-2.amazonaws.com"
    client = boto3.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return eval(get_secret_value_response['SecretString'])

# COMMAND ----------

username = secrets_get(secret_name)['username']
password = secrets_get(secret_name)['password']

spark.conf.set('username', username)
spark.conf.set('password', password)
spark.conf.set('url', url)

# COMMAND ----------

import glob
import os
import builtins
list_of_files = glob.glob('../../dbfs/mnt/rdma-load/rdma_product-full*') # * means all if need specific format then *.csv
latest_file = builtins.max(list_of_files, key=os.path.getctime)
latest_file = latest_file.split("/")[len(latest_file.split("/"))-1]
print(latest_file)

# COMMAND ----------

df = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", "") \
.load(f"s3a://hp-bigdata-prod-enrichment/ie2_deliverables/rdma/{latest_file}")

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

#write data to redhift staging
df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", url) \
  .option("dbtable", "stage.rdma_staging") \
  .option("tempdir", f"""s3a://dataos-core-{dbutils.widgets.get("stack")}-team-phoenix/redshift_temp/""") \
  .option("aws_iam_role", dbutils.widgets.get("aws_iam_role")) \
  .option("user", username) \
  .option("password", password) \
  .mode("overwrite") \
  .save()

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.sql.Connection
# MAGIC import java.sql.Statement
# MAGIC import java.sql.DriverManager
# MAGIC 
# MAGIC def submitRemoteQuery(url: String, username: String, password: String, query: String) {
# MAGIC   
# MAGIC   var conn: Connection = null
# MAGIC   conn = DriverManager.getConnection(url, username, password)
# MAGIC   
# MAGIC   if (conn != null) {
# MAGIC       print(s"""Connected to ${url}\n""")
# MAGIC   }
# MAGIC 
# MAGIC   val statement: Statement = conn.createStatement()
# MAGIC   
# MAGIC   statement.executeUpdate(query)
# MAGIC   
# MAGIC   conn.close()
# MAGIC }

# COMMAND ----------
# MAGIC %scala  
# MAGIC submitRemoteQuery({spark.conf.get("url")}, {spark.conf.get("username")}, {spark.conf.get("password")}, s"""CALL prod.p_rdma();""")          

# COMMAND ----------
# MAGIC %scala  
# MAGIC submitRemoteQuery({spark.conf.get("url")}, {spark.conf.get("username")}, {spark.conf.get("password")}, "GRANT ALL ON stage.rdma_staging TO group dev_arch_eng")          
