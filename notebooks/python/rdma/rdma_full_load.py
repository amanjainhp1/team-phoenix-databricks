# Databricks notebook source
import boto3
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import datediff,col
from pyspark.sql import functions as func
from pyspark.sql.types import DoubleType

# COMMAND ----------

import json
with open(f"""{dbutils.widgets.get("job_dbfs_path")}configs/constants.json""") as json_file:
  constants=json.load(json_file)

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

# Retrieve Latest file names
# rdma_base_to_sales latest file
import glob
import os
import builtins
rdma_base_to_sales_list_of_files = glob.glob('../../dbfs/mnt/rdma-load/rdma_base_to_sales*') # * means all if need specific format then *.csv
rdma_base_to_sales_latest_file = builtins.max(rdma_base_to_sales_list_of_files, key=os.path.getctime)
rdma_base_to_sales_latest_file = rdma_base_to_sales_latest_file.split("/")[len(rdma_base_to_sales_latest_file.split("/"))-1]
print(rdma_base_to_sales_latest_file)

# rdma latest file
rdma_list_of_files = glob.glob('../../dbfs/mnt/rdma-load/rdma_product-full*') # * means all if need specific format then *.csv
rdma_latest_file = builtins.max(rdma_list_of_files, key=os.path.getctime)
rdma_latest_file = rdma_latest_file.split("/")[len(rdma_latest_file.split("/"))-1]
print(rdma_latest_file)

# rdma_sales_product latest file
rdma_sales_product_full_list_of_files = glob.glob('../../dbfs/mnt/rdma-load/rdma_sales_product-full*') # * means all if need specific format then *.csv
rdma_sales_product_full_latest_file = builtins.max(rdma_sales_product_full_list_of_files, key=os.path.getctime)
rdma_sales_product_full_latest_file = rdma_sales_product_full_latest_file.split("/")[len(rdma_sales_product_full_latest_file.split("/"))-1]
print(rdma_sales_product_full_latest_file)

# rdma_sales_product_option latest file
rdma_sales_product_option_list_of_files = glob.glob('../../dbfs/mnt/rdma-load/rdma_sales_product_option*') # * means all if need specific format then *.csv
rdma_sales_product_option_latest_file = builtins.max(rdma_sales_product_option_list_of_files, key=os.path.getctime)
rdma_sales_product_option_latest_file = rdma_sales_product_option_latest_file.split("/")[len(rdma_sales_product_option_latest_file.split("/"))-1]
print(rdma_sales_product_option_latest_file)

# COMMAND ----------

# reading S3 data
rdma_base_to_sales_df = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", "") \
.load(f"s3a://hp-bigdata-prod-enrichment/ie2_deliverables/rdma/{rdma_base_to_sales_latest_file}")

rdma_df = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", "") \
.load(f"s3a://hp-bigdata-prod-enrichment/ie2_deliverables/rdma/{rdma_latest_file}")

rdma_sales_product_full_df = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", "") \
.load(f"s3a://hp-bigdata-prod-enrichment/ie2_deliverables/rdma/{rdma_sales_product_full_latest_file}")

rdma_sales_product_option_df = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", "") \
.load(f"s3a://hp-bigdata-prod-enrichment/ie2_deliverables/rdma/{rdma_sales_product_option_latest_file}")

# COMMAND ----------

def dynamic_date(col, frmts=("dd-MMM-yy","yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss:SSS", "yyyy-MM-dd HH:mm:ss.SSSSS")): ##add new
  return func.coalesce(*[func.to_timestamp(col, i) for i in frmts])

# COMMAND ----------

# transformations in rdma_base_to_sales_df
rdma_base_to_sales_df = rdma_base_to_sales_df.withColumn("base_product_id",rdma_base_to_sales_df["base_product_id"].cast(IntegerType()))
rdma_base_to_sales_df = rdma_base_to_sales_df.withColumn("Sales_Product_ID",rdma_base_to_sales_df["Sales_Product_ID"].cast(IntegerType()))
rdma_base_to_sales_df = rdma_base_to_sales_df.withColumn("Base_Prod_Per_Sales_Prod_Qty",rdma_base_to_sales_df["Base_Prod_Per_Sales_Prod_Qty"].cast(IntegerType()))
rdma_base_to_sales_df = rdma_base_to_sales_df.withColumn("Base_Product_Amount_Percent",rdma_base_to_sales_df["Base_Product_Amount_Percent"].cast(IntegerType()))
rdma_base_to_sales_df = rdma_base_to_sales_df.withColumn("Insert_Timestamp", dynamic_date(func.col("Insert_Timestamp")))
rdma_base_to_sales_df = rdma_base_to_sales_df.withColumn("Modified_Timestamp", dynamic_date(func.col("Modified_Timestamp")))
rdma_base_to_sales_df = rdma_base_to_sales_df.withColumn("load_date", dynamic_date(func.col("load_date")))

# COMMAND ----------

# Add colums in rdma_df
rdma_df = rdma_df.withColumn("Product_Status_Date", dynamic_date(func.col("Product_Status_Date")))
rdma_df = rdma_df.withColumn("Sheets_Per_Pack_Quantity", rdma_df["Sheets_Per_Pack_Quantity"].cast(DoubleType()))
rdma_df = rdma_df.withColumn("Ink_Available_CCs_Quantity", rdma_df["Ink_Available_CCs_Quantity"].cast(DoubleType()))
rdma_df = rdma_df.withColumn("Base_Prod_AFR_Goal_Percent", rdma_df["Base_Prod_AFR_Goal_Percent"].cast(DoubleType()))
rdma_df = rdma_df.withColumn("Base_Prod_Acpt_Goal_Range_Pc", rdma_df["Base_Prod_Acpt_Goal_Range_Pc"].cast(DoubleType()))
rdma_df = rdma_df.withColumn("Base_Prod_TFR_Goal_Percent", rdma_df["Base_Prod_TFR_Goal_Percent"].cast(DoubleType()))
rdma_df = rdma_df.withColumn('Category',lit(None).cast(StringType()))
rdma_df = rdma_df.withColumn('Tone',lit(None).cast(StringType()))
rdma_df = rdma_df.withColumn('ID',lit(None).cast(IntegerType()))
rdma_df=rdma_df.select('Base_Prod_Number','PL','Base_Prod_Name','Base_Prod_Desc','Product_Lab_Name','Prev_Base_Prod_Number','Product_Class','Platform','Product_Platform_Desc','Product_Platform_Status_CD','Product_Platform_Group','Product_Family','Product_Family_Desc','Product_Family_Status_CD','FMC','Platform_Subset','Platform_Subset_ID','Platform_Subset_Desc','Platform_Subset_Status_CD','Product_Technology_Name','Product_Technology_ID','Media_Size','Product_Status','Product_Status_Date','Product_Type','Sheets_Per_Pack_Quantity','After_Market_Flag','CoBranded_Product_Flag','Forecast_Assumption_Flag','Base_Product_Known_Flag','OEM_Flag','Selectability_Code','Supply_Color','Ink_Available_CCs_Quantity','SPS_Type','Warranty_Reporting_Flag','Base_Prod_AFR_Goal_Percent','Refurb_Product_Flag','Base_Prod_Acpt_Goal_Range_Pc','Base_Prod_TFR_Goal_Percent','Engine_Type','Engine_Type_ID','Market_Category_Name','Finl_Market_Category','Category','Tone','Platform_Subset_Group_Name','ID','Attribute_Extension1','Leveraged_Product_Platform_Name','Platform_Subset_Group_Status_Cd','Platform_Subset_Group_Desc','Product_Segment_Name')

# COMMAND ----------

# data type change in rdma_sales_product_full_df
rdma_sales_product_full_df = rdma_sales_product_full_df.withColumn("SALES_PROD_ID",rdma_sales_product_full_df["SALES_PROD_ID"].cast(IntegerType()))
rdma_sales_product_full_df = rdma_sales_product_full_df.withColumn("SALES_PROD_AFR_GOAL_PC",rdma_sales_product_full_df["SALES_PROD_AFR_GOAL_PC"].cast(IntegerType()))
rdma_sales_product_full_df = rdma_sales_product_full_df.withColumn("multipack_ky",rdma_sales_product_full_df["multipack_ky"].cast(IntegerType()))
rdma_sales_product_full_df = rdma_sales_product_full_df.withColumn("EARLIEST_INTRO_DT", dynamic_date(func.col("EARLIEST_INTRO_DT")))
rdma_sales_product_full_df = rdma_sales_product_full_df.withColumn("OBSOLETE_DT", dynamic_date(func.col("OBSOLETE_DT")))
rdma_sales_product_full_df = rdma_sales_product_full_df.withColumn("INSERT_TS", dynamic_date(func.col("INSERT_TS")))
rdma_sales_product_full_df = rdma_sales_product_full_df.withColumn("MODIFIED_TS", dynamic_date(func.col("MODIFIED_TS")))
rdma_sales_product_full_df = rdma_sales_product_full_df.withColumn("load_date", dynamic_date(func.col("load_date")))

# COMMAND ----------

# data type change in rdma_sales_product_full_df
rdma_sales_product_option_df = rdma_sales_product_option_df.withColumn("SALES_PROD_ID",rdma_sales_product_option_df["SALES_PROD_ID"].cast(IntegerType()))
rdma_sales_product_option_df = rdma_sales_product_option_df.withColumn("SALES_PROD_W_OPTION_KY",rdma_sales_product_option_df["SALES_PROD_W_OPTION_KY"].cast(IntegerType()))
rdma_sales_product_option_df = rdma_sales_product_option_df.withColumn("INSERT_TS", dynamic_date(func.col("INSERT_TS")))
rdma_sales_product_option_df = rdma_sales_product_option_df.withColumn("UPDATE_TS", dynamic_date(func.col("UPDATE_TS")))
rdma_sales_product_option_df = rdma_sales_product_option_df.withColumn("MODIFIED_TS", dynamic_date(func.col("MODIFIED_TS")))
rdma_sales_product_option_df = rdma_sales_product_option_df.withColumn("load_date", dynamic_date(func.col("load_date")))

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

redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")
spark.conf.set("redshift_username", redshift_secrets["username"])
spark.conf.set("redshift_password", redshift_secrets["password"])

dev_rs_url=constants['REDSHIFT_URLS'][dbutils.widgets.get("stack")]
dev_rs_dbname=dbutils.widgets.get("stack")
dev_rs_user_ref=spark.conf.get("redshift_username")
dev_rs_pw_ref=spark.conf.get("redshift_password")
dev_jdbc_url_ref = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(dev_rs_url, dev_rs_dbname, dev_rs_user_ref, dev_rs_pw_ref)

# COMMAND ----------

#write data to redhift
url = """jdbc:redshift://{}:5439/{}?ssl_verify=None""".format(constants['REDSHIFT_URLS'][dbutils.widgets.get("stack")],dbutils.widgets.get("stack"))
spark.conf.set('url', url)

rdma_base_to_sales_df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", url) \
  .option("dbtable", "prod.rdma_base_to_sales_product_map") \
  .option("tempdir", f"""s3a://dataos-core-{dbutils.widgets.get("stack")}-team-phoenix/redshift_temp/""") \
  .option("aws_iam_role", dbutils.widgets.get("aws_iam_role")) \
  .option("user", dev_rs_user_ref) \
  .option("password", dev_rs_pw_ref) \
  .option("postactions","GRANT ALL ON prod.rdma_base_to_sales_product_map TO group dev_arch_eng") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

# write data to redshift
rdma_df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", url) \
  .option("dbtable", "stage.rdma_staging") \
  .option("tempdir", f"""s3a://dataos-core-{dbutils.widgets.get("stack")}-team-phoenix/redshift_temp/""") \
  .option("aws_iam_role", dbutils.widgets.get("aws_iam_role")) \
  .option("user", dev_rs_user_ref) \
  .option("password", dev_rs_pw_ref) \
  .option("postactions","GRANT ALL ON stage.rdma_staging TO group dev_arch_eng") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

# MAGIC %scala  
# MAGIC submitRemoteQuery({spark.conf.get("url")}, {spark.conf.get("redshift_username")}, {spark.conf.get("redshift_password")}, s"""CALL prod.p_rdma();""") 

# COMMAND ----------

# write data to redshift
rdma_sales_product_full_df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", url) \
  .option("dbtable", "prod.rdma_sales_product") \
  .option("tempdir", f"""s3a://dataos-core-{dbutils.widgets.get("stack")}-team-phoenix/redshift_temp/""") \
  .option("aws_iam_role", dbutils.widgets.get("aws_iam_role")) \
  .option("user", dev_rs_user_ref) \
  .option("password", dev_rs_pw_ref) \
  .option("postactions","GRANT ALL ON prod.rdma_sales_product TO group dev_arch_eng") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

# write data to redshift
rdma_sales_product_option_df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", url) \
  .option("dbtable", "prod.rdma_sales_product_option") \
  .option("tempdir", f"""s3a://dataos-core-{dbutils.widgets.get("stack")}-team-phoenix/redshift_temp/""") \
  .option("aws_iam_role", dbutils.widgets.get("aws_iam_role")) \
  .option("tempformat", "CSV") \
  .option("user", dev_rs_user_ref) \
  .option("password", dev_rs_pw_ref) \
  .option("postactions","GRANT ALL ON prod.rdma_sales_product_option TO group dev_arch_eng") \
  .mode("overwrite") \
  .save()
