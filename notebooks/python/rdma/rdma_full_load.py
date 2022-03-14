# Databricks notebook source
import boto3
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as func
from pyspark.sql.types import DoubleType

# COMMAND ----------

import json

with open(dbutils.widgets.get("job_dbfs_path").replace("dbfs:", "/dbfs") + "/configs/constants.json") as json_file:
  constants = json.load(json_file)

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# Retrieve Latest file names
# rdma_base_to_sales latest file
# Retrieve Latest file names
# rdma_base_to_sales latest file
def retrieve_latest_s3_object_by_prefix(bucket, prefix):
  s3 = boto3.resource('s3')
  objects = list(s3.Bucket(bucket).objects.filter(Prefix=prefix))
  objects.sort(key=lambda o: o.last_modified)
  return objects[-1].key

rdma_base_bucket = "hp-bigdata-prod-enrichment"

rdma_base_to_sales_latest_file = retrieve_latest_s3_object_by_prefix(rdma_base_bucket, "ie2_deliverables/rdma/rdma_base_to_sales")
rdma_base_to_sales_latest_file = rdma_base_to_sales_latest_file.split("/")[len(rdma_base_to_sales_latest_file.split("/"))-1]
print(rdma_base_to_sales_latest_file)

rdma_latest_file = retrieve_latest_s3_object_by_prefix(rdma_base_bucket, "ie2_deliverables/rdma/rdma_product-full")
rdma_latest_file = rdma_latest_file.split("/")[len(rdma_latest_file.split("/"))-1]
print(rdma_latest_file)

rdma_sales_product_full_latest_file = retrieve_latest_s3_object_by_prefix(rdma_base_bucket, "ie2_deliverables/rdma/rdma_sales_product-full")
rdma_sales_product_full_latest_file = rdma_sales_product_full_latest_file.split("/")[len(rdma_sales_product_full_latest_file.split("/"))-1]
print(rdma_sales_product_full_latest_file)

rdma_sales_product_option_latest_file = retrieve_latest_s3_object_by_prefix(rdma_base_bucket, "ie2_deliverables/rdma/rdma_sales_product_option")
rdma_sales_product_option_latest_file = rdma_sales_product_option_latest_file.split("/")[len(rdma_sales_product_option_latest_file.split("/"))-1]
print(rdma_sales_product_option_latest_file)

# COMMAND ----------

# reading S3 data
rdma_base_to_sales_df = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", "") \
.load(f"s3a://{rdma_base_bucket}/ie2_deliverables/rdma/{rdma_base_to_sales_latest_file}")

rdma_df = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", "") \
.load(f"s3a://{rdma_base_bucket}/ie2_deliverables/rdma/{rdma_latest_file}")

rdma_sales_product_full_df = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", "") \
.load(f"s3a://{rdma_base_bucket}/ie2_deliverables/rdma/{rdma_sales_product_full_latest_file}")

rdma_sales_product_option_df = spark.read \
.format("com.databricks.spark.csv") \
.option("header","True") \
.option("sep", "") \
.load(f"s3a://{rdma_base_bucket}/ie2_deliverables/rdma/{rdma_sales_product_option_latest_file}")

# COMMAND ----------

def dynamic_date(col, frmts=("dd-MMM-yy","yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss:SSS", "yyyy-MM-dd HH:mm:ss.SSSSS")): ##add new
  return func.coalesce(*[func.to_timestamp(col, i) for i in frmts])

# COMMAND ----------

# transformations in rdma_base_to_sales_df
rdma_base_to_sales_df = rdma_base_to_sales_df \
  .withColumn("base_product_id", rdma_base_to_sales_df["base_product_id"].cast(IntegerType())) \
  .withColumn("Sales_Product_ID", rdma_base_to_sales_df["Sales_Product_ID"].cast(IntegerType())) \
  .withColumn("Base_Prod_Per_Sales_Prod_Qty", rdma_base_to_sales_df["Base_Prod_Per_Sales_Prod_Qty"].cast(IntegerType())) \
  .withColumn("Base_Product_Amount_Percent", rdma_base_to_sales_df["Base_Product_Amount_Percent"].cast(IntegerType())) \
  .withColumn("Insert_Timestamp", dynamic_date(func.col("Insert_Timestamp"))) \
  .withColumn("Modified_Timestamp", dynamic_date(func.col("Modified_Timestamp"))) \
  .withColumn("load_date", dynamic_date(func.col("load_date")))

# COMMAND ----------

# Add colums in rdma_df
rdma_df = rdma_df \
  .withColumn("Product_Status_Date", dynamic_date(func.col("Product_Status_Date"))) \
  .withColumn("Sheets_Per_Pack_Quantity", rdma_df["Sheets_Per_Pack_Quantity"].cast(DoubleType())) \
  .withColumn("Ink_Available_CCs_Quantity", rdma_df["Ink_Available_CCs_Quantity"].cast(DoubleType())) \
  .withColumn("Base_Prod_AFR_Goal_Percent", rdma_df["Base_Prod_AFR_Goal_Percent"].cast(DoubleType())) \
  .withColumn("Base_Prod_Acpt_Goal_Range_Pc", rdma_df["Base_Prod_Acpt_Goal_Range_Pc"].cast(DoubleType())) \
  .withColumn("Base_Prod_TFR_Goal_Percent", rdma_df["Base_Prod_TFR_Goal_Percent"].cast(DoubleType())) \
  .withColumn('Category', lit(None).cast(StringType())) \
  .withColumn('Tone', lit(None).cast(StringType())) \
  .withColumn('ID', lit(None).cast(IntegerType())) \
  .select('Base_Prod_Number','PL','Base_Prod_Name','Base_Prod_Desc','Product_Lab_Name','Prev_Base_Prod_Number','Product_Class','Platform','Product_Platform_Desc','Product_Platform_Status_CD','Product_Platform_Group','Product_Family','Product_Family_Desc','Product_Family_Status_CD','FMC','Platform_Subset','Platform_Subset_ID','Platform_Subset_Desc','Platform_Subset_Status_CD','Product_Technology_Name','Product_Technology_ID','Media_Size','Product_Status','Product_Status_Date','Product_Type','Sheets_Per_Pack_Quantity','After_Market_Flag','CoBranded_Product_Flag','Forecast_Assumption_Flag','Base_Product_Known_Flag','OEM_Flag','Selectability_Code','Supply_Color','Ink_Available_CCs_Quantity','SPS_Type','Warranty_Reporting_Flag','Base_Prod_AFR_Goal_Percent','Refurb_Product_Flag','Base_Prod_Acpt_Goal_Range_Pc','Base_Prod_TFR_Goal_Percent','Engine_Type','Engine_Type_ID','Market_Category_Name','Finl_Market_Category','Category','Tone','Platform_Subset_Group_Name','ID','Attribute_Extension1','Leveraged_Product_Platform_Name','Platform_Subset_Group_Status_Cd','Platform_Subset_Group_Desc','Product_Segment_Name')

# COMMAND ----------

# data type change in rdma_sales_product_full_df
rdma_sales_product_full_df = rdma_sales_product_full_df \
  .withColumn("SALES_PROD_ID", rdma_sales_product_full_df["SALES_PROD_ID"].cast(IntegerType())) \
  .withColumn("SALES_PROD_AFR_GOAL_PC", rdma_sales_product_full_df["SALES_PROD_AFR_GOAL_PC"].cast(IntegerType())) \
  .withColumn("multipack_ky", rdma_sales_product_full_df["multipack_ky"].cast(IntegerType())) \
  .withColumn("EARLIEST_INTRO_DT", dynamic_date(func.col("EARLIEST_INTRO_DT"))) \
  .withColumn("OBSOLETE_DT", dynamic_date(func.col("OBSOLETE_DT"))) \
  .withColumn("INSERT_TS", dynamic_date(func.col("INSERT_TS"))) \
  .withColumn("MODIFIED_TS", dynamic_date(func.col("MODIFIED_TS"))) \
  .withColumn("load_date", dynamic_date(func.col("load_date")))

# COMMAND ----------

# data type change in rdma_sales_product_full_df
rdma_sales_product_option_df = rdma_sales_product_option_df \
  .withColumn("SALES_PROD_ID", rdma_sales_product_option_df["SALES_PROD_ID"].cast(IntegerType())) \
  .withColumn("SALES_PROD_W_OPTION_KY", rdma_sales_product_option_df["SALES_PROD_W_OPTION_KY"].cast(IntegerType())) \
  .withColumn("INSERT_TS", dynamic_date(func.col("INSERT_TS"))) \
  .withColumn("UPDATE_TS", dynamic_date(func.col("UPDATE_TS"))) \
  .withColumn("MODIFIED_TS", dynamic_date(func.col("MODIFIED_TS"))) \
  .withColumn("load_date", dynamic_date(func.col("load_date")))

# COMMAND ----------

redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")

configs = {}
configs["redshift_temp_bucket"] = "{}redshift_temp/".format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")])
configs["redshift_username"] = redshift_secrets["username"]
configs["redshift_password"] = redshift_secrets["password"]
configs["redshift_url"] = constants['REDSHIFT_URLS'][dbutils.widgets.get("stack")]
configs["redshift_port"] = constants['REDSHIFT_PORTS'][dbutils.widgets.get("stack")]
configs["redshift_dbname"] = dbutils.widgets.get("stack")
configs["aws_iam_role"] =  dbutils.widgets.get("aws_iam_role")

# COMMAND ----------

#write data to redshift
write_df_to_redshift(configs, rdma_base_to_sales_df, "prod.rdma_base_to_sales_product_map", "overwrite")

# COMMAND ----------

# write data to redshift
write_df_to_redshift(configs, rdma_df, "stage.rdma_staging", "overwrite", "CALL prod.p_rdma();")

# COMMAND ----------

# write data to redshift
write_df_to_redshift(configs, rdma_sales_product_full_df, "prod.rdma_sales_product", "overwrite")

# COMMAND ----------

# write data to redshift
write_df_to_redshift(configs, rdma_sales_product_option_df, "prod.rdma_sales_product_option", "overwrite")