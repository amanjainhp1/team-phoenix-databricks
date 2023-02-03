# Databricks notebook source
import boto3
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as func
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %run ../common/configs

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

# data type change in rdma_sales_product_option_df
rdma_sales_product_option_df = rdma_sales_product_option_df \
    .withColumn("SALES_PROD_ID", rdma_sales_product_option_df["SALES_PROD_ID"].cast(IntegerType())) \
    .withColumn("SALES_PROD_W_OPTION_KY", rdma_sales_product_option_df["SALES_PROD_W_OPTION_KY"].cast(IntegerType())) \
    .withColumn("INSERT_TS", dynamic_date(func.col("INSERT_TS"))) \
    .withColumn("UPDATE_TS", dynamic_date(func.col("UPDATE_TS"))) \
    .withColumn("MODIFIED_TS", dynamic_date(func.col("MODIFIED_TS"))) \
    .withColumn("load_date", dynamic_date(func.col("load_date"))) \
    .withColumnRenamed(" insert_user_nm", "insert_user_nm")

# COMMAND ----------

#write data to redshift
write_df_to_redshift(configs=configs, df=rdma_base_to_sales_df, destination="mdm.rdma_base_to_sales_product_map", mode="append", preactions="TRUNCATE mdm.rdma_base_to_sales_product_map;")

# COMMAND ----------

# get the current date and time
current_date = datetime.now()
print("current_date:", current_date)

# COMMAND ----------

# write data to redshift

rdma_sproc = f"""
--------------------------------------CAPTURE NEW BASE_PROD_NUMBERS-----------------------------------------------
	
INSERT INTO mdm.rdma_changelog (base_prod_number 
	,pl 
	,base_prod_name 
	,base_prod_desc
	,product_lab_name
	,prev_base_prod_number
	,product_class 
	,platform 
	,product_platform_desc 
	,product_platform_status_cd 
	,product_platform_group 
	,product_family 
	,product_family_desc 
	,product_family_status_cd 
	,fmc 
	,platform_subset 
	,platform_subset_id
	,platform_subset_desc 
	,platform_subset_status_cd 
	,product_technology_name 
	,product_technology_id 
	,media_size 
	,product_status
	,product_status_date 
	,product_type 
	,sheets_per_pack_quantity 
	,after_market_flag 
	,cobranded_product_flag 
	,forecast_assumption_flag
	,base_product_known_flag 
	,oem_flag 
	,selectability_code 
	,supply_color 
	,ink_available_ccs_quantity 
	,sps_type 
	,warranty_reporting_flag 
	,base_prod_afr_goal_percent 
	,refurb_product_flag 
	,base_prod_acpt_goal_range_pc 
	,base_prod_tfr_goal_percent 
	,engine_type 
	,engine_type_id 
	,market_category_name 
	,finl_market_category 
	,category 
	,tone 
	,platform_subset_group_name 
	,id
	,attribute_extension1 
	,leveraged_product_platform_name 
	,platform_subset_group_status_cd 
	,platform_subset_group_desc 
	,product_segment_name
	,item_changed
	,load_date)
SELECT 
    stg.*
    , 'NEW BPN' AS item_changed
    , '{current_date}'
FROM stage.rdma_staging stg
LEFT JOIN mdm.rdma p
	ON stg.base_prod_number=p.base_prod_number
WHERE p.base_prod_number IS NULL;

---------------------------------------CAPTURE CHANGED PLATFORM_SUBSETS--------------------------------------------

INSERT INTO mdm.rdma_changelog (base_prod_number 
	,pl 
	,base_prod_name 
	,base_prod_desc
	,product_lab_name
	,prev_base_prod_number
	,product_class 
	,platform 
	,product_platform_desc 
	,product_platform_status_cd 
	,product_platform_group 
	,product_family 
	,product_family_desc 
	,product_family_status_cd 
	,fmc 
	,platform_subset 
	,platform_subset_id
	,platform_subset_desc 
	,platform_subset_status_cd 
	,product_technology_name 
	,product_technology_id 
	,media_size 
	,product_status
	,product_status_date 
	,product_type 
	,sheets_per_pack_quantity 
	,after_market_flag 
	,cobranded_product_flag 
	,forecast_assumption_flag
	,base_product_known_flag 
	,oem_flag 
	,selectability_code 
	,supply_color 
	,ink_available_ccs_quantity 
	,sps_type 
	,warranty_reporting_flag 
	,base_prod_afr_goal_percent 
	,refurb_product_flag 
	,base_prod_acpt_goal_range_pc 
	,base_prod_tfr_goal_percent 
	,engine_type 
	,engine_type_id 
	,market_category_name 
	,finl_market_category 
	,category 
	,tone 
	,platform_subset_group_name 
	,id
	,attribute_extension1 
	,leveraged_product_platform_name 
	,platform_subset_group_status_cd 
	,platform_subset_group_desc 
	,product_segment_name
	,item_changed
	,load_date)
SELECT
    stg.*
    ,'CHANGED PLATFORM SUBSET' AS item_changed
    , '{current_date}'
FROM stage.rdma_staging stg
JOIN mdm.rdma p
	ON stg.base_prod_number=p.base_prod_number
WHERE stg.platform_subset <> p.platform_subset;

---------------------------------------CLEAR MDM TABLE-------------------------------------------------------------

DELETE FROM mdm.rdma;

----------------------------------------COPY DATA FROM STAGE TO PROD-------------------------------------------------

INSERT INTO mdm.rdma 
    (base_prod_number
    ,pl
    ,base_prod_name
    ,base_prod_desc
    ,product_lab_name
    ,prev_base_prod_number
    ,product_class
    ,platform
    ,product_platform_desc
    ,product_platform_status_cd
    ,product_platform_group
    ,product_family
    ,product_family_desc
    ,product_family_status_cd
    ,fmc
    ,platform_subset
    ,platform_subset_id
    ,platform_subset_desc
    ,platform_subset_status_cd
    ,product_technology_name
    ,product_technology_id
    ,media_size
    ,product_status
    ,product_status_date
    ,product_type
    ,sheets_per_pack_quantity
    ,after_market_flag
    ,cobranded_product_flag
    ,forecast_assumption_flag
    ,base_product_known_flag
    ,oem_flag
    ,selectability_code
    ,supply_color
    ,ink_available_ccs_quantity
    ,sps_type
    ,warranty_reporting_flag
    ,base_prod_afr_goal_percent
    ,refurb_product_flag
    ,base_prod_acpt_goal_range_pc
    ,base_prod_tfr_goal_percent
    ,engine_type
    ,engine_type_id
    ,market_category_name
    ,finl_market_category
    ,category
    ,tone
    ,platform_subset_group_name
    ,id
    ,attribute_extension1
    ,leveraged_product_platform_name
    ,platform_subset_group_status_cd
    ,platform_subset_group_desc
    ,product_segment_name)
    SELECT * FROM stage.rdma_staging;
"""

write_df_to_redshift(configs=configs, df=rdma_df, destination="stage.rdma_staging", mode="append", postactions = rdma_sproc, preactions="TRUNCATE TABLE stage.rdma_staging")

# COMMAND ----------

# write data to redshift
write_df_to_redshift(configs=configs, df=rdma_sales_product_full_df, destination="mdm.rdma_sales_product", mode="append", preactions="TRUNCATE mdm.rdma_sales_product")

# COMMAND ----------

# write data to redshift
write_df_to_redshift(configs=configs, df=rdma_sales_product_option_df, destination="mdm.rdma_sales_product_option", mode="append", preactions="TRUNCATE mdm.rdma_sales_product_option")

# COMMAND ----------

# set up connection to SFAI for data write-back
import pymssql

def submit_remote_sqlserver_query(configs, db, query):
    conn = pymssql.connect(server="sfai.corp.hpicloud.net", user=configs["sfai_username"], password=configs["sfai_password"], database=db)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()

# COMMAND ----------

if stack == 'prod':

    # write rdma data out to SFAI
    rdma = read_redshift_to_df(configs) \
        .option('dbtable', 'mdm.rdma') \
        .load() \
        .drop(col("rdma_id")) \
        .drop(col("official")) \
        .drop(col("load_date"))

    submit_remote_sqlserver_query(configs, "IE2_Prod", "TRUNCATE TABLE IE2_Prod.dbo.rdma;")

    write_df_to_sqlserver(configs=configs, df=rdma, destination="IE2_Prod.dbo.rdma", mode="append")

    # write rdma_base_to_sales_product_map data out to SFAI
    rdma_base_to_sales_product_map = read_redshift_to_df(configs) \
        .option('dbtable', 'mdm.rdma_base_to_sales_product_map') \
        .load()

    submit_remote_sqlserver_query(configs, "IE2_Prod", "TRUNCATE TABLE IE2_Prod.dbo.rdma_base_to_sales_product_map;")

    write_df_to_sqlserver(configs=configs, df=rdma_base_to_sales_product_map, destination="IE2_Prod.dbo.rdma_base_to_sales_product_map", mode="append")

    # write rdma_sales_product data out to SFAI
    rdma_sales_product = read_redshift_to_df(configs) \
        .option('dbtable', 'mdm.rdma_sales_product') \
        .load() \
        .withColumn("update_ts", col("update_ts").cast("timestamp"))

    submit_remote_sqlserver_query(configs, "IE2_Prod", "TRUNCATE TABLE IE2_Prod.dbo.rdma_sales_product;")

    write_df_to_sqlserver(configs=configs, df=rdma_sales_product, destination="IE2_Prod.dbo.rdma_sales_product", mode="append")

    # write rdma_sales_product_option data out to SFAI
    rdma_sales_product_option = read_redshift_to_df(configs) \
        .option('dbtable', 'mdm.rdma_sales_product_option') \
        .load()

    submit_remote_sqlserver_query(configs, "IE2_Prod", "TRUNCATE TABLE IE2_Prod.dbo.rdma_sales_product_option;")

    write_df_to_sqlserver(configs=configs, df=rdma_sales_product_option, destination="IE2_Prod.dbo.rdma_sales_product_option", mode="append")
