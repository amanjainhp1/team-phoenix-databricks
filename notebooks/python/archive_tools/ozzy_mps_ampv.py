# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

## OZZY MPS Supplies shipments
# UserId:  Phoenix  
# pw:  KSfi3917!#1h2H

# COMMAND ----------

# build query to pull data from OZZY server
ozzy_mps_ampv_query = """
SELECT
    UPPER([Market]) as market
    ,UPPER([Country]) as country
    ,UPPER([BusinessTypeGrp]) as business_type_grp
    ,UPPER([Platform_Subset]) as platform_subset
    ,UPPER([Category_Grp]) as category_grp
    ,UPPER([Color_Type]) as color_type
    ,UPPER([SF_MF]) as sf_mf
    ,UPPER([Format]) as format
    ,UPPER([Product_Structure]) as product_structure
    ,UPPER([Product_Structure_Grp]) as product_structure_grp
    ,UPPER([PL]) as pl
    ,UPPER([Month]) as month
    ,UPPER([FY_Qtr]) as fy_qtr
    ,[Pages_Rptd_Mono] as pages_rptd_mono
    ,[Pages_Rptd_Color] as pages_rptd_color
    ,[Pages_Rptd_MonoColor] as pages_rptd_monocolor
    ,[Devices] as devices
    ,[AMPV_MonoColor] as ampv_monocolor
FROM ozzy.dbo.Planning_Install_Base_AMPVs
WHERE BusinessTypeGrp <> 'cMPS'
"""

# Connection details
ozzy_mps_ampv_records = spark.read \
  .format("jdbc") \
  .option("url", "jdbc:sqlserver://MSPBAPROD.CORP.HPICLOUD.NET:1433;") \
  .option("user", "Phoenix") \
  .option("password", "KSfi3917!#1h2H") \
  .option("query", ozzy_mps_ampv_query) \
  .load()


# COMMAND ----------

# ozzy_mps_ampv_records.show()

# COMMAND ----------

# reformat the mcc column from Binary to integer
ozzy_mps_ampv_records = ozzy_mps_ampv_records \
    .withColumn("month", col("month").cast("date"))

# COMMAND ----------

#ozzy_mps_ampv_records.show()

# COMMAND ----------

# add a record to the version table, then retrieve the max verison and load_date values
from pyspark.sql.functions import lit
max_version_info = call_redshift_addversion_sproc(configs, 'MPS_AMPV', 'OZZY')

max_version = max_version_info[0]
max_load_date = max_version_info[1]

# replace the existing version and load_date values with the retrieved results from the version table
ozzy_mps_ampv_records = ozzy_mps_ampv_records \
  .withColumn("load_date", lit(max_load_date)) \
  .withColumn("version", lit(max_version))


# COMMAND ----------

# set the bucket and folder:

# s3a://dataos-core-dev-team-phoenix/mps_ww_shipped_supply/[version]
s3_ib_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/mps/ampv/" + max_version

# write data to the bucket, overwrite if pulled more than once on the same day
write_df_to_s3(ozzy_mps_ampv_records, s3_ib_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# write dataframe to the stage database
write_df_to_redshift(configs, ozzy_mps_ampv_records, "stage.mps_ww_shipped_supply", "overwrite")

# COMMAND ----------

# write dataframe to the prod database
write_df_to_redshift(configs, ozzy_mps_supplies_shipments_records, "prod.mps_ww_shipped_supply", "overwrite")

# COMMAND ----------


