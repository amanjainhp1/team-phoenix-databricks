# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# build query to pull data from OZZY server

# retrieve ozzy secrets
ozzy_secret = secrets_get(constants["OZZY_SECRET_NAME"][stack], "us-west-2")

ozzy_mps_ib_query = """
SELECT  
    UPPER([Market_Region]) as market_region
    ,UPPER([Region_3]) as region_3
    ,UPPER([Region_4]) as region_4
    ,UPPER([CountryISO]) as country_alpha2
    ,UPPER([CountryName]) as country
    ,UPPER([Product_Number]) as product_number
    ,UPPER([Product_Description]) as product_description
    ,UPPER([Is_This_a_Printer]) as is_this_a_printer
    ,UPPER([Category_Grp]) as category_grp
    ,UPPER([Color_Type]) as color_type
    ,UPPER([SF_MF]) as sf_mf
    ,UPPER([Format]) as format
    ,UPPER([Platform_Subset]) as platform_subset
    ,UPPER([PL]) as pl
    ,UPPER([BusinessTypeGrp]) as business_type_grp
    ,CAST([Month] AS date) AS cal_date
    ,[Device_OP_IB_Total] as device_op_ib_total
FROM [Ozzy].[dbo].[SFAI_IB_Units]
"""

# Connection details
ozzy_mps_ib_records = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://{ozzy_secret['host']}:{ozzy_secret['port']};") \
    .option("user", ozzy_secret["username"]) \
    .option("password", ozzy_secret["password"]) \
    .option("query", ozzy_mps_ib_query) \
    .load()


# COMMAND ----------

# add a record to the version table, then retrieve the max verison and load_date values
from pyspark.sql.functions import lit
max_version_info = call_redshift_addversion_sproc(configs, 'IB_MPS', 'OZZY')

max_version = max_version_info[0]
max_load_date = max_version_info[1]

# replace the existing version and load_date values with the retrieved results from the version table
ozzy_mps_ib_records = ozzy_mps_ib_records \
  .withColumn("load_date", lit(max_load_date)) \
  .withColumn("version", lit(max_version))


# COMMAND ----------

# set the bucket and folder:

# s3://dataos-core-itg-team-phoenix/product/mps/ib/[version]
s3_ib_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/mps/ib/" + max_version

# write data to the bucket, overwrite if pulled more than once on the same day
write_df_to_s3(ozzy_mps_ib_records, s3_ib_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# write dataframe to the stage database
write_df_to_redshift(configs, ozzy_mps_ib_records, "stage.ib_mps_stage", "overwrite")

# COMMAND ----------

# write dataframe to the prod database
write_df_to_redshift(configs, ozzy_mps_ib_records, "prod.ib_mps", "overwrite")
