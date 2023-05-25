# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

## OZZY MPS Supplies shipments

# retrieve ozzy secrets
ozzy_secret = secrets_get(constants["OZZY_SECRET_NAME"][stack], "us-west-2")

# build query to pull data from OZZY server
ozzy_mps_supplies_shipments_query = """
SELECT DISTINCT
    'MPS_WW_SHIPPED_SUPPLY' as record
    ,[Region] as region
    ,UPPER([Country]) AS country
    ,CAST([Fiscal Year] as varchar(4)) as fiscal_year
    ,[Quarter] as quarter
    ,CAST([Month] AS date) AS [month]
    ,UPPER([Direct or indirect SF]) as direct_or_indirect_sf
    ,[Product Nbr] as product_nbr
    ,UPPER([Description]) as description
    ,[Prod Line] as prod_line
    ,UPPER([CC/OCC indicator]) as cc_occ_indicator
    ,[Shipped Qty] as shipped_qty
    ,UPPER([Category]) as category
    ,[Yield] as yield
    ,[Color] as color
    ,[MCC] as mcc
    ,CAST(YEAR([Month]) AS varchar(4)) + RIGHT('0'+CAST(MONTH([Month]) AS varchar(2)),2) AS [yearmo]
    ,GetDate() AS load_date
    ,'version' AS [version]
FROM [MPS_Supplies].[dbo].[WW_Shipped_Supply_Data]
WHERE country IS NOT NULL
"""

# Connection details
ozzy_mps_supplies_shipments_records = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://{ozzy_secret['host']}:{ozzy_secret['port']};trustServerCertificate=true;") \
    .option("user", ozzy_secret["username"]) \
    .option("password", ozzy_secret["password"]) \
    .option("query", ozzy_mps_supplies_shipments_query) \
    .load()

# COMMAND ----------

# add a record to the version table, then retrieve the max verison and load_date values
from pyspark.sql.functions import lit
max_version_info = call_redshift_addversion_sproc(configs, 'MPS_WW_SHIPPED_SUPPLY', 'OZZY')

max_version = max_version_info[0]
max_load_date = max_version_info[1]

# reformat the mcc column from Binary to integer
# replace the existing version and load_date values with the retrieved results from the version table
ozzy_mps_supplies_shipments_records = ozzy_mps_supplies_shipments_records \
    .withColumn("mcc", col("mcc").cast("integer")) \
    .withColumn("load_date", lit(max_load_date)) \
    .withColumn("version", lit(max_version))

# COMMAND ----------

# set the bucket and folder:

# s3a://dataos-core-dev-team-phoenix/mps_ww_shipped_supply/[version]
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/mps/mps_supplies_shipments/" + max_version

# write data to the bucket, overwrite if pulled more than once on the same day
write_df_to_s3(ozzy_mps_supplies_shipments_records, s3_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# write dataframe to the stage schema
write_df_to_redshift(configs, ozzy_mps_supplies_shipments_records, "stage.mps_ww_shipped_supply", "overwrite")

# COMMAND ----------

# write dataframe to the prod schema
write_df_to_redshift(configs, ozzy_mps_supplies_shipments_records, "prod.mps_ww_shipped_supply", "overwrite")
