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
ozzy_mps_supplies_shipments_query = """
SELECT DISTINCT
     [Region] as region
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
    ,CONCAT(YEAR(GETDATE()),'.',CONVERT(CHAR(2),CAST(GETDATE() AS DATE),101),'.',CONVERT(CHAR(2), CAST(GETDATE() AS DATE), 103),'.1') AS [version]
    ,CONVERT(char(10), GetDate(),126) AS load_date
  FROM [MPS_Supplies].[dbo].[WW_Shipped_Supply_Data]
  WHERE country IS NOT NULL
"""

# Connection details
ozzy_mps_supplies_shipments_records = spark.read \
  .format("jdbc") \
  .option("url", "jdbc:sqlserver://MSPBAPROD.CORP.HPICLOUD.NET:1433;") \
  .option("user", "Phoenix") \
  .option("password", "KSfi3917!#1h2H") \
  .option("query", ozzy_mps_supplies_shipments_query) \
  .load()


# COMMAND ----------

# reformat the mcc column from Binary to integer
ozzy_mps_supplies_shipments_records = ozzy_mps_supplies_shipments_records \
    .withColumn("mcc", col("mcc").cast("integer"))

# COMMAND ----------

# test that data looks ok
# ozzy_mps_supplies_shipments_records.show()

# COMMAND ----------

# get the version value that was dynamically created during the query stage
version = ozzy_mps_supplies_shipments_records.select('version').distinct().head()[0]
# write to parquet file in s3

# COMMAND ----------

# Check version value
# print(version)

# COMMAND ----------

# s3a://dataos-core-dev-team-phoenix/mps_ww_shipped_supply/[version]
# set the bucket and folder:
s3_ib_output_bucket = constants["S3_BASE_BUCKET"][stack] + "mps_ww_shipped_supply/" + version

# write data to the bucket, overwrite if pulled more than once on the same day
write_df_to_s3(ozzy_mps_supplies_shipments_records, s3_ib_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# write dataframe to the stage database
write_df_to_redshift(configs, ozzy_mps_supplies_shipments_records, "stage.mps_ww_shipped_supply", "overwrite")

# COMMAND ----------

# write dataframe to the prod database
write_df_to_redshift(configs, ozzy_mps_supplies_shipments_records, "prod.mps_ww_shipped_supply", "overwrite")

# COMMAND ----------


