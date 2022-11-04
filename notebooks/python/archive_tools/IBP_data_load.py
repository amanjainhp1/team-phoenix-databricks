# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

import pymssql

def submit_remote_sqlserver_query(configs, db, query):
    conn = pymssql.connect(server="sfai.corp.hpicloud.net", user=configs["sfai_username"], password=configs["sfai_password"], database=db)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()



# COMMAND ----------

# Fix the plan_date field
submit_remote_sqlserver_query(configs, "ie2_prod", "UPDATE IE2_ExternalPartners.dbo.ibp SET Plan_Date = (SELECT MAX(Plan_Date) FROM IE2_ExternalPartners.dbo.ibp WHERE [version]IS NULL) WHERE [version] IS NULL;")

# COMMAND ----------

# Add a record to the version table
submit_remote_sqlserver_query(configs, "ie2_prod", "EXEC [IE2_prod].[dbo].[AddVersion_sproc] 'ibp_fcst', 'IBP';")

# COMMAND ----------

# Update the latest records with version and load_date
submit_remote_sqlserver_query(configs, "ie2_prod", "UPDATE IE2_ExternalPartners.dbo.ibp	SET [version] = (SELECT MAX(version) FROM IE2_Prod.dbo.version WHERE record = 'ibp_fcst') WHERE [version] IS NULL;")

# COMMAND ----------

ibp_query = """

SELECT
       ibp.[Geo_Region]
	   ,NULL AS [Subregion2]
	   ,ibp.[Product_Category]
	   ,ibp.[Subregion4]
	   ,ibp.[Sales_Product]
	   ,ibp.Sales_Product_w_Opt
	   ,ibp.Primary_Base_Product
	   ,ibp.[Calendar_Year_Month]
	   ,ibp.[Units]
       ,ibp.[Plan_Date]
	   ,ibp.[version]
       ,ibp.[Subregion]  
  FROM [IE2_ExternalPartners].dbo.[ibp] ibp
  WHERE [version] = (SELECT MAX(version) FROM [IE2_ExternalPartners].dbo.[ibp])	
	AND (
          (Product_Category LIKE ('%SUPPLI%')) OR (Product_Category LIKE ('%PRINTER%'))
          OR (Product_Category IN ('LJ HOME BUSINESS SOLUTIONS','IJ HOME CONSUMER SOLUTIONS','IJ HOME BUSINESS SOLUTIONS','SCANNERS','LONG LIFE CONSUMABLES','LARGE FORMAT DESIGN','LARGE FORMAT PRODUCTION','TEXTILES'))
	    )
	AND ibp.[Calendar_Year_Month] < DATEADD(month, 17, getdate())
"""

all_ibp_records = read_sql_server_to_df(configs) \
    .option("query", ibp_query) \
    .load()

# COMMAND ----------

# write the base data out to S3
from datetime import datetime
date = datetime.today()
datestamp = date.strftime("%Y%m%d")

s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "archive/ibp/" + datestamp

write_df_to_s3(all_ibp_records, s3_output_bucket, "parquet", "overwrite")
