# Databricks notebook source
# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/s3_utils

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

hardware_xref = read_redshift_to_df(configs).option('dbtable', 'mdm.hardware_xref').load()
hardware_xref.createOrReplaceTempView('hardware_xref')

# COMMAND ----------

ref_segmentation = read_sql_server_to_df(configs).option('dbtable', 'archer_prod.dbo.ref_segmentation').load()
ref_segmentation.createOrReplaceTempView('ref_segmentation')

# COMMAND ----------

missing_platform_subsets = spark.sql("SELECT * FROM ref_segmentation rs LEFT JOIN hardware_xref hx ON UPPER(rs.platform_subset)=UPPER(hx.platform_subset) where hx.platform_subset IS NULL")




# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temp view missing_platforms as
# MAGIC SELECT
# MAGIC     rs.Platform_Subset as platform_subset,
# MAGIC     rs.PL as pl,
# MAGIC     CAST(NULL as varchar(100)) as technology,
# MAGIC     rs.Mono_PPM as mono_ppm,
# MAGIC     rs.Color_PPM as color_ppm,
# MAGIC     rs.Predecessor as predecessor,
# MAGIC     rs.Predecessor as predecessor_proxy,
# MAGIC     rs.Successor as successor,
# MAGIC     rs.VC_Category as vc_category,
# MAGIC     rs.Format as format,
# MAGIC     rs.SF_MF as sf_mf,
# MAGIC     rs.Mono_Color as mono_color,
# MAGIC     rs.Managed_NonManaged as managed_nonmanaged,
# MAGIC     rs.OEM_Vendor as oem_vendor,
# MAGIC     rs.Business_Feature as business_feature,
# MAGIC     CAST(NULL as varchar(100)) as business_segment,
# MAGIC     rs.Category_Feature as category_feature,
# MAGIC     rs.Category_Plus as category_plus,
# MAGIC     rs.Product_Structure as product_structure,
# MAGIC     CAST(NULL as double) as por_ampv,
# MAGIC     CAST(NULL as date) as intro_date,
# MAGIC     CAST(NULL as varchar(512)) as hw_product_family,
# MAGIC     CAST(NULL as varchar(150)) as supplies_mkt_cat,
# MAGIC     CAST(NULL as varchar(150)) as epa_family,
# MAGIC     CAST(NULL as varchar(100)) as brand,
# MAGIC     'N' as product_lifecycle_status,
# MAGIC     'N' as product_lifecycle_status_usage,
# MAGIC     'N' as product_lifecycle_status_share,
# MAGIC     CAST(NULL as double) as intro_price,
# MAGIC     TRUE as official,
# MAGIC     now() as last_modified_date,
# MAGIC     now() as load_date
# MAGIC FROM ref_segmentation rs LEFT JOIN hardware_xref hx ON UPPER(rs.platform_subset)=UPPER(hx.platform_subset) where hx.platform_subset IS NULL
# MAGIC                          

# COMMAND ----------

missing_platforms2 = spark.sql("SELECT * FROM missing_platforms")

# COMMAND ----------



# COMMAND ----------

missing_platforms2.display()

# COMMAND ----------

write_df_to_redshift(configs, missing_platforms2, "mdm.hardware_xref", "append")

# COMMAND ----------

submit_remote_query(configs, f"UPDATE mdm.hardware_xref SET technology = b.technology from mdm.product_line_xref b WHERE mdm.hardware_xref.pl = b.pl and mdm.hardware_xref.technology IS NULL")


