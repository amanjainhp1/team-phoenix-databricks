# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# create the function that will perform the actions on SFAI
import pymssql

def submit_remote_sqlserver_query(configs, db, query):
    conn = pymssql.connect(server="sfai.corp.hpicloud.net", user=configs["sfai_username"], password=configs["sfai_password"], database=db)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()



# COMMAND ----------

# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

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

# MAGIC %md
# MAGIC # All IBP

# COMMAND ----------

# get ALL the latest IBP records from SFAI and insert into a dataframe
all_ibp_query = """

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
    .option("query", all_ibp_query) \
    .load()

# COMMAND ----------

# all_ibp_records.display()
#write_df_to_redshift(configs, all_ibp_records, "stage.ibp_all_forecast_landing", "overwrite")

# COMMAND ----------

# write the base data out to S3
from datetime import datetime
date = datetime.today()
datestamp = date.strftime("%Y%m%d")

s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "archive/ibp/" + datestamp

write_df_to_s3(all_ibp_records, s3_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# need entries in the prod.version table for:
# ibp_fcst
# ibp_hw_fcst
# ibp_supplies_fcst

# COMMAND ----------

# MAGIC %md
# MAGIC # All IBP Supplies

# COMMAND ----------

# filter the base dataset to SUPPLIES records

# still need to clean up the version field

supplies_ibp_records = all_ibp_records.filter((all_ibp_records.Product_Category.contains('SUPPLI')) |
                                              (all_ibp_records.Product_Category.startswith('LARGE FORMAT')) |
                                              (all_ibp_records.Product_Category.startswith('TEXTILE')))


# supplies_ibp_records.display()

#write_df_to_redshift(configs, supplies_ibp_records, "stage.ibp_supplies_forecast_landing", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC # All IBP Hardware

# COMMAND ----------

# filter the base dataset to HARDWARE records

# still need to clean up the version field

hardware_ibp_records = all_ibp_records.filter((all_ibp_records.Product_Category.contains('SUPPLI')==False))

#write_df_to_redshift(configs, hardware_ibp_records, "stage.ibp_hardware_forecast_landing", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## SINAI

# COMMAND ----------

# Create a dataframe from the SINAI2 scenario in the iso_xref table:
# get a list of the IBP country grouping that we need to split
sinai2_query = """

SELECT
    country_level_1,
    country_alpha2
FROM mdm.iso_cc_rollup_xref
WHERE 1=1
  AND country_scenario = 'IBP_SINAI2'
"""

sinai2_records = read_redshift_to_df(configs) \
    .option("query", sinai2_query) \
    .load()

sinai2_records.sort(['country_level_1','country_alpha2'], ascending = True).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDMA and Product_life_xref

# COMMAND ----------

# get a list of the Sales Product to Technology Mappings
sales_to_tech_query = """

select distinct 
	a.sales_product,
	c.technology 	
from stage.ibp_supplies_forecast_landing a
left join mdm.rdma_base_to_sales_product_map b on a.sales_product = b.sales_product_number 
left join mdm.product_line_xref c on b.sales_product_line_code = c.pl
where 1=1
	and c.technology is not null
"""

sales_to_tech_records = read_redshift_to_df(configs) \
    .option("query", sales_to_tech_query) \
    .load()

# sales_to_tech_records.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Countries to split:

# COMMAND ----------

# get a list of the IBP country grouping that we need to split
countries_to_split_query = """

 SELECT
 	country_level_1, 
 	COUNT(country_alpha2) as country_count
 FROM mdm.iso_cc_rollup_xref
 WHERE 1=1
 	and country_scenario = 'IBP_SINAI2' 
 	and country_level_1 is not NULL
 GROUP BY country_level_1
 HAVING COUNT(country_alpha2) > 1
"""

countries_to_split_records = read_redshift_to_df(configs) \
    .option("query", countries_to_split_query) \
    .load()

#countries_to_split_records.show()

# COMMAND ----------

# get a list of the IBP countries that we do not need to split
countries_to_not_split_query = """

 SELECT
 	country_level_1, 
 	COUNT(country_alpha2) as country_count
 FROM mdm.iso_cc_rollup_xref
 WHERE 1=1
 	and country_scenario = 'IBP_SINAI2' 
 	and country_level_1 is not NULL
 GROUP BY country_level_1
 HAVING COUNT(country_alpha2) = 1
"""

countries_to_not_split_records = read_redshift_to_df(configs) \
    .option("query", countries_to_not_split_query) \
    .load()

countries_to_not_split_records.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Supplies actuals

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total Supplies actuals - numerator

# COMMAND ----------

# get a total of all supplies units, for the past 1 year, to use as a numerator in the % mix calculation
supplies_actuals_numerator_all_records_query = """

SELECT 
	sup_acts.country_alpha2,
	iso.country_level_1,
	xref.technology,
	sum(convert(decimal(30, 20),base_quantity)) as numerator
FROM prod.actuals_supplies sup_acts 
INNER JOIN mdm.iso_cc_rollup_xref iso 
	ON sup_acts.country_alpha2 = iso.country_alpha2
left join mdm.hardware_xref xref on sup_acts.platform_subset =xref.platform_subset 
WHERE 1=1
	AND cal_date > DATEADD(year,-1,GETDATE())
	AND iso.country_scenario = 'IBP_SINAI2'
	AND iso.country_level_1 IS NOT null
	and sup_acts.platform_subset <> 'NA'
	and xref.technology is not null
GROUP by 
	sup_acts.country_alpha2,
	iso.country_level_1,
	xref.technology
"""

supplies_actuals_numerator_all_records = read_redshift_to_df(configs) \
    .option("query", supplies_actuals_numerator_all_records_query) \
    .load()

# supplies_actuals_numerator_all_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total Supplies actuals - denominator

# COMMAND ----------

# get a total of all supplies units, for the past 1 year, to use as a denominator in the % mix calculation
supplies_actuals_denominator_all_records_query = """

SELECT
	isoccrollup.country_level_1,    
	hx.technology, 
	SUM(acts.base_quantity) AS denominator
FROM prod.actuals_supplies acts 
INNER JOIN mdm.iso_cc_rollup_xref isoccrollup 
	ON acts.country_alpha2 = isoccrollup.country_alpha2
LEFT JOIN mdm.hardware_xref hx on acts.platform_subset = hx.platform_subset 
WHERE 1=1
	and acts.cal_date > DATEADD(year,-1,GETDATE())
	and hx.technology is not NULL
    AND isoccrollup.country_scenario = 'IBP_SINAI2'
	AND isoccrollup.country_level_1 IS NOT NULL 
GROUP by
	isoccrollup.country_level_1,
	hx.technology
"""

supplies_actuals_denominator_all_records = read_redshift_to_df(configs) \
    .option("query", supplies_actuals_denominator_all_records_query) \
    .load()

#supplies_actuals_denominator_all_records.show()



# COMMAND ----------

# join the total denominator dataset with the countries_to_split dataset to fliter the list down
countries_to_split_records.createOrReplaceTempView("countries")
supplies_actuals_denominator_all_records.createOrReplaceTempView("totals")

#supplies_actuals_denominator_all_records_filtered = spark.sql("select totals.* from totals INNER JOIN countries on totals.country_level_1 == countries.country_level_1").show()
supplies_actuals_denominator_all_records_filtered = spark.sql("select totals.* from totals INNER JOIN countries on totals.country_level_1 == countries.country_level_1")

#supplies_actuals_denominator_all_records_filtered.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create split %

# COMMAND ----------

# MAGIC %md
# MAGIC ## LASER

# COMMAND ----------

# get a total of supplies units, for LASER, for the past 1 year, to use as a numerator in the % mix calculation, then drop the technology column
supplies_actuals_numerator_laser_records = supplies_actuals_numerator_all_records.filter(supplies_actuals_numerator_all_records.technology == 'LASER')
supplies_actuals_numerator_laser_records2 = supplies_actuals_numerator_laser_records.drop(supplies_actuals_numerator_laser_records.technology)

#supplies_actuals_numerator_laser_records2.sort(['country_level_1','country_alpha2'], ascending = True).display()


# COMMAND ----------

# get a total of supplies units, for LASER, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
supplies_actuals_denominator_laser_records = supplies_actuals_denominator_all_records_filtered.filter(supplies_actuals_denominator_all_records_filtered.technology == 'LASER')
supplies_actuals_denominator_laser_records2 = supplies_actuals_denominator_laser_records.drop(supplies_actuals_denominator_laser_records.technology)
# supplies_actuals_denominator_laser_records2.sort(['country_level_1'], ascending = True).display()

# COMMAND ----------

# join the SINAI2 scenario with the 

supplies_actuals_numerator_laser_records2.createOrReplaceTempView("laser_numerator")
supplies_actuals_denominator_laser_records2.createOrReplaceTempView("laser_denominator")

laser_percent_mix = spark.sql("select laser_denominator.country_level_1, laser_numerator.country_alpha2, laser_numerator.numerator / laser_denominator.denominator AS split_pct from laser_denominator INNER JOIN laser_numerator on laser_denominator.country_level_1 == laser_numerator.country_level_1")

# laser_percent_mix.sort(['country_level_1','country_alpha2'], ascending = True).display()


# COMMAND ----------

# create a combined laser mix for the geography groupings

sinai2_records.createOrReplaceTempView("sinai")
countries_to_not_split_records.createOrReplaceTempView("countries_no_split")

no_split_mix = spark.sql("select countries_no_split.country_level_1, sinai.country_alpha2, 1 AS split_pct from countries_no_split INNER JOIN sinai on countries_no_split.country_level_1 == sinai.country_level_1")

#no_split_mix.sort(['country_level_1','country_alpha2'], ascending = True).display()

#combined_laser_mix = no_split_mix.union(laser_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True).display()
combined_laser_mix = no_split_mix.union(laser_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)



# COMMAND ----------

# blow the Laser IBP units out to country based on the split %
laser_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records.technology == 'LASER')

laser_sales_to_tech_records.createOrReplaceTempView("laser_sales_prod")
supplies_ibp_records.createOrReplaceTempView("laser_ibp")

laser_supplies_ibp_records = spark.sql("select laser_ibp.* from laser_ibp INNER JOIN laser_sales_prod on laser_ibp.Sales_Product == laser_sales_prod.sales_product")
laser_supplies_ibp_records.createOrReplaceTempView("laser_ibp_records")
combined_laser_mix.createOrReplaceTempView("combined_laser_mix_vw")

allocated_laser_ibp = spark.sql("select laser_ibp_records.Subregion4, laser_ibp_records.Sales_Product, laser_ibp_records.Calendar_Year_Month, combined_laser_mix_vw.country_alpha2, laser_ibp_records.Units * combined_laser_mix_vw.split_pct as units, laser_ibp_records.Plan_date, laser_ibp_records.version from laser_ibp_records INNER JOIN combined_laser_mix_vw on laser_ibp_records.Subregion4 == combined_laser_mix_vw.country_level_1")
#laser_supplies_ibp_records.display()

allocated_laser_ibp.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## INK

# COMMAND ----------

# get a total of supplies units, for INK, for the past 1 year, to use as a numerator in the % mix calculation, then drop the technology column
supplies_actuals_numerator_ink_records = supplies_actuals_numerator_all_records.filter(supplies_actuals_numerator_all_records.technology == 'INK')
supplies_actuals_numerator_ink_records2 = supplies_actuals_numerator_ink_records.drop(supplies_actuals_numerator_ink_records.technology)
#supplies_actuals_numerator_ink_records2.show()

# COMMAND ----------

# get a total of supplies units, for INK, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
supplies_actuals_denominator_ink_records = supplies_actuals_denominator_all_records_filtered.filter(supplies_actuals_denominator_all_records_filtered.technology == 'INK')
supplies_actuals_denominator_ink_records2 = supplies_actuals_denominator_ink_records.drop(supplies_actuals_denominator_ink_records.technology)
#supplies_actuals_denominator_ink_records.display()

# COMMAND ----------

supplies_actuals_numerator_ink_records2.createOrReplaceTempView("ink_numerator")
supplies_actuals_denominator_ink_records2.createOrReplaceTempView("ink_denominator")

ink_percent_mix = spark.sql("select ink_denominator.country_level_1, ink_numerator.country_alpha2, ink_numerator.numerator / ink_denominator.denominator AS split_pct from ink_denominator INNER JOIN ink_numerator on ink_denominator.country_level_1 == ink_numerator.country_level_1")

#ink_percent_mix.sort(['country_level_1','country_alpha2'], ascending = True).display()

# COMMAND ----------

# create a combined ink mix for the geography groupings

combined_ink_mix = no_split_mix.union(ink_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PWA

# COMMAND ----------

# get a total of supplies units, for PWA, for the past 1 year, to use as a numerator in the % mix calculation, then drop the technology column
supplies_actuals_numerator_pwa_records = supplies_actuals_numerator_all_records.filter(supplies_actuals_numerator_all_records.technology == 'PWA')
supplies_actuals_numerator_pwa_records2 = supplies_actuals_numerator_pwa_records.drop(supplies_actuals_numerator_pwa_records.technology)

#supplies_actuals_numerator_pwa_records2.show()

# COMMAND ----------

# get a total of supplies units, for PWA, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
supplies_actuals_denominator_pwa_records = supplies_actuals_denominator_all_records_filtered.filter(supplies_actuals_denominator_all_records_filtered.technology == 'PWA')
supplies_actuals_denominator_pwa_records2 = supplies_actuals_denominator_pwa_records.drop(supplies_actuals_denominator_pwa_records.technology)
#supplies_actuals_denominator_pwa_records2.display()

# COMMAND ----------

supplies_actuals_numerator_pwa_records2.createOrReplaceTempView("pwa_numerator")
supplies_actuals_denominator_pwa_records2.createOrReplaceTempView("pwa_denominator")

pwa_percent_mix = spark.sql("select pwa_denominator.country_level_1, pwa_numerator.country_alpha2, pwa_numerator.numerator / pwa_denominator.denominator AS split_pct from pwa_denominator INNER JOIN pwa_numerator on pwa_denominator.country_level_1 == pwa_numerator.country_level_1")

#pwa_percent_mix.sort(['country_level_1','country_alpha2'], ascending = True).display()

# COMMAND ----------

# create a combined pwa mix for the geography groupings

combined_pwa_mix = no_split_mix.union(pwa_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LF

# COMMAND ----------

# get a total of supplies units, for LF, for the past 1 year, to use as a numerator in the % mix calculation, then drop the technology column
supplies_actuals_numerator_lf_records = supplies_actuals_numerator_all_records.filter(supplies_actuals_numerator_all_records.technology == 'LF')
supplies_actuals_numerator_lf_records2 = supplies_actuals_numerator_lf_records.drop(supplies_actuals_numerator_lf_records.technology)
#supplies_actuals_numerator_lf_records2.show()

# COMMAND ----------

# get a total of supplies units, for LF, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
supplies_actuals_denominator_lf_records = supplies_actuals_denominator_all_records_filtered.filter(supplies_actuals_denominator_all_records_filtered.technology == 'LF')
supplies_actuals_denominator_lf_records2 = supplies_actuals_denominator_lf_records.drop(supplies_actuals_denominator_lf_records.technology)
#supplies_actuals_denominator_lf_records2.display()

# COMMAND ----------

supplies_actuals_numerator_lf_records2.createOrReplaceTempView("lf_numerator")
supplies_actuals_denominator_lf_records2.createOrReplaceTempView("lf_denominator")

lf_percent_mix = spark.sql("select lf_denominator.country_level_1, lf_numerator.country_alpha2, lf_numerator.numerator / lf_denominator.denominator AS split_pct from lf_denominator INNER JOIN lf_numerator on lf_denominator.country_level_1 == lf_numerator.country_level_1")

#lf_percent_mix.sort(['country_level_1','country_alpha2'], ascending = True).display()

# COMMAND ----------

# create a combined LF mix for the geography groupings

combined_lf_mix = no_split_mix.union(lf_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Hardware
