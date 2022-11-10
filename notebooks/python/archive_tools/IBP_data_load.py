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
# ~30 mins
#submit_remote_sqlserver_query(configs, "ie2_prod", "UPDATE IE2_ExternalPartners.dbo.ibp SET Plan_Date = (SELECT MAX(Plan_Date) FROM IE2_ExternalPartners.dbo.ibp WHERE [version]IS NULL) WHERE [version] IS NULL;")

# COMMAND ----------

# Add a record to the version table
#submit_remote_sqlserver_query(configs, "ie2_prod", "EXEC [IE2_prod].[dbo].[AddVersion_sproc] 'ibp_fcst', 'IBP';")

# COMMAND ----------

# Update the latest records with version and load_date
# ~2 mins
#submit_remote_sqlserver_query(configs, "ie2_prod", "UPDATE IE2_ExternalPartners.dbo.ibp SET [version] = (SELECT MAX(version) FROM IE2_Prod.dbo.version WHERE record = 'ibp_fcst') WHERE [version] IS NULL;")

# COMMAND ----------

# MAGIC %md
# MAGIC # All IBP

# COMMAND ----------

# get ALL the latest IBP records from SFAI and insert into a dataframe
# yyyy-MM-dd HH:mm:ss

all_ibp_query = """

SELECT
       ibp.[Geo_Region]
	   ,NULL AS [Subregion2]
	   ,ibp.[Product_Category]
	   ,ibp.[Subregion4]
	   ,ibp.[Sales_Product]
	   ,ibp.Sales_Product_w_Opt
	   ,ibp.Primary_Base_Product
	   ,CAST(ibp.[Calendar_Year_Month] as date) as Calendar_Year_Month
	   ,ibp.[Units]
       ,CAST(ibp.[Plan_Date] as datetime) as Plan_Date
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



# COMMAND ----------

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

# create a list of valid supplies product numbers
rdma_mapping_valid_query = """

select distinct sales_product_number 
from mdm.rdma_base_to_sales_product_map
"""

rdma_mapping_valid_records = read_redshift_to_df(configs) \
    .option("query", rdma_mapping_valid_query) \
    .load()


# COMMAND ----------

# filter the base dataset to SUPPLIES records

# still need to clean up the version field

# memberDF.join(sectionDF,memberDF.dept_id == sectionDF.section_id,"inner").show(truncate=False)

supplies_ibp_records_prefiltered = all_ibp_records.filter((all_ibp_records.Product_Category.contains('SUPPLI')) |
                                              (all_ibp_records.Product_Category.startswith('LARGE FORMAT')) |
                                              (all_ibp_records.Product_Category.startswith('TEXTILE')))

supplies_ibp_records = supplies_ibp_records_prefiltered.join(rdma_mapping_valid_records,supplies_ibp_records_prefiltered.Sales_Product == rdma_mapping_valid_records.sales_product_number,"inner")

# supplies_ibp_records.display()

#this needs to stay enabled to complte one of the next cells
write_df_to_redshift(configs, supplies_ibp_records, "stage.ibp_supplies_forecast_landing", "overwrite")

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

# sinai2_records.sort(['country_level_1','country_alpha2'], ascending = True).display()
sinai2_records.sort(['country_level_1','country_alpha2'], ascending = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## sales_product to product_category

# COMMAND ----------

# 2nd option from above:
sales_to_tech_query = """
select distinct 
	sales_product, 
	product_category
from stage.ibp_supplies_forecast_landing
"""
sales_to_tech_records = read_redshift_to_df(configs) \
    .option("query", sales_to_tech_query) \
    .load()

#sales_to_tech_records.display()

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

# countries_to_not_split_records.display()

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
	sum(revenue_units) as numerator
FROM fin_prod.actuals_supplies_baseprod sup_acts
INNER JOIN mdm.iso_cc_rollup_xref iso 
	ON sup_acts.country_alpha2 = iso.country_alpha2
left join mdm.product_line_xref xref on sup_acts.pl =xref.pl 
WHERE 1=1
	AND sup_acts.cal_date > DATEADD(year,-1,GETDATE())
	AND iso.country_scenario = 'IBP_SINAI2'
	AND iso.country_level_1 IS NOT null
	and sup_acts.platform_subset <> 'NA'
	and xref.technology IN ('LASER', 'INK', 'PWA', 'LF')
	and xref.pl_category = 'SUP'
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
	iso.country_level_1, 
	xref.technology,
	sum(revenue_units) as denominator
FROM fin_prod.actuals_supplies_baseprod sup_acts
INNER JOIN mdm.iso_cc_rollup_xref iso 
	ON sup_acts.country_alpha2 = iso.country_alpha2
left join mdm.product_line_xref xref on sup_acts.pl =xref.pl 
WHERE 1=1
	AND sup_acts.cal_date > DATEADD(year,-1,GETDATE())
	AND iso.country_scenario = 'IBP_SINAI2'
	AND iso.country_level_1 IS NOT null
	and sup_acts.platform_subset <> 'NA'
	and xref.technology IN ('LASER', 'INK', 'PWA', 'LF')
	and xref.pl_category = 'SUP'
GROUP by 
	iso.country_level_1,
	xref.technology  
order by 2,1
"""

supplies_actuals_denominator_all_records = read_redshift_to_df(configs) \
    .option("query", supplies_actuals_denominator_all_records_query) \
    .load()

#supplies_actuals_denominator_all_records.display()

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

# Global View


# COMMAND ----------

# MAGIC %md
# MAGIC ## LASER

# COMMAND ----------

# Numerator
# get a total of supplies units, for LASER, for the past 1 year, to use as a numerator in the % mix calculation, then drop the technology column
supplies_actuals_numerator_laser_records = supplies_actuals_numerator_all_records.filter(supplies_actuals_numerator_all_records.technology == 'LASER')
supplies_actuals_numerator_laser_records2 = supplies_actuals_numerator_laser_records.drop(supplies_actuals_numerator_laser_records.technology)

#supplies_actuals_numerator_laser_records2.sort(['country_level_1','country_alpha2'], ascending = True).display()


# COMMAND ----------

# Denominator
# get a total of supplies units, for LASER, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
supplies_actuals_denominator_laser_records = supplies_actuals_denominator_all_records_filtered.filter(supplies_actuals_denominator_all_records_filtered.technology == 'LASER')
supplies_actuals_denominator_laser_records2 = supplies_actuals_denominator_laser_records.drop(supplies_actuals_denominator_laser_records.technology)
# supplies_actuals_denominator_laser_records2.sort(['country_level_1'], ascending = True).display()

# COMMAND ----------

# Create split percentage
# join the SINAI2 scenario with the laser supplies recordsets

supplies_actuals_numerator_laser_records2.createOrReplaceTempView("laser_numerator")
supplies_actuals_denominator_laser_records2.createOrReplaceTempView("laser_denominator")

laser_percent_mix = spark.sql("select laser_denominator.country_level_1, laser_numerator.country_alpha2, laser_numerator.numerator / laser_denominator.denominator AS split_pct from laser_denominator INNER JOIN laser_numerator on laser_denominator.country_level_1 == laser_numerator.country_level_1")

#laser_percent_mix.sort(['country_level_1','country_alpha2'], ascending = True).display()


# COMMAND ----------

# create a combined laser mix for the geography groupings

sinai2_records.createOrReplaceTempView("sinai")
countries_to_not_split_records.createOrReplaceTempView("countries_no_split")

no_split_mix = spark.sql("select countries_no_split.country_level_1, sinai.country_alpha2, 1 AS split_pct from countries_no_split INNER JOIN sinai on countries_no_split.country_level_1 == sinai.country_level_1")

#no_split_mix.sort(['country_level_1','country_alpha2'], ascending = True).display()

#combined_laser_mix = no_split_mix.union(laser_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True).display()
combined_laser_mix = no_split_mix.union(laser_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)



# COMMAND ----------

ls_laser = ['OEM SUPPLIES','HOME LASERJET SUPPLIES','HOME LASER S-PRINT SUPPLIES','A3 OFFICE LASERJET SUPPLIES','A3 OFFICE LASER GROWTH SUPPLIE','A4 OFFICE LASER S-PRINT SUPPLI','A4 OFFICE LASERJET SUPPLIES'] 

laser_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records["product_category"].isin(ls_laser))
#laser_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records.technology == 'LASER')

laser_sales_to_tech_records.createOrReplaceTempView("laser_sales_prod")
supplies_ibp_records.createOrReplaceTempView("laser_ibp")

laser_supplies_ibp_records = spark.sql("select laser_ibp.* from laser_ibp INNER JOIN laser_sales_prod on laser_ibp.Sales_Product == laser_sales_prod.sales_product")
laser_supplies_ibp_records.createOrReplaceTempView("laser_ibp_records")
combined_laser_mix.createOrReplaceTempView("combined_laser_mix_vw")

allocated_laser_ibp = spark.sql("select laser_ibp_records.Subregion4, laser_ibp_records.Sales_Product, laser_ibp_records.Calendar_Year_Month, combined_laser_mix_vw.country_alpha2, laser_ibp_records.Units * combined_laser_mix_vw.split_pct as units, laser_ibp_records.Plan_Date, laser_ibp_records.version from laser_ibp_records INNER JOIN combined_laser_mix_vw on laser_ibp_records.Subregion4 == combined_laser_mix_vw.country_level_1")
#laser_supplies_ibp_records.display()

#allocated_laser_ibp.display()


# COMMAND ----------

# write to redshift for testing:
# write_df_to_redshift(configs, allocated_laser_ibp, "stage.ibp_supplies_laser_forecast_landing", "overwrite")

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

# blow the INK IBP units out to country based on the split %
ls_ink = ['HOME INK SUPPLIES'] 

ink_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records["product_category"].isin(ls_ink))

#ink_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records.technology == 'INK')

ink_sales_to_tech_records.createOrReplaceTempView("ink_sales_prod")
supplies_ibp_records.createOrReplaceTempView("ink_ibp")

#ink_supplies_ibp_records = spark.sql("select ink_ibp.* from ink_ibp INNER JOIN ink_sales_prod on ink_ibp.Sales_Product == ink_sales_prod.sales_product")
ink_supplies_ibp_records = spark.sql("select ink_ibp.* from ink_ibp INNER JOIN ink_sales_prod on ink_ibp.Sales_Product == ink_sales_prod.sales_product")
ink_supplies_ibp_records.createOrReplaceTempView("ink_ibp_records")
combined_ink_mix.createOrReplaceTempView("combined_ink_mix_vw")

allocated_ink_ibp = spark.sql("select ink_ibp_records.Subregion4, ink_ibp_records.Sales_Product, ink_ibp_records.Calendar_Year_Month, combined_ink_mix_vw.country_alpha2, ink_ibp_records.Units * combined_ink_mix_vw.split_pct as units, ink_ibp_records.Plan_date, ink_ibp_records.version from ink_ibp_records INNER JOIN combined_ink_mix_vw on ink_ibp_records.Subregion4 == combined_ink_mix_vw.country_level_1")

# allocated_ink_ibp.display()

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

# blow the PWA IBP units out to country based on the split %

ls_pwa = ['A3 OFFICE PAGEWIDE INK SUPPLIE','A4 OFFICE PAGEWIDE INK SUPPLIE'] 

pwa_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records["product_category"].isin(ls_pwa))

#pwa_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records.technology == 'PWA')

pwa_sales_to_tech_records.createOrReplaceTempView("pwa_sales_prod")
supplies_ibp_records.createOrReplaceTempView("pwa_ibp")

pwa_supplies_ibp_records = spark.sql("select pwa_ibp.* from pwa_ibp INNER JOIN pwa_sales_prod on pwa_ibp.Sales_Product == pwa_sales_prod.sales_product")
pwa_supplies_ibp_records.createOrReplaceTempView("pwa_ibp_records")
combined_pwa_mix.createOrReplaceTempView("combined_pwa_mix_vw")

allocated_pwa_ibp = spark.sql("select pwa_ibp_records.Subregion4, pwa_ibp_records.Sales_Product, pwa_ibp_records.Calendar_Year_Month, combined_pwa_mix_vw.country_alpha2, pwa_ibp_records.Units * combined_pwa_mix_vw.split_pct as units, pwa_ibp_records.Plan_date, pwa_ibp_records.version from pwa_ibp_records INNER JOIN combined_pwa_mix_vw on pwa_ibp_records.Subregion4 == combined_pwa_mix_vw.country_level_1")

# allocated_pwa_ibp.display()

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

# blow the LF IBP units out to country based on the split %

ls_lf = ['LARGE FORMAT PRODUCTION','LARGE FORMAT DESIGN'] 

lf_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records["product_category"].isin(ls_lf))

#lf_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records.technology == 'LF')

lf_sales_to_tech_records.createOrReplaceTempView("lf_sales_prod")
supplies_ibp_records.createOrReplaceTempView("lf_ibp")

lf_supplies_ibp_records = spark.sql("select lf_ibp.* from lf_ibp INNER JOIN lf_sales_prod on lf_ibp.Sales_Product == lf_sales_prod.sales_product")
lf_supplies_ibp_records.createOrReplaceTempView("lf_ibp_records")
combined_lf_mix.createOrReplaceTempView("combined_lf_mix_vw")

allocated_lf_ibp = spark.sql("select lf_ibp_records.Subregion4, lf_ibp_records.Sales_Product, lf_ibp_records.Calendar_Year_Month, combined_lf_mix_vw.country_alpha2, lf_ibp_records.Units * combined_lf_mix_vw.split_pct as units, lf_ibp_records.Plan_date, lf_ibp_records.version from lf_ibp_records INNER JOIN combined_lf_mix_vw on lf_ibp_records.Subregion4 == combined_lf_mix_vw.country_level_1")

# allocated_lf_ibp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## UNION technologies

# COMMAND ----------

combined_lf_mix = allocated_laser_ibp.union(allocated_ink_ibp).union(allocated_pwa_ibp).union(allocated_lf_ibp)
#combined_lf_mix.display()


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# clean up the dataframe to a format that we're used to

combined_lf_mix1 = combined_lf_mix \
  .withColumn("record", lit('IBP_SUPPLIES_FCST'))

combined_lf_mix2 = combined_lf_mix1 \
  .select("record", "country_alpha2", "Sales_Product", "Calendar_Year_Month", "units", "Plan_date", "version")

combined_lf_mix2 = combined_lf_mix2 \
  .withColumnRenamed("Sales_Product", "sales_product_number") \
  .withColumnRenamed("Calendar_Year_Month", "cal_date") \
  .withColumnRenamed("Plan_date", "load_date")

#combined_lf_mix2.display()
write_df_to_redshift(configs, combined_lf_mix2, "stage.ibp_supplies_forecast_test", "overwrite")


# COMMAND ----------

# MAGIC %md
# MAGIC # Hardware
