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
       ,ibp.key_figure
  FROM [IE2_ExternalPartners].dbo.[ibp] ibp
  WHERE [version] = (SELECT MAX(version) FROM [IE2_ExternalPartners].dbo.[ibp])	
	AND (
          (Product_Category LIKE ('%SUPPLI%')) OR (Product_Category LIKE ('%PRINTER%'))
          OR (Product_Category IN ('LJ HOME BUSINESS SOLUTIONS','IJ HOME CONSUMER SOLUTIONS','IJ HOME BUSINESS SOLUTIONS','SCANNERS','LONG LIFE CONSUMABLES','LARGE FORMAT DESIGN','LARGE FORMAT PRODUCTION','TEXTILES','A4 ENTERPRISE LOW LASERJET PRI'))
	    )
	AND ibp.[Calendar_Year_Month] < DATEADD(month, 17, getdate())
"""

all_ibp_records = read_sql_server_to_df(configs) \
    .option("query", all_ibp_query) \
    .load()

# all_ibp_records.display()

# COMMAND ----------

# write the raw data to S3

from datetime import datetime
date = datetime.today()
datestamp = date.strftime("%Y%m%d")

# un-comment this section when finished with development
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "archive/ibp/" + datestamp
# write_df_to_s3(all_ibp_records, s3_output_bucket, "parquet", "overwrite")

print(s3_output_bucket)

# COMMAND ----------

# MAGIC %md
# MAGIC # VERSIONING STILL TBD

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
rdma_supplies_mapping_valid_query = """

select distinct sales_product_number 
from mdm.rdma_base_to_sales_product_map
"""
# rdma_mapping_valid_records
rdma_supplies_mapping_valid_records = read_redshift_to_df(configs) \
    .option("query", rdma_supplies_mapping_valid_query) \
    .load()


# COMMAND ----------

# filter the base dataset to SUPPLIES records

#filter to just supplies categories
supplies_ibp_records_prefiltered1 = all_ibp_records.filter((all_ibp_records.Product_Category.contains('SUPPLI')) |
                                              (all_ibp_records.Product_Category.startswith('LARGE FORMAT')) |
                                              (all_ibp_records.Product_Category.startswith('LONG LIFE CONSUMABLES')) |             
                                              (all_ibp_records.Product_Category.startswith('TEXTILE')))

#further filter to supplies key_figure since LARGE FORMAT show up for both hardware and software
supplies_ibp_records_prefiltered2 = supplies_ibp_records_prefiltered1.filter(supplies_ibp_records_prefiltered1.key_figure.contains('SUPPLIES'))

#drop the key_figure column
supplies_ibp_records_prefiltered3 = supplies_ibp_records_prefiltered2.drop(supplies_ibp_records_prefiltered2.key_figure)

#join to the RDMA table to filter out non-valid sales_product_numbers
supplies_ibp_records = supplies_ibp_records_prefiltered3.join(rdma_supplies_mapping_valid_records, supplies_ibp_records_prefiltered3.Sales_Product == rdma_supplies_mapping_valid_records.sales_product_number,"inner")

#this needs to stay enabled to complte one of the next cells
write_df_to_redshift(configs, supplies_ibp_records, "stage.ibp_supplies_forecast_landing", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC # All IBP Hardware

# COMMAND ----------

# create a list of valid primary base product numbers
rdma_hw_mapping_valid_query = """

select distinct base_prod_number
from mdm.rdma
"""

rdma_hw_mapping_valid_records = read_redshift_to_df(configs) \
    .option("query", rdma_hw_mapping_valid_query) \
    .load()

# COMMAND ----------

# filter the base dataset to HARDWARE records
hardware_ibp_records_prefiltered1 = all_ibp_records.filter((all_ibp_records.Product_Category.contains('SUPPLI')==False))

#further filter to supplies key_figure since LARGE FORMAT show up for both hardware and software
hardware_ibp_records_prefiltered2 = hardware_ibp_records_prefiltered1.filter(hardware_ibp_records_prefiltered1.key_figure.contains('FINAL CONSENSUS FCST (REV)'))

#drop the key_figure column
hardware_ibp_records_prefiltered3 = hardware_ibp_records_prefiltered2.drop(hardware_ibp_records_prefiltered2.key_figure)

#join to the RDMA table to filter out non-valid base_prod_numbers
hardware_ibp_records = hardware_ibp_records_prefiltered3.join(rdma_hw_mapping_valid_records, hardware_ibp_records_prefiltered3.Primary_Base_Product == rdma_hw_mapping_valid_records.base_prod_number,"inner")

#this needs to stay enabled to complte one of the next cells
write_df_to_redshift(configs, hardware_ibp_records, "stage.ibp_hardware_forecast_landing", "overwrite")
#write_df_to_redshift(configs, hardware_ibp_records_prefiltered2, "stage.ibp_hardware_forecast_landing", "overwrite")

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

sinai2_records.sort(['country_level_1','country_alpha2'], ascending = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## sales_product to product_category

# COMMAND ----------

# Get a mapping of sales_product numbers to produduct_category from the IBP Supplies data set
sales_to_tech_query = """
select distinct 
	sales_product, 
	product_category
from stage.ibp_supplies_forecast_landing
"""
sales_to_tech_records = read_redshift_to_df(configs) \
    .option("query", sales_to_tech_query) \
    .load()


# COMMAND ----------

# MAGIC %md
# MAGIC ## bpn to product_category

# COMMAND ----------

# Get a mapping of primay_base_product numbers to produduct_category from the IBP Hardware data set
hw_bpn_to_tech_query = """
select distinct 
	base_prod_number, 
	product_category
from stage.ibp_hardware_forecast_landing
"""
hw_bpn_to_tech_records = read_redshift_to_df(configs) \
    .option("query", hw_bpn_to_tech_query) \
    .load()


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

# create a view
countries_to_split_records.createOrReplaceTempView("countries")

# create views to be used later in the mix split %
sinai2_records.createOrReplaceTempView("sinai")
countries_to_not_split_records.createOrReplaceTempView("countries_no_split")

# create a dataset for the non-split countries
no_split_mix = spark.sql("select countries_no_split.country_level_1, sinai.country_alpha2, 1 AS split_pct from countries_no_split INNER JOIN sinai on countries_no_split.country_level_1 == sinai.country_level_1")

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

# join the total denominator dataset with the countries_to_split dataset to filter the list down

supplies_actuals_denominator_all_records.createOrReplaceTempView("supplies_totals")

supplies_actuals_denominator_all_records_filtered = spark.sql("select supplies_totals.* from supplies_totals INNER JOIN countries on supplies_totals.country_level_1 == countries.country_level_1")

#supplies_actuals_denominator_all_records_filtered.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Hardware actuals

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total Hardware actuals - numerator

# COMMAND ----------

# get a total of all hardware units, for the past 1 year, to use as a numerator in the % mix calculation
hardware_actuals_numerator_all_records_query = """

SELECT
	acts.[country_alpha2]
	,iso.country_level_1
    ,xref.technology
	,sum([base_quantity]) AS numerator
FROM prod.[actuals_hw] acts
	INNER JOIN mdm.iso_cc_rollup_xref iso ON acts.country_alpha2 = iso.country_alpha2
    LEFT JOIN mdm.hardware_xref xref on acts.platform_subset = xref.platform_subset
WHERE
	acts.cal_date > DATEADD(year,-1,GETDATE())
	AND iso.country_scenario = 'IBP_SINAI2'
	AND iso.country_level_1 IS NOT NULL
    AND xref.technology in ('INK','LASER','PWA','LF')
GROUP BY 
	acts.[country_alpha2]
	,iso.country_level_1
    ,xref.technology
order by 2,1
"""

hardware_actuals_numerator_all_records = read_redshift_to_df(configs) \
    .option("query", hardware_actuals_numerator_all_records_query) \
    .load()

# hardware_actuals_numerator_all_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total Hardware actuals - denominator

# COMMAND ----------

# get a total of all hardware units, for the past 1 year, to use as a denominator in the % mix calculation
hardware_actuals_denominator_all_records_query = """

SELECT 
  iso.country_level_1
  ,xref.technology
  ,SUM(acts.[base_quantity]) AS denominator
FROM prod.[actuals_hw] acts 
JOIN mdm.iso_cc_rollup_xref iso ON acts.country_alpha2 = iso.country_alpha2
LEFT JOIN mdm.hardware_xref xref on acts.platform_subset = xref.platform_subset
WHERE acts.cal_date > DATEADD(year,-1,GETDATE())
  AND iso.country_scenario = 'IBP_SINAI2'
  AND iso.country_level_1 IS NOT NULL
  AND xref.technology in ('INK','LASER','PWA','LF')
GROUP by 
  iso.country_level_1
  ,xref.technology

"""

hardware_actuals_denominator_all_records = read_redshift_to_df(configs) \
    .option("query", hardware_actuals_denominator_all_records_query) \
    .load()

#hardware_actuals_denominator_all_records.display()

# COMMAND ----------

# join the total denominator dataset with the countries_to_split dataset to filter the list down

hardware_actuals_denominator_all_records.createOrReplaceTempView("hardware_totals")

hardware_actuals_denominator_all_records_filtered = spark.sql("select hardware_totals.* from hardware_totals INNER JOIN countries on hardware_totals.country_level_1 == countries.country_level_1")

#hardware_actuals_denominator_all_records_filtered.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create split % (Supplies)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LASER

# COMMAND ----------

# Numerator
# get a total of supplies units, for LASER, for the past 1 year, to use as a numerator in the % mix calculation
supplies_actuals_numerator_laser_records = supplies_actuals_numerator_all_records.filter(supplies_actuals_numerator_all_records.technology == 'LASER')

# drop the technology column
supplies_actuals_numerator_laser_records2 = supplies_actuals_numerator_laser_records.drop(supplies_actuals_numerator_laser_records.technology)


# COMMAND ----------

# Denominator
# get a total of supplies units, for LASER, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
supplies_actuals_denominator_laser_records = supplies_actuals_denominator_all_records_filtered.filter(supplies_actuals_denominator_all_records_filtered.technology == 'LASER')

# drop the technology column
supplies_actuals_denominator_laser_records2 = supplies_actuals_denominator_laser_records.drop(supplies_actuals_denominator_laser_records.technology)


# COMMAND ----------

# Create split percentage
# join the SINAI2 scenario with the laser supplies recordsets

supplies_actuals_numerator_laser_records2.createOrReplaceTempView("supplies_laser_numerator")
supplies_actuals_denominator_laser_records2.createOrReplaceTempView("supplies_laser_denominator")

# this creates the mix %
supplies_laser_percent_mix = spark.sql("select supplies_laser_denominator.country_level_1, supplies_laser_numerator.country_alpha2, supplies_laser_numerator.numerator / supplies_laser_denominator.denominator AS split_pct from supplies_laser_denominator INNER JOIN supplies_laser_numerator on supplies_laser_denominator.country_level_1 == supplies_laser_numerator.country_level_1")


# COMMAND ----------

# create a combined laser mix for the geography groupings

# Union the non-split countries with the split countries mix % to create a full dataset
combined_supplies_laser_mix = no_split_mix.union(supplies_laser_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)


# COMMAND ----------

# create a filter list
ls_laser = ['OEM SUPPLIES','HOME LASERJET SUPPLIES','HOME LASER S-PRINT SUPPLIES','A3 OFFICE LASERJET SUPPLIES','A3 OFFICE LASER GROWTH SUPPLIE','A4 OFFICE LASER S-PRINT SUPPLI','A4 OFFICE LASERJET SUPPLIES'] 

#filter the mapping dataset to the filter list
laser_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records["product_category"].isin(ls_laser))

laser_sales_to_tech_records.createOrReplaceTempView("laser_sales_prod")
supplies_ibp_records.createOrReplaceTempView("laser_ibp")

# Inner join the total supplies records with the filtered mapping to reduce list to category
laser_supplies_ibp_records = spark.sql("select laser_ibp.* from laser_ibp INNER JOIN laser_sales_prod on laser_ibp.Sales_Product == laser_sales_prod.sales_product")

laser_supplies_ibp_records.createOrReplaceTempView("laser_ibp_records")
combined_supplies_laser_mix.createOrReplaceTempView("combined_supplies_laser_mix_vw")

# join the filter ibp supplies data with the combined mix dataset to blow out the subregion4 to country_alpha2
allocated_laser_ibp = spark.sql("select laser_ibp_records.Subregion4, laser_ibp_records.Sales_Product, laser_ibp_records.Calendar_Year_Month, combined_supplies_laser_mix_vw.country_alpha2, laser_ibp_records.Units * combined_supplies_laser_mix_vw.split_pct as units, laser_ibp_records.Plan_Date, laser_ibp_records.version from laser_ibp_records INNER JOIN combined_supplies_laser_mix_vw on laser_ibp_records.Subregion4 == combined_supplies_laser_mix_vw.country_level_1")


# COMMAND ----------

# MAGIC %md
# MAGIC ## INK

# COMMAND ----------

# get a total of supplies units, for INK, for the past 1 year, to use as a numerator in the % mix calculation
supplies_actuals_numerator_ink_records = supplies_actuals_numerator_all_records.filter(supplies_actuals_numerator_all_records.technology == 'INK')

# drop the technology column
supplies_actuals_numerator_ink_records2 = supplies_actuals_numerator_ink_records.drop(supplies_actuals_numerator_ink_records.technology)

# COMMAND ----------

# get a total of supplies units, for INK, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
supplies_actuals_denominator_ink_records = supplies_actuals_denominator_all_records_filtered.filter(supplies_actuals_denominator_all_records_filtered.technology == 'INK')

# drop the technology column
supplies_actuals_denominator_ink_records2 = supplies_actuals_denominator_ink_records.drop(supplies_actuals_denominator_ink_records.technology)

# COMMAND ----------

supplies_actuals_numerator_ink_records2.createOrReplaceTempView("supplies_ink_numerator")
supplies_actuals_denominator_ink_records2.createOrReplaceTempView("supplies_ink_denominator")

# this creates the mix %
ink_percent_mix = spark.sql("select supplies_ink_denominator.country_level_1, supplies_ink_numerator.country_alpha2, supplies_ink_numerator.numerator / supplies_ink_denominator.denominator AS split_pct from supplies_ink_denominator INNER JOIN supplies_ink_numerator on supplies_ink_denominator.country_level_1 == supplies_ink_numerator.country_level_1")

# COMMAND ----------

# create a combined ink mix for the geography groupings

# Union the non-split countries with the split countries mix % to create a full dataset
combined_ink_mix = no_split_mix.union(ink_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)

# COMMAND ----------

# blow the INK IBP units out to country based on the split %

#create a filter list
ls_ink = ['HOME INK SUPPLIES'] 

#filter the mapping dataset to the filter list
ink_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records["product_category"].isin(ls_ink))

ink_sales_to_tech_records.createOrReplaceTempView("ink_sales_prod")
supplies_ibp_records.createOrReplaceTempView("ink_ibp")

# Inner join the total supplies records with the filtered mapping to reduce list to category
ink_supplies_ibp_records = spark.sql("select ink_ibp.* from ink_ibp INNER JOIN ink_sales_prod on ink_ibp.Sales_Product == ink_sales_prod.sales_product")

ink_supplies_ibp_records.createOrReplaceTempView("ink_ibp_records")
combined_ink_mix.createOrReplaceTempView("combined_ink_mix_vw")

# join the filter ibp supplies data with the combined mix dataset to blow out the subregion4 to country_alpha2
allocated_ink_ibp = spark.sql("select ink_ibp_records.Subregion4, ink_ibp_records.Sales_Product, ink_ibp_records.Calendar_Year_Month, combined_ink_mix_vw.country_alpha2, ink_ibp_records.Units * combined_ink_mix_vw.split_pct as units, ink_ibp_records.Plan_date, ink_ibp_records.version from ink_ibp_records INNER JOIN combined_ink_mix_vw on ink_ibp_records.Subregion4 == combined_ink_mix_vw.country_level_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PWA

# COMMAND ----------

# get a total of supplies units, for PWA, for the past 1 year, to use as a numerator in the % mix calculation
supplies_actuals_numerator_pwa_records = supplies_actuals_numerator_all_records.filter(supplies_actuals_numerator_all_records.technology == 'PWA')

# drop the technology column
supplies_actuals_numerator_pwa_records2 = supplies_actuals_numerator_pwa_records.drop(supplies_actuals_numerator_pwa_records.technology)


# COMMAND ----------

# get a total of supplies units, for PWA, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
supplies_actuals_denominator_pwa_records = supplies_actuals_denominator_all_records_filtered.filter(supplies_actuals_denominator_all_records_filtered.technology == 'PWA')

# drop the technology column
supplies_actuals_denominator_pwa_records2 = supplies_actuals_denominator_pwa_records.drop(supplies_actuals_denominator_pwa_records.technology)


# COMMAND ----------

supplies_actuals_numerator_pwa_records2.createOrReplaceTempView("pwa_numerator")
supplies_actuals_denominator_pwa_records2.createOrReplaceTempView("pwa_denominator")

# this creates the mix %
pwa_percent_mix = spark.sql("select pwa_denominator.country_level_1, pwa_numerator.country_alpha2, pwa_numerator.numerator / pwa_denominator.denominator AS split_pct from pwa_denominator INNER JOIN pwa_numerator on pwa_denominator.country_level_1 == pwa_numerator.country_level_1")


# COMMAND ----------

# create a combined pwa mix for the geography groupings

# Union the non-split countries with the split countries mix % to create a full dataset
combined_pwa_mix = no_split_mix.union(pwa_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)

# COMMAND ----------

# blow the PWA IBP units out to country based on the split %

#create a filter list
ls_pwa = ['A3 OFFICE PAGEWIDE INK SUPPLIE','A4 OFFICE PAGEWIDE INK SUPPLIE'] 

#filter the mapping dataset to the filter list
pwa_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records["product_category"].isin(ls_pwa))

pwa_sales_to_tech_records.createOrReplaceTempView("pwa_sales_prod")
supplies_ibp_records.createOrReplaceTempView("pwa_ibp")

# Inner join the total supplies records with the filtered mapping to reduce list to category
pwa_supplies_ibp_records = spark.sql("select pwa_ibp.* from pwa_ibp INNER JOIN pwa_sales_prod on pwa_ibp.Sales_Product == pwa_sales_prod.sales_product")

pwa_supplies_ibp_records.createOrReplaceTempView("pwa_ibp_records")
combined_pwa_mix.createOrReplaceTempView("combined_pwa_mix_vw")

# join the filter ibp supplies data with the combined mix dataset to blow out the subregion4 to country_alpha2
allocated_pwa_ibp = spark.sql("select pwa_ibp_records.Subregion4, pwa_ibp_records.Sales_Product, pwa_ibp_records.Calendar_Year_Month, combined_pwa_mix_vw.country_alpha2, pwa_ibp_records.Units * combined_pwa_mix_vw.split_pct as units, pwa_ibp_records.Plan_date, pwa_ibp_records.version from pwa_ibp_records INNER JOIN combined_pwa_mix_vw on pwa_ibp_records.Subregion4 == combined_pwa_mix_vw.country_level_1")


# COMMAND ----------

# MAGIC %md
# MAGIC ## LF

# COMMAND ----------

# get a total of supplies units, for LF, for the past 1 year, to use as a numerator in the % mix calculation
supplies_actuals_numerator_lf_records = supplies_actuals_numerator_all_records.filter(supplies_actuals_numerator_all_records.technology == 'LF')

# drop the technology column
supplies_actuals_numerator_lf_records2 = supplies_actuals_numerator_lf_records.drop(supplies_actuals_numerator_lf_records.technology)


# COMMAND ----------

# get a total of supplies units, for LF, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
supplies_actuals_denominator_lf_records = supplies_actuals_denominator_all_records_filtered.filter(supplies_actuals_denominator_all_records_filtered.technology == 'LF')

# drop the technology column
supplies_actuals_denominator_lf_records2 = supplies_actuals_denominator_lf_records.drop(supplies_actuals_denominator_lf_records.technology)


# COMMAND ----------

supplies_actuals_numerator_lf_records2.createOrReplaceTempView("lf_numerator")
supplies_actuals_denominator_lf_records2.createOrReplaceTempView("lf_denominator")

# this creates the mix %
lf_percent_mix = spark.sql("select lf_denominator.country_level_1, lf_numerator.country_alpha2, lf_numerator.numerator / lf_denominator.denominator AS split_pct from lf_denominator INNER JOIN lf_numerator on lf_denominator.country_level_1 == lf_numerator.country_level_1")


# COMMAND ----------

# create a combined LF mix for the geography groupings

# Union the non-split countries with the split countries mix % to create a full dataset
combined_lf_mix = no_split_mix.union(lf_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)

# COMMAND ----------

# blow the LF IBP units out to country based on the split %

#create a filter list
ls_lf = ['LARGE FORMAT PRODUCTION','LARGE FORMAT DESIGN'] 

#filter the mapping dataset to the filter list
lf_sales_to_tech_records = sales_to_tech_records.filter(sales_to_tech_records["product_category"].isin(ls_lf))

lf_sales_to_tech_records.createOrReplaceTempView("lf_sales_prod")
supplies_ibp_records.createOrReplaceTempView("lf_ibp")

# Inner join the total supplies records with the filtered mapping to reduce list to category
lf_supplies_ibp_records = spark.sql("select lf_ibp.* from lf_ibp INNER JOIN lf_sales_prod on lf_ibp.Sales_Product == lf_sales_prod.sales_product")

lf_supplies_ibp_records.createOrReplaceTempView("lf_ibp_records")
combined_lf_mix.createOrReplaceTempView("combined_lf_mix_vw")

# join the filter ibp supplies data with the combined mix dataset to blow out the subregion4 to country_alpha2
allocated_lf_ibp = spark.sql("select lf_ibp_records.Subregion4, lf_ibp_records.Sales_Product, lf_ibp_records.Calendar_Year_Month, combined_lf_mix_vw.country_alpha2, lf_ibp_records.Units * combined_lf_mix_vw.split_pct as units, lf_ibp_records.Plan_date, lf_ibp_records.version from lf_ibp_records INNER JOIN combined_lf_mix_vw on lf_ibp_records.Subregion4 == combined_lf_mix_vw.country_level_1")


# COMMAND ----------

# MAGIC %md
# MAGIC ## UNION technologies

# COMMAND ----------

# UNION the LASER, INK, PWA, LF datasets together
combined_lf_mix = allocated_laser_ibp.union(allocated_ink_ibp).union(allocated_pwa_ibp).union(allocated_lf_ibp)


# COMMAND ----------

# MAGIC %md
# MAGIC # Create split % (Hardware)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LASER

# COMMAND ----------

# Numerator
# get a total of hardware units, for LASER, for the past 1 year, to use as a numerator in the % mix calculation
hardware_actuals_numerator_laser_records = hardware_actuals_numerator_all_records.filter(hardware_actuals_numerator_all_records.technology == 'LASER')

# drop the technology column
hardware_actuals_numerator_laser_records2 = hardware_actuals_numerator_laser_records.drop(hardware_actuals_numerator_laser_records.technology)

#hardware_actuals_numerator_laser_records2.display()


# COMMAND ----------

# Denominator
# get a total of hardware units, for LASER, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
hardware_actuals_denominator_laser_records = hardware_actuals_denominator_all_records_filtered.filter(hardware_actuals_denominator_all_records_filtered.technology == 'LASER')

# drop the technology column
hardware_actuals_denominator_laser_records2 = hardware_actuals_denominator_laser_records.drop(hardware_actuals_denominator_laser_records.technology)

#hardware_actuals_denominator_laser_records2.display()


# COMMAND ----------

# Create split percentage
# join the SINAI2 scenario with the laser supplies recordsets

hardware_actuals_numerator_laser_records2.createOrReplaceTempView("hardware_laser_numerator")
hardware_actuals_denominator_laser_records2.createOrReplaceTempView("hardware_laser_denominator")

# this creates the mix %
hardware_laser_percent_mix = spark.sql("select hardware_laser_denominator.country_level_1, hardware_laser_numerator.country_alpha2, hardware_laser_numerator.numerator / hardware_laser_denominator.denominator AS split_pct from hardware_laser_denominator INNER JOIN hardware_laser_numerator on hardware_laser_denominator.country_level_1 == hardware_laser_numerator.country_level_1")

#hardware_laser_percent_mix.display()

# COMMAND ----------

# create a combined laser mix for the geography groupings

# Union the non-split countries with the split countries mix % to create a full dataset
combined_hardware_laser_mix = no_split_mix.union(hardware_laser_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)

#combined_hardware_laser_mix.display()


# COMMAND ----------

# create a filter list
ls_laser_hw = ['A3 OFFICE GROWTH LASER PRINTER','A3 OFFICE LASERJET PRINTERS','A4 ENTERPRISE LASERJET PRINTER','A4 ENTERPRISE LOW LASERJET PRI','A4 OFFICE LASER S-PRINTERS   K','A4 SMB LASERJET PRINTERS','HOME LASER S-PRINTERS','HOME LASERJET PRINTERS'] 

#filter the mapping dataset to the filter list
laser_hw_bpn_to_tech_records = hw_bpn_to_tech_records.filter(hw_bpn_to_tech_records["product_category"].isin(ls_laser_hw))

laser_hw_bpn_to_tech_records.createOrReplaceTempView("laser_hw_base_prod")
hardware_ibp_records.createOrReplaceTempView("laser_hw_ibp")

# Inner join the total hardware records with the filtered mapping to reduce list to category
laser_hw_ibp_records = spark.sql("select laser_hw_ibp.* from laser_hw_ibp INNER JOIN laser_hw_base_prod on laser_hw_ibp.base_prod_number == laser_hw_base_prod.base_prod_number")

laser_hw_ibp_records.createOrReplaceTempView("laser_hw_ibp_records")
combined_hardware_laser_mix.createOrReplaceTempView("combined_hw_laser_mix_vw")

# join the filter ibp hw data with the combined mix dataset to blow out the subregion4 to country_alpha2
allocated_laser_hw_ibp = spark.sql("select laser_hw_ibp_records.Product_Category, laser_hw_ibp_records.Subregion4, laser_hw_ibp_records.Sales_Product_w_Opt, laser_hw_ibp_records.Calendar_Year_Month, combined_hw_laser_mix_vw.country_alpha2, laser_hw_ibp_records.Units * combined_hw_laser_mix_vw.split_pct as units, laser_hw_ibp_records.Plan_Date, laser_hw_ibp_records.version, laser_hw_ibp_records.base_prod_number from laser_hw_ibp_records INNER JOIN combined_hw_laser_mix_vw on laser_hw_ibp_records.Subregion4 == combined_hw_laser_mix_vw.country_level_1")

#allocated_laser_hw_ibp.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## INK

# COMMAND ----------

# Numerator
# get a total of hardware units, for INK, for the past 1 year, to use as a numerator in the % mix calculation
hardware_actuals_numerator_ink_records = hardware_actuals_numerator_all_records.filter(hardware_actuals_numerator_all_records.technology == 'INK')

# drop the technology column
hardware_actuals_numerator_ink_records2 = hardware_actuals_numerator_ink_records.drop(hardware_actuals_numerator_ink_records.technology)


# COMMAND ----------

# Denominator
# get a total of hardware units, for INK, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
hardware_actuals_denominator_ink_records = hardware_actuals_denominator_all_records_filtered.filter(hardware_actuals_denominator_all_records_filtered.technology == 'INK')

# drop the technology column
hardware_actuals_denominator_ink_records2 = hardware_actuals_denominator_ink_records.drop(hardware_actuals_denominator_ink_records.technology)

#hardware_actuals_denominator_ink_records2.display()

# COMMAND ----------

# Create split percentage
# join the SINAI2 scenario with the laser supplies recordsets

hardware_actuals_numerator_ink_records2.createOrReplaceTempView("hardware_ink_numerator")
hardware_actuals_denominator_ink_records2.createOrReplaceTempView("hardware_ink_denominator")

# this creates the mix %
hardware_ink_percent_mix = spark.sql("select hardware_ink_denominator.country_level_1, hardware_ink_numerator.country_alpha2, hardware_ink_numerator.numerator / hardware_ink_denominator.denominator AS split_pct from hardware_ink_denominator INNER JOIN hardware_ink_numerator on hardware_ink_denominator.country_level_1 == hardware_ink_numerator.country_level_1")

#hardware_ink_percent_mix.display()

# COMMAND ----------

# create a combined ink mix for the geography groupings

# Union the non-split countries with the split countries mix % to create a full dataset
combined_hardware_ink_mix = no_split_mix.union(hardware_ink_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)

#combined_hardware_ink_mix.display()


# COMMAND ----------

# create a filter list
ls_ink_hw = ['HOME INK BUSINESS PRINTERS','HOME INK CONSUMER PRINTERS'] 

#filter the mapping dataset to the filter list
ink_hw_bpn_to_tech_records = hw_bpn_to_tech_records.filter(hw_bpn_to_tech_records["product_category"].isin(ls_ink_hw))

ink_hw_bpn_to_tech_records.createOrReplaceTempView("ink_hw_base_prod")
hardware_ibp_records.createOrReplaceTempView("ink_hw_ibp")

# Inner join the total hardware records with the filtered mapping to reduce list to category
ink_hw_ibp_records = spark.sql("select ink_hw_ibp.* from ink_hw_ibp INNER JOIN ink_hw_base_prod on ink_hw_ibp.base_prod_number == ink_hw_base_prod.base_prod_number")

ink_hw_ibp_records.createOrReplaceTempView("ink_hw_ibp_records")
combined_hardware_ink_mix.createOrReplaceTempView("combined_hw_ink_mix_vw")

# join the filter ibp hw data with the combined mix dataset to blow out the subregion4 to country_alpha2
allocated_ink_hw_ibp = spark.sql("select ink_hw_ibp_records.Product_Category, ink_hw_ibp_records.Subregion4, ink_hw_ibp_records.Sales_Product_w_Opt, ink_hw_ibp_records.Calendar_Year_Month, combined_hw_ink_mix_vw.country_alpha2, ink_hw_ibp_records.Units * combined_hw_ink_mix_vw.split_pct as units, ink_hw_ibp_records.Plan_Date, ink_hw_ibp_records.version, ink_hw_ibp_records.base_prod_number from ink_hw_ibp_records INNER JOIN combined_hw_ink_mix_vw on ink_hw_ibp_records.Subregion4 == combined_hw_ink_mix_vw.country_level_1")

#allocated_ink_hw_ibp.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## PWA

# COMMAND ----------

# Numerator
# get a total of hardware units, for PWA, for the past 1 year, to use as a numerator in the % mix calculation
hardware_actuals_numerator_pwa_records = hardware_actuals_numerator_all_records.filter(hardware_actuals_numerator_all_records.technology == 'PWA')

# drop the technology column
hardware_actuals_numerator_pwa_records2 = hardware_actuals_numerator_pwa_records.drop(hardware_actuals_numerator_pwa_records.technology)

# hardware_actuals_numerator_pwa_records.display()

# COMMAND ----------

# Denominator
# get a total of hardware units, for PWA, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
hardware_actuals_denominator_pwa_records = hardware_actuals_denominator_all_records_filtered.filter(hardware_actuals_denominator_all_records_filtered.technology == 'PWA')

# drop the technology column
hardware_actuals_denominator_pwa_records2 = hardware_actuals_denominator_pwa_records.drop(hardware_actuals_denominator_pwa_records.technology)

#hardware_actuals_denominator_pwa_records.display()

# COMMAND ----------

# Create split percentage
# join the SINAI2 scenario with the laser supplies recordsets

hardware_actuals_numerator_pwa_records2.createOrReplaceTempView("hardware_pwa_numerator")
hardware_actuals_denominator_pwa_records2.createOrReplaceTempView("hardware_pwa_denominator")

# this creates the mix %
hardware_pwa_percent_mix = spark.sql("select hardware_pwa_denominator.country_level_1, hardware_pwa_numerator.country_alpha2, hardware_pwa_numerator.numerator / hardware_pwa_denominator.denominator AS split_pct from hardware_pwa_denominator INNER JOIN hardware_pwa_numerator on hardware_pwa_denominator.country_level_1 == hardware_pwa_numerator.country_level_1")

#hardware_ink_percent_mix.display()

# COMMAND ----------

# create a combined pwa mix for the geography groupings

# Union the non-split countries with the split countries mix % to create a full dataset
combined_hardware_pwa_mix = no_split_mix.union(hardware_pwa_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)

#combined_hardware_pwa_mix.display()


# COMMAND ----------

# create a filter list
ls_pwa_hw = ['A3 OFFICE PAGEWIDE INK PRINTER','A4 OFFICE PAGEWIDE INK PRINTER'] 

#filter the mapping dataset to the filter list
pwa_hw_bpn_to_tech_records = hw_bpn_to_tech_records.filter(hw_bpn_to_tech_records["product_category"].isin(ls_pwa_hw))

pwa_hw_bpn_to_tech_records.createOrReplaceTempView("pwa_hw_base_prod")
hardware_ibp_records.createOrReplaceTempView("pwa_hw_ibp")

# Inner join the total hardware records with the filtered mapping to reduce list to category
pwa_hw_ibp_records = spark.sql("select pwa_hw_ibp.* from pwa_hw_ibp INNER JOIN pwa_hw_base_prod on pwa_hw_ibp.base_prod_number == pwa_hw_base_prod.base_prod_number")

pwa_hw_ibp_records.createOrReplaceTempView("pwa_hw_ibp_records")
combined_hardware_pwa_mix.createOrReplaceTempView("combined_hw_pwa_mix_vw")

# join the filter ibp hw data with the combined mix dataset to blow out the subregion4 to country_alpha2
allocated_pwa_hw_ibp = spark.sql("select pwa_hw_ibp_records.Product_Category, pwa_hw_ibp_records.Subregion4, pwa_hw_ibp_records.Sales_Product_w_Opt, pwa_hw_ibp_records.Calendar_Year_Month, combined_hw_pwa_mix_vw.country_alpha2, pwa_hw_ibp_records.Units * combined_hw_pwa_mix_vw.split_pct as units, pwa_hw_ibp_records.Plan_Date, pwa_hw_ibp_records.version, pwa_hw_ibp_records.base_prod_number from pwa_hw_ibp_records INNER JOIN combined_hw_pwa_mix_vw on pwa_hw_ibp_records.Subregion4 == combined_hw_pwa_mix_vw.country_level_1")

#allocated_pwa_hw_ibp.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## LF

# COMMAND ----------

# Numerator
# get a total of hardware units, for LF, for the past 1 year, to use as a numerator in the % mix calculation
hardware_actuals_numerator_lf_records = hardware_actuals_numerator_all_records.filter(hardware_actuals_numerator_all_records.technology == 'LF')

# drop the technology column
hardware_actuals_numerator_lf_records2 = hardware_actuals_numerator_lf_records.drop(hardware_actuals_numerator_lf_records.technology)

#hardware_actuals_numerator_lf_records.display()

# COMMAND ----------

# Denominator
# get a total of hardware units, for LF, for the past 1 year, to use as a denominator in the % mix calculation, then drop the technology column
hardware_actuals_denominator_lf_records = hardware_actuals_denominator_all_records_filtered.filter(hardware_actuals_denominator_all_records_filtered.technology == 'LF')

# drop the technology column
hardware_actuals_denominator_lf_records2 = hardware_actuals_denominator_lf_records.drop(hardware_actuals_denominator_lf_records.technology)

#hardware_actuals_denominator_lf_records.display()

# COMMAND ----------

# Create split percentage
# join the SINAI2 scenario with the laser supplies recordsets

hardware_actuals_numerator_lf_records2.createOrReplaceTempView("hardware_lf_numerator")
hardware_actuals_denominator_lf_records2.createOrReplaceTempView("hardware_lf_denominator")

# this creates the mix %
hardware_lf_percent_mix = spark.sql("select hardware_lf_denominator.country_level_1, hardware_lf_numerator.country_alpha2, hardware_lf_numerator.numerator / hardware_lf_denominator.denominator AS split_pct from hardware_lf_denominator INNER JOIN hardware_lf_numerator on hardware_lf_denominator.country_level_1 == hardware_lf_numerator.country_level_1")

#hardware_lf_percent_mix.display()

# COMMAND ----------

# create a combined lf mix for the geography groupings

# Union the non-split countries with the split countries mix % to create a full dataset
combined_hardware_lf_mix = no_split_mix.union(hardware_lf_percent_mix).sort(['country_level_1','country_alpha2'], ascending = True)

#combined_hardware_lf_mix.display()


# COMMAND ----------

# create a filter list
ls_lf_hw = ['LARGE FORMAT DESIGN','LARGE FORMAT PRODUCTION'] 

#filter the mapping dataset to the filter list
lf_hw_bpn_to_tech_records = hw_bpn_to_tech_records.filter(hw_bpn_to_tech_records["product_category"].isin(ls_lf_hw))

lf_hw_bpn_to_tech_records.createOrReplaceTempView("lf_hw_base_prod")
hardware_ibp_records.createOrReplaceTempView("lf_hw_ibp")

# Inner join the total hardware records with the filtered mapping to reduce list to category
lf_hw_ibp_records = spark.sql("select lf_hw_ibp.* from lf_hw_ibp INNER JOIN lf_hw_base_prod on lf_hw_ibp.base_prod_number == lf_hw_base_prod.base_prod_number")

lf_hw_ibp_records.createOrReplaceTempView("lf_hw_ibp_records")
combined_hardware_lf_mix.createOrReplaceTempView("combined_hw_lf_mix_vw")

# join the filter ibp hw data with the combined mix dataset to blow out the subregion4 to country_alpha2
allocated_lf_hw_ibp = spark.sql("select lf_hw_ibp_records.Product_Category, lf_hw_ibp_records.Subregion4, lf_hw_ibp_records.Sales_Product_w_Opt, lf_hw_ibp_records.Calendar_Year_Month, combined_hw_lf_mix_vw.country_alpha2, lf_hw_ibp_records.Units * combined_hw_lf_mix_vw.split_pct as units, lf_hw_ibp_records.Plan_Date, lf_hw_ibp_records.version, lf_hw_ibp_records.base_prod_number from lf_hw_ibp_records INNER JOIN combined_hw_lf_mix_vw on lf_hw_ibp_records.Subregion4 == combined_hw_lf_mix_vw.country_level_1")

#allocated_lf_hw_ibp.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## UNION technologies

# COMMAND ----------

# UNION the LASER, INK, PWA, LF hardware datasets together
combined_hw_allocated_mix = allocated_laser_hw_ibp.union(allocated_ink_hw_ibp).union(allocated_pwa_hw_ibp).union(allocated_lf_hw_ibp)

#combined_hw_allocated_mix.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Final Datasets (stage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## supplies

# COMMAND ----------

# clean up the dataframe to a format that we're used to seening, and write to the staging database

from pyspark.sql.functions import *

combined_lf_mix1 = combined_lf_mix \
  .withColumn("record", lit('IBP_SUPPLIES_FCST'))

combined_lf_mix2 = combined_lf_mix1 \
  .select("record", "country_alpha2", "Sales_Product", "Calendar_Year_Month", "units", "Plan_date", "version")

combined_lf_mix3 = combined_lf_mix2 \
  .withColumnRenamed("Sales_Product", "sales_product_number") \
  .withColumnRenamed("Calendar_Year_Month", "cal_date") \
  .withColumnRenamed("Plan_date", "load_date")

write_df_to_redshift(configs, combined_lf_mix3, "stage.ibp_supplies_forecast_stage", "overwrite")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Hardware

# COMMAND ----------

# clean up the dataframe to a format that we're used to seening, and write to the staging database

from pyspark.sql.functions import *

combined_hw_allocated_mix1 = combined_hw_allocated_mix \
  .withColumn("record", lit('IBP_HW_FCST'))

combined_hw_allocated_mix2 = combined_hw_allocated_mix1 \
  .select("Product_Category", "record", "country_alpha2", "base_prod_number", "Sales_Product_w_Opt", "Calendar_Year_Month", "units", "Plan_date", "version")

combined_hw_allocated_mix3 = combined_hw_allocated_mix2 \
  .withColumnRenamed("Sales_Product_w_Opt", "sales_product_w_opt") \
  .withColumnRenamed("base_prod_number", "base_product_number") \
  .withColumnRenamed("Calendar_Year_Month", "cal_date") \
  .withColumnRenamed("Plan_date", "load_date")

#combined_hw_allocated_mix2.display()
write_df_to_redshift(configs, combined_hw_allocated_mix3, "stage.ibp_hw_forecast_stage", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC # Final Datasets (prod)

# COMMAND ----------

# MAGIC %md
# MAGIC ## supplies

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## hardware

# COMMAND ----------


