# Databricks notebook source
dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.dropdown("stack", "dev", ["dev", "itg", "prod"])
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("job_dbfs_path", "")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as f

hardware_ltf = read_redshift_to_df(configs) \
    .option("dbtable", "prod.hardware_ltf") \
    .load() \
    .select("record", "version", "cal_date", "base_product_number", "country_alpha2", "units") \
    .distinct()

flash_wd3 = read_redshift_to_df(configs) \
    .option("dbtable", "prod.flash_wd3") \
    .load() \
    .withColumn('max_version', f.max('version').over(Window.partitionBy('record')))\
    .where(f.col('version') == f.col('max_version'))\
    .drop('max_version')

flash = flash_wd3.filter("record = 'FLASH'")

rdma = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.rdma") \
    .load() \
    .select("base_prod_number", "platform_subset", "pl") \
    .distinct()

hardware_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.hardware_xref") \
    .load() \
    .select("platform_subset", "category_feature", "technology") \
    .distinct()

iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load() \
    .select("country_alpha2", "region_5") \
    .distinct()

wd3 = flash_wd3.filter("record = 'WD3'")

product_line_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.product_line_xref") \
    .load() \
    .select("technology", "pl", "pl_category") \
    .where("PL_category = 'HW'") \
    .distinct()

hardware_ltf.createOrReplaceTempView("hardware_ltf")
flash.createOrReplaceTempView("flash")
rdma.createOrReplaceTempView("rdma")
hardware_xref.createOrReplaceTempView("hardware_xref")
iso_country_code_xref.createOrReplaceTempView("iso_country_code_xref")
wd3.createOrReplaceTempView("wd3")
product_line_xref.createOrReplaceTempView("product_line_xref")

# COMMAND ----------

ltf_version = spark.sql("""SELECT MAX(version) AS version FROM hardware_ltf WHERE UPPER(record) = 'HW_FCST'""").head()[0]
print("ltf_version: " + ltf_version)

ltf_record = "HW_FCST"
print("ltf_record: " + ltf_record)

wd3_max_cal_date = str(spark.sql("""SELECT MAX(cal_date) AS date FROM wd3 WHERE record LIKE ('WD3%')""").head()[0])
print("wd3_max_cal_date: " + wd3_max_cal_date)

flash_version = spark.sql("""SELECT MAX(version) AS version FROM flash""").head()[0]
print("flash_version: " + flash_version)

flash_record = "flash"
print("flash_record: " + flash_record)

wd3_load_date = str(spark.sql("""SELECT MAX(load_date) AS load_date FROM wd3 WHERE record LIKE ('WD3%')""").head()[0])
print("wd3_load_date: " + wd3_load_date)

flash_forecast_name = spark.sql(f"""SELECT DISTINCT source_name FROM flash WHERE version = '{flash_version}'""").head()[0]
print("flash_forecast_name: " + flash_forecast_name)

wd3_record_name = spark.sql(f"""SELECT DISTINCT record FROM wd3 WHERE record LIKE ('WD3%') AND load_date = '{wd3_load_date}'""").head()[0]
print("wd3_record_name: " + wd3_record_name)

# COMMAND ----------

# --populate ltf combos
wd3_allocated_ltf_ltf_combos = spark.sql(f"""
SELECT DISTINCT
      d.region_5
    , a.cal_date
    , c.category_feature
FROM hardware_ltf a
LEFT JOIN rdma b
    ON UPPER(a.base_product_number)=UPPER(b.base_prod_number)
LEFT JOIN hardware_xref c
    ON UPPER(b.platform_subset)=UPPER(c.platform_subset)
LEFT JOIN iso_country_code_xref d
    ON a.country_alpha2=d.country_alpha2
WHERE 1=1
    AND a.record = "{ltf_record}"
    AND a.version =  "{ltf_version}"
    AND a.cal_date <= "{wd3_max_cal_date}"
""")

wd3_allocated_ltf_ltf_combos.createOrReplaceTempView("wd3_allocated_ltf_ltf_combos")

# COMMAND ----------

# --populate flash combos
wd3_allocated_ltf_flash_combos = spark.sql(f"""
SELECT DISTINCT
      d.region_5
    , a.cal_date
    , c.category_feature
FROM flash a
LEFT JOIN rdma b
    ON UPPER(a.base_product_number)=UPPER(b.base_prod_number)
LEFT JOIN hardware_xref c
    ON UPPER(b.platform_subset)=UPPER(c.platform_subset)
LEFT JOIN iso_country_code_xref d
    ON a.country_alpha2=d.country_alpha2
ORDER BY a.cal_date
""")

wd3_allocated_ltf_flash_combos.createOrReplaceTempView("wd3_allocated_ltf_flash_combos")

# COMMAND ----------

# --populate wd3 combos
wd3_allocated_ltf_wd3_combos = spark.sql(f"""
SELECT DISTINCT
      d.region_5
    , a.cal_date
    , c.category_feature
FROM wd3 a
LEFT JOIN rdma b
    ON UPPER(a.base_prod_number)=UPPER(b.base_prod_number)
LEFT JOIN hardware_xref c
    ON UPPER(b.Platform_Subset)=UPPER(c.platform_subset)
LEFT JOIN iso_country_code_xref d
    ON a.country_alpha2=d.country_alpha2
WHERE 1=1
	AND a.units > 0
	AND c.technology IN ('INK','LASER','PWA')
""")

wd3_allocated_ltf_wd3_combos.createOrReplaceTempView("wd3_allocated_ltf_wd3_combos")

# COMMAND ----------

# --populate combos that are in the ltf, that are not in the WD3 combos
wd3_allocated_ltf_missing_ltf_combos = spark.sql("""
SELECT a.*
FROM wd3_allocated_ltf_ltf_combos a
LEFT JOIN wd3_allocated_ltf_wd3_combos b
ON a.region_5 = b.region_5
	AND a.cal_date = b.cal_date
	AND UPPER(a.category_feature)=UPPER(b.category_feature)
WHERE b.region_5 IS NULL
ORDER BY 3,1,2
""")

wd3_allocated_ltf_missing_ltf_combos.createOrReplaceTempView("wd3_allocated_ltf_missing_ltf_combos")

# COMMAND ----------

# --populate combos that are in the flash, that are not in the WD3 combos
wd3_allocated_ltf_missing_flash_combos = spark.sql("""
SELECT a.*
FROM wd3_allocated_ltf_flash_combos a
LEFT JOIN wd3_allocated_ltf_wd3_combos b
ON a.region_5 = b.region_5
	AND a.cal_date = b.cal_date
	AND UPPER(a.category_feature)=UPPER(b.category_feature)
WHERE b.region_5 IS NULL
ORDER BY 3,1,2
""")

wd3_allocated_ltf_missing_flash_combos.createOrReplaceTempView("wd3_allocated_ltf_missing_flash_combos")

# COMMAND ----------

# --populate ltf units
wd3_allocated_ltf_ltf_units = spark.sql(f"""
SELECT
	  d.region_5
	, a.cal_date
	, c.category_feature
	, SUM(a.units) AS units
FROM hardware_ltf a
LEFT JOIN rdma b ON UPPER(a.base_product_number)=UPPER(b.base_prod_number)
LEFT JOIN hardware_xref c ON UPPER(b.platform_subset)=UPPER(c.platform_subset)
LEFT JOIN iso_country_code_xref d on a.country_alpha2=d.country_alpha2
WHERE 1=1
	AND UPPER(a.record) = "{ltf_record}"
	AND a.version = "{ltf_version}"
	AND a.cal_date <= "{wd3_max_cal_date}"
GROUP BY 
	  d.region_5
	, cal_date
	, category_feature
ORDER BY 3,2,1;
""")

wd3_allocated_ltf_ltf_units.createOrReplaceTempView("wd3_allocated_ltf_ltf_units")

# COMMAND ----------

# --populate flash units
wd3_allocated_ltf_flash_units = spark.sql(f"""
SELECT
	  d.region_5
	, a.cal_date
	, c.category_feature
	, SUM(a.units) AS units
FROM flash a
LEFT JOIN rdma b ON UPPER(a.base_product_number)=UPPER(b.base_prod_number)
LEFT JOIN hardware_xref c ON UPPER(b.platform_subset)=UPPER(c.platform_subset)
LEFT JOIN iso_country_code_xref d on a.country_alpha2=d.country_alpha2
GROUP BY 
	  d.region_5
	, cal_date
	, category_feature
ORDER BY 3,2,1;
""")

wd3_allocated_ltf_flash_units.createOrReplaceTempView("wd3_allocated_ltf_flash_units")

# COMMAND ----------

# --populate wd3 units
wd3_allocated_ltf_wd3_units = spark.sql("""
SELECT 
	  a.record
	, d.region_5
	, a.cal_date
	, a.country_alpha2
	, a.base_product_number
	, c.category_feature
	, a.version
	, sum(a.units) AS units
FROM wd3 a
LEFT JOIN rdma b ON UPPER(a.base_product_number)=UPPER(b.base_prod_number)
LEFT JOIN hardware_xref c ON UPPER(b.platform_subset)=UPPER(c.platform_subset)
LEFT JOIN iso_country_code_xref d on a.country_alpha2=d.country_alpha2
WHERE 1=1
	AND units > 0
	AND UPPER(c.technology) IN ('INK','LASER','PWA')
GROUP BY 
	  a.record
	, region_5
	, cal_date
	, a.country_alpha2
	, a.base_product_number
	, c.category_feature
	, a.version
""")

wd3_allocated_ltf_wd3_units.createOrReplaceTempView("wd3_allocated_ltf_wd3_units")

# COMMAND ----------

# --populate wd3 pct
wd3_allocated_ltf_wd3_pct = spark.sql("""
SELECT
	  region_5
	, cal_date
	, country_alpha2
	, base_product_number
	, category_feature
	, units
	, (units /sum(units) OVER (PARTITION BY region_5, cal_date, category_feature)) AS pct
FROM wd3_allocated_ltf_wd3_units
WHERE 1=1
""")

wd3_allocated_ltf_wd3_pct.createOrReplaceTempView("wd3_allocated_ltf_wd3_pct")

# COMMAND ----------

# --populate allocated ltf units
wd3_allocated_ltf_allocated_ltf_units = spark.sql("""
SELECT 
	  a.region_5
	, a.cal_date
	, a.country_alpha2
	, a.base_product_number
	, a.category_feature
	, (a.pct * b.units) AS allocated_units
FROM wd3_allocated_ltf_wd3_pct a
INNER JOIN wd3_allocated_ltf_ltf_units b 
	ON a.cal_date=b.cal_date 
	AND a.region_5=b.region_5
	AND UPPER(a.category_feature) = UPPER(b.category_feature)
WHERE 1=1;
""")

wd3_allocated_ltf_allocated_ltf_units.createOrReplaceTempView("wd3_allocated_ltf_allocated_ltf_units")

# COMMAND ----------

# --populate allocated flash units
wd3_allocated_ltf_allocated_flash_units = spark.sql("""
SELECT
	  a.region_5
	, a.cal_date
	, a.country_alpha2
	, a.base_product_number
	, a.category_feature
	, (a.pct * b.units) AS units
FROM wd3_allocated_ltf_wd3_pct a
INNER JOIN wd3_allocated_ltf_flash_units b 
	ON a.cal_date=b.cal_date 
	AND a.region_5=b.region_5
	AND UPPER(a.category_feature) = UPPER(b.category_feature)
WHERE 1=1;
""")

wd3_allocated_ltf_allocated_flash_units.createOrReplaceTempView("wd3_allocated_ltf_allocated_flash_units")

# COMMAND ----------

# --unallocated ltf units
wd3_allocated_ltf_unallocated_ltf_units = spark.sql(f"""
SELECT
	  d.region_5
	, a.cal_date
	, a.country_alpha2
	, a.base_product_number
	, c.category_feature
	, SUM(a.units) as units
FROM hardware_ltf a
LEFT JOIN rdma b ON UPPER(a.base_product_number)=UPPER(b.Base_Prod_Number)
LEFT JOIN hardware_xref c ON UPPER(b.Platform_Subset)=UPPER(c.platform_subset)
LEFT JOIN iso_country_code_xref d on a.country_alpha2=d.country_alpha2
INNER JOIN wd3_allocated_ltf_missing_ltf_combos f
	ON UPPER(c.category_feature)=UPPER(f.category_feature)
	AND a.cal_date = f.cal_date
	AND d.region_5=f.region_5
WHERE 1=1
	AND UPPER(a.record) = "{ltf_record}"
	AND a.version = "{ltf_version}"
	AND a.cal_date <= "{wd3_max_cal_date}"
GROUP BY
	  d.region_5
	, a.cal_date
	, a.country_alpha2
	, a.base_product_number
	, c.category_feature
""")

wd3_allocated_ltf_unallocated_ltf_units.createOrReplaceTempView("wd3_allocated_ltf_unallocated_ltf_units")

# COMMAND ----------

# --unallocated flash units
wd3_allocated_ltf_unallocated_flash_units = spark.sql(f"""
SELECT
	  d.region_5
	, a.cal_date
	, a.country_alpha2
	, a.base_product_number
	, c.category_feature
	, SUM(a.units) as units
FROM flash a
LEFT JOIN rdma b ON UPPER(a.base_product_number)=UPPER(b.base_prod_number)
LEFT JOIN hardware_xref c ON UPPER(b.platform_subset)=UPPER(c.platform_subset)
LEFT JOIN iso_country_code_xref d on a.country_alpha2=d.country_alpha2
INNER JOIN wd3_allocated_ltf_missing_flash_combos f
	ON UPPER(c.category_feature)=UPPER(f.category_feature)
	AND a.cal_date = f.cal_date
	AND d.region_5=f.region_5
GROUP BY
	  d.region_5
	, a.cal_date
	, a.country_alpha2
	, a.base_product_number
	, c.category_feature;
""")

wd3_allocated_ltf_unallocated_flash_units.createOrReplaceTempView("wd3_allocated_ltf_unallocated_flash_units")

# COMMAND ----------

# --bring them all together
wd3_allocated_ltf_final = spark.sql(f"""
SELECT
	"{flash_forecast_name}" as source
	,*
FROM wd3_allocated_ltf_allocated_flash_units
UNION ALL
SELECT
	"{flash_forecast_name}" as source
	,*
FROM wd3_allocated_ltf_unallocated_flash_units
UNION ALL
SELECT
	"{wd3_record_name}" as source
	,*
FROM wd3_allocated_ltf_allocated_ltf_units
UNION ALL
SELECT
	"{wd3_record_name}" as source
	,*
FROM wd3_allocated_ltf_unallocated_ltf_units
ORDER BY source, region_5, category_feature, cal_date
""")

wd3_allocated_ltf_final.createOrReplaceTempView("wd3_allocated_ltf_final")

# COMMAND ----------

# --load data to allocated_ltf_landing table
# --update load_date
allocated_ltf_landing = spark.sql(f"""
SELECT
	  'STF' AS record
	, cal_date
	, country_alpha2
	, base_product_number
	, SUM(units) AS units
	, source
FROM wd3_allocated_ltf_final 
WHERE 1=1
GROUP BY
	  cal_date
	, country_alpha2
	, base_product_number
	, source
""")

allocated_ltf_landing.createOrReplaceTempView("allocated_ltf_landing")

# COMMAND ----------

# --load latest stitched dataset to hardware_stf landing table
hardware_stf_landing = spark.sql("""
SELECT 
	  'ALLOCATED FLASH PLUS LTF' AS record
    , country_alpha2 AS geo
    , base_product_number as base_prod_number
    , cal_date as date
	, units
FROM allocated_ltf_landing
""")

hardware_stf_landing.createOrReplaceTempView("hardware_stf_landing")

# COMMAND ----------

# --Add version to version table
max_version_info = call_addversion_sproc('HW_STF_FCST', 'ARCHER')

max_forecast_version = max_version_info[0]
max_forecast_load_date = max_version_info[1]

print("max_forecast_version: " + max_forecast_version)
print("max_forecast_load_date: " + str(max_forecast_load_date))

# COMMAND ----------

# --load data to staging table
# --UPDATE staging load_date and version
hardware_stf_staging = spark.sql(f"""
SELECT DISTINCT
      'HW_STF_FCST' AS record
	, s.record AS forecast_name
	, s.date AS cal_date
	, i.region_5
	, s.geo AS country_alpha2
	, rdma.platform_subset
	, s.base_prod_number AS base_product_number
	, s.units
	, CAST("{max_forecast_load_date}" AS date) AS load_date
	, CAST('true' AS BOOLEAN) AS official
	, "{max_forecast_version}" AS version
FROM hardware_stf_landing s
	LEFT JOIN rdma ON UPPER(rdma.base_prod_number)=UPPER(s.base_prod_number)
	LEFT JOIN iso_country_code_xref i ON s.geo=i.country_alpha2
""")

hardware_stf_staging.createOrReplaceTempView("hardware_stf_staging")

# COMMAND ----------

submit_remote_query(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], "UPDATE prod.hardware_ltf SET official = 0 WHERE UPPER(record) = 'HW_STF_FCST' AND official=1;")

# COMMAND ----------

# --move to prod
hardware_ltf = spark.sql("""
SELECT DISTINCT
      a.record
    , a.forecast_name
    , a.cal_date
    , a.country_alpha2
    , b.platform_subset
    , a.base_product_number
    , a.units
    , a.official
    , a.load_date
    , a.version
FROM hardware_stf_staging a 
    LEFT JOIN rdma b ON UPPER(a.platform_subset)=UPPER(b.platform_subset)
    LEFT JOIN product_line_xref c ON UPPER(b.pl) = UPPER(c.PL)
WHERE UPPER(c.Technology) IN ('INK','LASER','PWA') AND UPPER(c.pl_category) = 'HW'
	AND UPPER(a.platform_subset) NOT LIKE ('ACCESSORY %')
	AND UPPER(a.platform_subset) <> 'MOBILE DONGLE'
	AND UPPER(a.platform_subset) <> 'PAGEWIDE ACCESSORIES'
	AND units <> 0
""")

# COMMAND ----------

wd3_allocated_ltf_ltf_units.cache()
write_df_to_redshift(configs, wd3_allocated_ltf_ltf_units, "stage.wd3_allocated_ltf_ltf_units", "overwrite")

wd3_allocated_ltf_flash_units.cache()
write_df_to_redshift(configs, wd3_allocated_ltf_flash_units, "stage.wd3_allocated_ltf_flash_units", "overwrite")

wd3_allocated_ltf_wd3_units.cache()
write_df_to_redshift(configs, wd3_allocated_ltf_wd3_units, "stage.wd3_allocated_ltf_wd3_units", "overwrite")

wd3_allocated_ltf_wd3_pct.cache()
write_df_to_redshift(configs, wd3_allocated_ltf_wd3_pct, "stage.wd3_allocated_ltf_wd3_pct", "overwrite")

write_df_to_redshift(configs, hardware_ltf, "prod.hardware_ltf", "append")
