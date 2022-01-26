// Databricks notebook source
dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.dropdown("stack", "dev", Seq("dev", "itg", "prod"))
dbutils.widgets.text("aws_iam_role", "")

// COMMAND ----------

// MAGIC %run ../common/Constants.scala

// COMMAND ----------

// MAGIC %run ../common/DatabaseUtils.scala

// COMMAND ----------

// MAGIC %run ../../python/common/secrets_manager_utils.py

// COMMAND ----------

// MAGIC %python
// MAGIC # retrieve secrets based on incoming/inputted secrets name - variables will be accessible across languages
// MAGIC 
// MAGIC redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")
// MAGIC spark.conf.set("redshift_username", redshift_secrets["username"])
// MAGIC spark.conf.set("redshift_password", redshift_secrets["password"])
// MAGIC 
// MAGIC sqlserver_secrets = secrets_get(dbutils.widgets.get("sqlserver_secrets_name"), "us-west-2")
// MAGIC spark.conf.set("sfai_username", sqlserver_secrets["username"])
// MAGIC spark.conf.set("sfai_password", sqlserver_secrets["password"])

// COMMAND ----------

var configs: Map[String, String] = Map()
configs += ("env" -> dbutils.widgets.get("stack"),
            "sfaiUsername" -> spark.conf.get("sfai_username"),
            "sfaiPassword" -> spark.conf.get("sfai_password"),
            "sfaiUrl" -> SFAI_URL,
            "sfaiDriver" -> SFAI_DRIVER,
            "redshiftUsername" -> spark.conf.get("redshift_username"),
            "redshiftPassword" -> spark.conf.get("redshift_password"),
            "redshiftAwsRole" -> dbutils.widgets.get("aws_iam_role"),
            "redshiftUrl" -> s"""jdbc:redshift://${REDSHIFT_URLS(dbutils.widgets.get("stack"))}:${REDSHIFT_PORTS(dbutils.widgets.get("stack"))}/${dbutils.widgets.get("stack")}?ssl_verify=None""",
            "redshiftTempBucket" -> s"""${S3_BASE_BUCKETS(dbutils.widgets.get("stack"))}redshift_temp/""")

// COMMAND ----------

val hardwareLtf = readSqlServerToDF(configs)
  .option("dbtable", "ie2_prod.dbo.hardware_ltf")
  .load()
  .select("record", "version", "cal_date", "base_product_number", "country_alpha2", "units")
  .distinct()

val flash = readSqlServerToDF(configs)
  .option("dbtable", "ie2_prod.dbo.flash")
  .load()
  .select("record", "version", "cal_date", "base_product_number", "country_alpha2", "units", "forecast_name")
  .distinct()

val rdma = readSqlServerToDF(configs)
  .option("dbtable", "ie2_prod.dbo.rdma")
  .load()
  .select("Base_Prod_Number", "Platform_Subset", "pl")
  .distinct()

val hardwareXref = readSqlServerToDF(configs)
  .option("dbtable", "ie2_prod.dbo.hardware_xref")
  .load()
  .select("platform_subset", "category_feature", "technology")
  .distinct()

val isoCountryCodeXref = readSqlServerToDF(configs)
  .option("dbtable", "ie2_prod.dbo.iso_country_code_xref")
  .load()
  .select("country_alpha2", "region_5")
  .distinct()

val stfWd3CountrySpeedlicVw = readSqlServerToDF(configs)
  .option("dbtable", "archer_prod.dbo.stf_wd3_country_speedlic_vw")
  .load()
  .select("date", "base_prod_number", "geo", "record", "load_date", "units", "version")
  .distinct()

val productLineXref = readSqlServerToDF(configs)
  .option("dbtable", "ie2_prod.dbo.product_line_xref")
  .load()
  .select("Technology", "PL", "PL_category")
  .where("PL_category = 'HW'")
  .distinct()

hardwareLtf.createOrReplaceTempView("hardware_ltf")
flash.createOrReplaceTempView("flash")
rdma.createOrReplaceTempView("rdma")
hardwareXref.createOrReplaceTempView("hardware_xref")
isoCountryCodeXref.createOrReplaceTempView("iso_country_code_xref")
stfWd3CountrySpeedlicVw.createOrReplaceTempView("stf_wd3_country_speedlic_vw")
productLineXref.createOrReplaceTempView("product_line_xref")

// COMMAND ----------

val ltfVersion: String = spark.sql("""SELECT MAX(version) AS version FROM hardware_ltf WHERE record = 'hw_fcst'""").collect().map(_.getString(0)).array(0)

val ltfRecord: String = "hw_fcst"

val ltfMaxCalDate: String = spark.sql("""SELECT MAX(date) AS date FROM stf_wd3_country_speedlic_vw WHERE record LIKE ('WD3%')""").collect().map(_.getDate(0).toString).array(0)

val flashVersion: String = spark.sql("""SELECT MAX(version) AS version FROM flash""").collect().map(_.getString(0)).array(0)

val flashRecord: String = "flash"

val wd3LoadDate: java.sql.Timestamp = spark.sql("""SELECT MAX(load_date) AS load_date FROM stf_wd3_country_speedlic_vw WHERE record LIKE ('WD3%')""").collect().map(_.getTimestamp(0)).array(0)

val flashForecastName: String = spark.sql(s"""SELECT DISTINCT forecast_name FROM flash WHERE 1=1 AND version = '${flashVersion}'""").collect().map(_.getString(0)).array(0)

val wd3RecordName: String = spark.sql(s"""SELECT DISTINCT record FROM stf_wd3_country_speedlic_vw WHERE record LIKE ('WD3%') AND load_date = '${wd3LoadDate}'""").collect().map(_.getString(0)).array(0)

// COMMAND ----------

// --populate ltf combos
val wd3AllocatedLtfLtfCombos = spark.sql(s"""
SELECT DISTINCT
    d.region_5
  , a.cal_date
  , c.category_feature
FROM hardware_ltf a
LEFT JOIN rdma b
  ON a.base_product_number=b.Base_Prod_Number
LEFT JOIN hardware_xref c
  ON b.Platform_Subset=c.platform_subset
LEFT JOIN iso_country_code_xref d
  ON a.country_alpha2=d.country_alpha2
WHERE 1=1
  AND a.record = "${ltfRecord}"
  AND a.version =  "${ltfVersion}"
  AND a.cal_date <= "${ltfMaxCalDate}"
""")

wd3AllocatedLtfLtfCombos.createOrReplaceTempView("wd3_allocated_ltf_ltf_combos")

// COMMAND ----------

// --populate flash combos
val wd3AllocatedLtfFlashCombos = spark.sql(s"""
SELECT DISTINCT
      d.region_5
	, a.cal_date
	, c.category_feature
FROM flash a
LEFT JOIN rdma b
  ON a.base_product_number=b.Base_Prod_Number
LEFT JOIN hardware_xref c
  ON b.Platform_Subset=c.platform_subset
LEFT JOIN iso_country_code_xref d
  ON a.country_alpha2=d.country_alpha2
WHERE 1=1
	AND a.record = "${flashRecord}"
	AND a.version = "${flashVersion}"
ORDER BY a.cal_date
""")

wd3AllocatedLtfFlashCombos.createOrReplaceTempView("wd3_allocated_ltf_flash_combos")

// COMMAND ----------

// --populate wd3 combos
val wd3AllocatedLtfWd3Combos = spark.sql(s"""
SELECT DISTINCT
      d.region_5
	, a.date AS cal_date
	, c.category_feature
FROM stf_wd3_country_speedlic_vw a
LEFT JOIN rdma b
  ON a.base_prod_number=b.Base_Prod_Number
LEFT JOIN hardware_xref c
  ON b.Platform_Subset=c.platform_subset
LEFT JOIN iso_country_code_xref d
  ON a.geo=d.country_alpha2
WHERE 1=1
	AND a.record LIKE ('WD3%')
	AND a.load_date = "${wd3LoadDate}"
	AND a.units > 0
	AND c.technology IN ('INK','LASER','PWA')
""")

wd3AllocatedLtfWd3Combos.createOrReplaceTempView("wd3_allocated_ltf_wd3_combos")

// COMMAND ----------

// --populate combos that are in the ltf, that are not in the WD3 combos
val wd3AllocatedLtfMissingLtfCombos = spark.sql("""
SELECT a.*
FROM wd3_allocated_ltf_ltf_combos a
LEFT JOIN wd3_allocated_ltf_wd3_combos b
ON a.region_5 = b.region_5
	AND a.cal_date = b.cal_date
	AND a.category_feature=b.category_feature
WHERE b.region_5 IS NULL
ORDER BY 3,1,2
""")

wd3AllocatedLtfMissingLtfCombos.createOrReplaceTempView("wd3_allocated_ltf_missing_ltf_combos")

// COMMAND ----------

// --populate combos that are in the flash, that are not in the WD3 combos
val wd3AllocatedLtfMissingFlashCombos = spark.sql("""
SELECT a.*
FROM wd3_allocated_ltf_flash_combos a
LEFT JOIN wd3_allocated_ltf_wd3_combos b
ON a.region_5 = b.region_5
	AND a.cal_date = b.cal_date
	AND a.category_feature=b.category_feature
WHERE b.region_5 IS NULL
ORDER BY 3,1,2
""")

wd3AllocatedLtfMissingFlashCombos.createOrReplaceTempView("wd3_allocated_ltf_missing_flash_combos")

// COMMAND ----------

// --populate ltf units
val wd3AllocatedLtfLtfUnits = spark.sql(s"""
SELECT
	  d.region_5
	, a.cal_date
	, c.category_feature
	, SUM(a.units) AS units
FROM hardware_ltf a
LEFT JOIN rdma b ON a.base_product_number=b.Base_Prod_Number
LEFT JOIN hardware_xref c ON b.Platform_Subset=c.platform_subset
LEFT JOIN iso_country_code_xref d on a.country_alpha2=d.country_alpha2
WHERE 1=1
	AND a.record = "${ltfRecord}"
	AND a.version = "${ltfVersion}"
	AND a.cal_date <= "${ltfMaxCalDate}"
GROUP BY 
	  d.region_5
	, cal_date
	, category_feature
ORDER BY 3,2,1;
""")

wd3AllocatedLtfLtfUnits.createOrReplaceTempView("wd3_allocated_ltf_ltf_units")

// COMMAND ----------

// --populate flash units
val wd3AllocatedLtfFlashUnits = spark.sql(s"""
SELECT
	  d.region_5
	, a.cal_date
	, c.category_feature
	, SUM(a.units) AS units
FROM flash a
LEFT JOIN rdma b ON a.base_product_number=b.Base_Prod_Number
LEFT JOIN hardware_xref c ON b.Platform_Subset=c.platform_subset
LEFT JOIN iso_country_code_xref d on a.country_alpha2=d.country_alpha2
WHERE 1=1
	AND a.version = "${flashVersion}"
GROUP BY 
	  d.region_5
	, cal_date
	, category_feature
ORDER BY 3,2,1;
""")

wd3AllocatedLtfFlashUnits.createOrReplaceTempView("wd3_allocated_ltf_flash_units")

// COMMAND ----------

//--populate wd3 units
val wd3AllocatedLtfWd3Units = spark.sql("""
SELECT 
	  a.record
	, d.region_5
	, a.date AS cal_date
	, a.geo AS country_alpha2
	, a.base_prod_number AS base_product_number
	, c.category_feature
	, a.version
	, sum(a.units) AS units
FROM stf_wd3_country_speedlic_vw a
LEFT JOIN rdma b ON a.base_prod_number=b.Base_Prod_Number
LEFT JOIN hardware_xref c ON b.Platform_Subset=c.platform_subset
LEFT JOIN iso_country_code_xref d on a.geo=d.country_alpha2
WHERE 1=1
	AND a.record LIKE ('WD3%')
	AND a.load_date = "${wd3LoadDate}"
	AND units > 0
	AND c.technology IN ('INK','LASER','PWA')
GROUP BY 
	  a.record
	, region_5
	, date
	, geo
	, a.base_prod_number
	, c.category_feature
	, a.version
""")

wd3AllocatedLtfWd3Units.createOrReplaceTempView("wd3_allocated_ltf_wd3_units")

// COMMAND ----------

//--populate wd3 pct
val wd3AllocatedLtfWd3Pct = spark.sql("""
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

wd3AllocatedLtfWd3Pct.createOrReplaceTempView("wd3_allocated_ltf_wd3_pct")

// COMMAND ----------

//--populate allocated ltf units
val wd3AllocatedLtfAllocatedLtfUnits = spark.sql("""
SELECT 
	a.region_5
	,a.cal_date
	,a.country_alpha2
	,a.base_product_number
	,a.category_feature
	,(a.pct * b.units) AS allocated_units
FROM wd3_allocated_ltf_wd3_pct a
INNER JOIN wd3_allocated_ltf_ltf_units b 
	ON a.cal_date=b.cal_date 
	AND a.region_5=b.region_5
	AND a.category_feature = b.category_feature
WHERE 1=1;
""")

wd3AllocatedLtfAllocatedLtfUnits.createOrReplaceTempView("wd3_allocated_ltf_allocated_ltf_units")

// COMMAND ----------

// --populate allocated flash units
val wd3AllocatedLtfAllocatedFlashUnits = spark.sql("""
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
	AND a.category_feature = b.category_feature
WHERE 1=1;
""")

wd3AllocatedLtfAllocatedFlashUnits.createOrReplaceTempView("wd3_allocated_ltf_allocated_flash_units")

// COMMAND ----------

//--unallocated ltf units
val wd3AllocatedLtfUnallocatedLtfUnits = spark.sql(s"""
SELECT
	  d.region_5
	, a.cal_date
	, a.country_alpha2
	, a.base_product_number
	, c.category_feature
	, SUM(a.units) as units
FROM hardware_ltf a
LEFT JOIN rdma b ON a.base_product_number=b.Base_Prod_Number
LEFT JOIN hardware_xref c ON b.Platform_Subset=c.platform_subset
LEFT JOIN iso_country_code_xref d on a.country_alpha2=d.country_alpha2
INNER JOIN wd3_allocated_ltf_missing_ltf_combos f
	ON c.category_feature=f.category_feature
	AND a.cal_date = f.cal_date
	AND d.region_5=f.region_5
WHERE 1=1
	AND a.record = "${ltfRecord}"
	AND a.version = "${ltfVersion}"
	AND a.cal_date <= "${ltfMaxCalDate}"
GROUP BY
	  d.region_5
	, a.cal_date
	, a.country_alpha2
	, a.base_product_number
	, c.category_feature
""")

wd3AllocatedLtfUnallocatedLtfUnits.createOrReplaceTempView("wd3_allocated_ltf_unallocated_ltf_units")

// COMMAND ----------

// --unallocated flash units
val wd3AllocatedLtfUnallocatedFlashUnits = spark.sql(s"""
SELECT
	  d.region_5
	, a.cal_date
	, a.country_alpha2
	, a.base_product_number
	, c.category_feature
	, SUM(a.units) as units
FROM flash a
LEFT JOIN rdma b ON a.base_product_number=b.Base_Prod_Number
LEFT JOIN hardware_xref c ON b.Platform_Subset=c.platform_subset
LEFT JOIN iso_country_code_xref d on a.country_alpha2=d.country_alpha2
INNER JOIN wd3_allocated_ltf_missing_flash_combos f
	ON c.category_feature=f.category_feature
	AND a.cal_date = f.cal_date
	AND d.region_5=f.region_5
WHERE 1=1
	AND a.version = "${flashVersion}"
GROUP BY
	  d.region_5
	, a.cal_date
	, a.country_alpha2
	, a.base_product_number
	, c.category_feature;
""")

wd3AllocatedLtfUnallocatedFlashUnits.createOrReplaceTempView("wd3_allocated_ltf_unallocated_flash_units")

// COMMAND ----------

//--bring them all together
val wd3AllocatedLtfFinal = spark.sql(s"""
SELECT
	"${flashForecastName}" as source
	,*
FROM wd3_allocated_ltf_allocated_flash_units
UNION ALL
SELECT
	"${flashForecastName}" as source
	,*
FROM wd3_allocated_ltf_unallocated_flash_units
UNION ALL
SELECT
	"${wd3RecordName}" as source
	,*
FROM wd3_allocated_ltf_allocated_ltf_units
UNION ALL
SELECT
	"${wd3RecordName}" as source
	,*
FROM wd3_allocated_ltf_unallocated_ltf_units
ORDER BY source, region_5, category_feature, cal_date
""")

wd3AllocatedLtfFinal.createOrReplaceTempView("wd3_allocated_ltf_final")

// COMMAND ----------

// --add record to version table
submitRemoteQuery(configs("redshiftUrl"), configs("redshiftUsername"), configs("redshiftPassword"), """CALL prod.addversion_sproc('allocated_ltf', 'wd3_allocated_flash_plus_ltf');""")

// COMMAND ----------

val versionQuery = s"""
SELECT
      record
	, MAX(version) AS version
	, MAX(load_date) as load_date
FROM prod.version
WHERE record = 'allocated_ltf'
GROUP BY record
"""

val version = readRedshiftToDF(configs)
  .option("query", versionQuery)
  .load()

val maxAllocatedVersion = version.select("version").distinct().collect().map(_.getString(0)).mkString("")
val maxAllocatedLoadDate = version.select("load_date").distinct().collect().map(_.getTimestamp(0)).array(0)

// COMMAND ----------

// --load data to allocated_ltf_landing table
// --update load_date
val allocatedLtfLanding = spark.sql(s"""
SELECT
	  'stf' AS record
	, cal_date
	, country_alpha2
	, base_product_number
	, SUM(units) AS units
    , CAST("${maxAllocatedLoadDate}" as date) AS load_date
	, source
	,"${maxAllocatedVersion}" AS version
FROM wd3_allocated_ltf_final 
WHERE 1=1
GROUP BY
	  cal_date
	, country_alpha2
	, base_product_number
	, source
""")

allocatedLtfLanding.createOrReplaceTempView("allocated_ltf_landing")

// COMMAND ----------

// --load latest stitched dataset to hardware_stf landing table
val hardwareStfLanding = spark.sql("""
SELECT 
	 'allocated flash plus ltf' AS record
    , country_alpha2 AS geo
	, 'CtryCd' as geo_type
    , base_product_number as base_prod_number
    , cal_date as date
	, units
    , load_date
    , version
FROM allocated_ltf_landing
--WHERE version = "${maxAllocatedVersion}";
""")

hardwareStfLanding.createOrReplaceTempView("hardware_stf_landing")

// COMMAND ----------

// --Add version to version table
submitRemoteQuery(configs("redshiftUrl"), configs("redshiftUsername"), configs("redshiftPassword"), """CALL prod.addversion_sproc('hw_stf_fcst', 'Archer');""")

// COMMAND ----------

val versionQuery = s"""
SELECT
      record
	, MAX(version) AS version
	, MAX(load_date) as load_date
FROM prod.version
WHERE record = 'hw_stf_fcst'
GROUP BY record
"""

val version = readRedshiftToDF(configs)
  .option("query", versionQuery)
  .load()

val maxFcstVersion = version.select("version").distinct().collect().map(_.getString(0)).mkString("")
val maxFcstLoadDate = version.select("load_date").distinct().collect().map(_.getTimestamp(0)).array(0)

// COMMAND ----------

// --load data to staging table
// --UPDATE staging load_date and version
val hardwareStfStaging = spark.sql(s"""
SELECT DISTINCT
     'hw_stf_fcst' AS record
	, s.record AS forecast_name
	, s.date AS cal_date
	, i.region_5
	, s.geo AS country_alpha2
	, rdma.platform_subset
	, s.base_prod_number AS base_product_number
	, s.units
	, CAST("${maxFcstLoadDate}" AS date) AS load_date
	, CAST('true' AS BOOLEAN) AS official
	, "${maxFcstVersion}" AS version
FROM hardware_stf_landing s
	LEFT JOIN rdma ON rdma.base_prod_number=s.base_prod_number
	LEFT JOIN iso_country_code_xref i ON s.geo=i.country_alpha2
--WHERE s.version = "${maxAllocatedVersion}";
""")

hardwareStfStaging.createOrReplaceTempView("hardware_stf_staging")

// COMMAND ----------

submitRemoteQuery(configs("redshiftUrl"), configs("redshiftUsername"), configs("redshiftPassword"), "UPDATE prod.hardware_ltf SET official = 0 WHERE record = 'hw_stf_fcst' AND official=1;")

// COMMAND ----------

// --move to prod
val hardwareLtf = spark.sql("""
SELECT DISTINCT
	    a.record
      , a.forecast_name
      , a.cal_date
      , CAST(NULL AS int) AS geo_id
      , a.country_alpha2
      , CAST(NULL AS int) AS platform_subset_id
      , b.platform_subset
      , a.base_product_number
      , a.units
      , a.load_date
      , a.official
      , a.version
FROM hardware_stf_staging a 
      LEFT JOIN rdma b ON a.platform_subset=b.Platform_Subset
	  LEFT JOIN product_line_xref c ON b.pl = c.PL
WHERE c.Technology IN ('INK','LASER','PWA') AND c.PL_category = 'HW'
	AND a.platform_subset NOT LIKE ('ACCESSORY %')
	AND a.platform_subset <> 'MOBILE DONGLE'
	AND a.platform_subset <> 'PAGEWIDE ACCESSORIES'
	AND units <> 0
""")

// COMMAND ----------

// DBTITLE 0,Untitled
wd3AllocatedLtfLtfUnits.cache()
writeDFToRedshift(configs, wd3AllocatedLtfLtfUnits, "stage.wd3_allocated_ltf_ltf_units", "overwrite")

wd3AllocatedLtfFlashUnits.cache()
writeDFToRedshift(configs, wd3AllocatedLtfFlashUnits, "stage.wd3_allocated_ltf_flash_units", "overwrite")

wd3AllocatedLtfWd3Units.cache()
writeDFToRedshift(configs, wd3AllocatedLtfWd3Units, "stage.wd3_allocated_ltf_wd3_units", "overwrite")

wd3AllocatedLtfWd3Pct.cache()
writeDFToRedshift(configs, wd3AllocatedLtfWd3Pct, "stage.wd3_allocated_ltf_wd3_pct", "overwrite")

writeDFToRedshift(configs, hardwareLtf, "prod.hardware_ltf", "append")
