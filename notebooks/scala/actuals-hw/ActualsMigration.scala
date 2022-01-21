// Databricks notebook source
dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.dropdown("stack", "dev", Seq("dev", "itg", "prd"))
dbutils.widgets.text("aws_iam_role", "")

// COMMAND ----------

// MAGIC %run ../../scala/common/Constants.scala

// COMMAND ----------

// MAGIC %run ../../scala/common/DatabaseUtils.scala

// COMMAND ----------

// MAGIC %run ../../scala/common/DatetimeUtils.scala

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

// MAGIC %scala
// MAGIC var configs: Map[String, String] = Map()
// MAGIC configs += ("stack" -> dbutils.widgets.get("stack"),
// MAGIC             "sfaiUsername" -> spark.conf.get("sfai_username"),
// MAGIC             "sfaiPassword" -> spark.conf.get("sfai_password"),
// MAGIC             "sfaiUrl" -> SFAI_URL,
// MAGIC             "sfaiDriver" -> SFAI_DRIVER,
// MAGIC             "redshiftUsername" -> spark.conf.get("redshift_username"),
// MAGIC             "redshiftPassword" -> spark.conf.get("redshift_password"),
// MAGIC             "redshiftAwsRole" -> dbutils.widgets.get("aws_iam_role"),
// MAGIC             "redshiftUrl" -> s"""jdbc:redshift://${REDSHIFT_URLS(dbutils.widgets.get("stack"))}:${REDSHIFT_PORTS(dbutils.widgets.get("stack"))}/${dbutils.widgets.get("stack")}?ssl_verify=None""",
// MAGIC             "redshiftTempBucket" -> s"""${S3_BASE_BUCKETS(dbutils.widgets.get("stack"))}redshift_temp/""")

// COMMAND ----------

import java.sql.SQLException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md
// MAGIC ####Populate Initial Redshift Actuals Hardware Table

// COMMAND ----------

// Check if destination table has row count > 0, if not, then copy full source table to destination
var sourceDatabase: String = ""
var sourceSchema: String = ""
var sourceTable: String = ""

var destinationSchema: String = "prod"
var destinationTable: String = "actuals_hw"

var query: String = ""

var initialDataLoad: Boolean = false
var destinationTableExists: Boolean = false

val currentTime = new CurrentTime
val datestamp = currentTime.getDatestamp()
val timestamp = currentTime.getTimestamp().toString

try {
  query = s"""SELECT COUNT(*) FROM ${destinationSchema}.${destinationTable}"""
  
  val destinationTableRowCount: Long = readRedshiftToDF(configs)
    .option("query", query)
    .load()
    .collect()
    .map(_.getLong(0)).array(0)

  // if destination table exists, continue else will hit catch block
  destinationTableExists = true
  
  if (destinationTableRowCount == 0) initialDataLoad = true
  
  if (initialDataLoad) {
    sourceDatabase = "IE2_Prod"
    sourceSchema = "dbo"
    sourceTable = "actuals_hw"

    val sourceTableDF = readSqlServerToDF(configs)
      .option("dbTable", s"""${sourceSchema}.${sourceTable}""")
      .load()

  //   save "landing" data to S3
    writeDFToS3(sourceTableDF, s"s3a://dataos-core-${dbutils.widgets.get("stack")}-team-phoenix/proto/${destinationTable}/${datestamp}/${timestamp}/", "csv")

  //   save data to "stage" and final/"prod" schema
    writeDFToRedshift(configs, sourceTableDF, s"stage.${destinationTable}", "append")
    writeDFToRedshift(configs, sourceTableDF, s"${destinationSchema}.${destinationTable}", "append")
  }
} catch {
  case e: SQLException => println(e)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ####Update Redshift Actuals Hardware Table - Staging

// COMMAND ----------

var appendToProdTable:Boolean = false

if (!initialDataLoad && destinationTableExists) {
// check the max date from archer and compare to max cal_date in prod table

  query = s"""
    SELECT MAX(date) AS archer_date
    FROM [Archer_Prod].dbo.stf_flash_country_speedlic_vw
    WHERE record = 'Planet-Actuals'
  """

  val sourceDatabase = "Archer_Prod"
  val sourceTable = "actuals_units"

  val archerActualsUnitsMaxDate = readSqlServerToDF(configs)
      .option("query", query)
      .load()
      .select("archer_date").collect().map(_.getDate(0)).mkString("")

  query = s"""
    SELECT MAX(cal_date) AS ie2_date
    FROM prod.actuals_hw
    WHERE 1=1 AND source = 'Archer'
  """

  val redshiftActualsUnitsMaxDate = readRedshiftToDF(configs)
    .option("query", query)
    .load()
    .select("ie2_date").collect().map(_.getDate(0)).mkString("")
  
  /*
  if archer has newer data
     * build stage dataframe
     * run stored proc to create new version
     * retrieve new version and load_date
     * modify version and load_date values in stage dataframe
     * write stage table
  */
  
  if(archerActualsUnitsMaxDate > redshiftActualsUnitsMaxDate) appendToProdTable = true
  
  if(appendToProdTable) {
    // create empty data frame 
    val actualsSchema = StructType(List(
      StructField("record", StringType, true),
      StructField("cal_date", DateType, true),
      StructField("country_alpha2", StringType, true),
      StructField("base_product_number", StringType, true),
      StructField("Platform_Subset", StringType, true),
      StructField("base_quantity", DataTypes.createDecimalType(38,2), true),
      StructField("load_date", TimestampType, true),
      StructField("official", IntegerType, true),
      StructField("version", IntegerType, true),
      StructField("source", StringType, true)
     ))
    var stageActualsDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], actualsSchema)

    query = s"""
      SELECT
      'actuals - hw' AS record,
      a.date AS cal_date,
      a.geo AS country_alpha2,
      a.base_prod_number AS base_product_number,
      b.platform_subset,
      a.units AS base_quantity,
      a.load_date,
      1 AS official,
      a.version,
      'Archer' AS source
      FROM [Archer_Prod].dbo.stf_flash_country_speedlic_vw AS a
      LEFT JOIN [ie2_prod].dbo.rdma AS b
        ON a.base_prod_number = b.Base_Prod_Number
      WHERE a.record = 'Planet-Actuals'
        AND a.date = '${archerActualsUnitsMaxDate}'
    """ 

    //retrieve relevant data from source db
    val database = "Archer_Prod"
    var stagingActualsUnitsHw = readSqlServerToDF(configs)
      .option("query", query)
      .load()
    display(stagingActualsUnitsHw)
    
    //   save "landing" data to S3
    writeDFToS3(stagingActualsUnitsHw, s"s3a://dataos-core-${dbutils.widgets.get("stack")}-team-phoenix/proto/${destinationTable}/${datestamp}/${timestamp}/", "csv", "overwrite")
    
    //execute stored procedure to create new version and load date
    submitRemoteQuery(configs("redshiftUrl"), configs("redshiftUsername"), configs("redshiftPassword"), "CALL prod.addversion_sproc('actuals - hw','Archer');")
    
    //retrieve new version info
    val newVersionInfo = readRedshiftToDF(configs)
      .option("query", s"""SELECT MAX(load_date) as max_load_date, MAX(version) as max_version FROM prod.version WHERE record = 'actuals - hw'""")
      .load()
    
    var maxLoadDate = newVersionInfo.select("max_load_date").collect().map(_.getTimestamp(0))
    var maxVersion: String = newVersionInfo.select("max_version").collect().map(_.getString(0)).mkString("")
    
    stagingActualsUnitsHw = stagingActualsUnitsHw
      .withColumn("version", when(col("version") =!= (maxVersion),maxVersion))
      .withColumn("load_date", when(col("load_date") =!= (maxLoadDate(0)),maxLoadDate(0)))
    
    stageActualsDF = stageActualsDF.union(stagingActualsUnitsHw)
    
    //retrieve large format
    query = s"""
      SELECT
      'actuals_lf' AS record,
      c.Date AS cal_date,
      p.country_alpha2,
      e.ipg_product_base_product_number AS base_product_number,
      r.Platform_Subset,
      SUM(working_PandL_summary_base_quantity) AS base_quantity,
      NULL AS load_date,
      1 AS official,
      NULL AS version,
      'EDW-LF' AS source
      FROM [ie2_landing].dbo.edw_revenue_units_base_landing e
      LEFT JOIN [ie2_prod].dbo.calendar c on c.Edw_fiscal_yr_mo = e.revenue_recognition_fiscal_year_month_code AND c.Day_of_Month = 1
      LEFT JOIN [ie2_prod].dbo.rdma r on r.Base_Prod_Number = e.ipg_product_base_product_number
      LEFT JOIN [ie2_prod].dbo.hardware_xref hw on hw.platform_subset = r.Platform_Subset
      LEFT JOIN [ie2_prod].dbo.profit_center_code_xref p on p.profit_center_code = e.profit_center_code
      WHERE 1=1
        AND hw.technology = 'LF'
        AND e.revenue_recognition_fiscal_year_month_code = (SELECT MAX(revenue_recognition_fiscal_year_month_code) FROM "IE2_Landing"."dbo"."edw_revenue_units_base_landing")
        AND e.working_PandL_summary_base_quantity <> 0
      GROUP BY
      c.Date,
      p.country_alpha2,
      e.ipg_product_base_product_number,
      r.Platform_Subset,
      e.load_date
    """
    
    var stagingActualsLF = readSqlServerToDF(configs)
      .option("query", query)
      .load()
      .withColumn("load_date", col("load_date").cast(TimestampType))
    
    if (stagingActualsLF.count() > 0 ) {
      //   save "landing" data to S3
      writeDFToS3(stagingActualsLF, s"s3a://dataos-core-${dbutils.widgets.get("stack")}-team-phoenix/proto/actuals_lf/{datestamp}/${timestamp}/", "csv", "overwrite")
      
      submitRemoteQuery(configs("redshiftUrl"), configs("redshiftUsername"), configs("redshiftPassword"), "CALL prod.addversion_sproc('actuals_lf','EDW-LF');")
      
      //retrieve new version info
      val newVersionInfoActualsLF = readRedshiftToDF(configs)
        .option("query", s"""SELECT MAX(load_date) as max_load_date, MAX(version) as max_version FROM prod.version WHERE record = 'actuals_lf'""")
        .load()
      
      maxLoadDate = newVersionInfoActualsLF.select("max_load_date").collect().map(_.getTimestamp(0))
      maxVersion = newVersionInfoActualsLF.select("max_version").collect().map(_.getString(0)).mkString("")
    
      stagingActualsLF = stagingActualsLF
        .withColumn("version", lit(maxVersion))
        .withColumn("load_date", lit(maxLoadDate(0)))
      
      stageActualsDF = stageActualsDF.union(stagingActualsLF)
    }
    
    if (stageActualsDF.count() > 0) {

      //write to redshift
      writeDFToRedshift(configs, stageActualsDF, "stage.actuals_hw", "overwrite")
      
      submitRemoteQuery(configs("redshiftUrl"), configs("redshiftUsername"), configs("redshiftPassword"), "GRANT ALL ON ALL TABLES IN schema stage TO group dev_arch_eng")
    }
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ####Update Redshift Actuals Hardware Table - Prod

// COMMAND ----------

// create final dataframe and write out to table

if (!initialDataLoad && appendToProdTable && destinationTableExists) {
  
  val query = s"""
  SELECT
    record,
    cal_date,
    country_alpha2,
    base_product_number,
    a.platform_subset,
    base_quantity,
    a.load_date,
    official,
    a.version,
    source
  FROM stage.actuals_hw a
  LEFT JOIN prod.hardware_xref b ON a.platform_subset = b.platform_subset
  LEFT JOIN prod.product_line_xref c ON b.pl = c.pl
  WHERE c.PL_category = 'HW'
    AND c.Technology IN ('INK','LASER','PWA','LF')
  """

  val redshiftStageActualsHw = readRedshiftToDF(configs)
    .option("query", query)
    .load()
  
  if (appendToProdTable) {
    writeDFToRedshift(configs, redshiftStageActualsHw, s"""${destinationSchema}.${destinationTable}""", "append")
  }
}