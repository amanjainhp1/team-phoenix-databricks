// Databricks notebook source
// MAGIC %md
// MAGIC #### Step 1: Setup

// COMMAND ----------

dbutils.widgets.text("REDSHIFT_SECRET_NAME", "")
dbutils.widgets.dropdown("REDSHIFT_REGION_NAME", "us-west-2", Seq("us-west-2", "us-east-2"))

// COMMAND ----------

// import libraries
import org.apache.spark.sql.DataFrameReader

// COMMAND ----------

// MAGIC %run ../python/common/secrets_manager_utils

// COMMAND ----------

// MAGIC %run ./common/S3Utils

// COMMAND ----------

// set common parameters
val sfaiHostname = "sfai.corp.hpicloud.net"
val sfaiPort = 1433
val sfaiUrl = s"""jdbc:sqlserver://${sfaiHostname}:${sfaiPort};database="""
val sfaiUsername = "databricks_user" // placeholder
val sfaiPassword = "databricksdemo123" // placeholder

// COMMAND ----------

// MAGIC %python
// MAGIC redshift_secret_name = dbutils.widgets.get("REDSHIFT_SECRET_NAME")
// MAGIC redshift_region_name = dbutils.widgets.get("REDSHIFT_REGION_NAME")
// MAGIC 
// MAGIC redshift_username = secrets_get(redshift_secret_name, redshift_region_name)["username"]
// MAGIC redshift_password = secrets_get(redshift_secret_name, redshift_region_name)["password"]
// MAGIC 
// MAGIC dbutils.widgets.text("REDSHIFT_USERNAME", redshift_username)
// MAGIC dbutils.widgets.text("REDSHIFT_PASSWORD", redshift_password)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2: Extract

// COMMAND ----------

// set table specific parameters
val database = "IE2_Prod"
val schema = "dbo"
val table = "ib"

//  extract data from SFAI
val idDF = spark.read
  .format("jdbc")
  .option("url", sfaiUrl + database)
  .option("dbTable", s"""${schema}.${table}""")
  .option("user", sfaiUsername)
  .option("password", sfaiPassword)
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .load()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 3: Transform

// COMMAND ----------

idDF.createOrReplaceTempView("ib_table")
val filteredIbDF0 = spark.sql("SELECT * FROM ib_table WHERE version = '2021.09.28.1'")

// COMMAND ----------

filteredIbDF0.createOrReplaceTempView("filtered_ib_table0")
val filteredIbDF1 = spark.sql("SELECT * FROM filtered_ib_table0 WHERE platform_subset='THINKJET'")

// COMMAND ----------

display(filteredIbDF1)

// COMMAND ----------

val filteredIbDF2 = filteredIbDF1.filter("country = 'US'")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 4: Load

// COMMAND ----------

// mount s3 bucket to cluster
s3Mount("dataos-core-dev-team-phoenix/proto/example-bucket/", "example-bucket")

// COMMAND ----------

filteredIbDF2.write
  .format("csv")
  .mode("overwrite")
  .save("dbfs:/mnt/example-bucket/test/")

// COMMAND ----------

// MAGIC %sh
// MAGIC ls -lh ../../dbfs/mnt/example-bucket/test/

// COMMAND ----------

filteredIbDF2.write
  .format("com.databricks.spark.redshift")
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None")
  .option("dbtable", "stage.example_notebook_table")
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/")
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role")
  .option("user", dbutils.widgets.get("REDSHIFT_USERNAME"))
  .option("password", dbutils.widgets.get("REDSHIFT_PASSWORD"))
  .mode("overwrite")
  .save()

// COMMAND ----------


