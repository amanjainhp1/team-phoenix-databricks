// Databricks notebook source
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

// COMMAND ----------

dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.dropdown("stack", "dev", Seq("dev", "itg", "prod"))
dbutils.widgets.text("aws_iam_role", "")

// COMMAND ----------

// MAGIC %run ../../scala/common/Constants

// COMMAND ----------

// MAGIC %run ../../python/common/secrets_manager_utils

// COMMAND ----------

// MAGIC %run ../../scala/common/DatetimeUtils

// COMMAND ----------

// MAGIC %run ../../scala/common/ParallelNotebooks

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

val currentTime = new CurrentTime

var notebooks: Seq[NotebookData] = Seq()

val tables: Seq[String] = Seq("calendar",
                              "decay",
                              "hardware_xref",
                              "iso_cc_rollup_xref",
                              "iso_country_code_xref",
                              "product_line_xref",
                              "profit_center_code_xref",
                              "supplies_hw_mapping",
                              "supplies_xref",
                              "version",
                              "yield")

var configs: Map[String, String] = Map()
configs += ("stack" -> dbutils.widgets.get("stack"),
            "sfaiUsername" -> spark.conf.get("sfai_username"),
            "sfaiPassword" -> spark.conf.get("sfai_password"),
            "sfaiUrl" -> SFAI_URL,
            "redshiftUsername" -> spark.conf.get("redshift_username"),
            "redshiftPassword" -> spark.conf.get("redshift_password"),
            "redshiftAwsRole" -> dbutils.widgets.get("aws_iam_role"),
            "redshiftUrl" -> s"""jdbc:redshift://${REDSHIFT_URLS(dbutils.widgets.get("stack"))}:${REDSHIFT_PORTS(dbutils.widgets.get("stack"))}/${REDSHIFT_DATABASE(dbutils.widgets.get("stack"))}?ssl_verify=None""",
            "redshiftTempBucket" -> s"""${S3_BASE_BUCKETS(dbutils.widgets.get("stack"))}redshift_temp/""",
            "sfaiDatabase" -> "IE2_Prod",
            "datestamp" -> currentTime.getDatestamp(),
            "timestamp" -> currentTime.getTimestamp().toString,
            "redshiftTimestamp" -> currentTime.getRedshiftTimestampWithTimezone.toString,
            "redshiftDevGroup" -> s"""${REDSHIFT_DEV_GROUP(dbutils.widgets.get("stack"))}""")

for (table <- tables) {
  configs += ("table" -> table)
  notebooks = NotebookData("MoveSfaiDataToRedshift",
                          0,
                          configs
                         ) +: notebooks
}

val res = parallelNotebooks(notebooks)

Await.result(res, 30 minutes) // this is a blocking call.
res.value

// COMMAND ----------

if (res.toString.contains("FAILED")) {
  throw new Exception("Job failed. At least one notebook job has returned a FAILED status. Check jobs above for more information.")
}
