// Databricks notebook source
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

// COMMAND ----------

dbutils.widgets.text("REDSHIFT_USERNAME", "")
dbutils.widgets.text("REDSHIFT_PASSWORD", "")
dbutils.widgets.text("SFAI_USERNAME", "")
dbutils.widgets.text("SFAI_PASSWORD", "")
dbutils.widgets.dropdown("ENVIRONMENT", "dev", Seq("dev", "itg", "prd"))
dbutils.widgets.text("AWS_IAM_ROLE", "")

// COMMAND ----------

// MAGIC %run ../common/Constants

// COMMAND ----------

// MAGIC %run ../common/DatetimeUtils

// COMMAND ----------

// MAGIC %run ../common/ParallelNotebooks

// COMMAND ----------

val currentTime = new CurrentTime

var notebooks: Seq[NotebookData] = Seq()

val tables: Seq[String] = Seq("calendar",
                              "hardware_xref",
                              "iso_cc_rollup_xref",
                              "iso_country_code_xref",
                              "product_line_xref",
                              "profit_center_code_xref",
                              "rdma",
                              "supplies_hw_mapping",
                              "supplies_xref",
                              "version",
                              "yield")

var configs: Map[String, String] = Map()
configs += ("env" -> dbutils.widgets.get("ENVIRONMENT"),
            "sfaiUsername" -> dbutils.widgets.get("SFAI_USERNAME"),
            "sfaiPassword" -> dbutils.widgets.get("SFAI_PASSWORD"),
            "sfaiUrl" -> SFAI_URL,
            "redshiftUsername" -> dbutils.widgets.get("REDSHIFT_USERNAME"),
            "redshiftPassword" -> dbutils.widgets.get("REDSHIFT_PASSWORD"),
            "redshiftAwsRole" -> dbutils.widgets.get("AWS_IAM_ROLE"),
            "redshiftUrl" -> s"""jdbc:redshift://${REDSHIFT_URLS(dbutils.widgets.get("ENVIRONMENT"))}:${REDSHIFT_PORTS(dbutils.widgets.get("ENVIRONMENT"))}/${dbutils.widgets.get("ENVIRONMENT")}?ssl_verify=None""",
            "redshiftTempBucket" -> s"""${S3_BASE_BUCKETS(dbutils.widgets.get("ENVIRONMENT"))}redshift_temp/""",
            "sfaiDatabase" -> "IE2_Prod",
            "datestamp" -> currentTime.getDatestamp(),
            "timestamp" -> currentTime.getTimestamp().toString)

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


