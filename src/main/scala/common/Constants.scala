// Databricks notebook source
// SFAI SQL Server
val SFAI_URL: String = "jdbc:sqlserver://sfai.corp.hpicloud.net:1433;"
val SFAI_DRIVER: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

// Redshift
val REDSHIFT_URLS: Map[String, String] = Map("dev" -> "dataos-redshift-core-dev-01.hp8.us",
                                            "itg" -> "dataos-redshift-core-itg-01.hp8.us",
                                            "prd" -> "dataos-redshift-core-prd-01.hp8.us")
val REDSHIFT_PORTS: Map[String, String] = Map("dev" -> "5439",
                                            "itg" -> "5439",
                                            "prd" -> "5439")

//S3
val S3_BASE_BUCKETS: Map[String, String] = Map("dev" -> "s3a://dataos-core-dev-team-phoenix/",
                                            "itg" -> "s3a://dataos-core-itg-team-phoenix/",
                                            "prd" -> "s3a://dataos-core-prd-team-phoenix/")

// COMMAND ----------


