// Databricks notebook source
// SFAI SQL Server
val SFAI_URL: String = "jdbc:sqlserver://sfai.corp.hpicloud.net:1433;"
val SFAI_DRIVER: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

//S3
val S3_BASE_BUCKETS: Map[String, String] = Map("dev" -> "s3a://dataos-core-dev-team-phoenix/",
                                            "itg" -> "s3a://dataos-core-itg-team-phoenix/",
                                            "prod" -> "s3a://dataos-core-prod-team-phoenix/")

// Redshift
val REDSHIFT_URLS: Map[String, String] = Map("dev" -> "dataos-core-dev-team-phoenix.dev.hpdataos.com",
                                            "itg" -> "dataos-core-team-phoenix-itg.hpdataos.com",
                                            "prod" -> "dataos-core-team-phoenix.hpdataos.com",
                                            "reporting" -> "dataos-core-team-phoenix-reporting.hpdataos.com")

val REDSHIFT_PORTS: Map[String, String] = Map("dev" -> "5439",
                                              "itg" -> "5439",
                                              "prod" -> "5439",
                                              "reporting" -> "5439")

val REDSHIFT_DATABASE: Map[String, String] = Map("dev" -> "dev",
                                                "itg" -> "itg",
                                                "prod" -> "prod",
                                                "reporting" -> "prod")

val REDSHIFT_DEV_GROUP: Map[String, String] = Map("dev" -> "dev_arch_eng",
                                                "itg" -> "dev_arch_eng",
                                                "prod" -> "phoenix_dev")
