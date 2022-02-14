// Databricks notebook source
// SFAI SQL Server
val SFAI_URL: String = "jdbc:sqlserver://sfai.corp.hpicloud.net:1433;"
val SFAI_DRIVER: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

// Redshift
val REDSHIFT_URLS: Map[String, String] = Map("dev" -> "dataos-core-dev-team-phoenix.dev.hpdataos.com",
                                            "itg" -> "dataos-core-team-phoenix.stg.hpdataos.com",
                                            "prod" -> "dataos-core-team-phoenix.hpdataos.com",
                                            "reporting" -> "dataos-core-team-phoenix-reporting.hpdataos.com")

val REDSHIFT_PORTS: Map[String, String] = Map("dev" -> "5439",
                                              "itg" -> "5439",
                                              "prod" -> "5439",
                                              "reporting" -> "5439")

val REDSHIFT_DATABASE: Map[String, String] = Map("dev" -> "dev",
                                                "itg" -> "stg",
                                                "prod" -> "prod",
                                                "reporting" -> "prod")
