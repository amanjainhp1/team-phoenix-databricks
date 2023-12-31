# Databricks notebook source
constants = {
    "SFAI_URL": "jdbc:sqlserver://sfai.corp.hpicloud.net:1433;",
    "SFAI_DRIVER": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "S3_BASE_BUCKET" : {
        "dev" : "s3a://dataos-core-dev-team-phoenix/",
        "itg" : "s3a://dataos-core-itg-team-phoenix/",
        "prod" : "s3a://dataos-core-prod-team-phoenix/"
    },
    "REDSHIFT_URLS" : {
        "dev" : "dataos-core-dev-team-phoenix.dev.hpdataos.com",
        "itg" : "dataos-core-team-phoenix-itg.hpdataos.com",
        "prod" : "dataos-core-team-phoenix.hpdataos.com",
        "reporting" : "dataos-core-team-phoenix-reporting.hpdataos.com"
    },
    "REDSHIFT_PORTS" : {
        "dev" : "5439",
        "itg" : "5439",
        "prod" : "5439",
        "reporting" : "5439"
    },
    "REDSHIFT_DATABASE" : {
        "dev" : "dev",
        "itg" : "itg",
        "prod" : "prod",
        "reporting" : "prod"
    },
    "REDSHIFT_DEV_GROUP" : {
        "dev" : "dev_arch_eng",
        "itg" : "dev_arch_eng",
        "prod" : "phoenix_dev"
    }
}
