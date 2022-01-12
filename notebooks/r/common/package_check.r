# Databricks notebook source
package_check <- lapply(
  packages,
  FUN = function(x) {
    if (!require(x, character.only = TRUE)) {
      if (x == "rJava") {
        install.packages("/dbfs/FileStore/r_packages/rJava_1_0_6_tar.gz", repos = NULL)
      } else if (x == "RJDBC") {
        install.packages("/dbfs/FileStore/r_packages/RJDBC_0_2_8_tar.gz", repos = NULL)
      } else if (x == "zoo") {
        install.packages("/dbfs/FileStore/r_packages/zoo_1_8_9_tar.gz", repos = NULL)
      } else {
        install.packages(x, dependencies = TRUE)
      }
      
      
      if (x == "rJava") {
        dyn.load('/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server/libjvm.so')
      }
      
      library(x, character.only = TRUE)
    }
  }
)
