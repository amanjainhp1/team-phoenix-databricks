# Databricks notebook source
package_check <- lapply(
  packages,
  FUN = function(x) {
    if (!require(x, character.only = TRUE)) {
      install.packages(x, dependencies = TRUE)
      
      if (x == "rJava") {
        dyn.load('/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server/libjvm.so')
      }
      
      library(x, character.only = TRUE)
    }
  }
)
