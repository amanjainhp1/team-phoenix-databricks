// Databricks notebook source
// mount S3 bucket to cluster
def s3Mount (awsBucketName: String, dbfsMountName: String) {
  var mounted = ""
  try {
    dbutils.fs.mount(s"""s3a://${awsBucketName}""", s"""/mnt/${dbfsMountName}""")
    println(s"""/mnt/${dbfsMountName} successfully mounted""")
  } catch {
    case _: Throwable => println("WARNING: mount does not exist or is already mounted to cluster")
  }
}

// COMMAND ----------


