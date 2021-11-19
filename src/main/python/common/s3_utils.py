# Databricks notebook source
# mount S3 bucket to cluster
def s3_mount (bucket_name, mount_name):
  try:
    dbutils.fs.mount(f"s3a://{bucket_name}", f"/mnt/{mount_name}")
  except:
    print("WARNING: mount does not exist or is already mounted to cluster")
