# Databricks notebook source
import boto3

# COMMAND ----------

# mount S3 bucket to cluster
def s3_mount (bucket_name, mount_name):
    try:
        dbutils.fs.mount(f"s3a://{bucket_name}", f"{mount_name}")
    except:
        print("WARNING: mount does not exist or is already mounted to cluster")

# COMMAND ----------

def retrieve_latest_s3_object_by_prefix(bucket, prefix):
    s3 = boto3.resource('s3')
    objects = list(s3.Bucket(bucket).objects.filter(Prefix=prefix))
    objects.sort(key=lambda o: o.last_modified)
    return objects[-1].key

# COMMAND ----------

def get_file_content_from_s3(bucket, path):
  
  s3 = boto3.resource('s3')
  obj = s3.Object(bucket, path)
  content = obj.get()['Body'].read().decode('utf-8')
  
  return content
