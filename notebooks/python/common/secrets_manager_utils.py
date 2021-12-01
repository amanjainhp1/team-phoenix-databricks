# Databricks notebook source
# Retrieve username and password from AWS secrets manager
import boto3

def secrets_get(secret_name, region_name):
    endpoint_url = "https://secretsmanager.us-west-2.amazonaws.com"
    client = boto3.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return eval(get_secret_value_response['SecretString'])
