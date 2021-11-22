// Databricks notebook source
// Retrieve username and password from AWS secrets manager
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.services.secretsmanager._
import com.amazonaws.services.secretsmanager.model._

def secretsGet (secretName: String, regionName: String): String = {
  val endpointUrl: String = "https://secretsmanager.us-west-2.amazonaws.com"
  
  val config: com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration = new AwsClientBuilder.EndpointConfiguration(endpointUrl, regionName)
  val clientBuilder: com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder = AWSSecretsManagerClientBuilder.standard()
  clientBuilder.setEndpointConfiguration(config)

  val client: com.amazonaws.services.secretsmanager.AWSSecretsManager = clientBuilder.build()
  val getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName).withVersionStage("AWSCURRENT")
  val getSecretValueResult = client.getSecretValue(getSecretValueRequest)
  
  return getSecretValueResult.getSecretString
}

// COMMAND ----------

secretsGet("arn:aws:secretsmanager:us-west-2:740156627385:secret:dev/redshift/dataos-core-dev-01/auto_glue-dj6tOj", "us-west-2")
