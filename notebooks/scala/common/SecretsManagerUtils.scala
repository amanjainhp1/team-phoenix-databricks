// Databricks notebook source
import com.amazonaws.regions.Regions;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

def getSecretValueCreds(secretName: String, secretRegion: String = "us-west-2"): Map[String, String] = {
  
  val region: Region = Region.of(secretRegion);
  val secretsClient: SecretsManagerClient = SecretsManagerClient.builder()
          .region(region)
          .build();

  val valueRequest: GetSecretValueRequest = GetSecretValueRequest.builder()
                  .secretId(secretName)
                  .build();

  val valueResponse: GetSecretValueResponse = secretsClient.getSecretValue(valueRequest);
  val secret: String = valueResponse.secretString();
  secretsClient.close();
  
  val secretsMap: Map[String, String] = secret.substring(1, secret.length - 1)
        .split(", ")
        .map(_.split(": "))
        .map { case Array(k, v) => (k.substring(1, k.length-1), v.substring(0, v.length))}
        .toMap
  
  return secretsMap
}
