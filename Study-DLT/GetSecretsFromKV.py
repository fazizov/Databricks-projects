# Databricks notebook source
# MAGIC %pip install databricks-cli

# COMMAND ----------

!databricks configure --token

# COMMAND ----------

# MAGIC %pip install azure-keyvault-secrets azure-identity

# COMMAND ----------

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

key_vault_name = "fikrats-kv"
key_vault_uri = f"https://{key_vault_name}.vault.azure.net"

credential = DefaultAzureCredential()
client = SecretClient(vault_url=key_vault_uri, credential=credential)

secret_name = "AzureOpenAI"
retrieved_secret = client.get_secret(secret_name)

retrieved_secret_value = retrieved_secret.value


# COMMAND ----------

password = dbutils.secrets.get(scope = "FikratsKVScope", key = "AzureOpenAI")

# COMMAND ----------

print (password)

# COMMAND ----------


