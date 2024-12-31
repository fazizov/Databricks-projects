# Databricks notebook source
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

w.secrets.put_secret("FikratsDbrScope","AzureOpenAIKey",string_value ="")

# COMMAND ----------

password = dbutils.secrets.get(scope = "FikratsDbrScope", key = "AzureOpenAIKey")
print (password)

# COMMAND ----------


