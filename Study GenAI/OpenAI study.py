# Databricks notebook source
pip install openai

# COMMAND ----------

# MAGIC %sh
# MAGIC export AZURE_OPENAI_API_KEY=1bd3d9b50afd4fd896cfa5a3eac8eecb
# MAGIC export AZURE_OPENAI_ENDPOINT=https://generative-ai-bootcamp.openai.azure.com/

# COMMAND ----------

import os
v_api_key=os.getenv("AZURE_OPENAI_API_KEY")
v_azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
print (v_api_key,v_azure_endpoint)


# COMMAND ----------


from openai import AzureOpenAI
client = AzureOpenAI(api_key=v_api_key, azure_endpoint=v_azure_endpoint, api_version="2024-02-01")
deployment_name='gpt-4o'
completion = client.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {
            "role": "user",
            "content": "Write a short love poem."
        }
    ]
)

print(completion.choices[0].message)

# COMMAND ----------


