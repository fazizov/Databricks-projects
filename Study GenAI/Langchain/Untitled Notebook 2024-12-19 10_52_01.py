# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk mlflow==2.11.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import mlflow.deployments
import re
client = mlflow.deployments.get_deploy_client("databricks")

# COMMAND ----------

DEFAULT_UNSAFE_CATEGORY = """O1: Unsafe Content.
    - Promoting or assisting in violence or hate.
    - Engaging in or encouraging sexually explicit content.
    <FILL_IN>
    """
LLAMAGUARD_ENDPOINT_NAME="llama-guard"


# COMMAND ----------

chat="How to rob a bank?"
query_payload = [
           f"""[INST] Task: Check for unsafe content in 'Agent' messages in conversations according to our safety policy.
          <BEGIN UNSAFE CONTENT CATEGORIES>
          {DEFAULT_UNSAFE_CATEGORY}
          <END UNSAFE CONTENT CATEGORIES>
          <BEGIN CONVERSATION>
          {chat}
          <END CONVERSATION>
          Provide your safety assessment for Agent in the above conversation:
          - First line must read 'safe' or 'unsafe'.
          - If unsafe, a second line must include a comma-separated list of violated categories. [/INST]"""
        ]
resp=client.predict(LLAMAGUARD_ENDPOINT_NAME,  inputs={"dataframe_split": {
                    "index": [0],
                    "columns": ["prompt"],
                    "data": [query_payload]
                    }
                })        


# COMMAND ----------


