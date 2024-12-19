# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from mlflow.deployments import get_deploy_client
from langchain.vectorstores import DatabricksVectorSearch
from langchain.embeddings import DatabricksEmbeddings
from databricks.vector_search.client import VectorSearchClient

# COMMAND ----------

# MAGIC %md
# MAGIC # **Exploring code LLM call functionality**

# COMMAND ----------

test_question = "What is Apache Spark?"

# COMMAND ----------

# MAGIC %md
# MAGIC **Using Foundation Model APIs**

# COMMAND ----------

dep_client=get_deploy_client("databricks")

# COMMAND ----------

# DBTITLE 1,Chat model
resp=dep_client.predict(endpoint="databricks-dbrx-instruct", inputs={"messages":[{"role":"user","content":test_question}]})
print(resp)

# COMMAND ----------

# DBTITLE 1,Embedding models
embeddings = dep_client.predict(endpoint="databricks-bge-large-en", inputs={"input": ["What is Apache Spark?"]})
print(embeddings)

# COMMAND ----------

# MAGIC %md
# MAGIC **Using direct LLM APIs**

# COMMAND ----------

# DBTITLE 1,Chat models
from langchain.chat_models import ChatDatabricks
chat_model=ChatDatabricks(endpoint="databricks-dbrx-instruct", temperature=0.1)
chat_model.invoke(test_question)

# COMMAND ----------

# MAGIC %md
# MAGIC Using streaming method

# COMMAND ----------

for token in chat_model.stream(test_question):
    print (token.content,'\n')

# COMMAND ----------

# DBTITLE 1,Embedding models
emb_model=DatabricksEmbeddings(endpoint="databricks-bge-large-en")
emb_model.embed_query("What is Apache Spark?")

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare UC data

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG training;
# MAGIC USE fikrat

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pdf_text_embeddings where id=7
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE pdf_text_embeddings_abridged2 as 
# MAGIC select id,pdf_name,content from training.fikrat.pdf_text_embeddings where id <50;
# MAGIC
# MAGIC ALTER TABLE `training`.`fikrat`.`pdf_text_embeddings_abridged2`
# MAGIC  SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploring Vector Search

# COMMAND ----------

# MAGIC %md
# MAGIC Creating index

# COMMAND ----------

vsc=VectorSearchClient()
index=vsc.create_delta_sync_index(endpoint_name='vs_endpoint_8', 
                            index_name='training.fikrat.vs_pdf_index', 
                            source_table_name='training.fikrat.pdf_text_embeddings_abridged2', embedding_source_column='content', 
                            pipeline_type="TRIGGERED",
                            columns_to_sync=['id', 'content', 'pdf_name'],
                            primary_key='id',
                            embedding_model_endpoint_name='databricks-gte-large-en',
                            # embedding_vector_column='embedding_col',
                            embedding_dimension=768)

# COMMAND ----------

# MAGIC %md
# MAGIC Querying index

# COMMAND ----------

# DBTITLE 1,Using SQL to query vector search
# MAGIC %sql
# MAGIC SELECT content FROM VECTOR_SEARCH(index => "training.fikrat.vs_pdf_index", query => "What is InstructGPT", num_results => 2)

# COMMAND ----------

my_question="What is InstructGPT"

# COMMAND ----------

# MAGIC %md
# MAGIC Using python to search

# COMMAND ----------

# DBTITLE 1,Using similarity search
quest_embeddings = dep_client.predict(endpoint="databricks-bge-large-en", 
                                      inputs={"input": [my_question]}).data[0]['embedding'] 
ind=vsc.get_index(index_name='training.fikrat.vs_pdf_index')
resp=ind.similarity_search(query_vector=quest_embeddings,columns=["pdf_name", "content"])
print (resp['result']['data_array'])

# COMMAND ----------

# DBTITLE 1,Using Vector store as retriever
    ind=vsc.get_index(index_name='training.fikrat.vs_pdf_index')
    emb_model=DatabricksEmbeddings(endpoint="databricks-bge-large-en")
    vss=DatabricksVectorSearch(index=ind, embedding=emb_model)
    vss.as_retriever().invoke(my_question)


# COMMAND ----------

def get_retriever():
    ind=vsc.get_index(index_name='training.fikrat.vs_pdf_index')
    emb_model=DatabricksEmbeddings(endpoint="databricks-bge-large-en")
    vss=DatabricksVectorSearch(index=ind, embedding=emb_model)
    return vss.as_retriever()

# COMMAND ----------

# MAGIC %md
# MAGIC **Building RAG chain **

# COMMAND ----------

# DBTITLE 1,Using legacy Langchain API
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain.chat_models import ChatDatabricks


TEMPLATE = """You are an assistant for GENAI teaching class. You are answering questions related to Generative AI and how it impacts humans life. If the question is not related to one of these topics, kindly decline to answer. If you don't know the answer, just say that you don't know, don't try to make up an answer. Keep the answer as concise as possible.
Use the following pieces of context to answer the question at the end:

<context>
{context}
</context>

Question: {question}

Answer:
"""
prompt = PromptTemplate(template=TEMPLATE, input_variables=["context", "question"])

chain = RetrievalQA.from_chain_type(
    llm=chat_model,
    chain_type="stuff",
    retriever=get_retriever(),
    chain_type_kwargs={"prompt": prompt}
)

chain.invoke({"query": "What is generative AI?"})

# COMMAND ----------

# MAGIC %md
# MAGIC Using new chain APIs

# COMMAND ----------

from langchain.chains import create_retrieval_chain
chain=create_retrieval_chain(llm=chat_model,retriever=get_retriever())
chain.invoke({"query": "What is generative AI?"})

# COMMAND ----------

# MAGIC %md
# MAGIC
