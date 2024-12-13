# Databricks notebook source
# MAGIC %pip install -q  langchain langchain-core langchain-openai youtube-transcript-api langchain-community

# COMMAND ----------

# MAGIC %pip install pytube

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet  youtube-transcript-api

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from langchain_openai import AzureChatOpenAI
from langchain.document_loaders import YoutubeLoader
from langchain.text_splitter import  RecursiveCharacterTextSplitter 
from langchain_openai import AzureOpenAIEmbeddings  
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain.vectorstores import  FAISS
import os


# model = AzureChatOpenAI(azure_deployment=  os.environ["AZURE_OPENAI_DEPLOYMENT_NAME"],temperature=0.5)
# video_id = YoutubeLoader.extract_video_id("https://www.youtube.com/watch?v=QsYGlZkevEg")
url="https://www.youtube.com/watch?v=QsYGlZkevEg"
loader=YoutubeLoader.from_youtube_url(youtube_url=url,add_video_info=False)
trns=loader.load()
print (trns)


# docs=FAISS.from_documents()

# llm_emb=AzureOpenAIEmbeddings()

# COMMAND ----------

len(trns)

# COMMAND ----------

text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
docs = text_splitter.split_documents(trns)
print

# COMMAND ----------

llm_emb=AzureOpenAIEmbeddings( azure_deployment=  "text-embedding-ada-002",temperature=0.5)
