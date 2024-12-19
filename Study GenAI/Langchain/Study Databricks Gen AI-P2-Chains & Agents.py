# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Chains

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG training;
# MAGIC USE fikrat

# COMMAND ----------

from langchain.vectorstores import DatabricksVectorSearch
from langchain.embeddings import DatabricksEmbeddings
from databricks.vector_search.client import VectorSearchClient
from langchain.prompts import PromptTemplate
from langchain.chat_models import ChatDatabricks
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# COMMAND ----------

vsc=VectorSearchClient()
emb_model=DatabricksEmbeddings(endpoint="databricks-bge-large-en")
ind=vsc.get_index(index_name='training.fikrat.vs_pdf_index')
vss=DatabricksVectorSearch(index=ind, embedding=emb_model)
dbrx=ChatDatabricks(endpoint="databricks-meta-llama-3-70b-instruct")
vss.as_retriever().invoke("What is InstructGPT")


# COMMAND ----------

# MAGIC %md
# MAGIC Chain of vector search, prompt and LLM

# COMMAND ----------

prompt=PromptTemplate(template="""Find the following passage and answer the question.
    {context}
    {question}
    Translate results to Turkish
     Answer:""", input_variables=["context","question"])
my_question="What is InstructGPT"
chain ={"context":vss.as_retriever(),"question": RunnablePassthrough()}|prompt|dbrx|StrOutputParser()
chain.invoke(my_question)

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating Agent tools**

# COMMAND ----------

# MAGIC %md
# MAGIC **UC- based agent tools**

# COMMAND ----------

# MAGIC %md
# MAGIC Using SQL UDFs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_pdf_text_embeddings(pid INT)
# MAGIC RETURNS TABLE
# MAGIC RETURN SELECT embedding from pdf_text_embeddings where id=pid LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from get_pdf_text_embeddings(7) 

# COMMAND ----------

# MAGIC %md
# MAGIC Using Python UC UDF

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC training.fikrat.python_exec (
# MAGIC code STRING COMMENT 'Python code to execute. Remember to print the final result to stdout.'
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC DETERMINISTIC
# MAGIC COMMENT 'Executes Python code in the sandboxed environment and returns its stdout. The runtime is stateless and you can not read output of the previous tool executions. i.e. No such variables "rows", "observation" defined. Calling another tool inside a Python code is NOT allowed. Use standard python libraries only.'
# MAGIC AS $$
# MAGIC import sys
# MAGIC from io import StringIO
# MAGIC sys_stdout = sys.stdout
# MAGIC redirected_output = StringIO()
# MAGIC sys.stdout = redirected_output
# MAGIC exec(code)
# MAGIC sys.stdout = sys_stdout
# MAGIC return redirected_output.getvalue()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select training.fikrat.python_exec("print(2*5)")

# COMMAND ----------

# MAGIC %sql
# MAGIC select training.fikrat.python_exec("def mult_func(x,y):\n return x*y\nprint(mult_func(3,5))") as res

# COMMAND ----------

# MAGIC %md
# MAGIC **Exploring vectoc search**

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Using SQL to query vector search

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT content FROM VECTOR_SEARCH(index => "training.fikrat.vs_pdf_index", query => "What is InstructGPT", num_results => 2)

# COMMAND ----------

my_question="What is InstructGPT"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  training.fikrat.pdf_vector_search( "What is InstructGPT") 

# COMMAND ----------

# MAGIC %md
# MAGIC Using python to search

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION training.fikrat.databricks_docs_vector_search (
# MAGIC   query STRING
# MAGIC   COMMENT 'The query string for searching Databricks documentation.'
# MAGIC ) RETURNS TABLE
# MAGIC COMMENT 'Executes a search on Databricks documentation to retrieve text documents most relevant to the input query.'
# MAGIC RETURN
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   vector_search(embedding_column_type => 'array',
# MAGIC     index => 'training.fikrat.pdf_text_self_managed_vs_index',
# MAGIC     query => query,
# MAGIC     num_results => 5
# MAGIC   )

# COMMAND ----------

from langchain.chat_models import ChatDatabricks
chat_model=ChatDatabricks(endpoint="databricks-dbrx-instruct", temperature=0.1)
#test chat model
chat_model.invoke("When USSR collapsed?")

# COMMAND ----------


def get_retriever():
    ind=vsc.get_index(index_name='training.fikrat.vs_pdf_index')
    emb_model=DatabricksEmbeddings(endpoint="databricks-bge-large-en")
    vss=DatabricksVectorSearch(index=ind, embedding=emb_model)
    return vss.as_retriever()

# COMMAND ----------

rtv=get_retriever()
rtv.invoke("What is InstructGPT?")

# COMMAND ----------

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

from langchain.chains import create_retrieval_chain
chain=create_retrieval_chain(llm=chat_model,retriever=get_retriever())
chain.invoke({"query": "What is generative AI?"})

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploring Agents

# COMMAND ----------

# MAGIC %md
# MAGIC Create SQL UDF tool 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION training.fikrat.pdf_vector_search (
# MAGIC   -- The agent uses this comment to determine how to generate the query string parameter.
# MAGIC   query STRING
# MAGIC   COMMENT 'The query string for searching pdfs.'
# MAGIC ) RETURNS TABLE
# MAGIC -- The agent uses this comment to determine when to call this tool. It describes the types of documents and information contained within the index.
# MAGIC COMMENT 'Executes a search on Databricks documentation to retrieve text documents most relevant to the input query.' RETURN
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   VECTOR_SEARCH( index => 'training.fikrat.vs_pdf_index',
# MAGIC     query => query,
# MAGIC     num_results => 5
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC
