# Databricks notebook source
# MAGIC %md
# MAGIC TO DO:
# MAGIC - Read from provided csv file
# MAGIC - Convert data types to correct types
# MAGIC - Add metadata columns for source file name and ingestion timestamp
# MAGIC - Add sales month and year columns
# MAGIC - Add sequential ID column
# MAGIC - Calculate difference between previous and current sales for the same product, based on sales dates
# MAGIC - Create temp view named vw_sales_silver based on this dataframe

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC TO DO:
# MAGIC
# MAGIC Read sample product data in JSON format with expicit schema
# MAGIC Flatten data and add ID column
# MAGIC Write into delta table named product_silver

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC TO DO:
# MAGIC
# MAGIC - Read from product_silver and vw_sales_silver
# MAGIC - Join 2 dataframes using inner join type
# MAGIC - Deduplicate rows
# MAGIC - Keep only sales related to office category
# MAGIC - Calculate total, max and min of sales amounts per product furniture type, year and month
# MAGIC - Create a temp view named vw_sales_aggregates from above dataframe

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC TO DO:
# MAGIC - Read raw sales_data_corrupted.csv files
# MAGIC - Cleanse all empty values and upsert it into sales_bronze table, using Merge INTO command
