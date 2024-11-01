-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC catalog_name = 'tech_summit_data'
-- MAGIC schema_name = 'default'
-- MAGIC # user_name = spark.sql('SELECT current_user() as user').first()['user'].split('@')[0].replace('.','_')
-- MAGIC
-- MAGIC
-- MAGIC ## Create catalog and schema
-- MAGIC spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
-- MAGIC # spark.sql(f'ALTER CATALOG {catalog_name} DISABLE PREDICTIVE OPTIMIZATION')   
-- MAGIC # spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
-- MAGIC
-- MAGIC
-- MAGIC ## Use catalog and schema
-- MAGIC spark.sql(f"USE CATALOG {catalog_name}")
-- MAGIC spark.sql(f"USE SCHEMA {schema_name}")
-- MAGIC
-- MAGIC ## Set spark variables
-- MAGIC spark.conf.set("my.catalog", catalog_name)
-- MAGIC spark.conf.set("my.schema", schema_name)

-- COMMAND ----------

USE CATALOG ${my.catalog};
USE SCHEMA ${my.schema};

SELECT current_catalog(), current_schema()

-- COMMAND ----------


