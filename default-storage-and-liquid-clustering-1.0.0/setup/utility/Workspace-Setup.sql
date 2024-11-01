-- Databricks notebook source
-- UNCOMMENT TO DROP ALL OF THE DATA
-- DROP CATALOG IF EXISTS tech_summit_data CASCADE;

-- COMMAND ----------

-- MAGIC %run ./setup

-- COMMAND ----------

-- MAGIC %run ./table_exists

-- COMMAND ----------

-- MAGIC %run ./create_small_external_data

-- COMMAND ----------

-- MAGIC %run ./create_large_tables

-- COMMAND ----------

-- CATALOG
GRANT USE CATALOG ON CATALOG ${my.catalog} TO `account users`;
-- GRANT USE CATALOG, SELECT ON CATALOG ${my.catalog} TO `account users`;

-- SCHEMA
GRANT USE SCHEMA, SELECT ON SCHEMA ${my.catalog}.${my.schema} TO `account users`;
-- GRANT SELECT ON SCHEMA ${my.catalog}.${my.schema} TO `account users`;
-- GRANT SELECT ON TABLE ${my.catalog}.${my.schema}.airlines_final TO `account users`;
-- GRANT SELECT ON TABLE ${my.catalog}.${my.schema}.airline_cluster_origin_id TO `account users`;
-- GRANT SELECT ON TABLE ${my.catalog}.${my.schema}.airline_partitioned_origin TO `account users`;
-- GRANT SELECT ON TABLE ${my.catalog}.${my.schema}.airline_partitioned_origin_zid TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Does not work on lab.

-- COMMAND ----------

-- GRANT SELECT ON CATALOG saystem.storage TO `account users`;
-- GRANT USE CATALOG, SELECT ON CATALOG system TO `account users`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA system.storage TO `account users`;
