-- Databricks notebook source
CREATE OR REFRESH MATERIALIZED VIEW dlt_batch_bronze
AS
select * from csv.`dbfs:/FileStore/flights/`

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW dlt_batch_silver
AS
SELECT * FROM LIVE.dlt_batch_bronze

-- COMMAND ----------


