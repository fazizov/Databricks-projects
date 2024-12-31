# Databricks notebook source
spark.read.csv('dbfs:/databricks-datasets/learning-spark-v2/flights/departuredelays.csv').write.format('csv').save('bronze.source_schema.landing')

# COMMAND ----------


