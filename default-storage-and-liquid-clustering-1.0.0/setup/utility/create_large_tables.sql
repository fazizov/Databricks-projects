-- Databricks notebook source
-- MAGIC %md
-- MAGIC Save as a partitioned Delta table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ##
-- MAGIC ## Read the  airlines data as a Spark DF and create paritioned table
-- MAGIC ##
-- MAGIC
-- MAGIC import random
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC def create_airlines_partitioned():
-- MAGIC
-- MAGIC   total_unique_ids = 100000
-- MAGIC
-- MAGIC   sdf_airlines = (spark
-- MAGIC                   .read.csv('dbfs:/databricks-datasets/asa/airlines/200[5678].csv', inferSchema=True, header=True)            
-- MAGIC                   # .withColumn("ID", ( (rand(seed=5) * total_unique_ids + 1) * 5).cast("int"))
-- MAGIC                   # .withColumn('id', monotonically_increasing_id())
-- MAGIC                 )
-- MAGIC   
-- MAGIC   spark.sql(f'DROP TABLE IF EXISTS {catalog_name}.{schema_name}.airline_partitioned')
-- MAGIC   
-- MAGIC   (sdf_airlines
-- MAGIC   .write
-- MAGIC   .format('delta')
-- MAGIC   .mode('overwrite')
-- MAGIC   .partitionBy('Origin')
-- MAGIC   #.save(path)                           ## External Delta table
-- MAGIC   .saveAsTable(f"{catalog_name}.{schema_name}.airline_partitioned")
-- MAGIC   )
-- MAGIC
-- MAGIC   spark.sql(f'ALTER TABLE {catalog_name}.{schema_name}.airline_partitioned DISABLE PREDICTIVE OPTIMIZATION')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Clone the partitioned table and add ZORDER.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC def create_airlines_partitioned_zid():
-- MAGIC
-- MAGIC     spark.sql(f'DROP TABLE IF EXISTS {catalog_name}.{schema_name}.airline_partitioned_z')
-- MAGIC
-- MAGIC     ## Copy the partitioned table
-- MAGIC     spark.sql(f'''
-- MAGIC     CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.airline_partitioned_z
-- MAGIC     DEEP CLONE {catalog_name}.{schema_name}.airline_partitioned
-- MAGIC     ''')
-- MAGIC
-- MAGIC     ## Add ZORDER ID
-- MAGIC     spark.sql(f'OPTIMIZE {catalog_name}.{schema_name}.airline_partitioned_z ZORDER BY (ArrTime)')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a table using the partitioned table and add CLUSTER BY Origin and ID.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def create_cluster_by_table():
-- MAGIC
-- MAGIC     spark.sql(f'DROP TABLE IF EXISTS {catalog_name}.{schema_name}.airline_cluster')
-- MAGIC
-- MAGIC     ## Create default table without partitioning
-- MAGIC     spark.sql(f'''
-- MAGIC               CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.airline_cluster
-- MAGIC               AS 
-- MAGIC               SELECT *
-- MAGIC              FROM {catalog_name}.{schema_name}.airline_partitioned
-- MAGIC     ''')
-- MAGIC
-- MAGIC
-- MAGIC     ## Add cluster by
-- MAGIC     spark.sql(f'''
-- MAGIC               ALTER TABLE {catalog_name}.{schema_name}.airline_cluster
-- MAGIC               CLUSTER BY (Origin, ArrTime)
-- MAGIC     ''')
-- MAGIC
-- MAGIC     ## Optimize
-- MAGIC     spark.sql(f'OPTIMIZE {catalog_name}.{schema_name}.airline_cluster')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ## Check if table exists
-- MAGIC airline_partitioned_origin_exists = check_table_exists('tech_summit_data', 'default', 'airline_partitioned')
-- MAGIC airline_partitioned_origin_zid_exists = check_table_exists('tech_summit_data', 'default', 'airline_partitioned_z')
-- MAGIC airline_cluster_origin_id_exists = check_table_exists('tech_summit_data', 'default', 'airline_cluster')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Check if any variable is False
-- MAGIC if not (airline_partitioned_origin_exists and airline_partitioned_origin_zid_exists and airline_cluster_origin_id_exists):
-- MAGIC     print("One of these tables doesn't exist. Recreating tables...")
-- MAGIC     create_airlines_partitioned()
-- MAGIC     create_airlines_partitioned_zid()
-- MAGIC     create_cluster_by_table()
-- MAGIC else:
-- MAGIC     print("All tables are available in tech_summit_catalog No action needed.")
