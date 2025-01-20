# Databricks notebook source
# MAGIC %sql
# MAGIC Use catalog learn_adb_fikrat;
# MAGIC create schema if not exists bronze;
# MAGIC Use bronze

# COMMAND ----------

# DBTITLE 1,Infer schema from a static DataFrame
data_root_folder='/Volumes/learn_adb_fikrat/bronze/landing'
static_df = spark.read.json(f'{data_root_folder}/sensor')
json_schema = static_df.schema


# COMMAND ----------

checkpoint_root_path = "/Volumes/learn_adb_fikrat/bronze/ext_landing_volume/streaming-checkpoints"

# COMMAND ----------

# DBTITLE 1,Reset environment
checkpoint_path_sensor=f'{checkpoint_root_path}/iot_measurements'
spark.sql("DROP TABLE IF EXISTS iot_measurements")
dbutils.fs.rm(checkpoint_path_sensor, True)

# COMMAND ----------

# DBTITLE 1,Reading stream
import pyspark.sql.functions as F
dfStrm = spark.readStream.format('json') \
    .schema(json_schema) \
    .option('maxFilesPerTrigger', 5) \
    .load(data_folder)\
    .withColumn('IngestionTimestamp', F.to_timestamp(F.col('Timestamp')))\
    .withColumn('SourceFileName', F.input_file_name() )    

display(dfStrm)

# COMMAND ----------

display(dfStrm.groupBy('Office').count())

# COMMAND ----------

# DBTITLE 1,Writing sensor data to Delta table
# catalog_name="learn_adb_fikrat"
# schema_name = "bronze"
# table_name ='iot_measurements'
# table_fqn  = f"{catalog_name}.{schema_name}.{table_name}"
stmHandle=dfStrm.writeStream.format("delta")\
    .option('checkpointLocation', checkpoint_path)\
    .trigger(processingTime='1 seconds')\
    .toTable('iot_measurements')

# COMMAND ----------

# DBTITLE 1,Writing weather data
strmw=spark.readStream.table('w')

# COMMAND ----------

stmHandle.status

# COMMAND ----------

stmHandle.recentProgress

# COMMAND ----------

stmHandle.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iot_measurements

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from iot_measurements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto loader

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files('/Volumes/learn_adb_fikrat/bronze/landing/sensor', format=>'json') limit 10

# COMMAND ----------

al_table_name='iot_measurements_al'
checkpoint_path = "/Volumes/learn_adb_fikrat/bronze/ext_landing_volume/autoloader-checkpoints/iot_measurements2"
schema_path = f"{data_folder}/autoloader-checkpoints/iot_measurements"
table_fqn  = f"{catalog_name}.{schema_name}.{al_table_name}"
strm_handle_al = spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format", "json")\
  .option("cloudFiles.schemaLocation", schema_path)\
  .load(data_folder)\
  .writeStream.format("delta")\
  .option("checkpointLocation", checkpoint_path)\
  .option('mergeSchema', 'True')\
  .toTable(table_fqn)

# COMMAND ----------

strm_handle_al.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.iot_measurements_al

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming between Delta tables

# COMMAND ----------

checkpoint_path="/Volumes/learn_adb_fikrat/bronze/ext_landing_volume/autoloader-checkpoints/iot_measurements_silver"
dft=spark.readStream.table(table_fqn)\
    .writeStream.format("delta")\
    .option("checkpointLocation", checkpoint_path)\
    .toTable('iot_measurements_silver')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iot_measurements_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create OR REPLACE VIEW vw_iot
# MAGIC AS
# MAGIC select * from bronze.iot_measurements

# COMMAND ----------

dft.stop()

# COMMAND ----------

checkpoint_path="/Volumes/learn_adb_fikrat/bronze/ext_landing_volume/autoloader-checkpoints/iot_measurements_silver2"
dft=spark.readStream.table('vw_iot')\
    .writeStream.format("delta")\
    .option("checkpointLocation", checkpoint_path)\
    .toTable('iot_measurements_silver')


# COMMAND ----------


