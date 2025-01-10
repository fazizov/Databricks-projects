# Databricks notebook source
# MAGIC %md
# MAGIC # Preparing environment

# COMMAND ----------

# MAGIC %sql
# MAGIC Use catalog learn_adb_fikrat;
# MAGIC create schema if not exists bronze;
# MAGIC create schema if not exists silver;
# MAGIC Use bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze.iot_measurements;
# MAGIC DROP TABLE IF EXISTS weather;
# MAGIC DROP TABLE IF EXISTS office;
# MAGIC DROP TABLE IF EXISTS iot_pivoted;
# MAGIC DROP TABLE IF EXISTS silver.iot_measurements;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver.iot_measurements;

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Defining variables
root_data_folder='/Volumes/learn_adb_fikrat/bronze/landing'
root_archive_folder='/Volumes/learn_adb_fikrat/bronze/landing/archive'
checkpoint_root_path = "/Volumes/learn_adb_fikrat/bronze/ext_landing_volume/streaming-checkpoints"

checkpoint_path_sensor=f'{checkpoint_root_path}/iot_measurements'
checkpoint_path_sensor1=f'{checkpoint_root_path}/iot_measurements1'
checkpoint_path_sensor2=f'{checkpoint_root_path}/iot_measurements2'
checkpoint_path_sensor3=f'{checkpoint_root_path}/iot_measurements3'
checkpoint_path_sensor4=f'{checkpoint_root_path}/iot_measurements4'
checkpoint_path_sensor5=f'{checkpoint_root_path}/iot_measurements5'
checkpoint_path_weather=f'{checkpoint_root_path}/weather'


# COMMAND ----------

# DBTITLE 1,Read/write office data
spark.read\
  .format('csv')\
  .option('header','true')\
  .load(f'{root_data_folder}/office')\
  .write.format('delta')\
  .mode('overwrite')\
  .saveAsTable('bronze.office')


# COMMAND ----------

# DBTITLE 1,Infer schema from a static DataFrame
static_df = spark.read.json(f'{root_data_folder}/sensor')
iot_schema = static_df.schema
weather_schema = spark.read.json(f'{root_data_folder}/weather').schema


# COMMAND ----------

# DBTITLE 1,Reset environment

dbutils.fs.rm(checkpoint_path_sensor, True)
dbutils.fs.rm(checkpoint_path_sensor1, True)
dbutils.fs.rm(checkpoint_path_sensor2, True)
dbutils.fs.rm(checkpoint_path_sensor3, True)
dbutils.fs.rm(checkpoint_path_sensor4, True)
dbutils.fs.rm(checkpoint_path_sensor5, True)
dbutils.fs.rm(checkpoint_path_weather, True)

# COMMAND ----------

dbutils.fs.mkdirs(root_archive_folder)

# COMMAND ----------

# MAGIC %md
# MAGIC #Basic streaming operations: read and write

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic read/write

# COMMAND ----------

# DBTITLE 1,Reading stream
dfSensor = spark.readStream.format('json') \
    .schema(iot_schema) \
    .load(f'{root_data_folder}/sensor')\
    .withColumn('IngestionTimestamp', F.current_timestamp())\
    .withColumn('SourceFileName', F.input_file_name() )    

# display(dfSensor)

# COMMAND ----------

# DBTITLE 1,Writing sensor data to Delta table
strms=dfSensor.writeStream.format("delta")\
    .option('checkpointLocation', checkpoint_path_sensor)\
    .queryName('iot_measurements')\
    .toTable('bronze.iot_measurements')

# COMMAND ----------

# DBTITLE 1,Inspecting data in Delta table
# MAGIC %sql
# MAGIC select  * from iot_measurements 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table- to-table streams
# MAGIC (Source table should be append only)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update iot_measurements set EventTime=current_timestamp()  where Office='Office 1'

# COMMAND ----------

# DBTITLE 1,Table to table streams
dfstt=spark.readStream.table('iot_measurements')\
    .selectExpr('Measurements.Sensor as Sensor',
           'Measurements.MeasurementType  as MeasurementType',
           'cast(Measurements.MeasurementValue as float) as MeasurementValue',
           'cast(EventTime as timestamp) as EventTime',
           'Office','IngestionTimestamp')\
    .writeStream.format("delta")\
    .option('checkpointLocation', checkpoint_path_sensor5)\
    .toTable('silver.iot_measurements')    


# COMMAND ----------

# MAGIC %md
# MAGIC ## Managing and monitoring  stream

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger options

# COMMAND ----------

dfWeather=spark.readStream.format('json') \
    .schema(weather_schema) \
    .load(f'{root_data_folder}/weather')\
    .withColumn('IngestionTimestamp', F.current_timestamp())\
    .withColumn('SourceFileName', F.input_file_name() )    

# COMMAND ----------

# DBTITLE 1,Fixed intervals
strmw=dfWeather.writeStream.format("delta")\
    .trigger(processingTime='10 seconds')\
    .queryName('weather')\
    .option('checkpointLocation', checkpoint_path_weather)\
    .toTable('weather')

# COMMAND ----------

# DBTITLE 1,Process once
strms=dfSensor.writeStream.format("delta")\
    .option('checkpointLocation', checkpoint_path_sensor2)\
    .trigger(availableNow=True)\
    .queryName('iot_measurements_once')\
    .toTable('bronze.iot_measurements_once')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source options
# MAGIC (See https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html#input-sources for more details)

# COMMAND ----------

# DBTITLE 1,Limiting number of input files
dfStrm = spark.readStream.format('json') \
    .schema(iot_schema) \
    .option('maxFilesPerTrigger', 5)\
    .option('latestFirst',True)\
    .load(f'{root_data_folder}/sensor')\
    .withColumn('IngestionTimestamp', F.current_timestamp())\
    .withColumn('SourceFileName', F.input_file_name())
display(dfStrm)        



# COMMAND ----------

# DBTITLE 1,Source file cleaning options (archive,delete,off)

#Archiving option didn't work on UC volumes
dfStrm = spark.readStream.format('json') \
    .schema(iot_schema) \
    .option("spark.sql.streaming.fileSource.log.compactInterval","1")\
    .option("spark.sql.streaming.fileSource.log.cleanupDelay","1")\
    .option('cleanSource', 'archive') \
    .option('sourceArchiveDir', root_archive_folder)\
    .load(f'{root_data_folder}/sensor')\
    .withColumn('IngestionTimestamp', F.current_timestamp())\
    .withColumn('SourceFileName', F.input_file_name())\
    .writeStream.format("delta")\
    .option('checkpointLocation', checkpoint_path_sensor2)\
    .queryName('iot_measurements2')\
    .toTable('bronze.iot_measurements_archive')       


# COMMAND ----------

dbutils.fs.ls(root_archive_folder)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sink options

# COMMAND ----------

# DBTITLE 1,Writing to memory sink
dfStrm = spark.readStream.format('json') \
    .schema(iot_schema) \
    .load(f'{root_data_folder}/sensor')\
    .withColumn('IngestionTimestamp', F.current_timestamp())\
    .writeStream.format("memory")\
    .outputMode("append")\
    .option('checkpointLocation', checkpoint_path_sensor3)\
    .queryName('iot_measurements3')\
    .start()
    

# COMMAND ----------

# DBTITLE 1,Browsing memory sink
# MAGIC %sql
# MAGIC select * from iot_measurements3

# COMMAND ----------

dfStrm = dfSensor.selectExpr('cast(EventTime as Timestamp) as EventTime',
                             'Measurements.Sensor as Sensor',
                             'Measurements.MeasurementType  as MeasurementType',
                             'cast(Measurements.MeasurementValue as float) as MeasurementValue',
                             'Office','IngestionTimestamp')
display(dfStrm)                             

# COMMAND ----------

# DBTITLE 1,Using foreachBatch sink
def fn_batch_processing(df,batchid):
    df.groupBy('Office','Sensor')\
        .pivot('MeasurementType',['temperature','humidity'])\
        .sum('MeasurementValue')\
        .write.mode('overwrite')\
        .saveAsTable('iot_pivoted')
    pass    

dfsp = dfSensor.selectExpr('cast(EventTime as Timestamp) as EventTime','Measurements.Sensor as Sensor',
                             'Measurements.MeasurementType  as MeasurementType',
                             'cast(Measurements.MeasurementValue as float) as MeasurementValue',
                             'Office','IngestionTimestamp')\
    .writeStream\
    .option('checkpointLocation', checkpoint_path_sensor4)\
    .queryName('iot_measurements4')\
    .foreachBatch(fn_batch_processing)\
    .start()


# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete from iot_pivoted;
# MAGIC select * from iot_pivoted

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitoring and controlling stream execution

# COMMAND ----------

# DBTITLE 1,Get stream status
strms.status

# COMMAND ----------

# DBTITLE 1,Read last ingestion
print(strms.lastProgress)

# COMMAND ----------

# DBTITLE 1,Reading multiple runs history
strms.recentProgress

# COMMAND ----------

# DBTITLE 1,List all active streams
for strm in spark.streams.active:
    print(f'Stream {strm.name} is active')

# COMMAND ----------

# DBTITLE 1,Block execution flow
# strms.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Stopping stream
# strms.stop()

for strm in spark.streams.active:
    strm.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from weather 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stateless transformations

# COMMAND ----------

dfo=spark.table('office')
dfso=spark.readStream.table('iot_measurements')\
  .join(dfo, 'Office')\
  .selectExpr('cast(EventTime as Timestamp) as EventTime','Measurements.*','Office','City')
# display(df)  

# COMMAND ----------

# DBTITLE 1,Column projections
dfso=spark.readStream.table('iot_measurements')\
   .selectExpr('cast(EventTime as Timestamp) as EventTime','Measurements.*','Office','City')
display(dfso)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stateful transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregations

# COMMAND ----------

dfSensor = spark.readStream.format('json') \
    .schema(iot_schema) \
    .load(f'{root_data_folder}/sensor')\
    .withColumn('IngestionTimestamp', F.current_timestamp())\
    .withColumn('SourceFileName', F.input_file_name() )    

# COMMAND ----------

# DBTITLE 1,Global aggregations
display(dfSensor.groupBy().count())

# COMMAND ----------

dfStrm = dfSensor.selectExpr('cast(EventTime as Timestamp) as EventTime',
                             'Measurements.Sensor as Sensor',
                             'Measurements.MeasurementType  as MeasurementType',
                             'cast(Measurements.MeasurementValue as float) as MeasurementValue',
                             'Office','IngestionTimestamp')


# COMMAND ----------

# DBTITLE 1,Grouped aggregations
dfso=dfStrm.groupBy('Office','Sensor')\
    .sum('MeasurementValue').alias('TotalValue')
display(dfso)    

# COMMAND ----------

# DBTITLE 1,Time-based aggregates
dfso=dfStrm.groupBy('Office','Sensor',F.window('EventTime','30 seconds'))\
    .sum('MeasurementValue').alias('TotalValue')
display(dfso)    

# COMMAND ----------

# DBTITLE 1,Allowing watermark delays
dfso=dfStrm.withWatermark('EventTime','5 seconds')\
    .groupBy('Office','Sensor',F.window('EventTime','15 seconds'))\
    .sum('MeasurementValue').alias('TotalValue')
display(dfso)    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table joins

# COMMAND ----------

dfo=spark.table('office')
dfso=spark.readStream.table('silver.iot_measurements')

# COMMAND ----------

# DBTITLE 1,Stream to static joins
dfj=dfso.join(dfo, 'Office')\
  .select(dfso.EventTime,dfso.MeasurementType,dfso.MeasurementValue,
          dfso.Office,dfso.Sensor,dfo.City)
display(dfj)  

# COMMAND ----------

display(spark.read.table('weather'))

# COMMAND ----------

# DBTITLE 1,Set up watermarking delay
dfwstm=spark.readStream.table('weather')\
    .withColumn('WeatherEventTime', F.col('EventTime').cast('Timestamp'))\
    .drop('EventTime')\
    .withWatermark('WeatherEventTime','5 seconds')


# COMMAND ----------

# DBTITLE 1,Stream to stream joins- time equailty condition
dfssj=dfj.withWatermark('EventTime','5 seconds')\
    .join(dfwstm,
    (dfj.City==dfwstm.City)  &  (dfj.EventTime == dfwstm.WeatherEventTime),how='left')\
    .select(dfj.EventTime,'WeatherEventTime','MeasurementType','MeasurementValue','Office','Sensor',
    dfj.City,dfwstm.Temperature)
display(dfssj)

# COMMAND ----------

# DBTITLE 1,Stream to stream joins- time aproximity condition
dfssj=dfj.withWatermark('EventTime','5 seconds')\
    .join(dfwstm,
    (dfj.City==dfwstm.City)  &
    F.expr('EventTime BETWEEN WeatherEventTime - interval 15 seconds AND WeatherEventTime'),how='left')\
    .select(dfj.EventTime,'WeatherEventTime','MeasurementType','MeasurementValue','Office','Sensor',
    dfj.City,dfwstm.Temperature)
display(dfssj)

# COMMAND ----------

from pyspark.sql.functions import col
dfwstm=spark.readStream.table('weather')\
    .withColumn('WeatherEventTime', col('EventTime').cast('Timestamp'))\
    .withWatermark('WeatherEventTime','5 seconds')
dfssj=dfj.withWatermark('EventTime','5 seconds')\
    .join(dfwstm,
    (dfj.City==dfwstm.City)  &
    (dfj.EventTime.between(F.expr('WeatherEventTime - interval 15 seconds'), dfwstm.WeatherEventTime)),how='left')\
    .select(dfj.EventTime,'WeatherEventTime','MeasurementType','MeasurementValue','Office','Sensor',
    dfj.City,dfwstm.Temperature)
display(dfssj)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iot_measurements

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iot_measurements

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from iot_measurements

# COMMAND ----------

display(dfStrm.groupBy('Office').count())

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


