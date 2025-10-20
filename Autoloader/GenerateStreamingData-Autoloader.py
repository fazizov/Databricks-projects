# Databricks notebook source
import random
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.functions import col,struct,collect_list
import json
import os

# COMMAND ----------

def prepare_data(root_data_folder):
    dbutils.fs.rm(f'{root_data_folder}',True)
    #create folders
    dbutils.fs.mkdirs(f'{root_data_folder}/office')
    dbutils.fs.mkdirs(f'{root_data_folder}/sensor')
    dbutils.fs.mkdirs(f'{root_data_folder}/weather')
    dbutils.fs.mkdirs(f'{root_data_folder}/iot_agg')
    dbutils.fs.mkdirs(f'{root_data_folder}/archive')
    pass
def drop_all_tables():
    spark.sql("DROP TABLE IF EXISTS bronze.iot_measurements_autoloader")
    spark.sql("DROP TABLE IF EXISTS bronze.iot_measurements_autoloader1")
    spark.sql("DROP TABLE IF EXISTS bronze.iot_measurements_autoloader2")
    spark.sql("DROP TABLE IF EXISTS bronze.iot_measurements_autoloader3")
    spark.sql("DROP TABLE IF EXISTS bronze.iot_measurements_autoloader4")

def reset_checkpoints(checkpoint_path_sensor,schema_path_sensor):
    dbutils.fs.rm(checkpoint_path_sensor, True)
    dbutils.fs.rm(schema_path_sensor, True)
    dbutils.fs.rm(checkpoint_path_sensor1, True)
    dbutils.fs.rm(checkpoint_path_sensor2, True)
    dbutils.fs.rm(checkpoint_path_sensor3, True)
    dbutils.fs.rm(checkpoint_path_sensor4, True)

def reset_environment(checkpoint_path_sensor,schema_path_sensor,root_data_folder):
    drop_all_tables()
    reset_checkpoints(checkpoint_path_sensor,schema_path_sensor)
    prepare_data(root_data_folder)

def generate_measurements(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date, freq='H')
    data = []
    for date in date_range:
        for office in range(1, 3):
            for sensor in range(1, 3):
                temperature = round(random.uniform(20.0, 25.0), 2)
                humidity = round(random.uniform(30.0, 50.0), 2)
                data.append([date, f'Office {office}', f'Sensor {sensor}', 'temperature', temperature])
                data.append([date, f'Office {office}', f'Sensor {sensor}', 'humidity', humidity])
    
    return data

def generate_measurements_enhanced(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date, freq='H')
    data = []
    
    for date in date_range:
        for office in range(1,3):
            for sensor in range(1, 3):
                temperature = round(random.uniform(20.0, 25.0), 2)
                humidity = round(random.uniform(30.0, 50.0), 2)
                pressure= round(random.uniform(30.0, 50.0), 2)
                data.append([date, f'Office {office}', f'Sensor {sensor}', 'temperature', temperature,pressure])
                data.append([date, f'Office {office}', f'Sensor {sensor}', 'humidity', humidity
                             ,pressure])
    return data

def write_json_data(df_sensor,sensor_folder):
    for row in df_sensor.collect():
        rowDict={'EventTime':row.EventTime,'Office':row.Office,
                 'Sensor':row.Sensor, 
                 'MeasurementType':row.Measurement, 
                 'MeasurementValue':row.Value}
        file_path=f"{sensor_folder}/{rowDict['Office']}_{rowDict['EventTime']}.json"
        with open(file_path, 'w') as f:
            json.dump(rowDict,f)
        # print (f'File written to {file_path}')
        
    pass


def write_json_data_enhanced(df_sensor,sensor_folder):
    for row in df_sensor.collect():
        rowDict={'EventTime':row.EventTime,'Office':row.Office,
                 'Sensor':row.Sensor, 
                 'MeasurementType':row.Measurement, 
                 'MeasurementValue':row.Value,
                 'Pressure':row.Pressure}
        file_path=f"{sensor_folder}/{rowDict['Office']}_{rowDict['EventTime']}_enhanced.json"
        with open(file_path, 'w') as f:
            json.dump(rowDict,f)
        # print (f'File written to {file_path}')
        
    pass

def generate_persist_streaming_data(start_date, ndays,root_folder):
    end_date = start_date + timedelta(days=ndays)
    measurements = generate_measurements(start_date, end_date)
    df = pd.DataFrame(measurements, columns=['EventTime', 'Office', 'Sensor', 'Measurement', 'Value'])
    dfs=spark.createDataFrame(df).withColumn('EventTime',col('EventTime').cast('string'))
    write_json_data(dfs,f'{root_data_folder}/sensor')
    pass


def generate_persist_streaming_data_enhanced(start_date, ndays,root_folder):
    end_date = start_date + timedelta(days=ndays)
    measurements = generate_measurements_enhanced(start_date, end_date)
    df = pd.DataFrame(measurements, columns=['EventTime', 'Office', 'Sensor', 'Measurement', 'Value','Pressure'])
    dfs=spark.createDataFrame(df).withColumn('EventTime',col('EventTime').cast('string'))
    write_json_data_enhanced(dfs,f'{root_data_folder}/sensor')

    pass

# COMMAND ----------

# root_data_folder='/Volumes/learn_adb_fikrat/bronze/landing/autoloader'

# COMMAND ----------

# prepare_data(root_data_folder)

# COMMAND ----------

# DBTITLE 1,Generate  events- regular schema
# gernerate_persist_streaming_data(datetime(2025, 1, 3),2,root_data_folder)

# COMMAND ----------

# DBTITLE 1,Generate  events-  Enhanced schema
# gernerate_persist_streaming_data_enhanced(datetime(2025, 1, 8),3,root_data_folder)

# COMMAND ----------

# file_count = len(dbutils.fs.ls(f'{root_data_folder}/sensor'))
# print(file_count)

# COMMAND ----------

# MAGIC %md
# MAGIC
