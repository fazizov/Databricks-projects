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

def generate_sensor_data(start_date, end_date,root_data_folder):
    measurements = generate_measurements(start_date, end_date)
    df = pd.DataFrame(measurements, columns=['EventTime', 'Office', 'Sensor', 'Measurement', 'Value'])
    dfs=spark.createDataFrame(df).withColumn('EventTime',col('EventTime').cast('string'))
    dfagg=dfs.groupBy('EventTime','Office')\
        .agg(collect_list(struct("Sensor", "Measurement", "Value"))\
        .alias("Measurements"))
    write_json_data(dfagg,f'{root_data_folder}/sensor')

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

def generate_sensor_data_enhanced(start_date, end_date,root_data_folder):
    measurements = generate_measurements_enhanced(start_date, end_date)
    df = pd.DataFrame(measurements, columns=['EventTime', 'Office', 'Sensor', 'Measurement', 'Value','Pressure'])
    dfs=spark.createDataFrame(df).withColumn('EventTime',col('EventTime').cast('string'))
    dfagg=dfs.groupBy('EventTime','Office','Pressure')\
        .agg(collect_list(struct("Sensor", "Measurement", "Value"))\
        .alias("Measurements"))
    write_json_data_enhanced(dfagg,f'{root_data_folder}/sensor')

def write_json_data(df_sensor,sensor_folder):
    for row in df_sensor.collect():
        rowDict={'EventTime':row.EventTime,'Office':row.Office,
                 'Measurements':{'Sensor':row.Measurements[0].Sensor, 'MeasurementType':row.Measurements[0].Measurement, 'MeasurementValue':row.Measurements[0].Value}}
        file_path=f"{sensor_folder}/{rowDict['Office']}_{rowDict['EventTime']}.json"
        with open(file_path, 'w') as f:
            json.dump(rowDict,f)
        print (f'File written to {file_path}')
        
    pass


def write_json_data_enhanced(df_sensor,sensor_folder):
    for row in df_sensor.collect():
        rowDict={'EventTime':row.EventTime,'Office':row.Office,
                 'Measurements':{'Sensor':row.Measurements[0].Sensor, 'MeasurementType':row.Measurements[0].Measurement, 'MeasurementValue':row.Measurements[0].Value},                                  'Pressure':row.Pressure}
        file_path=f"{sensor_folder}/{rowDict['Office']}_{rowDict['EventTime']}_enhanced.json"
        with open(file_path, 'w') as f:
            json.dump(rowDict,f)
        print (f'File written to {file_path}')
        
    pass

def gernerate_persist_streaming_data(start_date, ndays,root_folder):
    end_date = start_date + timedelta(days=ndays)
    generate_sensor_data(start_date, end_date,root_folder)
    pass


def gernerate_persist_streaming_data_enhanced(start_date, ndays,root_folder):
    end_date = start_date + timedelta(days=ndays)
    generate_sensor_data_enhanced(start_date, end_date,root_folder)
    pass

# COMMAND ----------

root_data_folder='/Volumes/learn_adb_fikrat/bronze/landing/autoloader'

# COMMAND ----------

# prepare_data(root_data_folder)

# COMMAND ----------

# DBTITLE 1,Generate  events- regular schema
# gernerate_persist_streaming_data(datetime(2025, 1, 3),2,root_data_folder)

# COMMAND ----------

# DBTITLE 1,Generate  events-  Enhanced schema
# gernerate_persist_streaming_data_enhanced(datetime(2025, 1, 8),3,root_data_folder)

# COMMAND ----------

file_count = len(dbutils.fs.ls(f'{root_data_folder}/sensor'))
print(file_count)

# COMMAND ----------

# MAGIC %md
# MAGIC
