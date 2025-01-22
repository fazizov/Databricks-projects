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
    date_range = pd.date_range(start=start_date, end=end_date, freq='min')
    data = []
    
    for date in date_range:
        for office in range(1, 6):
            for sensor in range(1, 3):
                temperature = round(random.uniform(20.0, 25.0), 2)
                humidity = round(random.uniform(30.0, 50.0), 2)
                data.append([date, f'Office {office}', f'Sensor {sensor}', 'temperature', temperature])
                data.append([date, f'Office {office}', f'Sensor {sensor}', 'humidity', humidity])
    
    return data

def generate_weather_forecats(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date, freq='H')
    data = []
    
    for date in date_range:
        for city in ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']:
                temperature = round(random.uniform(20.0, 25.0), 2)
                data.append([date, city, temperature]) 
    return data


def generate_sensor_data(start_date, end_date):
    measurements = generate_measurements(start_date, end_date)
    df = pd.DataFrame(measurements, columns=['EventTime', 'Office', 'Sensor', 'Measurement', 'Value'])
    dfs=spark.createDataFrame(df).withColumn('EventTime',col('EventTime').cast('string'))
    dfagg=dfs.groupBy('EventTime','Office')\
        .agg(collect_list(struct("Sensor", "Measurement", "Value"))\
        .alias("Measurements"))
    return dfagg

def generate_weather_data(start_date, end_date):
    measurements = generate_weather_forecats(start_date, end_date)
    df = pd.DataFrame(measurements, columns=['EventTime','City','Temperature'])
    dfs=spark.createDataFrame(df).withColumn('EventTime',col('EventTime').cast('string'))
    return dfs


def generate_data(start_date, end_date,root_data_folder):
    dfs=generate_sensor_data(start_date, end_date)
    shifted_start_date = start_date + timedelta(seconds=5)
    shifted_end_date = end_date + timedelta(seconds=5)
    dfw=generate_weather_data(shifted_start_date, shifted_end_date)
    dfs=generate_sensor_data(start_date, end_date)
    write_json_data(dfs,dfw,f'{root_data_folder}/sensor',f'{root_data_folder}/weather')       

def write_json_data(df_sensor,df_weather,sensor_folder,weather_folder):
    for row in df_sensor.collect():
        rowDict={'EventTime':row.EventTime,'Office':row.Office,
                 'Measurements':{'Sensor':row.Measurements[0].Sensor, 'MeasurementType':row.Measurements[0].Measurement, 'MeasurementValue':row.Measurements[0].Value}}
        file_path=f"{sensor_folder}/{rowDict['Office']}_{rowDict['EventTime']}.json"
        with open(file_path, 'w') as f:
            json.dump(rowDict,f)
        print (f'File written to {file_path}')
    for row in df_weather.collect():
        rowDict=row.asDict()
        file_path=f"{weather_folder}/{rowDict['City']}_{rowDict['EventTime']}.json"
        with open(file_path, 'w') as f:
            json.dump(rowDict,f)
        print (f'File written to {file_path}')            
    pass

def generate_persist_office_data(root_data_folder):
    file_path=f'{root_data_folder}/office'
    data = []
    city_list= ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    for office in range(1, 6):
        data.append([f'Office {office}', city_list[office-1]])
    df=spark.createDataFrame(data,['Office','City'])
    df.write.format('csv').mode('overwrite').option('header','true').save(file_path)
    pass

def gernerate_persist_streaming_data(start_date, ndays,root_folder):
    end_date = start_date + timedelta(days=ndays)
    generate_persist_office_data(root_folder)
    generate_data(start_date, end_date,root_folder)
    pass


# COMMAND ----------

root_folder='/Volumes/learn_adb_fikrat/bronze/landing'

# COMMAND ----------

prepare_data(root_folder)

# COMMAND ----------

# DBTITLE 1,Generate 1 day events
gernerate_persist_streaming_data(datetime(2025, 1, 2),1,root_folder)

# COMMAND ----------

file_count = len(dbutils.fs.ls(f'{root_data_folder}/sensor'))
print(file_count)

# COMMAND ----------

# MAGIC %md
# MAGIC
