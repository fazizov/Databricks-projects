# Databricks notebook source
import random
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.functions import col,struct,collect_list
import json

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

import os

def generate_measurements(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date, freq='H')
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
    df = pd.DataFrame(measurements, columns=['Timestamp', 'Office', 'Sensor', 'Measurement', 'Value'])
    dfs=spark.createDataFrame(df).withColumn('Timestamp',col('Timestamp').cast('string'))
    dfagg=dfs.groupBy('Timestamp','Office')\
        .agg(collect_list(struct("Sensor", "Measurement", "Value"))\
        .alias("Measurements"))
    return dfagg

def generate_weather_data(start_date, end_date):
    measurements = generate_weather_forecats(start_date, end_date)
    df = pd.DataFrame(measurements, columns=['Timestamp','City','Temperature'])
    dfs=spark.createDataFrame(df).withColumn('Timestamp',col('Timestamp').cast('string'))
    return dfs

def generate_data(start_date, end_date,root_data_folder):
    dfs=generate_sensor_data(start_date, end_date)
    shifted_start_date = start_date + timedelta(minutes=3)
    shifted_end_date = end_date + timedelta(minutes=3)
    dfw=generate_weather_data(shifted_start_date, shifted_end_date)
    dfw=generate_json_data(dfs,dfw,f'{root_data_folder}/sensor',f'{root_data_folder}/weather')       

def generate_json_data(df_sensor,df_weather,sensor_folder,weather_folder):
    for row in df_sensor.collect():
        rowDict={'Timestamp':row.Timestamp,'Office':row.Office,
                 'Measurements':{'Sensor':row.Measurements[0].Sensor, 'MeasurementType':row.Measurements[0].Measurement, 'MeasurementValue':row.Measurements[0].Value}}
        file_path=f"{sensor_folder}/{rowDict['Office']}_{rowDict['Timestamp']}.json"
        with open(file_path, 'w') as f:
            json.dump(rowDict,f)
        print (f'File written to {file_path}')
    for row in df_weather.collect():
        rowDict=row.asDict()
        file_path=f"{weather_folder}/{rowDict['City']}_{rowDict['Timestamp']}.json"
        with open(file_path, 'w') as f:
            json.dump(rowDict,f)
        print (f'File written to {file_path}')            

    pass    


# COMMAND ----------

start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
end_date = start_date + timedelta(days=7)
# generate_sensor_data(start_date, end_date,'/Volumes/learn_adb_fikrat/default/landing')
# generate_sensor_data(start_date, end_date,'data')
dbutils.fs.mkdirs('/Volumes/learn_adb_fikrat/bronze/landing/sensor')
dbutils.fs.mkdirs('/Volumes/learn_adb_fikrat/bronze/landing/weather')
generate_data(start_date, end_date,'/Volumes/learn_adb_fikrat/bronze/landing')


# COMMAND ----------

start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
end_date = start_date + timedelta(minutes=3)
print (start_date, end_date)


# COMMAND ----------

start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
end_date = start_date + timedelta(days=7)
generate_weather_data(start_date, end_date,'/Volumes/learn_adb_fikrat/default/landing/weather')

# COMMAND ----------

import json
for row in dfagg.limit(5).collect():
    rowDict=row.asDict()
    fileName=f"./data/{rowDict['Office']}_{rowDict['Timestamp']}.json"
    with open(fileName, 'w') as f:
        json.dump(rowDict,f)

# COMMAND ----------

# DBTITLE 1,epad
from pyspark.sql.functions import collect_list, struct

# Initialize Spark session

# Sample data
data = [
    ("Office1", "2025-01-01 00:00:00", "Sensor1", "Temperature", 22.5),
    ("Office1", "2025-01-01 00:00:00", "Sensor2", "Humidity", 45),
    ("Office1", "2025-01-01 01:00:00", "Sensor1", "Temperature", 23.0),
    ("Office1", "2025-01-01 01:00:00", "Sensor2", "Humidity", 40),
    ("Office2", "2025-01-01 00:00:00", "Sensor1", "Temperature", 21.5),
    ("Office2", "2025-01-01 00:00:00", "Sensor2", "Humidity", 50),
    # Add more data as needed
]

# Create DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([StructField("Office", StringType(), True), StructField("Timestamp", StringType(), True), StructField("Sensor", StringType(), True), StructField("MeasureType", StringType(), True), StructField("MeasureValue", DoubleType(), True)])
df = spark.createDataFrame(data, schema)

# Group by Office and Timestamp, and collect measurements into a list
df_grouped = df.groupBy("Office", "Timestamp").agg(
    collect_list(struct("Sensor", "MeasureType", "MeasureValue")).alias("Measurements")
)

# Convert to JSON format
json_data = df_grouped.toJSON().collect()

# Print JSON data
for item in json_data:
    print(item)


# COMMAND ----------

display(df_grouped)

# COMMAND ----------

jsondata=Row(Timestamp='2025-01-08 17:00:00', Office='Office 3', Measurements=[Row(Sensor='Sensor 1', Measurement='temperature', Value=21.39), Row(Sensor='Sensor 1', Measurement='humidity', Value=48.58), Row(Sensor='Sensor 2', Measurement='temperature', Value=20.24), Row(Sensor='Sensor 2', Measurement='humidity', Value=47.87)])"
fileName=f"./data/test.json"
jsondict=jsondata.asDict()
with open(fileName, 'w') as f:
       json.dumps(jsondict,4)



# COMMAND ----------


