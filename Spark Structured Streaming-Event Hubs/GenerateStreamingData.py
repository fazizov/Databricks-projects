# Databricks notebook source
import random
import pandas as pd
from datetime import datetime, timedelta
import pyspark.sql.functions as F
import os
from databricks.sdk.runtime import *
from pyspark.sql.types import *

# COMMAND ----------

def generate_measurements(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date, freq='min')
    data = []
    for date in date_range:
        for office in range(1, 6):
            for sensor in range(1, 3):
                temperature = random.uniform(20.0, 25.0)
                humidity = random.uniform(30.0, 50.0)
                data.append([date, f'Office {office}', f'Sensor {sensor}', 'temperature', temperature])
                data.append([date, f'Office {office}', f'Sensor {sensor}', 'humidity', humidity])
    
    df = pd.DataFrame(data, columns=['EventTime', 'Office', 'Sensor', 'Measurement', 'Value'])
    dfs=spark.createDataFrame(df)
    return dfs


def generate_sensor_data(start_date, end_date,eh_conn_str,eh_name,checkpoint_path,temp_table_name):
    dfs = generate_measurements(start_date, end_date)
    ehConf={}
    ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eh_conn_str)
    ehConf['eventhubs.eventHubName']=eh_name
    dfagg=dfs.withColumn('EventTime',F.col('EventTime').cast('string'))\
        .groupBy('EventTime','Office')\
        .agg(F.collect_list(F.struct("Sensor", "Measurement", "Value")).alias("Measurements"))\
        .write.mode("append").saveAsTable(temp_table_name)
    dfstm = spark.readStream.table('bronze.iot_measurements_tmp')
    dfstm.withColumn('body',F.to_json(F.struct(*dfstm.columns)))\
        .select('body')\
        .writeStream.format("eventhubs")\
        .options(**ehConf)\
        .option("checkpointLocation", checkpoint_path)\
        .trigger(availableNow=True).start()
     
    

# COMMAND ----------

# MAGIC %md
# MAGIC
