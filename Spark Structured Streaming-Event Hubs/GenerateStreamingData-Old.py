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


def generate_sensor_data(start_date, end_date,eh_name,kafka_options,checkpoint_path,temp_table_name):
    dfs = generate_measurements(start_date, end_date)
    dfagg=dfs.withColumn('EventTime',F.col('EventTime').cast('string'))\
        .groupBy('EventTime','Office')\
        .agg(F.collect_list(F.struct("Sensor", "Measurement", "Value")).alias("Measurements"))\
        .write.mode("append").saveAsTable(temp_table_name)
    dfstm=spark.readStream.table(temp_table_name)\
       .select(F.to_json(F.struct("EventTime","Office","Measurements")).alias("value"))\
       .writeStream.format("kafka")\
       .options(**kafka_options)\
       .option("topic",eh_name)\
       .option("checkpointLocation", checkpoint_path)\
       .trigger(availableNow=True).start()
     
    

# COMMAND ----------

# MAGIC %md
# MAGIC
