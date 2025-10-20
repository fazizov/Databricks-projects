# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG learn_adb_fikrat;
# MAGIC CREATE SCHEMA bronze;
# MAGIC CREATE SCHEMA silver;
# MAGIC CREATE SCHEMA gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS logs;
# MAGIC DROP TABLE IF EXISTS bronze.vehicle_accidents;
# MAGIC DROP TABLE IF EXISTS bronze.vehicle_accidents_cleansed;
# MAGIC DROP TABLE IF EXISTS bronze.Accident_locations

# COMMAND ----------

# DBTITLE 1,Cleaning destination  path
dbutils.fs.rm(dest_file_path,True)

# COMMAND ----------

root_folder='/Volumes/learn_adb_fikrat/bronze/landing/crash-data/'
source_file_path=f'{root_folder}/Motor_Vehicle_Collisions_-_Crashes.csv'
dest_file_path=f'{root_folder}/vehicle_collisions'

# COMMAND ----------

def load_transform_data(source_file_path):
    return spark.read.csv(source_file_path, header=True)            

# COMMAND ----------

def save_one_day(df,collision_date,dest_root_path):
    coll_date=collision_date.replace('/','-')
    dest_file_path=f'{dest_root_path}/{coll_date}'
    date_filter=f"`CRASH DATE`='{collision_date}'"
    df.where(date_filter).write.format('csv')\
        .mode('overwrite').option('header',True).save(dest_file_path)

# COMMAND ----------

dfd=load_transform_data(source_file_path)
display(dfd.groupBy('CRASH DATE').count().orderBy('CRASH DATE'))
# df=load_transform_data(source_file_path,dest_file_path,'01/01/2013')

# COMMAND ----------

save_one_day(dfd,'01/01/2013',dest_file_path)

# COMMAND ----------

save_one_day(dfd,'01/02/2013',dest_file_path)

# COMMAND ----------

save_one_day(dfd,'01/03/2013',dest_file_path)

# COMMAND ----------

save_one_day(dfd,'01/01/2016',dest_file_path)

# COMMAND ----------

save_one_day(dfd,'01/01/2017',dest_file_path)

# COMMAND ----------

save_one_day(dfd,'01/01/2018',dest_file_path)

# COMMAND ----------

print(spark.read.csv(source_file_path, header=True).where("`CRASH DATE`='01/01/2013'").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG learn_adb_fikrat;
# MAGIC SELECT * FROM bronze.vehicle_accidents_batch

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Bronze_Ingestion_Timestamp,count(*) FROM bronze.vehicle_accidents_batch 
# MAGIC group by Bronze_Ingestion_Timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Bronze_Ingestion_Timestamp,count(*) FROM bronze.vehicle_accidents_stream 
# MAGIC group by Bronze_Ingestion_Timestamp order by Bronze_Ingestion_Timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(ACCIDENT_DATE_TIME as date), count(*) FROM silver.vehicle_accidents_cleansed_batch 
# MAGIC group by CAST(ACCIDENT_DATE_TIME as date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logs.dlt_logs_batch

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logs.dlt_logs_stream

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logs.dlt_logs_batch where event_type='flow_update_statistics'

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType,StructField,StructType,IntegerType


update_stats_schema = StructType([
    StructField("flow_update_statistics", StructType([
        StructField("statistics", ArrayType(StructType([
            StructField("write_into_statistics", StructType([
                StructField("write_statistics", StructType([
                    StructField("operator_statistics", ArrayType(StructType([
                        StructField("node_id", IntegerType(), True),
                        StructField("parent_node_id", IntegerType(), True),
                        StructField("operator_type", StringType(), True),
                        StructField("num_rows_out", IntegerType(), True),
                        StructField("num_rows_in", ArrayType(IntegerType()), True),
                        StructField("exclusive_cpu_time_ms", IntegerType(), True),
                        StructField("file_scan_statistics", StructType([
                            StructField("num_files_scanned", IntegerType(), True),
                            StructField("num_bytes_scanned", IntegerType(), True),
                            StructField("num_rows_scanned", IntegerType(), True)
                        ]), True)
                    ])))
                ]))
            ]))
        ])))
    ]))
])
df = spark.table('logs.dlt_logs_batch').where("event_type='flow_update_statistics'") \
    .withColumn("details_json", F.from_json(F.col('details'), update_stats_schema))\
    .select("details_json.*",'details_json.flow_update_statistics[0].statistics.write_into_statistics.write_statistics.operator_statistics.*')  
    
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.vehicle_accidents_cleansed 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM bronze.accidents_qa_mult_condition_drop 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM bronze.accidents_qa_drop

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.accidents_qa_drop

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.accidents_qa_multi_condition_quaranteened

# COMMAND ----------

qa_conditions = {"Valid borough":"BOROUGH IS NOT NULL","Passenger vehicle":"VEHICLE_TYPE_CODE1 ='PASSENGER VEHICLE'"}
qa_quaranteened_conditions = f"NOT({' AND '.join(qa_conditions.values())})"
qa_quaranteened_conditions

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType
import pyspark.sql.functions as F

# COMMAND ----------

input_schema_raw=StructType([StructField('CRASH DATE', StringType(), True), StructField('CRASH TIME', StringType(), True), StructField('BOROUGH', StringType(), True), StructField('ZIP CODE', StringType(), True), StructField('LATITUDE', DoubleType(), True), StructField('LONGITUDE', DoubleType(), True), StructField('LOCATION', StringType(), True), StructField('ON STREET NAME', StringType(), True), StructField('CROSS STREET NAME', StringType(), True), StructField('OFF STREET NAME', StringType(), True), StructField('NUMBER OF PERSONS INJURED', StringType(), True), StructField('NUMBER OF PERSONS KILLED', IntegerType(), True), StructField('NUMBER OF PEDESTRIANS INJURED', IntegerType(), True), StructField('NUMBER OF PEDESTRIANS KILLED', IntegerType(), True), StructField('NUMBER OF CYCLIST INJURED', IntegerType(), True), StructField('NUMBER OF CYCLIST KILLED', StringType(), True), StructField('NUMBER OF MOTORIST INJURED', StringType(), True), StructField('NUMBER OF MOTORIST KILLED', IntegerType(), True), StructField('CONTRIBUTING FACTOR VEHICLE 1', StringType(), True), StructField('CONTRIBUTING FACTOR VEHICLE 2', StringType(), True), StructField('CONTRIBUTING FACTOR VEHICLE 3', StringType(), True), StructField('CONTRIBUTING FACTOR VEHICLE 4', StringType(), True), StructField('CONTRIBUTING FACTOR VEHICLE 5', StringType(), True), StructField('COLLISION ID', IntegerType(), True), StructField('VEHICLE TYPE CODE 1', StringType(), True), StructField('VEHICLE TYPE CODE 2', StringType(), True), StructField('VEHICLE TYPE CODE 3', StringType(), True), StructField('VEHICLE TYPE CODE 4', StringType(), True), StructField('VEHICLE TYPE CODE 5', StringType(), True)])

# COMMAND ----------

dest_file_path2=f'/Volumes/learn_adb_fikrat/bronze/landing/crash-data/vehicle_collisions/01-01-2013/'
display(spark.read.format('csv').schema(input_schema_raw).load(dest_file_path2))
    

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH MATERIALIZED VIEW learn_adb_fikrat.bronze.vehicle_accidents

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists learn_adb_fikrat.bronze.vehicle_accidents_batch

# COMMAND ----------


