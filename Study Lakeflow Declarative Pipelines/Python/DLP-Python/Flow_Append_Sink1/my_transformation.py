from pyspark import pipelines as dp
from utilities.schemas import bronze_schema,silver_schema
import pyspark.sql.functions as F

#Streaming to Delta Lake tables

dp.create_streaming_table(name='vehicle_accidents_stream')
@dp.append_flow (name='vehicle_accidents_flw1',target='vehicle_accidents_stream')
def vehicle_accidents_flw1():
  file_path='/Volumes/learn_adb_fikrat/bronze/landing/crash-data/vehicle_collisions/01-02-2013/'
  return spark.readStream.format('cloudFiles')\
     .option("cloudFiles.format", "csv")\
     .option('header','true')\
     .schema(bronze_schema)\
     .option("recursiveFileLookup", "true")\
     .load(file_path)\
     .withColumn('Bronze_Ingestion_Timestamp',F.current_timestamp())

dp.create_sink(name='vehicle_accidents_delta', format='delta',
                options={'tableName':'learn_adb_fikrat.bronze.vehicle_accidents_delta'})
@dp.append_flow (name='vehicle_accidents_flw3',target='vehicle_accidents_delta')
def vehicle_accidents_flw3():
  return dp.read_stream('vehicle_accidents_stream')
