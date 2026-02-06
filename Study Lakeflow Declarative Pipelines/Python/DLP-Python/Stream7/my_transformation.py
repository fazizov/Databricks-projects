from pyspark import pipelines as dp
from utilities.schemas import bronze_schema,silver_schema,bronze_schema_sql
import pyspark.sql.functions as F

# Trigger interval options

file_path= spark.conf.get('source_path')

@dp.table(name='vehicle_accidents_stream_event_base')

def vehicle_accidents_stream_event_base():
  return spark.readStream.format('cloudFiles')\
     .option("cloudFiles.format", "csv")\
     .option('header','true')\
     .schema(bronze_schema)\
     .option("recursiveFileLookup", "true")\
     .load(file_path)\
     .withColumn('Bronze_Ingestion_Timestamp',F.current_timestamp())

#Using schema parameter
@dp.table(name='vehicle_accidents_stream_with_interval',
          comment='Bronze quality',
          spark_conf={"pipelines.trigger.interval" : "15 seconds"})
def vehicle_accidents_stream_with_interval():
  return spark.readStream.format('cloudFiles')\
     .option("cloudFiles.format", "csv")\
     .option('header','true')\
     .option("recursiveFileLookup", "true")\
     .schema(bronze_schema)\
     .load(file_path)\
     .withColumn('Bronze_Ingestion_Timestamp',F.current_timestamp())



