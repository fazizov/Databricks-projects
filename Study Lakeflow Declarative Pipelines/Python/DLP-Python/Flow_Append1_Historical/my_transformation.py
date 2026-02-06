from pyspark import pipelines as dp
from utilities.schemas import bronze_schema,silver_schema
import pyspark.sql.functions as F

#Using backfill option to load historical data

dp.create_streaming_table(name='vehicle_accidents_stream')
@dp.append_flow (name='vehicle_accidents_historical',target='vehicle_accidents_stream',
                 once=True)
def vehicle_accidents_historical():
  file_path='/Volumes/learn_adb_fikrat/bronze/landing/crash-data/vehicle_collisions/'
  return spark.read.format('csv')\
     .option('header','true')\
     .schema(bronze_schema)\
     .option("recursiveFileLookup", "true")\
     .load(file_path)\
     .withColumn('Bronze_Ingestion_Timestamp',F.current_timestamp())

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


@dp.append_flow (name='vehicle_accidents_flw2',target='vehicle_accidents_stream')
def vehicle_accidents_flw2():
  file_path='/Volumes/learn_adb_fikrat/bronze/landing/crash-data/vehicle_collisions/01-03-2013/'

  return spark.readStream.format('cloudFiles')\
     .option("cloudFiles.format", "csv")\
     .option('header','true')\
     .schema(bronze_schema)\
     .option("recursiveFileLookup", "true")\
     .load(file_path)\
     .withColumn('Bronze_Ingestion_Timestamp',F.current_timestamp())

# Silver transformations: Data type conversions,enrichment and schema standarization 
@dp.table(name='silver.vehicle_accidents_cleansed_stream')
def vehicle_accidents_cleansed_stream():
    df = spark.readStream.table('vehicle_accidents_stream')
    for clm in df.schema:
        col_name=clm.name
        col_type=silver_schema[col_name].dataType
        df = df.withColumn(col_name,F.col(col_name).cast(col_type))

    df=df.withColumn('ACCIDENT_DATE_TIME', F.to_timestamp(\
        F.concat(F.col('CRASH_DATE'), F.lit(' '), F.lpad(F.col('CRASH_TIME'), 5, '0')), 'MM/dd/yyyy HH:mm'))\
        .drop('LATITUDE','LONGITUDE','CRASH_DATE','CRASH_TIME')\
        .filter(F.col('ACCIDENT_DATE_TIME').isNotNull())    
    
    return df


