from pyspark import pipelines as dp
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType
import pyspark.sql.functions as F

# file_path='/Volumes/learn_adb_fikrat/bronze/landing/crash-data/vehicle_collisions/'

file_path= spark.conf.get('source_path')

input_schema_raw=StructType([StructField('CRASH_DATE', StringType(), True), StructField('CRASH_TIME', StringType(), True), StructField('BOROUGH', StringType(), True), StructField('ZIP_CODE', StringType(), True), StructField('LATITUDE', DoubleType(), True), StructField('LONGITUDE', DoubleType(), True), StructField('LOCATION', StringType(), True), StructField('ON_STREET_NAME', StringType(), True), StructField('CROSS_STREET_NAME', StringType(), True), StructField('OFF_STREET_NAME', StringType(), True), StructField('NUMBER_OF_PERSONS_INJURED', StringType(), True), StructField('NUMBER_OF_PERSONS_KILLED', IntegerType(), True), StructField('NUMBER_OF_PEDESTRIANS_INJURED', IntegerType(), True), StructField('NUMBER_OF_PEDESTRIANS_KILLED', IntegerType(), True), StructField('NUMBER_OF_CYCLIST_INJURED', IntegerType(), True), StructField('NUMBER_OF_CYCLIST_KILLED', StringType(), True), StructField('NUMBER_OF_MOTORIST_INJURED', StringType(), True), StructField('NUMBER_OF_MOTORIST_KILLED', IntegerType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_1', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_2', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_3', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_4', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_5', StringType(), True), StructField('COLLISION_ID', IntegerType(), True), StructField('VEHICLE_TYPE_CODE_1', StringType(), True), StructField('VEHICLE_TYPE_CODE_2', StringType(), True), StructField('VEHICLE_TYPE_CODE_3', StringType(), True), StructField('VEHICLE_TYPE_CODE_4', StringType(), True), StructField('VEHICLE_TYPE_CODE_5', StringType(), True)])


@dp.table(name='vehicle_accidents_stream',
           table_properties={'quality': 'bronze', 'delta.columnMapping.mode': 'name',
   'delta.minReaderVersion' : '3',   'delta.minWriterVersion' : '7'})

def bronze_vehicle_crashes():
  return spark.readStream.format('cloudFiles')\
     .option("cloudFiles.format", "csv")\
     .option('header','true')\
     .schema(input_schema_raw)\
     .option("recursiveFileLookup", "true")\
     .load(file_path)\
     .withColumn('Bronze_Ingestion_Timestamp',F.current_timestamp())

# Data type conversions
input_schema_silver=StructType([StructField('CRASH_DATE', StringType(), True), StructField('CRASH_TIME', StringType(), True), StructField('BOROUGH', StringType(), True), StructField('ZIP_CODE', StringType(), True), StructField('LATITUDE', DoubleType(), True), StructField('LONGITUDE', DoubleType(), True), StructField('LOCATION', StringType(), True), StructField('ON_STREET_NAME', StringType(), True), StructField('CROSS_STREET_NAME', StringType(), True), StructField('OFF_STREET_NAME', StringType(), True), StructField('NUMBER_OF_PERSONS_INJURED', StringType(), True), StructField('NUMBER_OF_PERSONS_KILLED', IntegerType(), True), StructField('NUMBER_OF_PEDESTRIANS_INJURED', IntegerType(), True), StructField('NUMBER_OF_PEDESTRIANS_KILLED', IntegerType(), True), StructField('NUMBER_OF_CYCLIST_INJURED', IntegerType(), True), StructField('NUMBER_OF_CYCLIST_KILLED', StringType(), True), StructField('NUMBER_OF_MOTORIST_INJURED', StringType(), True), StructField('NUMBER_OF_MOTORIST_KILLED', IntegerType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_1', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_2', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_3', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_4', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_5', StringType(), True), StructField('COLLISION_ID', IntegerType(), True), StructField('VEHICLE_TYPE_CODE_1', StringType(), True), StructField('VEHICLE_TYPE_CODE_2', StringType(), True), StructField('VEHICLE_TYPE_CODE_3', StringType(), True), StructField('VEHICLE_TYPE_CODE_4', StringType(), True), StructField('VEHICLE_TYPE_CODE_5', StringType(), True),
 StructField('Bronze_Ingestion_Timestamp', TimestampType(), True)])
 
@dp.table(name='silver.vehicle_accidents_cleansed_stream', table_properties={'schema': 'silver'})
def vehicle_accidents_cleansed_stream():
    df = spark.readStream.table('vehicle_accidents_stream')
    df=df.withColumn('ACCIDENT_DATE_TIME', F.to_timestamp(\
        F.concat(F.col('CRASH_DATE'), F.lit(' '), F.lpad(F.col('CRASH_TIME'), 5, '0')), 'MM/dd/yyyy HH:mm'))\
        .drop('LATITUDE','LONGITUDE','CRASH_DATE','CRASH_TIME')\
        .filter(F.col('ACCIDENT_DATE_TIME').isNotNull())    
    
    return df
