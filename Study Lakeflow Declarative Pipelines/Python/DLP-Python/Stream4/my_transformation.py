from pyspark import pipelines as dp
from utilities.schemas import bronze_schema,silver_schema
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType


accidents_file_path= spark.conf.get('source_path')
claims_file_path='/Volumes/learn_adb_fikrat/bronze/landing/crash-data/insurance_claims/'

claims_schema=StructType([StructField('CollisionID', StringType(), True), StructField('ClaimAmount', StringType(), True), StructField('ClaimDateTime', StringType(), True), StructField('Bronze_Ingestion_Timestamp', TimestampType(), False)])


#Streaming table: accidents

@dp.table(name='vehicle_accidents_stream')
def bronze_vehicle_crashes():
  return spark.readStream.format('cloudFiles')\
     .option("cloudFiles.format", "csv")\
     .option('header','true')\
     .schema(bronze_schema)\
     .option("recursiveFileLookup", "true")\
     .load(accidents_file_path)\
     .withColumn('Bronze_Ingestion_Timestamp',F.current_timestamp())\
     .withColumn('ACCIDENT_DATE_TIME', F.to_timestamp(\
        F.concat(F.col('CRASH_DATE'), F.lit(' '), F.lpad(F.col('CRASH_TIME'), 5, '0')), 'MM/dd/yyyy HH:mm'))\
     .filter(F.col('BOROUGH').isNotNull())


# Stream to batch joins
@dp.table(name='silver.vehicle_accidents_claims_stream')
def vehicle_accidents_claims_stream():
   df1 = spark.readStream\
    .table('vehicle_accidents_stream').alias('df1')
   df2 = spark.read.format("csv")\
     .option('header','true')\
     .option("recursiveFileLookup", "true")\
     .schema(claims_schema)\
     .load(claims_file_path)\
     .alias('df2')
   return df1.join(df2,df1.COLLISION_ID==df2.CollisionID) \
     .select (F.col('df1.collision_id').alias('df1_collision_id'),\
      F.col('df1.ACCIDENT_DATE_TIME'), \
      F.col('df1.BOROUGH'),\
      F.col('df1.NUMBER_OF_PERSONS_INJURED'),\
      F.col('df2.ClaimAmount'))
  
