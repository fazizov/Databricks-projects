from pyspark import pipelines as dp
from utilities.schemas import bronze_schema,silver_schema
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType

#Stream to stream joins

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


#Streaming table: claims

@dp.table(name='insurance_claims_stream')
def insurance_claims_stream():
  return spark.readStream.format('cloudFiles')\
     .option("cloudFiles.format", "csv")\
     .option('header','true')\
     .option("recursiveFileLookup", "true")\
     .schema(claims_schema)\
     .load(claims_file_path)\
     .withColumn('ClaimDateTime',F.col("ClaimDateTime").cast(TimestampType()))


# Stream to Stream joins
@dp.table(name='silver.vehicle_accidents_claims_stream2')
def vehicle_accidents_claims_stream2():
    df1 = spark.readStream\
    .table('vehicle_accidents_stream').alias('df1')\
    .withWatermark('ACCIDENT_DATE_TIME', '10 minutes')
    df2 = spark.readStream\
    .table('insurance_claims_stream').alias('df2')\
    .withWatermark('ClaimDateTime', '10 minutes')
    dfjoin=df1.join(df2,(df1.COLLISION_ID==df2.CollisionID) \
     & (df2.ClaimDateTime >= df1.ACCIDENT_DATE_TIME) &
     (df2.ClaimDateTime <= df1.ACCIDENT_DATE_TIME \
      + F.expr('INTERVAL 10 MINUTES')),'leftOuter')\
     .select (F.col('df1.collision_id').alias('df1_collision_id'),\
      F.col('df1.ACCIDENT_DATE_TIME'), \
      F.col('df1.BOROUGH'),\
      F.col('df1.NUMBER_OF_PERSONS_INJURED'),\
      F.col('df2.ClaimAmount'))
    return dfjoin

