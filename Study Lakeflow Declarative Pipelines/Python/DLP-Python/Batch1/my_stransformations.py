from pyspark import pipelines as dp
from utilities.schemas import bronze_schema,silver_schema
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType

file_path= spark.conf.get('source_path')

claims_file_path='/Volumes/learn_adb_fikrat/bronze/landing/crash-data/insurance_claims/'

claims_schema=StructType([StructField('CollisionID', StringType(), True), StructField('ClaimAmount', StringType(), True), StructField('ClaimDateTime', StringType(), True), StructField('Bronze_Ingestion_Timestamp', TimestampType(), False)])

#Streaming table -Accidents
@dp.table(name='vehicle_accidents_stream')
def bronze_vehicle_crashes():
  return spark.readStream.format('cloudFiles')\
     .option("cloudFiles.format", "csv")\
     .option('header','true')\
     .schema(bronze_schema)\
     .option("recursiveFileLookup", "true")\
     .load(file_path)\
     .withColumn('Bronze_Ingestion_Timestamp',F.current_timestamp())

#Streaming table -Claims
@dp.table(name='insurance_claims_stream')
def insurance_claims_stream():
  return spark.readStream.format('cloudFiles')\
     .option("cloudFiles.format", "csv")\
     .option('header','true')\
     .option("recursiveFileLookup", "true")\
     .schema(claims_schema)\
     .load(claims_file_path)\
     .withColumn('ClaimDateTime',F.col("ClaimDateTime").cast(TimestampType()))


# Silver transformations: Data cleansing and schema standarization
@dp.materialized_view(name='silver.vehicle_accidents_cleansed_mv')
def vehicle_accidents_cleansed_mv():
    df=spark.read.table('vehicle_accidents_stream')
    for clm in df.schema:
        col_name=clm.name
        col_type=silver_schema[col_name].dataType
        df = df.withColumn(col_name,F.col(col_name).cast(col_type))
    df=df.withColumn('ACCIDENT_DATE_TIME', \
        F.to_timestamp(F.concat(F.col('CRASH_DATE'),\
             F.lit(' '), F.lpad(F.col('CRASH_TIME'), 5, '0')),\
                  'MM/dd/yyyy HH:mm'))\
        .drop('LATITUDE','LONGITUDE','CRASH_DATE','CRASH_TIME')\
        .filter(F.col('ACCIDENT_DATE_TIME').isNotNull())    

    return df

#Batch joins 
@dp.table(name='silver.vehicle_accidents_claims_mv')
def vehicle_accidents_claims_mv():
    df1 = spark.read.table('silver.vehicle_accidents_cleansed_mv')\
        .alias('df1')
    df2 = spark.read.table('insurance_claims_stream').alias('df2')\
   
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


# #Aggregations
@dp.materialized_view(name='gold.vehicle_accidents_aggregated_mv')
def vehicle_accidents_aggregated_mv():
    return spark.read.table('silver.vehicle_accidents_cleansed_mv')\
        .groupby('BOROUGH').agg(F.sum('NUMBER_OF_PERSONS_INJURED')\
        .alias('Total_Persons_Injured')) 

 

    
