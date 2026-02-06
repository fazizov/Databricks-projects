from pyspark import pipelines as dp
from utilities.schemas import bronze_schema,silver_schema
import pyspark.sql.functions as F

file_path= spark.conf.get('source_path')

@dp.table(name='vehicle_accidents_stream')
def bronze_vehicle_crashes():
  return spark.readStream.format('cloudFiles')\
     .option("cloudFiles.format", "csv")\
     .option('header','true')\
     .schema(bronze_schema)\
     .option("recursiveFileLookup", "true")\
     .load(file_path)\
     .withColumn('Bronze_Ingestion_Timestamp',F.current_timestamp())


#Combined expectations
expectations={"comb_valid_BOROUGH": "BOROUGH IS NOT NULL","comb_valid_zipcode":"ZIP_CODE IS NOT NULL"}
# Silver transformations: Data cleansing and schema standarization with QA control
#First uncomment fail expectation
@dp.table(name='silver.vehicle_accidents_cleansed_stream')
# @dp.expect_or_fail('valid_BOROUGH', "BOROUGH IS NOT NULL")
# @dp.expect('valid_zipcode', 'ZIP_CODE IS NOT NULL')
@dp.expect_or_drop('valid_VEHICLE_TYPE_CODE_1', "VEHICLE_TYPE_CODE_1 ='TAXI'")    
# @dp.expect_all_or_drop(expectations) 
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
@dp.on_event_hook(max_allowable_consecutive_failures=3)   
def event_hook(event):
    if event['event_type'] == 'flow_progress':
        print(f"Hook metrics: {event['details']['flow_progress']['metrics']}")

