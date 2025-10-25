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


# Silver transformations: Data cleansing and schema standarization with QA control

#Combined expectations
expectations={"comb_valid_BOROUGH": "BOROUGH IS NOT NULL","comb_valid_zipcode":"ZIP_CODE IS NOT NULL"}

@dp.table(name='silver.vehicle_accidents_cleansed_stream')
@dp.expect_all_or_drop(expectations)    

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

@dp.table(name='silver.accident_locations')
def accident_locations():
    return spark.readStream.table('silver.vehicle_accidents_cleansed_stream')\
        .select('ZIP_CODE','BOROUGH').distinct()
        
#PK/duplicate validations
@dp.materialized_view(name='silver.accident_locations_QA')
@dp.expect_or_drop("Duplicate items","count>1")
def accident_locations_QA():
    return spark.table('silver.accident_locations')\
        .groupBy('ZIP_CODE').agg(F.count(F.col('BOROUGH'))\
        .alias('count'))\
        .withColumn('QA_Rule',F.lit('PK validations'))
        
