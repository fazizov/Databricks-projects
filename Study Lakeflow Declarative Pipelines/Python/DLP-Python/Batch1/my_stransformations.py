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
    
# Gold Aggregations
@dp.materialized_view(name='gold.vehicle_accidents_aggregated_mv')
def vehicle_accidents_aggregated_mv():
    return spark.read.table('silver.vehicle_accidents_cleansed_mv')\
        .groupby('BOROUGH').agg(F.sum('NUMBER_OF_PERSONS_INJURED')\
        .alias('Total_Persons_Injured')) 
    
