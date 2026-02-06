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

expectations={"valid_BOROUGH": "BOROUGH IS NOT NULL",
              "valid_LOCATION": "LOCATION IS NOT NULL",
              "valid_zipcode":"ZIP_CODE IS NOT NULL"}
@dp.table(name='silver.accident_locations')
@dp.expect_all_or_drop(expectations)
def accident_locations():
    return spark.readStream\
        .table('silver.vehicle_accidents_cleansed_stream')\
        .select('ZIP_CODE','LOCATION','BOROUGH').distinct()

@dp.table(name='silver.boroughs')
@dp.expect_or_drop('empty_borough',"BOROUGH IS NOT NULL")
def accident_boroughs():
    return spark.readStream\
        .table('silver.vehicle_accidents_cleansed_stream')\
        .filter(F.col('BOROUGH') !='BROOKLYN')\
        .select('BOROUGH').distinct()

#PK/duplicate validations (Validating ZIP_CODE as a PK)
@dp.materialized_view(name='silver.accident_locations_QA')
@dp.expect_or_fail("No duplicate items","count=1")
def accident_locations_QA():
    return spark.table('silver.accident_locations')\
        .groupBy('ZIP_CODE')\
        .agg(F.count(F.col('ZIP_CODE'))\
        .alias('count'))\
        .withColumn('QA_Rule',F.lit('PK validations'))

#FK validations (Validating BOROUGH match)        
@dp.materialized_view(name='silver.boroughs_QA')
@dp.expect_or_fail("No FK violations","FK_BOROUGH IS NOT NULL")
def accident_locations_QA():
    df1=spark.table('silver.accident_locations').alias('left')
    df2=spark.table('silver.boroughs').alias('right')
    return df1.join(df2,df1.BOROUGH==df2.BOROUGH,'left')\
        .select('left.*',F.col('right.BOROUGH').alias('FK_BOROUGH'))\
        .withColumn('QA_Rule',F.lit('FK validations'))


