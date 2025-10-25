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
expectations={"comb_valid_BOROUGH": "BOROUGH IS NOT NULL","comb_valid_zipcode":"ZIP_CODE IS NOT NULL"}
reverse_qa_condition="!("+" AND ".join(expectations.values())+")"

@dp.table(name='silver.vehicle_accidents_qa_stream')
def vehicle_accidents_qa_stream():
    df = spark.readStream.table('vehicle_accidents_stream')
    for clm in df.schema:
        col_name=clm.name
        col_type=silver_schema[col_name].dataType
        df = df.withColumn(col_name,F.col(col_name).cast(col_type))

    df=df.withColumn('ACCIDENT_DATE_TIME', F.to_timestamp(\
        F.concat(F.col('CRASH_DATE'), F.lit(' '), F.lpad(F.col('CRASH_TIME'), 5, '0')), 'MM/dd/yyyy HH:mm'))\
        .drop('LATITUDE','LONGITUDE','CRASH_DATE','CRASH_TIME')\
        .filter(F.col('ACCIDENT_DATE_TIME').isNotNull())\
        .withColumn('is_quaranteed',F.expr(reverse_qa_condition))    
    return df

@dp.table(name='silver.vehicle_accidents_cleansed_stream')
def vehicle_accidents_cleansed_stream():
    return spark.readStream.table('silver.vehicle_accidents_qa_stream')\
        .filter(~F.col('is_quaranteed'))\
        .drop('is_quaranteed')    


@dp.table(name='silver.vehicle_accidents_quarantine')
def vehicle_accidents_quarantine():
    return spark.readStream.table('silver.vehicle_accidents_qa_stream')\
        .withColumn('Violation_condition',
                    F.lit(reverse_qa_condition))\
        .filter(F.col('is_quaranteed'))\
        .drop('is_quaranteed')
