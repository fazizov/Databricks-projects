from pyspark import pipelines as dp
from utilities.schemas import bronze_schema,silver_schema
import pyspark.sql.functions as F

#Delta Lake tables sinks
accidents_file_path= spark.conf.get('source_path')

uc_name= spark.conf.get('uc_name')

dp.create_sink(name='vehicle_accidents_delta', format='delta',
                options={'tableName':f'{uc_name}.bronze.vehicle_accidents_delta'})

@dp.append_flow (name='vehicle_accidents_flw3',target='vehicle_accidents_delta')
def vehicle_accidents_flw3():
  return spark.readStream.format('cloudFiles')\
     .option("cloudFiles.format", "csv")\
     .option('header','true')\
     .schema(bronze_schema)\
     .option("recursiveFileLookup", "true")\
     .load(accidents_file_path)\
     .withColumn('Bronze_Ingestion_Timestamp',F.current_timestamp())

#Kafka (Event Hub) sinks

connection_string_ehs = dbutils.secrets.get(scope = "fikrats_study_scope", key = "eh-dbr-target-connstr")

event_hub_namespace="eh-dbr"
target_event_hub_name="dbr-target"


KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{event_hub_namespace}.servicebus.windows.net:9093",
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{connection_string_ehs}\";",
  "topic"                    : target_event_hub_name
}

dp.create_sink(name='accidents_kafka', format='kafka', options=KAFKA_OPTIONS)
@dp.append_flow (name='accidents_kafka_flw', target='accidents_kafka')
def stocks_kafka_flw():
  df=spark.readStream.table('silver.vehicle_accidents_cleansed_stream')\
      .withWatermark("ACCIDENT_DATE_TIME", "10 minutes")
  df1=df.groupBy(F.window('ACCIDENT_DATE_TIME','1 hours'),"BOROUGH")\
      .agg(F.sum("NUMBER_OF_PERSONS_INJURED")\
      .alias("TOTAL_NUMBER_OF_PERSONS_INJURED"))\
      .select(F.to_json(F.struct("window","BOROUGH","TOTAL_NUMBER_OF_PERSONS_INJURED")).alias("value"))\
      .selectExpr("CAST(value AS STRING)")   
  return df1    

# # For Each Batch sink
@dp.foreach_batch_sink(name="accidents_feb_aggregations")
def accidents_feb_aggregations(df, batch_id):
  df.groupBy(F.window('ACCIDENT_DATE_TIME','1 hours'),
               "VEHICLE_TYPE_CODE_1","BOROUGH")\
    .agg(F.sum("NUMBER_OF_PERSONS_INJURED")\
    .alias("TOTAL_NUMBER_OF_PERSONS_INJURED"))\
    .withColumn("batch_id", F.lit(batch_id))\
    .write.format("delta").mode("append")\
    .saveAsTable(f"{uc_name}.silver.Accidents_Hourly_Aggregations" )

  df.groupBy(F.window('ACCIDENT_DATE_TIME','1 hours'),"VEHICLE_TYPE_CODE_1")\
    .pivot("BOROUGH")\
    .agg(F.sum("NUMBER_OF_PERSONS_INJURED")\
    .alias("TOTAL_NUMBER_OF_PERSONS_INJURED"))\
    .withColumn("batch_id", F.lit(batch_id))\
    .write.format("delta").mode("append")\
    .saveAsTable(f"{uc_name}.silver.Accidents_Hourly_Aggregations_pivot" )
  return

@dp.append_flow(name='accidents_feb_aggregations_af',target="accidents_feb_aggregations")
def accidents_feb_aggregations_af():
  return spark.readStream.table('silver.vehicle_accidents_cleansed_stream')\
    .filter(F.col('BOROUGH').isNotNull())\
    .withColumn('BOROUGH',F.regexp_replace(F.col('BOROUGH'), ' ', '_'))           
 

