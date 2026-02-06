from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType,LongType
from pyspark.sql import Window

#Aggregations and time windows 

file_path= spark.conf.get('source_path')
connection_string_ehs = dbutils.secrets.get(scope = "fikrats_study_scope", key = "eh-dbr-source-connstr")


event_hub_namespace="eh-dbr"
source_event_hub_name="dbr-source"


KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{event_hub_namespace}.servicebus.windows.net:9093",
  "subscribe"                : source_event_hub_name,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{connection_string_ehs}\";",
  "kafka.request.timeout.ms" : 1000,
  "kafka.session.timeout.ms" : 6000,
  "maxOffsetsPerTrigger"     : 10,
  "failOnDataLoss"           : False,
  "startingOffsets"          : "latest"
}

@dp.table(name='Stocks')
def stocks():
  df= spark.readStream\
        .format("kafka")\
        .options(**KAFKA_OPTIONS)\
        .load()
  return  df.withColumn('Bronze_Ingestion_Timestamp',F.current_timestamp())
        

json_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", StructType([
        StructField("currency", StringType(), True),
        StructField("value", DoubleType(), True)
    ]), True),
    StructField("volume", LongType(), True),
    StructField("market_Cap", StructType([
        StructField("currency", StringType(), True),
        StructField("value", DoubleType(), True)
    ]), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True)
])

@dp.table(name='silver.stocks_cleansed1')
def stocks_cleansed():
  return spark.readStream.table('Stocks')\
    .withColumn("event_payload", F.col("value").cast("string"))\
    .withColumn("EventPayload", \
         F.from_json("event_payload", json_schema))\
    .select("EventPayload",F.col("timestamp").alias("eventTimestamp"))


@dp.table(name='silver.stocks_cleansed2')
def stocks_cleansed():
  return spark.readStream.table('silver.stocks_cleansed1')\
         .selectExpr("eventTimestamp",\
                     "current_timestamp() as ProcessingTime", 
                     "EventPayload.symbol as symbol",
                     "EventPayload.price.* ",
                     ) 
         
# Recommended grouping (time windows) approach
@dp.table(name='silver.stocks_tw_aggregated')
def stocks_tw_aggregated():
  return spark.readStream.table('silver.stocks_cleansed2')\
        .groupBy(F.window("eventTimestamp", "10 seconds"),
                 "symbol", "currency")\
        .agg(F.avg("value").alias("avg_value"))


#Supported (not recommended) global aggregartions 
@dp.table(name='silver.stocks_aggregated')
def stocks_cleansed_agg():
  return spark.readStream.table('silver.stocks_cleansed2')\
        .groupBy("symbol", "currency")\
        .agg(F.avg("value").alias("avg_value"))

                
#Unsupported global aggregartions: ranking

@dp.table(name='silver.stocks_ranking')
def stocks_ranking():
  window_spec = Window.partitionBy("symbol").orderBy(F.desc("value"))
  return spark.readStream.table('silver.stocks_cleansed2')\
        .withColumn("RowNum",F.row_number().over(window_spec))


# Unsupported global aggregartions:  pivot
        
@dp.table(name='silver.stocks_pivot')
def stocks_pivot():
   return spark.readStream.table('silver.stocks_cleansed2')\
        .groupBy("symbol")\
        .pivot("currency")\
        .agg(F.sum("value").alias("total_value"))
        
        
