# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG learn_adb_fikrat;
# MAGIC
# MAGIC CREATE SCHEMA  IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA  IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA  IF NOT EXISTS gold;
# MAGIC CREATE SCHEMA  IF NOT EXISTS qa_logs;
# MAGIC DROP TABLE IF EXISTS bronze.vehicle_accidents;
# MAGIC DROP TABLE IF EXISTS bronze.vehicle_accidents_cleansed;
# MAGIC DROP TABLE IF EXISTS bronze.Accident_locations

# COMMAND ----------

# DBTITLE 1,Cleaning destination  path
dbutils.fs.rm(dest_file_path,True)

# COMMAND ----------

root_folder='/Volumes/learn_adb_fikrat/bronze/landing/crash-data/'
source_file_path=f'{root_folder}/Motor_Vehicle_Collisions_-_Crashes.csv'
dest_file_path=f'{root_folder}/vehicle_collisions'

# COMMAND ----------

def load_transform_data(source_file_path):
    return spark.read.csv(source_file_path, header=True)            

# COMMAND ----------

def save_one_day(df,collision_date,dest_root_path):
    coll_date=collision_date.replace('/','-')
    dest_file_path=f'{dest_root_path}/{coll_date}'
    date_filter=f"`CRASH DATE`='{collision_date}'"
    df.where(date_filter).write.format('csv')\
        .mode('overwrite').option('header',True).save(dest_file_path)

# COMMAND ----------

dfd=load_transform_data(source_file_path)
display(dfd.groupBy('CRASH DATE').count().orderBy('CRASH DATE'))
# df=load_transform_data(source_file_path,dest_file_path,'01/01/2013')

# COMMAND ----------

save_one_day(dfd,'01/01/2013',dest_file_path)

# COMMAND ----------

save_one_day(dfd,'01/02/2013',dest_file_path)

# COMMAND ----------

save_one_day(dfd,'01/03/2013',dest_file_path)

# COMMAND ----------

save_one_day(dfd,'01/01/2016',dest_file_path)

# COMMAND ----------

save_one_day(dfd,'01/01/2017',dest_file_path)

# COMMAND ----------

save_one_day(dfd,'01/01/2018',dest_file_path)

# COMMAND ----------

print(spark.read.csv(source_file_path, header=True).where("`CRASH DATE`='01/01/2013'").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying DLP event logs

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG learn_adb_fikrat;
# MAGIC -- SELECT * FROM qa_logs.qa_events;
# MAGIC -- SELECT *, parse_json(details) as details 
# MAGIC --  FROM qa_logs.qa_events WHERE event_type='flow_progress' and level='INFO' 
# MAGIC WITH flow_progress_raw AS (
# MAGIC   SELECT
# MAGIC     origin.pipeline_name         AS pipeline_name,
# MAGIC     origin.pipeline_id           AS pipeline_id,
# MAGIC     origin.flow_name             AS table_name,
# MAGIC     origin.update_id             AS update_id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.status AS status,
# MAGIC     TRY_CAST(details:flow_progress.metrics.num_output_rows AS BIGINT)      AS num_output_rows,
# MAGIC     TRY_CAST(details:flow_progress.metrics.num_upserted_rows AS BIGINT)    AS num_upserted_rows,
# MAGIC     TRY_CAST(details:flow_progress.metrics.num_deleted_rows AS BIGINT)     AS num_deleted_rows,
# MAGIC     TRY_CAST(details:flow_progress.data_quality.dropped_records AS BIGINT) AS num_expectation_dropped_rows,
# MAGIC     FROM_JSON(
# MAGIC       details:flow_progress.data_quality.expectations,
# MAGIC       SCHEMA_OF_JSON("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]")
# MAGIC     ) AS expectations_array
# MAGIC
# MAGIC   FROM qa_logs.qa_events
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC     AND origin.flow_name IS NOT NULL
# MAGIC     AND origin.flow_name != 'pipelines.flowTimeMetrics.missingFlowName'
# MAGIC ),
# MAGIC
# MAGIC aggregated_flows AS (
# MAGIC   SELECT
# MAGIC     pipeline_name,
# MAGIC     pipeline_id,
# MAGIC     update_id,
# MAGIC     table_name,
# MAGIC     MIN(CASE WHEN status IN ('STARTING', 'RUNNING', 'COMPLETED') THEN timestamp END) AS start_timestamp,
# MAGIC     MAX(CASE WHEN status IN ('STARTING', 'RUNNING', 'COMPLETED') THEN timestamp END) AS end_timestamp,
# MAGIC     MAX_BY(status, timestamp) FILTER (
# MAGIC       WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXCLUDED', 'SKIPPED', 'STOPPED', 'IDLE')
# MAGIC     ) AS final_status,
# MAGIC     SUM(COALESCE(num_output_rows, 0))              AS total_output_records,
# MAGIC     SUM(COALESCE(num_upserted_rows, 0))            AS total_upserted_records,
# MAGIC     SUM(COALESCE(num_deleted_rows, 0))             AS total_deleted_records,
# MAGIC     MAX(COALESCE(num_expectation_dropped_rows, 0)) AS total_expectation_dropped_records,
# MAGIC     MAX(expectations_array)                        AS total_expectations
# MAGIC
# MAGIC   FROM flow_progress_raw
# MAGIC   GROUP BY pipeline_name, pipeline_id, update_id, table_name
# MAGIC )
# MAGIC SELECT
# MAGIC   af.pipeline_name,
# MAGIC   af.pipeline_id,
# MAGIC   af.update_id,
# MAGIC   af.table_name,
# MAGIC   af.start_timestamp,
# MAGIC   af.end_timestamp,
# MAGIC   af.final_status,
# MAGIC   CASE
# MAGIC     WHEN af.start_timestamp IS NOT NULL AND af.end_timestamp IS NOT NULL THEN
# MAGIC       ROUND(TIMESTAMPDIFF(MILLISECOND, af.start_timestamp, af.end_timestamp) / 1000)
# MAGIC     ELSE NULL
# MAGIC   END AS duration_seconds,
# MAGIC
# MAGIC   af.total_output_records,
# MAGIC   af.total_upserted_records,
# MAGIC   af.total_deleted_records,
# MAGIC   af.total_expectation_dropped_records,
# MAGIC   af.total_expectations
# MAGIC FROM aggregated_flows af
# MAGIC -- Optional: filter to latest update only
# MAGIC WHERE af.update_id = (
# MAGIC   SELECT update_id
# MAGIC   FROM aggregated_flows
# MAGIC   ORDER BY end_timestamp DESC
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC ORDER BY af.end_timestamp DESC, af.pipeline_name, af.pipeline_id, af.update_id, af.table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploring Auto CDC

# COMMAND ----------

# MAGIC %md
# MAGIC Generating CDF data

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG learn_adb_fikrat;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table bronze.sales_orders_cdf

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze.sales_orders;
# MAGIC CREATE TABLE IF NOT EXISTS bronze.sales_orders (
# MAGIC   id STRING,
# MAGIC   order_id STRING,
# MAGIC   order_date TIMESTAMP,
# MAGIC   order_status STRING)
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC INSERT INTO bronze.sales_orders (id, order_id, order_date, order_status) VALUES
# MAGIC   ('1', 'SO1001', '2023-10-01 09:15:00', 'PENDING'),
# MAGIC   ('2', 'SO1002', '2023-10-02 10:30:00', 'COMPLETED'),
# MAGIC   ('3', 'SO1003', '2023-10-03 11:45:00', 'CANCELLED'),
# MAGIC   ('4', 'SO1004', '2023-10-04 12:00:00', 'PENDING'),
# MAGIC   ('5', 'SO1005', '2023-10-05 13:20:00', 'COMPLETED');
# MAGIC  
# MAGIC  

# COMMAND ----------

display(spark.read.option('readChangeFeed', 'true')\
    .option('startingVersion', 0)\
    .table('bronze.sales_orders'))

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming CDF to Delta Lake table

# COMMAND ----------

import pyspark.sql.functions as F
spark.readStream.option('readChangeFeed', 'true')\
     .table('bronze.sales_orders')\
     .filter(F.col('_change_type') != 'update_preimage')\
     .writeStream.outputMode('append')\
     .option('checkpointLocation', '/tmp/checkpoint')\
     .table('bronze.sales_orders_cdf')
            

# COMMAND ----------

# MAGIC %md
# MAGIC Simulating table changes

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronze.sales_orders set order_status='COMPLETED' where id='1';
# MAGIC delete from  bronze.sales_orders where id='2'

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronze.sales_orders set order_status='CANCELLED' where id='1';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Creating  snaphshot history 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.sales_orders_history AS
# MAGIC SELECT *,1 as ingestion_version FROM bronze.sales_orders
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.sales_orders_history 
# MAGIC SELECT *,4 as ingestion_version FROM bronze.sales_orders
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.sales_orders_history 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from silver.sales_orders_snapshot_scd2    
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Bronze_Ingestion_Timestamp,count(*) FROM bronze.vehicle_accidents_batch 
# MAGIC group by Bronze_Ingestion_Timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Bronze_Ingestion_Timestamp,count(*) FROM bronze.vehicle_accidents_stream 
# MAGIC group by Bronze_Ingestion_Timestamp order by Bronze_Ingestion_Timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(ACCIDENT_DATE_TIME as date), count(*) FROM silver.vehicle_accidents_cleansed_batch 
# MAGIC group by CAST(ACCIDENT_DATE_TIME as date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logs.dlt_logs_batch

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logs.dlt_logs_stream

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logs.dlt_logs_batch where event_type='flow_update_statistics'

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType,StructField,StructType,IntegerType


update_stats_schema = StructType([
    StructField("flow_update_statistics", StructType([
        StructField("statistics", ArrayType(StructType([
            StructField("write_into_statistics", StructType([
                StructField("write_statistics", StructType([
                    StructField("operator_statistics", ArrayType(StructType([
                        StructField("node_id", IntegerType(), True),
                        StructField("parent_node_id", IntegerType(), True),
                        StructField("operator_type", StringType(), True),
                        StructField("num_rows_out", IntegerType(), True),
                        StructField("num_rows_in", ArrayType(IntegerType()), True),
                        StructField("exclusive_cpu_time_ms", IntegerType(), True),
                        StructField("file_scan_statistics", StructType([
                            StructField("num_files_scanned", IntegerType(), True),
                            StructField("num_bytes_scanned", IntegerType(), True),
                            StructField("num_rows_scanned", IntegerType(), True)
                        ]), True)
                    ])))
                ]))
            ]))
        ])))
    ]))
])
df = spark.table('logs.dlt_logs_batch').where("event_type='flow_update_statistics'") \
    .withColumn("details_json", F.from_json(F.col('details'), update_stats_schema))\
    .select("details_json.*",'details_json.flow_update_statistics[0].statistics.write_into_statistics.write_statistics.operator_statistics.*')  
    
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.vehicle_accidents_cleansed 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM bronze.accidents_qa_mult_condition_drop 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM bronze.accidents_qa_drop

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.accidents_qa_drop

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.accidents_qa_multi_condition_quaranteened

# COMMAND ----------

qa_conditions = {"Valid borough":"BOROUGH IS NOT NULL","Passenger vehicle":"VEHICLE_TYPE_CODE1 ='PASSENGER VEHICLE'"}
qa_quaranteened_conditions = f"NOT({' AND '.join(qa_conditions.values())})"
qa_quaranteened_conditions

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType
import pyspark.sql.functions as F

# COMMAND ----------

dest_file_path2=f'/Volumes/learn_adb_fikrat/bronze/landing/crash-data/vehicle_collisions/01-01-2013/'
display(spark.read.format('csv').schema(input_schema_raw).load(dest_file_path2))
    

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH MATERIALIZED VIEW learn_adb_fikrat.bronze.vehicle_accidents

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists learn_adb_fikrat.bronze.vehicle_accidents_batch

# COMMAND ----------


