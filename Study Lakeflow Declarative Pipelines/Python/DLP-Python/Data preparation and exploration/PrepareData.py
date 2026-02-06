# Databricks notebook source
import pyspark.sql.functions as F
# import random

# COMMAND ----------

dbutils.widgets.text("unity_catalog","learn_adb_fikrat")
uc_name=dbutils.widgets.get("unity_catalog")
print(uc_name)

# COMMAND ----------

# DBTITLE 1,Setting catalog context
spark.sql(f'USE CATALOG {uc_name}');

# COMMAND ----------

# DBTITLE 1,Create required  schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_name}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_name}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_name}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_name}.ldp_logs")

# COMMAND ----------

# DBTITLE 1,Creating volume for  raw data
# MAGIC %sql
# MAGIC CREATE VOLUME  IF NOT EXISTS bronze.landing

# COMMAND ----------

# DBTITLE 1,Setting path  variables
root_folder=f'/Volumes/{uc_name}/bronze/landing/crash-data/'

accidents_source_file_path=f'{root_folder}/Motor_Vehicle_Collisions_-_Crashes.csv'
accidents_dest_file_path=f'{root_folder}/vehicle_collisions'

claims_source_file_path=f'{root_folder}/Insurance_claims_all.csv'
claims_dest_file_path=f'{root_folder}/insurance_claims'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resetting environemnt

# COMMAND ----------

# DBTITLE 1,Cleansing target tables
# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS bronze.vehicle_accidents_stream;
# MAGIC -- DROP TABLE IF EXISTS bronze.vehicle_accidents_cleansed;
# MAGIC -- DROP TABLE IF EXISTS bronze.Accident_locations;
# MAGIC -- DROP TABLE IF EXISTS silver.vehicle_accidents_cleansed_stream

# COMMAND ----------

# DBTITLE 1,Cleaning destination  path
# dbutils.fs.rm(accidents_dest_file_path,True)
# dbutils.fs.rm(claims_dest_file_path,True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating sample data 

# COMMAND ----------

# DBTITLE 1,Data extraction utility functions
def load_transform_data(source_file_path):
    return spark.read.csv(source_file_path, header=True) 

def save_accidents_one_day(collision_date,source_file_path,dest_root_path):
    coll_date=collision_date.replace('/','-')
    dest_file_path=f'{dest_root_path}/{coll_date}'
    df=load_transform_data(source_file_path)
    df=df.where(f"`CRASH DATE`='{collision_date}'")
    df.write.format('csv')\
        .mode('overwrite').option('header',True).save(dest_file_path)               
    print (f"Generated {df.count()} collision records")    

def save_claims_one_day(collision_date,source_file_path,dest_root_path):
    coll_date=collision_date.replace('/','-')
    dest_file_path=f'{dest_root_path}/{coll_date}'
    df=load_transform_data(source_file_path)
    df.filter(F.col("ClaimDateTime").cast('date')==F.to_date(F.lit(collision_date),\
        'dd/MM/yyyy')).write.format('csv').mode('overwrite').option('header',True).save(dest_file_path)                                   

# COMMAND ----------

# DBTITLE 1,Browsing data sample
dfd=load_transform_data(accidents_source_file_path)
display(dfd)

# COMMAND ----------

# MAGIC %md
# MAGIC Generating **accidents** csv files for one month data

# COMMAND ----------

save_accidents_one_day('01/01/2013',accidents_source_file_path,accidents_dest_file_path)

# COMMAND ----------

save_accidents_one_day('01/02/2013',accidents_source_file_path,accidents_dest_file_path)

# COMMAND ----------

save_accidents_one_day('01/03/2013',accidents_source_file_path,accidents_dest_file_path)

# COMMAND ----------

save_accidents_one_day('01/04/2013',accidents_source_file_path,accidents_dest_file_path)

# COMMAND ----------

save_accidents_one_day('01/06/2013',accidents_source_file_path,accidents_dest_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Generating **claims** csv files for one month 

# COMMAND ----------

save_claims_one_day('01/01/2013',claims_source_file_path,claims_dest_file_path)

# COMMAND ----------

save_claims_one_day('01/02/2013',claims_source_file_path,claims_dest_file_path)

# COMMAND ----------

save_claims_one_day('01/03/2013',claims_source_file_path,claims_dest_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Inspecting data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploring Auto CDC

# COMMAND ----------

# MAGIC %md
# MAGIC Generating CDF data

# COMMAND ----------

spark.sql(f'USE CATALOG {uc_name}');

# COMMAND ----------

# DBTITLE 1,Creating Sales order table with CDF enabled
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze.sales_orders;
# MAGIC DROP TABLE IF EXISTS bronze.sales_orders_cdf;
# MAGIC CREATE TABLE IF NOT EXISTS bronze.sales_orders (
# MAGIC   id STRING,
# MAGIC   order_id STRING,
# MAGIC   order_date TIMESTAMP,
# MAGIC   order_status STRING)
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming CDF to Delta Lake table

# COMMAND ----------

# DBTITLE 1,Spark Structured Streaming CDF to Delta Lake table
import pyspark.sql.functions as F
spark.readStream.option('readChangeFeed', 'true')\
     .table('bronze.sales_orders')\
     .filter(F.col('_change_type') != 'update_preimage')\
     .writeStream.outputMode('append')\
     .option('checkpointLocation', '/tmp/checkpoint4')\
     .table('bronze.sales_orders_cdf')
            

# COMMAND ----------

# MAGIC %md
# MAGIC Simulating table changes

# COMMAND ----------

# DBTITLE 1,Insert commands
# MAGIC %sql
# MAGIC INSERT INTO bronze.sales_orders (id, order_id, order_date, order_status) VALUES
# MAGIC   ('1', 'SO1001', '2023-10-01 09:15:00', 'PENDING'),
# MAGIC   ('2', 'SO1002', '2023-10-02 10:30:00', 'COMPLETED'),
# MAGIC   ('3', 'SO1003', '2023-10-03 11:45:00', 'CANCELLED'),
# MAGIC   ('4', 'SO1004', '2023-10-04 12:00:00', 'PENDING'),
# MAGIC   ('5', 'SO1005', '2023-10-05 13:20:00', 'COMPLETED');

# COMMAND ----------

# DBTITLE 1,Update and delete commands
# MAGIC %sql
# MAGIC update bronze.sales_orders set order_status='COMPLETED' where id='1';
# MAGIC delete from  bronze.sales_orders where id='2'

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronze.sales_orders set order_status='COMPLETED' where id='4';
# MAGIC

# COMMAND ----------

# DBTITLE 1,Browsing data in CDF table
# MAGIC %sql
# MAGIC select * from bronze.sales_orders_cdf

# COMMAND ----------

# MAGIC %md
# MAGIC Creating  snaphshot history 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze.sales_orders2;
# MAGIC DROP TABLE IF EXISTS bronze.sales_orders_history;
# MAGIC  
# MAGIC CREATE TABLE IF NOT EXISTS bronze.sales_orders2 (
# MAGIC   id STRING,
# MAGIC   order_id STRING,
# MAGIC   order_date TIMESTAMP,
# MAGIC   order_status STRING)  ;

# COMMAND ----------

# MAGIC %md
# MAGIC Simulating source data transactions

# COMMAND ----------

# DBTITLE 1,Insert command
# MAGIC %sql
# MAGIC INSERT INTO bronze.sales_orders2 (id, order_id, order_date, order_status) VALUES
# MAGIC   ('1', 'SO1001', '2023-10-01 09:15:00', 'PENDING'),
# MAGIC   ('2', 'SO1002', '2023-10-02 10:30:00', 'COMPLETED'),
# MAGIC   ('3', 'SO1003', '2023-10-03 11:45:00', 'CANCELLED'),
# MAGIC   ('4', 'SO1004', '2023-10-04 12:00:00', 'PENDING'),
# MAGIC   ('5', 'SO1005', '2023-10-05 13:20:00', 'COMPLETED');

# COMMAND ----------

# DBTITLE 1,Collecting first snapshot
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.sales_orders_history AS
# MAGIC SELECT *,1 as ingestion_version FROM bronze.sales_orders2
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronze.sales_orders2 set order_status='Pending' where id='3';
# MAGIC

# COMMAND ----------

# DBTITLE 1,Second snapshot
# MAGIC %sql
# MAGIC INSERT INTO bronze.sales_orders_history 
# MAGIC SELECT *,2 as ingestion_version FROM bronze.sales_orders2
# MAGIC     
# MAGIC

# COMMAND ----------

# DBTITLE 1,Browsing snapshots table
# MAGIC %sql
# MAGIC select * from bronze.sales_orders_history 

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronze.sales_orders set order_status='Pending' where id='5';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.sales_orders_history 
# MAGIC SELECT *,4 as ingestion_version FROM bronze.sales_orders
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

# MAGIC %md
# MAGIC ### Querying LDP event logs

# COMMAND ----------

spark.sql(f'USE CATALOG {uc_name}');

# COMMAND ----------

# DBTITLE 1,Querying default logs
# MAGIC %sql
# MAGIC select * from event_log('cfe0c903-662b-46a5-a6c2-bace3ef906fc')

# COMMAND ----------

# DBTITLE 1,Querying LDP event logs
# MAGIC %sql
# MAGIC select * from ldp_logs.events

# COMMAND ----------

# DBTITLE 1,Querying table update and DQ stats
# MAGIC %sql
# MAGIC  WITH Logs_CTE AS (
# MAGIC  SELECT
# MAGIC     origin.pipeline_name         AS pipeline_name,
# MAGIC     origin.pipeline_id           AS pipeline_id,
# MAGIC     origin.flow_name             AS table_name,
# MAGIC     origin.update_id             AS update_id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.status AS status,
# MAGIC     TRY_CAST(details:flow_progress.metrics.num_output_rows AS BIGINT)      AS num_output_rows,
# MAGIC     TRY_CAST(details:flow_progress.metrics.num_upserted_rows AS BIGINT)    AS num_upserted_rows,
# MAGIC     TRY_CAST(details:flow_progress.metrics.num_deleted_rows AS BIGINT)     AS num_deleted_rows,
# MAGIC     TRY_CAST(details:flow_progress.metrics.backlog_records AS BIGINT)     AS total_backlog_records_rows,
# MAGIC     TRY_CAST(details:flow_progress.metrics.backlog_seconds AS BIGINT)     AS total_backlog_seconds,
# MAGIC     TRY_CAST(details:flow_progress.metrics.source_metrics.source_name AS BIGINT)     AS source_name,
# MAGIC     TRY_CAST(details:flow_progress.metrics.source_metrics.backlog_seconds AS BIGINT)     AS source_backlog_seconds,
# MAGIC     TRY_CAST(details:flow_progress.data_quality.dropped_records AS BIGINT) AS num_expectation_dropped_rows,
# MAGIC     TRY_CAST(details:flow_progress.data_quality.warned_records AS BIGINT) AS num_expectation_warned_records,
# MAGIC     FROM_JSON(details:flow_progress.data_quality.expectations,
# MAGIC       SCHEMA_OF_JSON('[{"name":"valid_VEHICLE_TYPE_CODE_1","dataset":"silver.vehicle_accidents_cleansed_stream","passed_records":97,"failed_records":1712}]')
# MAGIC     ) AS expectations_array
# MAGIC
# MAGIC   FROM ldp_logs.events
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC     AND origin.flow_name IS NOT NULL
# MAGIC     AND origin.flow_name != 'pipelines.flowTimeMetrics.missingFlowName'
# MAGIC  )
# MAGIC Select * from Logs_CTE WHERE status='RUNNING' AND num_expectation_dropped_rows > 0 
# MAGIC OR num_upserted_rows > 0 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Materilialized view Incremental refresh

# COMMAND ----------

# DBTITLE 1,Debugging Materialized view incremental status
# MAGIC %sql
# MAGIC
# MAGIC WITH latest_update AS (
# MAGIC   SELECT
# MAGIC     origin.pipeline_id,
# MAGIC     origin.update_id AS latest_update_id
# MAGIC   FROM ldp_logs.events AS origin
# MAGIC   WHERE origin.event_type = 'create_update'
# MAGIC   ORDER BY timestamp DESC
# MAGIC   -- LIMIT 1 -- remove if you want to get all of the update_ids
# MAGIC ),
# MAGIC parsed_planning AS (
# MAGIC   SELECT
# MAGIC     origin.pipeline_name,
# MAGIC     origin.pipeline_id,
# MAGIC     origin.flow_name,
# MAGIC     lu.latest_update_id,
# MAGIC     from_json(
# MAGIC       details:planning_information,
# MAGIC       'struct<
# MAGIC         technique_information: array<struct<
# MAGIC           maintenance_type: string,
# MAGIC           is_chosen: boolean,
# MAGIC           is_applicable: boolean,
# MAGIC           cost: double,
# MAGIC           incrementalization_issues: array<struct<
# MAGIC             issue_type: string,
# MAGIC             prevent_incrementalization: boolean,
# MAGIC             operator_name: string,
# MAGIC             plan_not_incrementalizable_sub_type: string,
# MAGIC             expression_name: string,
# MAGIC             plan_not_deterministic_sub_type: string
# MAGIC           >>
# MAGIC         >>
# MAGIC       >'
# MAGIC     ) AS parsed
# MAGIC   FROM ldp_logs.events AS origin
# MAGIC   JOIN latest_update lu
# MAGIC     ON origin.update_id = lu.latest_update_id
# MAGIC   WHERE details:planning_information IS NOT NULL
# MAGIC ),
# MAGIC chosen_technique AS (
# MAGIC   SELECT
# MAGIC     pipeline_name,
# MAGIC     pipeline_id,
# MAGIC     flow_name,
# MAGIC     latest_update_id,
# MAGIC     FILTER(parsed.technique_information, t -> t.is_chosen = true)[0] AS chosen_technique,
# MAGIC     parsed.technique_information AS planning_information
# MAGIC   FROM parsed_planning
# MAGIC )
# MAGIC SELECT
# MAGIC   pipeline_name,
# MAGIC   pipeline_id,
# MAGIC   flow_name,
# MAGIC   latest_update_id,
# MAGIC   chosen_technique.maintenance_type,
# MAGIC   chosen_technique,
# MAGIC   planning_information
# MAGIC FROM chosen_technique
# MAGIC ORDER BY latest_update_id DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspecting LDP output data

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG learn_adb_fikrat;
# MAGIC USE bronze;
# MAGIC CREATE TABLE event_hooks (event_details STRING)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from event_hooks

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.Accidents_Hourly_Aggregations

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.Accidents_Hourly_Aggregations_pivot

# COMMAND ----------


