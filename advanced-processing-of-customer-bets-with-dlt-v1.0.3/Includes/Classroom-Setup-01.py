# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

DA = DBAcademyHelper(course_config, lesson_config)  # Create the DA object
DA.reset_lesson()                                   # Reset the lesson to a clean state
DA.init()                                           # Performs basic intialization including creating schemas and catalogs
DA.conclude_setup()                                 # Finalizes the state and prints the config for the student

# COMMAND ----------

# Create a UC schema for the student
schema = "techsummit_2024_dlt_bets_lab"
createSchemaSql = """CREATE SCHEMA IF NOT EXISTS {}.{}""".format(DA.catalog_name, schema)
spark.sql(createSchemaSql)

# COMMAND ----------

# Create an external Delta table for the user that the DLT pipeline can write to (required for the new Sink API)
labUser = DA.username
head, sep, tail = labUser.partition('@')
externalTableLocation = "s3://uc-external-locations-us-west-2/external-tables/user/{}/dlt-bets-lab".format(head)

createTableSql = """
CREATE TABLE IF NOT EXISTS {}.{}.bets_by_customer_event_agg (
  customerId BIGINT,
  betCount BIGINT,
  totalBetsAmount BIGINT,
  betEventTimestamp TIMESTAMP,
  betEmitTimestamp TIMESTAMP,
  betProcessTimestamp TIMESTAMP,
  durationInState BIGINT,
  operationType STRING,
  currentWatermarkTimestamp TIMESTAMP,
  isTimeout BOOLEAN)
USING delta
LOCATION '{}/betsByCustomerEventAgg'
  """.format(DA.catalog_name, schema, externalTableLocation)

spark.sql(createTableSql)

# COMMAND ----------


