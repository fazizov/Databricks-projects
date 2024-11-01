# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## This is the DLT code for the Advanced Processing of Customer Bets using DLT Lab for Tech Summit 2024
# MAGIC ### Logic Description:
# MAGIC #### Two Bronze Streaming Tables - Raw bets and raw customer information
# MAGIC * The sources are customer bets and customer information through autoloader
# MAGIC #### Two Silver Streaming Tables
# MAGIC * The first Silver table is a streaming table that only saves the latest bet per customer with apply_changes
# MAGIC * For the second, applyInPandasWithState is used to increment the bet count and amount and decrement after each bet has been in the system for approximately a minute.  This logic outputs to an external Delta table using the new DLT sinks API
# MAGIC #### One Gold Materialized View
# MAGIC * The bets data is enriched with the customer info using a left-outer join
# MAGIC ### Lab Requirements:
# MAGIC * This lab is creating tables in UC
# MAGIC * The pipeline must use the DLT preview channel in order for applyInPandasWithState and the new DLT sinks API to be supported
# MAGIC * The external table must be created before running this pipeline
# MAGIC * Required DLT pipeline settings:
# MAGIC   * pipelines.externalSink.enabled true
# MAGIC   * pipelines.enableNewFlowTypes true
# MAGIC   * spark.databricks.delta.withEventTimeOrder.enabled true
# MAGIC   * catalog - your unique UC catalog name created for this lab
# MAGIC   * schema - the schema name created for this lab

# COMMAND ----------

# MAGIC %md
# MAGIC ### Do not modify any of these paths or your lab will not work

# COMMAND ----------

# Paths that the autoloader is pointed to
dataRootPath = "/Volumes/dbacademy/techsummit_2024_dlt_bets/datasets"
betsPath = "{}/bets".format(dataRootPath)
customersPath = "{}/customers".format(dataRootPath)

# The name of the bronze streaming tables
rawBetsTableName = "raw_bets"
customerInfoTableName = "customer_info"

# The name of the silver streaming table
betsLatestTableName = "bets_latest"

# The name of the gold materialized view
betsEnrichedMVName = "bets_customer_info_mv"

# The name of the silver table that is defined with the DLT Sinks API, which must be a fully-qualified name because it is an external Delta table
externalTableCatalog = spark.conf.get("catalog")
externalTableSchema = spark.conf.get("schema")
betsByCustomerAgg = "{}.{}.bets_by_customer_event_agg".format(externalTableCatalog, externalTableSchema)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest bets data with Auto Loader and write to a bronze Streaming Table

# COMMAND ----------

import dlt
from dlt import *

# First define the streaming table
create_streaming_table(rawBetsTableName)

# Next define one or more "flows" - the streams of data that will be ingested into the table.  Using flows allows you to have more than one source of data for a given table
# A one-time load or new continuous sources can be added, and the table will not have to be recreated from scratch
# These append flows are for append-only Streaming Tables - they cannot be used for tables created with apply_changes.
@append_flow(target=rawBetsTableName)
def rawBets():
  return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.validateOptions", "true")
    .load(betsPath)
    .selectExpr("customerId", "stake", "cast(creationTimestamp as timestamp) eventTimestamp")
  )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest customer data with Auto Loader and write to a bronze Streaming Table

# COMMAND ----------

# Define the streaming table
create_streaming_table(customerInfoTableName)

# Define the append flow to populate it
@append_flow(target=customerInfoTableName)
def customerInfo():
  return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header","true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.validateOptions", "true")
    .load(customersPath)
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now save only the latest bet per customer using apply_changes and write to a Silver Streaming Table

# COMMAND ----------

# Define the streaming table
create_streaming_table(betsLatestTableName)

# Define a change flow to populate it.  Change flows are only for apply_changes streaming tables
apply_changes(
  flow_name = "betsLatestFlow",
  target = betsLatestTableName,
  source = rawBetsTableName,
  keys = ["customerId"],
  sequence_by = "eventTimestamp",
  stored_as_scd_type = 1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Call applyInPandasWithState to calculate the count of bets and their amounts from the raw bets table and write to an external Delta table using the new Sinks API.  
# MAGIC ### The count and amount will be updated and emitted for every bet that comes in and for each bet that is removed

# COMMAND ----------

# First create an external Delta table sink.  At the moment the sink name cannot have upper-case letters, otherwise the flow will not be found and doesn't execute.
# Currently the sink is shown in the DLT pipeline graph as a view, but that will be fixed.  Look at the list view and you can see the number of records written.
# If the pipeline is re-run, the data will be rewritten to the external Delta table.  The table needs to be truncated separately before a complete refresh of the DLT pipeline if you don't want to see duplicate data.
create_sink("bet_agg_sink", "delta", {"tableName": betsByCustomerAgg})

# Then create an append flow that will calculate and append the results of the aggregation to the sink
@append_flow(name = "stream_from_raw", target = "bet_agg_sink")
def stream_from_raw():
  rawBetsDf = dlt.readStream(rawBetsTableName)

  # Notice the watermark, which is required since we're using an event time-based timeout.  We're allowing incoming data to be 5 seconds late before it is dropped
  # The name of the function that is defined below is passed to the applyInPandasWithState call, along with the DDL schemas for the state and the output, the output mode and the type of timeout
  # The timeout is optional - if you don't need to use them then specify GroupStateTimeout.NoTimeout as the timeout parameter
  applyInPandasWithStateResultDf = (
    rawBetsDf
    .withWatermark("eventTimestamp", "5 seconds")
    .groupBy(rawBetsDf["customerId"])
    .applyInPandasWithState(updateState, outputSchema, stateSchema, "append", GroupStateTimeout.EventTimeTimeout)
  )

  return applyInPandasWithStateResultDf


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Materialized View in the Gold layer that enriches the bets data with customer information

# COMMAND ----------

# DLT will automatically create the correct type of table based on the function that is defined to populate it
@dlt.table(name=betsEnrichedMVName,
                  comment = "Enrich the bets data with customer information for further analysis")
def enrichBets():
  return dlt.read(betsLatestTableName).join(dlt.read(customerInfoTableName), ["customerId"], "left")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Define applyInPandasWithState logic (since DLT does not execute things in the order of the cells this logic can be defined at the bottom of the notebook)
# MAGIC * This is the Python version of arbitrary stateful operations
# MAGIC * This logic will count bets and add bet amounts as they come into the system, and then decrement those counts and amounts after each bet has been in state for approximately 1 minute
# MAGIC * If no clicks have been received for a period of time for a given user, the logic will still emit a count and amount and will remove records from state that are more than 1 minute old
# MAGIC * If the stream has no data coming through at all then nothing will be updated.  Something must be coming through the stream for this logic to be executed
# MAGIC * Check https://www.databricks.com/blog/2022/10/18/python-arbitrary-stateful-processing-structured-streaming.html

# COMMAND ----------

import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
from collections import namedtuple
from typing import Tuple, Iterator, List
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout


# Documented for completeness - the expected structure of the input
#inputSchema = "customerId BIGINT, stake BIGINT, eventTimestamp TIMESTAMP"

# The schema for what will be stored in state - a list of tuples with the bet stake, the event timestamp (when the bet was generated), the emit timestamp (when the bet's aggregation should be calculated and removed) and the process timestamp (when the bet entered the system)
stateSchema = "betCount BIGINT, totalBetsAmount BIGINT, betList ARRAY<STRUCT<`stake`: BIGINT, `eventTimestamp`: TIMESTAMP, `emitTimestamp` TIMESTAMP, `processTimestamp` TIMESTAMP>>"

# The schema for the values being emitted
outputSchema = "customerId BIGINT, betCount BIGINT, totalBetsAmount BIGINT, betEventTimestamp TIMESTAMP, betEmitTimestamp TIMESTAMP, betProcessTimestamp TIMESTAMP, durationInState BIGINT, operationType STRING, currentWatermarkTimestamp TIMESTAMP, isTimeout BOOLEAN"

# The number of minutes a bet stays in the system before its count and amount are decremented from the totals
emitMinutes = 1

# A named tuple for the structure of the state and output so fields can be referred to by name
StateRecord = namedtuple("StateRecord", "betCount totalBetsAmount betList")
BetInputRecord = namedtuple("BetInputRecord", "stake, eventTimestamp")
BetRecord = namedtuple("BetRecord", "stake eventTimestamp emitTimestamp processTimestamp")
AggregationRecord = namedtuple("AggregationRecord", "betCount totalBetsAmount betEventTimestamp betEmitTimestamp betProcessTimestamp durationInState operationType")

# Generate new bet records.  Since processing order is important with event time this function sorts the list before returning
def generateBetRecords(bets: List[int], betTimestamps: List[datetime]):
  betList: List[BetRecord] = []
  currentTimestamp = datetime.now()
  numRecords = len(bets)
  index = 0
  while index < numRecords:
    stake = bets[index]
    eventTimestamp = betTimestamps[index]
    emitTimestamp = eventTimestamp + timedelta(minutes=emitMinutes)
    newBet = BetRecord(stake, eventTimestamp, emitTimestamp, currentTimestamp)
    betList.append(newBet)
    index+=1

  betList.sort(key=lambda a: a[1])  
  return betList

# Create the output Pandas dataframe from the aggregation record list
def generatePandasDataframe(aggregationRecordList: List[AggregationRecord], customerId: int, isTimeout: bool, currentWatermarkTimestamp: datetime):
  # Since the key (the customer), the current watermark timestamp and the timeout boolean are the same value for all rows, generate a list of that value the same length as the number of output records
  numRows = len(aggregationRecordList)
  keyList = [customerId] * numRows
  timeoutList = [isTimeout] * numRows
  currentWatermarkTimestampList = [currentWatermarkTimestamp] * numRows

  # Go through each row of tuples and change into a set of lists, one per column, which is the format needed to create a Pandas dataframe
  betCountList = []
  totalBetsAmountList = []
  betEventTimestampList = []
  betEmitTimestampList = []
  betProcessTimestampList = []
  durationInStateTimestampList = []
  operationTypeList = []
  currentTimestamp = datetime.now()
  for outputRecord in aggregationRecordList:
    betCountList.append(outputRecord.betCount)
    totalBetsAmountList.append(outputRecord.totalBetsAmount)
    betEventTimestampList.append(outputRecord.betEventTimestamp)
    betEmitTimestampList.append(outputRecord.betEmitTimestamp)
    betProcessTimestamp = outputRecord.betProcessTimestamp
    betProcessTimestampList.append(betProcessTimestamp)
    durationInStateTimestampList.append((currentTimestamp - betProcessTimestamp).total_seconds())
    operationTypeList.append(outputRecord.operationType)

  # Return a Pandas dataframe
  return pd.DataFrame({"customerId": keyList, "betCount": betCountList, "totalBetsAmount": totalBetsAmountList, "betEventTimestamp": betEventTimestampList, "betEmitTimestamp": betEmitTimestampList, "betProcessTimestamp": betProcessTimestampList, "durationInState": durationInStateTimestampList, "operationType": operationTypeList, "currentWatermarkTimestamp": currentWatermarkTimestampList, "isTimeout": timeoutList})

# This is the function that is called with applyInPandasWithState.
# This function will be called in two ways -
#   If one or more bets for a given customer are received
#   If no bets are received for a given customer within a timeout duration since the last time this function was called
def updateState (
  key: Tuple[int],  # This is the key we are grouping on.  It can be a tuple of one or more fields
  values: Iterator[pd.DataFrame],  # These are the records coming into the function
  state: GroupState # The state we're storing in-between microbatches
) -> Iterator[pd.DataFrame]:   # The records we're outputting

  # If we haven't timed out then there are values for this key
  if not state.hasTimedOut:
    isTimeout = False

    # Get the state if it exists.  Initialize the bet count and total bet amount to 0 and the bet list with an empty list if this is the first time we've seen this key
    betCount = 0
    totalBetsAmount = 0
    prevBetList: List[StateRecord] = []
    if state.exists:
      (betCount, totalBetsAmount, prevBetList) = state.get

    # Get the key value
    # Since the key in this case is a tuple with just one value, in Python you need the hanging comma to get the value instead of the object
    (customerId,) = key

    # Get the current watermark timestamp in milliseconds and convert it to a datetime
    currentWatermarkTimestamp = dt.datetime.fromtimestamp(state.getCurrentWatermarkMs()/1000)

    # Go through the state list if it exists.  For each record, if the emit timestamp is less than or equal to the currentWatermarkTimestamp, get that bet record, remove it from the state list, and create an aggregation record for it
    # Initialize a list for the aggregation records
    aggregationRecordList: List[AggregationRecord] = []

    # Make a copy of the list of bets in state to iterate through.
    if len(prevBetList) > 0:
      betsInState = prevBetList.copy()
      for bet in betsInState:
        # If this bet's emit is less than or equal to the current watermark timestamp, remove it from state, decrement the bet count and total bets amount, and create its aggregate record
        if bet.emitTimestamp <= currentWatermarkTimestamp:
          currentTimestamp = datetime.now()
          prevBetList.remove(bet)
          betCount = betCount - 1
          totalBetsAmount = totalBetsAmount - bet.stake
          durationInState = (currentTimestamp - bet.processTimestamp).total_seconds()
          aggregateRecord = AggregationRecord(betCount, totalBetsAmount, bet.eventTimestamp, bet.emitTimestamp, bet.processTimestamp, durationInState, "subtract")
          aggregationRecordList.append(aggregateRecord)

    # Get all the input values from the iterator for this key. Each call to the iterator will generate a list of one or more records
    newBets: List[int] = []
    newBetTimestamps: List[datetime] = []
    for row in values:
      newBets = newBets + row['stake'].tolist()
      newBetTimestamps = newBetTimestamps + row['eventTimestamp'].tolist()

    # Now that all of the bets that have passed their emit timestamp have been removed from state, for each new bet increment the bet count and total bets amount, then generate an output record
    newBetList: List[BetRecord] = generateBetRecords(newBets, newBetTimestamps)
    for bet in newBetList:
      currentTimestamp = datetime.now()
      betCount = betCount + 1
      totalBetsAmount = totalBetsAmount + bet.stake
      durationInState = (currentTimestamp - bet.processTimestamp).total_seconds()
      aggregateRecord = AggregationRecord(betCount, totalBetsAmount, bet.eventTimestamp, bet.emitTimestamp, bet.processTimestamp, durationInState, "add")
      aggregationRecordList.append(aggregateRecord)

    # Next, add the new bets to the state, along with the new bet count and total bets amount
    newBetList = prevBetList + newBetList
    newState = StateRecord(betCount, totalBetsAmount, newBetList)

    # Update the state.  Since the state is a tuple with multiple fields, there is no need to use the ((newState,)) syntax
    state.update(newState)

    # Get the minimum event timestamp value for the bets that are still in state (the bets that haven't expired yet) converted to millis, and
    # the year from the watermark value.  Need this only for the very first microbatch in the stream
    minEventTimestamp = int(min(newBetList)[1].timestamp() * 1000)
    watermarkYear = currentWatermarkTimestamp.year

    # Reset the timeout. When no data has been seen for a period of time for a given key, this timeout will trigger the else clause below
    # Since we're using event time, we have to pass a timestamp in milliseconds that the watermark has to advance beyond
    # We set it up to fire a timeout when the watermark advances half as much as the emit interval minutes to decrement counts and 
    # bet amounts and remove records from state
    # On the very first microbatch, the watermark value is going to be in the year 1970.  In that case, we don't want to fire a timeout based on
    # that timestamp.  Instead we'll use the minimum event timestamp from the bet list
    if (watermarkYear > 1970):
      timeoutMs = currentWatermarkTimestamp + (emitMinutes*60*1000)/2
      state.setTimeoutTimestamp(timeoutMs)
    else:
      timeoutMs = minEventTimestamp + (emitMinutes*60*1000)/2
      state.setTimeoutTimestamp(timeoutMs)

    # Generate the output.  The list of aggregation records we have has to be converted to a Pandas Dataframe
    pandasDf = generatePandasDataframe(aggregationRecordList, customerId, isTimeout, currentWatermarkTimestamp)

    # Return an iterator of the Pandas dataframe
    yield pandasDf


  # This section of code will only execute if no data has been received for a given key for the time period specified by the timeout
  # We set the timeout to fire for the time when the next bet in state was supposed to be emitted
  else:
    # Since a timeout fired we know there is state and that no new data has arrived for this key
    isTimeout = True
    (betCount, totalBetsAmount, prevBetList) = state.get
    (customerId,) = key

    # Get the current watermark timestamp in milliseconds and convert it to a datetime
    currentWatermarkTimestamp = dt.datetime.fromtimestamp(state.getCurrentWatermarkMs()/1000)

    # Go through the state list.  For each record, if the emit timestamp is less than the currentWatermarkTimestamp, get that bet record, remove it from the state list, and create an aggregation record for it
    # Initialize a list for the aggregation records
    aggregationRecordList: List[AggregationRecord] = []

    # Make a copy of the list of bets in state to iterate through.
    betsInState = prevBetList.copy()
    for bet in betsInState:
      # If this bet's emit is less than or equal to the current watermark timestamp, remove it from state, decrement the bet count and total bets amount, and create its aggregate record
      if bet.emitTimestamp <= currentWatermarkTimestamp:
        currentTimestamp = datetime.now()
        prevBetList.remove(bet)
        betCount = betCount - 1
        totalBetsAmount = totalBetsAmount - bet.stake
        durationInState = (currentTimestamp - bet.processTimestamp).total_seconds()
        aggregateRecord = AggregationRecord(betCount, totalBetsAmount, bet.eventTimestamp, bet.emitTimestamp, bet.processTimestamp, durationInState, "subtract")
        aggregationRecordList.append(aggregateRecord)

    # Is the betCount now 0?  If so remove the entire state and do not set a timeout.
    # Note the parentheses are required in the state.remove() call, otherwise the state is not removed and the existing timeout will fire over and over
    if betCount == 0:
      state.remove()
    
    # Else if there are still bets left then update the state and set a timeout
    else:
      newState = StateRecord(betCount, totalBetsAmount, prevBetList)
      state.update(newState)

      # Reset the timeout. When no data has been seen for a period of time for a given key, this timeout will trigger the else clause below
      # Since we're using event time, we have to pass a timestamp in milliseconds that the watermark has to advance beyond
      # We set it up to fire a timeout when the watermark advances half as much as the emit interval minutes to decrement counts and 
      # bet amounts and remove records from state
      timeoutMs = state.getCurrentWatermarkMs() + (emitMinutes*60*1000)/2
      state.setTimeoutTimestamp(timeoutMs)
    
    # Generate the output.  The list of aggregation records we have has to be converted to a Pandas dataframe
    pandasDf = generatePandasDataframe(aggregationRecordList, customerId, isTimeout, currentWatermarkTimestamp)

    # Return an iterator of the Pandas dataframe
    yield pandasDf


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
