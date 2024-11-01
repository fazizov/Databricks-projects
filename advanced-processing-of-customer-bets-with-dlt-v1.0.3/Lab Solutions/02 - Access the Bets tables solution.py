# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## This is the Structured Streaming and SQL code for the Advanced Processing of Customer Bets using DLT Lab for Tech Summit 2024

# COMMAND ----------

# MAGIC %md
# MAGIC ### Required initialization

# COMMAND ----------

# MAGIC %run ../Includes/_common

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Run this cell to get the table names for your External Delta table, your Silver Streaming table and your Materialized View that were populated in your DLT pipeline

# COMMAND ----------

DA = DBAcademyHelper(course_config, lesson_config)
catalog = DA.catalog_name
schema = "techsummit_2024_dlt_bets_lab"

# The apply_changes Streaming Table containing the latest bet from each customer
betsLatest = "{}.{}.bets_latest".format(catalog, schema)

# The external Delta table that was populated from the DLT pipeline showing the bet count and amount over time for each customer
betsByCustomerAgg = "{}.{}.bets_by_customer_event_agg".format(catalog, schema)

# The materialized view with the enriched customer bets
betsEnriched = "{}.{}.bets_customer_info_mv".format(catalog, schema)

print(betsLatest)
print(betsByCustomerAgg)
print(betsEnriched)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now you can read the Change Data Feed from Streaming Tables!  This includes Streaming Tables that are created with apply_changes

# COMMAND ----------

# Since this is a small amount of data everything was processed in one microbatch.  
# Notice how DLT sorted all of the incoming bets data and just inserted the latest for each customer, minimizing the amount of data that actually had to be written!
changeFeedDf = spark.readStream.option("readChangeFeed", "true").table(betsLatest)
changeFeedDf.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### You can see the trend of bet count and amount by customer.  Notice anything interesting in the data?
# MAGIC ### Some of the records are still in state and have not been emitted because no new data is arriving so timeouts aren't firing.  Stay tuned for the new TransformWithState API, which has actual timers!

# COMMAND ----------

# Fun fact: the watermark for the very first microbatch in a stateful stream is set to January, 1970.  If you use the watermark value in your logic make sure to take that into account!
betTrendDf = spark.sql("select * from {} order by customerId, betEventTimestamp, betEmitTimestamp".format(betsByCustomerAgg))
display(betTrendDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query the materialized view to see the latest bets per customer enriched with customer data

# COMMAND ----------

betEnrichedDf = spark.sql("select * from {}".format(betsEnriched))
display(betEnrichedDf)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
