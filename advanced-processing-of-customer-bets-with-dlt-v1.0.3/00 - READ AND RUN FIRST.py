# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Welcome to the Advanced Processing of Customer Bets with DLT Lab for Tech Summit 2024! 
# MAGIC Are you a fan of "Run All"?  If so, go ahead and click it for this notebook once you've got a single-node personal cluster spun up for yourself.  Then say goodbye to it - you won't learn if you try it on the other notebooks, and oh yeah - the notebooks won't work correctly if you do.  Let the fun begin!
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC 1. Set up a DLT pipeline with the UI
# MAGIC 1. Learn how to use Append Flows, Change Flows, Materialized Views, and Arbitrary Stateful Operations in DLT with UC using Python
# MAGIC 1. Learn how to use the new Sinks API to write data to an external Delta table in DLT using Python
# MAGIC 1. Learn how to get a Change Data Feed (CDF) from a Streaming Table created with apply_changes
# MAGIC 1. [Optional] Learn how to use applyInPandasWithState

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Classroom Setup - If you didn't click "Run all" earlier, run this cell now

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DLT Pipeline Setup
# MAGIC ### You need to set up a DLT pipeline for this lab.  I know it's fun to get creative, but don't do it here - please follow the instructions exactly so that you get the full benefits of the lab!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run this cell - you will need these variables for the DLT pipeline setup

# COMMAND ----------

pipelineName = DA.username + " DLT Pipeline"
print(f"DLT Pipeline Name: {pipelineName}")
print(f"UC Catalog Name:   {DA.catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now follow the instructions on the right-side panel of this lab to set up your pipeline.  You can navigate away from this notebook and the panel on the right will remain in view

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Have your cluster and DLT pipeline set up?  Great!  Now open the notebook you chose to include in your DLT pipeline: either 01 - Process Bets with a DLT pipeline or 01A - Process Bets with a DLT pipeline Advanced.  Enjoy!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
