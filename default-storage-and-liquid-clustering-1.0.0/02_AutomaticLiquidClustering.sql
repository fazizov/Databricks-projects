-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 02 - Automatic Liquid Clustering Key Selection
-- MAGIC
-- MAGIC In this session, we will explore the **Automatic Liquid Clustering Key Selection (CLUSTER BY AUTO)** and **Predictive Optimization (PO)** functionality on Delta Lake.
-- MAGIC
-- MAGIC When `CLUSTER BY AUTO` is enabled in liquid clustering, predictive optimization intelligently selects liquid clustering keys for new or existing tables, enhancing query performance by analyzing historical query workloads to identify optimal candidate columns. Clustering keys are updated only when the anticipated cost savings from data skipping exceed the clustering costs. PO persistently monitors data and query patterns for these tables.
-- MAGIC
-- MAGIC #### Lab Objectives
-- MAGIC - Understand the implementation of the `CLUSTER BY AUTO` feature within liquid clustering.
-- MAGIC - View the predictive optimization history of a table using the predictive optimization system table.
-- MAGIC
-- MAGIC #### Lab Prerequisites
-- MAGIC - You have knowledge of partitioning and Z-order.
-- MAGIC - You have a basic understanding of liquid clustering.
-- MAGIC
-- MAGIC #### Lab Summary
-- MAGIC - **Partitioning** divides data into smaller, manageable chunks based on specified columns, improving query performance and efficiency by enabling parallel processing.
-- MAGIC - **Z-ordering** optimizes data storage by colocating related information based on specified columns, significantly speeding up query performance on those columns by minimizing data scanning.
-- MAGIC - **Liquid clustering** combines properties of both partitioning and Z-ordering. It automatically optimizes Delta tables for faster queries by reorganizing data dynamically without manual intervention.
-- MAGIC   - Refer to the [Use liquid clustering for Delta tables documentation](https://docs.databricks.com/delta/clustering.html) for more information.
-- MAGIC    - **NOTE:** You can change the clustered columns easily, which is not possible in partitioning without rewriting all the data.
-- MAGIC
-- MAGIC #### Lab Scenario
-- MAGIC As a data engineer, you want to increase table performance for your consumers while minimizing time spent optimizing a table. Implementing `CLUSTER BY AUTO` will automatically select the optimal clustering columns through predictive optimization.
-- MAGIC
-- MAGIC #### Lab Notes
-- MAGIC - The training lab we use is a shared workspace and does not include the latest private preview features. As a result, this demo primarily consists of screenshots and code snippets to provide the necessary knowledge for applying these techniques with customers.
-- MAGIC - We will start with the private preview feature of liquid clustering by auto.
-- MAGIC - For an end-to-end comparison of partitioning, Z-order, and liquid clustering with column selection, please refer to the next lab.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Exploring the airline_cluster_auto Table
-- MAGIC
-- MAGIC We have created a table named **airline_cluster_auto**. Initially, this table is neither partitioned nor clustered on any column.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Table Properties - Before
-- MAGIC
-- MAGIC ![table_details_before](files/images/default-storage-and-liquid-clustering-1.0.0/table_details_before.png)
-- MAGIC
-- MAGIC **The table `airline_cluster_auto` is displaying only the Delta table properties as shown in the snippet above.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Enabling Auto Clustering Key Selection on a New Table
-- MAGIC
-- MAGIC - You can enable automatic liquid clustering key selection on a new table with the `CLUSTER BY AUTO` keyword.
-- MAGIC <br></br>
-- MAGIC     ```sql
-- MAGIC     ALTER TABLE airline_cluster_auto 
-- MAGIC     CLUSTER BY AUTO
-- MAGIC     ```
-- MAGIC <br></br>
-- MAGIC   - This command applies **Automatic Liquid Clustering Key Selection** to the table.
-- MAGIC   - Once set, you won’t see immediate key changes. 
-- MAGIC   - As you query the table, PO will choose and set the clustering columns based on your table access patterns to maximize your data skipping across all queries. 
-- MAGIC   - PO will only change your clustering keys when the predicted cost savings from data skipping outweigh the predicted cost of clustering. 
-- MAGIC   - PO will continue to monitor the data and query pattern for these liquid tables. If your query or data 
-- MAGIC pattern changes, you should see the clustering columns change over time. 
-- MAGIC
-- MAGIC #### Automatic Liquid Clustering Key Selection Prerequisites 
-- MAGIC - The table is a UC-managed table. 
-- MAGIC - DBR 15.3 is required to CREATE a new AUTO table or ALTER existing Liquid / unpartitioned tables. 
-- MAGIC   - Please note that the clustering key selection heuristics rely on workload data gathered from DBR 15.3 and later versions. For instance, if the table is created in DBR 15.3 but predominantly accessed in DBR 14.1, our algorithm will not consider the workload from DBR 14.1 when determining clustering keys.
-- MAGIC - PO is enabled for the account, the catalog, the schema, and the table.
-- MAGIC - The table is in a PO-supported region.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Table Properties and History
-- MAGIC
-- MAGIC For demonstration purposes, we executed multiple queries across different columns in the table with various filter conditions to simulate querying patterns. After analyzing these queries, predictive optimization intelligently selected the liquid clustering columns based on the historical query data.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Table Properties - After
-- MAGIC
-- MAGIC ![table_details_after](files/images/default-storage-and-liquid-clustering-1.0.0/table_details_after.png)
-- MAGIC
-- MAGIC **The table `airline_cluster_auto` now includes clustering details in its properties.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Table History
-- MAGIC
-- MAGIC To fetch historical details of **airline_cluster_auto**, we use a temp view named **airline_cluster_auto_history** to easily filter the table for important columns. Refer to the code snippet below:
-- MAGIC <br>
-- MAGIC <br>
-- MAGIC ```python
-- MAGIC history_df = spark.sql('''DESCRIBE HISTORY airline_cluster_auto''')
-- MAGIC history_df.createOrReplaceTempView('airline_cluster_auto_history')
-- MAGIC ```
-- MAGIC
-- MAGIC **NOTE**: Instead of using pyspark, you can simply use the SQL DESCRIBE HISTORY statement for a complete list.
-- MAGIC <br>
-- MAGIC
-- MAGIC ![table_history](files/images/default-storage-and-liquid-clustering-1.0.0/table_history.png)
-- MAGIC
-- MAGIC <br>
-- MAGIC
-- MAGIC #####Key Highlight from table history of airline_cluster_auto:
-- MAGIC
-- MAGIC - Multiple versions for *CLUSTER BY* in the  **operation** column indicate predictive optimization searching for columns to cluster. In Version 3, predictive optimization attempts to find columns to cluster by but fails.
-- MAGIC - In **Version 5**, the clustered columns selected are **Origin** and **Arrtime**. 
-- MAGIC - In **Version 7**, predictive optimization again selects **Origin** and **Arrtime** as the clustering columns.
-- MAGIC - Notably, in **Versions 4 and 6**, optimization is applied to the table. Specifically, in **Version 4**, the number of files is reduced from *400* to *10* to improve query performance.
-- MAGIC - Vacuum operations are continuously run from **Version 8** to **Version 11**, which helps in managing storage.
-- MAGIC - All of these optimizations were done behind the scenes through predictive optimization.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Examining Predictive Optimization History
-- MAGIC
-- MAGIC With predictive optimization, maintenance for Unity Catalog tables in Databricks is simplified. It automatically identifies tables needing upkeep and performs the necessary optimization tasks, so you don’t have to manage or track them manually. 
-- MAGIC
-- MAGIC Please refer to 
-- MAGIC [Predictive optimization system table reference documentation](https://docs.databricks.com/en/admin/system-tables/predictive-optimization.html) for more details around predictive optimization history.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Querying the Predictive Optimization System Table
-- MAGIC The query provided below will access the predictive optimization system table and filter for the **airline_cluster_auto** table to display all optimizations. Review the results to see the following columns: **operation_type**, **operation_metrics**, **usage_quantity** and **end_time**.
-- MAGIC
-- MAGIC
-- MAGIC [Predictive optimization system table reference](https://docs.databricks.com/en/admin/system-tables/predictive-optimization.html)
-- MAGIC
-- MAGIC ![predictive_opt_history](files/images/default-storage-and-liquid-clustering-1.0.0/predictive_opt_history.png)
-- MAGIC
-- MAGIC <br>
-- MAGIC
-- MAGIC ##### Key Highlights:
-- MAGIC - The above query includes the usage quantity (in DBU) for each associated operation.
-- MAGIC - There are mainly two types of operations being executed: clustering and column selection for clustering.
-- MAGIC - The schema must be enabled to be visible in your system catalog. For more information, see [Enable system table schemas](https://docs.databricks.com/en/admin/system-tables/index.html#enable-system-table-schemas).
-- MAGIC - Data can take up to 24 hours to be populated.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC
-- MAGIC - `CLUSTER BY AUTO` is ease of use feature to optimize your table by application of predictive optimization.
-- MAGIC - It automatically picks the clustering columns based on query scan statistics (including query predicates and JOIN filter).
-- MAGIC - It reduces the need for manual tuning and improves efficiency.
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
