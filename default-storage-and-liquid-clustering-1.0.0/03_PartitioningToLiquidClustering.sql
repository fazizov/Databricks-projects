-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 03 - Optimizing Table Performance Using Liquid Clustering
-- MAGIC
-- MAGIC In this lab, we are going to work with Liquid Clustering, a Delta Lake optimization feature that replaces table partitioning and ZORDER to simplify data layout decisions and optimize query performance. It provides the flexibility to redefine clustering keys without rewriting data.
-- MAGIC
-- MAGIC #### Lab Objective
-- MAGIC - Compare the performance of partition, ZORDER, and liquid clustering.
-- MAGIC
-- MAGIC #### Lab Scenario
-- MAGIC As a new data engineer, you created a table and partitioned it based on a column frequently used in queries. While queries on the partitioned column perform well, you’ve observed that queries on other columns are becoming increasingly slower as the data grows.
-- MAGIC
-- MAGIC How can you improve the performance of these queries?
-- MAGIC
-- MAGIC #### Lab Notes
-- MAGIC - The data in this lab was poorly partitioned to simulate customer scenarios. It uses sample airline data and includes enough data to demonstrate performance improvements for the lab. However, as the amount of data increases, performance gains are likely to become more significant. 
-- MAGIC - Photon is not enabled in the lab to make optimizations more apparent and to avoid the need to create extremely large tables.
-- MAGIC - When optimizing table performance for customers, many factors must be considered. This lab uses a simple example to illustrate these principles. This small data might perform well without any manual intervention.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Run Setup (REQUIRED)
-- MAGIC 1. Before starting, please confirm that you are using classic compute instead of serverless.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the following cell to set up the lab by selecting the **tech_summit_data** catalog and the **default** schema.

-- COMMAND ----------

-- MAGIC %run ./setup/utility/setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Set the required Spark configuration variable to **disable caching**, making the effect of the optimizations more apparent.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Set the spark configuration variable "io.cache" as "False"
-- MAGIC spark.conf.set('spark.databricks.io.cache.enabled', False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Explore the Catalog
-- MAGIC 4. Run the cell to view detailed information about the metadata of a catalog. Confirm the catalog exists.

-- COMMAND ----------

DESCRIBE CATALOG EXTENDED ${my.catalog};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Explore the Data
-- MAGIC
-- MAGIC #### Notes
-- MAGIC
-- MAGIC - The tables have been pre-created to reduce execution times.
-- MAGIC - They are located in the **tech_summit_data** catalog within the **default** schema.
-- MAGIC - The data used is from airlines for the years 2006 through 2008.
-- MAGIC - Each table is essentially the same but optimized differently.
-- MAGIC
-- MAGIC
-- MAGIC #### The following tables will be used:
-- MAGIC
-- MAGIC - **airline_partitioned** - Partitioned table
-- MAGIC - **airline_partitioned_z** - Partitioned with ZORDER
-- MAGIC - **airline_cluster** - Liquid clustering
-- MAGIC - Each table contains 28,745,461 records.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Run the following query to count the total number of rows for each distinct **Origin**. 
-- MAGIC
-- MAGIC     Answer the following questions:
-- MAGIC     - a. What is the number of rows in the **Origin** with the most rows?
-- MAGIC     - b. What is the number of rows in the **Origin** with the fewest rows?
-- MAGIC     - c. How many distinct **Origin** are there?

-- COMMAND ----------

SELECT Origin, count(*) as TotalRows
FROM airline_partitioned
GROUP BY Origin
ORDER BY TotalRows DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **5. ANSWERS**
-- MAGIC - a. What is the number of rows in the **Origin** with the most rows? **ATL, 1,670,013 rows**
-- MAGIC - b. What is the number of rows in the **Origin** with the fewest rows? **(Multiple origins), 1 row**
-- MAGIC - c. How many distinct **Origin** are there? **321**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Working with the Partitioned Table
-- MAGIC #### Table: airline_partitioned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Table details and history

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. View the details of the **airline_partitioned** table. View the results and answer the following questions:
-- MAGIC - a. How is the table partitioned?
-- MAGIC - b. How many total files exist in the table?

-- COMMAND ----------

DESCRIBE DETAIL airline_partitioned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **6. ANSWERS**
-- MAGIC - a. How is the table partitioned? **By the Origin column (under the partitionColumns column in the results)**
-- MAGIC - b. How many total files exist in the table? **5,955 (under the numFiles column in the results)**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. View the history of the **airline_partitioned** table. View the results and answer the following questions:
-- MAGIC - a. How many versions does this table have?
-- MAGIC - b. Has this table been optimized?

-- COMMAND ----------

DESCRIBE HISTORY airline_partitioned;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **ANSWERS**
-- MAGIC - a. How many versions does this table have? **1 Version (0)**
-- MAGIC - b. Has this table been optimized? **No**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Query the airline_partitioned table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Execute the query to count the total number of flights in the **airline_partitioned** table where the **Origin** is in the list of airports.
-- MAGIC
-- MAGIC     Take note of how long the query took to execute. Why do you think it executed quickly?

-- COMMAND ----------

SELECT
  count(*) as Total
FROM airline_partitioned
WHERE Origin in ('BUF','RDU','ROC','ATL');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Execute a query on the **airline_partitioned** table to count the total number of flights where **ArrTime** occurred prior to *1000*. Take note of how long the query took to execute.
-- MAGIC
-- MAGIC     **NOTE:** This may take a painful 2-3 minutes to execute. While it's executing you can move to **Step 10** to view the spark job.
-- MAGIC     
-- MAGIC     While it's executing, consider why this query is taking longer than expected. What steps can be taken to optimize the query's performance?

-- COMMAND ----------

SELECT count(*) 
FROM airline_partitioned
WHERE ArrTime < 1000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. In the previous cell complete the following:
-- MAGIC - a. Expand **Spark Jobs**.
-- MAGIC - b. Right-click on **View** in any of the jobs and select **Open Link in New Tab** (Note: In the lab, you must open the view in a new tab,  otherwise you will encounter an error window).
-- MAGIC - c. In the window select the number to the right of **Associated SQL Query**. 
-- MAGIC - d. Scroll to the bottom of the window and expand **Scan parquet tech_summit_data.default.airline_partitioned**.
-- MAGIC
-- MAGIC Use the open window to answer the following questions:
-- MAGIC - a. How many partitions did your query read? 
-- MAGIC - b. How many files did your query read? 
-- MAGIC - c. How many files did your query prune?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **10. ANSWERS**
-- MAGIC - a. How many partitions did your query read? **321**
-- MAGIC - b. How many files did your query read? **5,955**
-- MAGIC - c. How many files did your query prune? **0**
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Summary
-- MAGIC - Querying by the partitioned **Origin** column returns results quickly.
-- MAGIC - Querying by the **ArrTime** column results in long execution times because the table is partitioned by **Origin**.
-- MAGIC - The partitioned table contains hundreds of partitions and thousands of small files, so the query must scan all the files within each partition to filter non partitioned columns like the **ArrTime** column.
-- MAGIC - Issues with this partitioning include:
-- MAGIC   - Too many partitions, leading to performance overhead and slow metadata operations.
-- MAGIC   - Small files in partitions with only a few records, causing inefficient file management and increased I/O overhead due to the need to read many files.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Working with the Partitioned and OPTIMIZED ZORDER Table
-- MAGIC
-- MAGIC #### Table: airline_partitioned_z
-- MAGIC The following table contains the same partitions as the previous table, but a ZORDER has been applied and the table has been optimized.
-- MAGIC  <br></br>
-- MAGIC #### Example Code
-- MAGIC ```sql
-- MAGIC ALTER TABLE airline_partitioned_z
-- MAGIC ZORDER BY (ArrTime);
-- MAGIC
-- MAGIC OPTIMIZE airline_partitioned_z;
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Table details and history

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11. Execute the DESCRIBE statement on the **airline_partitioned_z** table. View the results and confirm that this table is partitioned by **Origin** and has 321 files.

-- COMMAND ----------

DESCRIBE DETAIL airline_partitioned_z;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12. Describe the history of the **airline_partitioned_z** and view the results. Answer the following questions:
-- MAGIC
-- MAGIC - a. Has this table been optimized? How?
-- MAGIC - b. How many files were removed? How many files were added?

-- COMMAND ----------

DESCRIBE HISTORY airline_partitioned_z;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **12. ANSWERS**
-- MAGIC - a. Has this table been optimized? How? **Yes, the table has been optimized. In the operationParameters column, you will see that a ZORDER by the ArrTime column was added.**
-- MAGIC - b. How many files were removed? How many files were added? **In the operationMetrics column, it specifies that 5,949 files were removed and 315 files were added. This consolidates the small files within the partitions.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Query the airline_partitioned_z table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 13. Execute the query to count the total number of flights in the **airline_partitioned_z** table where the **Origin** is in the list of airports.
-- MAGIC
-- MAGIC     How long did the query take to execute compared to the original partitioned table?

-- COMMAND ----------

SELECT
  count(*) as Total
FROM airline_partitioned_z
WHERE Origin in ('BUF','RDU','ROC','ATL');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 14. Execute the query to count the total number of flights in the **airline_partitioned_z** table where **ArrTime** occurred prior to *1000*. Take note of how long the query took to execute.
-- MAGIC
-- MAGIC     How long did the query take to execute compared to the **airline_partitioned** query?

-- COMMAND ----------

SELECT count(*) 
FROM airline_partitioned_z
WHERE ArrTime < 1000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 15. In the previous cell, complete the following:
-- MAGIC - a. Expand **Spark Jobs**.
-- MAGIC - b. Right-click on **View** in any of the jobs and select **Open Link in New Tab** (Note: In the lab, you must open the view in a new tab, 
-- MAGIC  otherwise, you will encounter an error window).
-- MAGIC - c. In the window, select the number to the right of **Associated SQL Query**. 
-- MAGIC - d. Scroll to the bottom of the window and expand **Scan parquet tech_summit_data.default.airline_partitioned_z**.
-- MAGIC
-- MAGIC Use the open window to answer the following questions:
-- MAGIC - a. How many partitions did your query read? 
-- MAGIC - b. How many files did your query read? 
-- MAGIC - c. How many files did your query prune?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **15. ANSWERS**
-- MAGIC - a. How many partitions did your query read? **321**
-- MAGIC - b. How many files did your query read? **321**
-- MAGIC - c. How many files did your query prune? **0**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Summary
-- MAGIC
-- MAGIC
-- MAGIC Executing OPTIMIZE and ZORDER on the table increases query performance because the OPTIMIZE statement compacts all of the small files within partitions and ZORDER will colocate related information in the same set of files for the **ArrTime** column within files in each partition.
-- MAGIC
-- MAGIC This co-locality is automatically used by Delta Lake on Databricks data-skipping functionality. This behavior can dramatically reduce the amount of data that Delta Lake on Databricks needs to read. However, in this example, every file was still read because the size of the partitions is relatively small. The query does run much faster since small files were compacted.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Working with the Liquid Clustering Table
-- MAGIC
-- MAGIC #### Table: airline_cluster
-- MAGIC
-- MAGIC Liquid clustering was applied to the **airline_cluster** using the following statements:
-- MAGIC
-- MAGIC <br></br>
-- MAGIC #### Example Code
-- MAGIC ```sql
-- MAGIC ALTER TABLE airline_cluster
-- MAGIC CLUSTER BY (Origin, ArrTime);
-- MAGIC
-- MAGIC OPTIMIZE airline_cluster;
-- MAGIC ```
-- MAGIC
-- MAGIC Refer to the [Use liquid clustering for Delta tables documentation](https://docs.databricks.com/en/delta/clustering.html) for more information.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Table details and history

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 16. View the details of the **airline_cluster** table. Confirm that the table is clustered by **Origin** and **ArrTime**, and that there are *5* files.

-- COMMAND ----------

DESCRIBE DETAIL airline_cluster;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 17. Describe the history of the **airline_cluster** table and view the results. Confirm that adding liquid clustering and optimizing the table introduces a variety of **operations**  in the history.

-- COMMAND ----------

DESCRIBE HISTORY airline_cluster;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Query the clustered table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 18. Execute the query to count the total number of flights in the **airline_cluster** table where the **Origin** is in the list of airports.
-- MAGIC
-- MAGIC     How long did the query take to execute compared to the original partitioned and zordered tables?

-- COMMAND ----------

SELECT
  count(*) as Total
FROM airline_cluster
WHERE Origin in ('BUF','RDU','ROC','ATL');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 19. Execute the query to count the total number of flights in the **airline_cluster** table where **ArrTime** occurred prior to *1000*. Take note of how long the query took to execute.

-- COMMAND ----------

SELECT count(*) 
FROM airline_cluster
WHERE ArrTime < 1000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Summary
-- MAGIC
-- MAGIC Delta Lake's liquid clustering improves how your data is organized and how queries are run. Instead of using traditional table partitioning and ZORDER methods, liquid clustering simplifies the process.
-- MAGIC
-- MAGIC With liquid clustering, you can change how your data is organized without needing to rewrite all of it. This means you can adjust the data layout as your analysis needs change over time, making it easier to keep your data optimized for performance.
-- MAGIC
-- MAGIC [Use liquid clustering for Delta tables](https://docs.databricks.com/en/delta/clustering.html) documentation.
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
