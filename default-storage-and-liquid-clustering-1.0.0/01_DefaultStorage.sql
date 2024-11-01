-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 01 - Default Storage for Unity Catalog (Private Preview)
-- MAGIC The Private Preview for Default Storage provides a fully-managed storage location for Unity Catalog catalogs, which allows you to create catalogs without configuring storage credentials or external locations on Unity Catalog.
-- MAGIC
-- MAGIC #### Here are the key benefits of Default Storage for UC:
-- MAGIC - **Seamless onboarding**: Default Storage for Unity Catalog seamlessly integrates with your existing Databricks platform, making it easy to get started with a new catalog for your new team and data projects. 
-- MAGIC - **Zero cloud management overhead**: Your time is the most valuable asset. Default Storage allows you to focus on data in Unity Catalog without worrying about managing the storage under the hood.
-- MAGIC - **Out-of-the-box security and unified access control**: Your data's security is our top priority. Default Storage for Unity Catalog comes with out-of-the-box governance, fine-grained encryption, and a secured network, giving you peace of mind.
-- MAGIC - **Unified billing and cost observability**: Default Storage simplifies your organization’s storage billing and cost observability by consolidating the view in a single platform instead of two.
-- MAGIC
-- MAGIC #### Default Storage Private Preview Requirements 
-- MAGIC - You must be enrolled in the Private Preview for Default Storage to use this functionality.
-- MAGIC - Default Storage is only supported on AWS. Azure and GCP are on the roadmap.
-- MAGIC - You must use serverless compute to access Default Storage. Classic compute support for Default Storage is on the roadmap. 
-- MAGIC - Only the following regions support Default Storage. More regions will be added in the next milestone.
-- MAGIC   - us-east-1
-- MAGIC   - us-east-2
-- MAGIC   - us-west-2
-- MAGIC   - eu-west-1
-- MAGIC - Your workspace must have Unity Catalog enabled.
-- MAGIC - Refer to the [Default Storage documentation](https://docs.databricks.com/en/default-storage.html) for more information.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #### Lab Objectives
-- MAGIC - Learn how to create a catalog using Default Storage for Unity Catalog.
-- MAGIC - Learn how to migrate managed or external tables from customer-managed storage to managed tables in Default Storage.
-- MAGIC
-- MAGIC #### Demonstration Notes
-- MAGIC - We are using a small table for training purposes to avoid long wait times for transferring data.
-- MAGIC - We are transferring a managed table for use in the learning lab. However, the same process will apply for an external table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup (REQUIRED)
-- MAGIC 1. Navigate to the top of the notebook and select your pre-created compute cluster. **Do not use Serverless.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the setup script below to set your default catalog and schema. View the results to confirm that your current catalog is **tech_summit_data** and is using the **default** schema.

-- COMMAND ----------

-- MAGIC %run ./setup/utility/setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Create Catalogs using Default Storage for Unity Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. To create Default Storage for Unity Catalog, complete the following steps:
-- MAGIC
-- MAGIC - a. In the left navigation menu find **Catalog**, right-click and select **Open Link in New Tab**.
-- MAGIC - b. Above the Catalogs, select **Add data** (+).
-- MAGIC - c. Choose **Add a catalog**.
-- MAGIC - d. In the **Create a new catalog** window, name the catalog using your last name, first initial, and a random number. Something easy to remember. Please remember your new Catalog name.
-- MAGIC - e. In the **Storage location** section, select **Use default storage Preview**.
-- MAGIC - f. Click **Create**.
-- MAGIC - g. A **Catalog created!** note appears. Please select **View catalog** and examine the **Details**.
-- MAGIC - h. Confirm that **Storage root** and **Storage location** specify *Default Storage*.
-- MAGIC - i. Close the **Catalog Explorer** web page and return back to this notebook.
-- MAGIC <br></br>
-- MAGIC ##### Example Image
-- MAGIC ![default_storage](files/images/default-storage-and-liquid-clustering-1.0.0/default_storage.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Migrating Managed or External Delta Tables in Customer Managed Storage to Managed Tables in Default Storage
-- MAGIC
-- MAGIC You can use traditional techniques to migrate your customers managed or external tables.
-- MAGIC
-- MAGIC - DEEP CLONE
-- MAGIC - CREATE TABLE AS (CTAS)
-- MAGIC
-- MAGIC **NOTE:** For the purpose of the lab, you will migrate a managed table in customer managed storage to your catalog using default storage. The same methods apply for external tables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Explore the Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use the DESCRIBE statement to view the metadata information of the **airlines_final** table. View the results. Is the **airlines_final** table *MANAGED* or *EXTERNAL*?

-- COMMAND ----------

DESCRIBE EXTENDED tech_summit_data.default.airlines_final;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Use Classic Compute to Migrate the Table to Default Storage (error)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Check the cluster your notebook is using. Verify that you are using a classic compute cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Complete the code below to create a deep clone of the managed table **airlines_final** and save it as a new table in your catalog that uses the default storage settings.
-- MAGIC
-- MAGIC     Complete the following:
-- MAGIC       - a. Specify your catalog name and use the **default** schema. 
-- MAGIC       - b. Name the table **airlines_default**.
-- MAGIC       - c. Execute the code and view the results. 
-- MAGIC       - d. Confirm an error occurred. What was the error?

-- COMMAND ----------

CREATE OR REPLACE TABLE    -- TO DO: Add your new catalog, use the default schema and add the table name airlines_default
DEEP CLONE tech_summit_data.default.airlines_final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Summary
-- MAGIC
-- MAGIC You should have received the following error if you were using a classic compute cluster:
-- MAGIC
-- MAGIC *DatabricksRemoteException: BAD_REQUEST: Databricks Default Storage cannot be accessed using Classic Compute. Please use Serverless compute to access data in Default Storage.*
-- MAGIC
-- MAGIC As of this private preview, you must use Serverless compute to access default storage. Classic compute support for Default Storage is on the roadmap.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Use Serverless Compute (REQUIRED)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Please navigate to the top of the notebook and change your compute to **Serverless**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### METHOD 1: DEEP CLONE
-- MAGIC
-- MAGIC 2. Use DEEP CLONE to migrate the managed table to your catalog with Default Storage.
-- MAGIC
-- MAGIC     Complete the following:
-- MAGIC
-- MAGIC     - a. Specify your catalog name and use the **default** schema. 
-- MAGIC     - b. Name the table **airline_default_dc**.
-- MAGIC     - c. Execute the code and view the results.
-- MAGIC
-- MAGIC
-- MAGIC **NOTE:** The lab uses small data for demonstration purposes, but this approach scales to larger datasets as well. However, we do not have formal benchmarks for execution time with large data.

-- COMMAND ----------

CREATE OR REPLACE TABLE -- TO DO: Add your new catalog, use the default schema and add the table name airlines_default_dc
DEEP CLONE tech_summit_data.default.airlines_final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Describe your table **airline_default_dc** in your default storage. Please confirm that the table has been successfully migrated to your catalog using default storage.
-- MAGIC
-- MAGIC     View the location of the table. You'll notice that it is empty when the table is in a Catalog using Default Storage.

-- COMMAND ----------

DESCRIBE DETAIL -- TO DO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### METHOD 2: CREATE TABLE AS (CTAS)
-- MAGIC
-- MAGIC 1. Use the CREATE TABLE AS statement to migrate the managed table to your catalog with Default Storage.
-- MAGIC
-- MAGIC     Complete the following:
-- MAGIC     - a. Specify your catalog name and use the **default** schema. 
-- MAGIC     - b. Name the table **airline_default_ctas**.
-- MAGIC     - c. Execute the code and view the results.
-- MAGIC
-- MAGIC **NOTE:** The lab uses small data for demonstration purposes, but this approach scales to larger datasets as well. However, we do not have formal benchmarks for execution time with large data.

-- COMMAND ----------

CREATE OR REPLACE TABLE -- TO DO: Add your new catalog, use the default schema and add the table name airlines_default_ctas
AS
SELECT * 
FROM tech_summit_data.default.airlines_final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Describe your table **airline_default_ctas** in the default storage. Please confirm that the table has been successfully migrated to your catalog using default storage.
-- MAGIC
-- MAGIC     View the location of the table. You'll notice that it is empty when the table is in a Catalog using Default Storage.

-- COMMAND ----------

DESCRIBE DETAIL -- TO DO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Lab Completion (REQUIRED)
-- MAGIC 1. Please drop your Default Storage catalog.

-- COMMAND ----------

DROP CATALOG <your-catalog-name> CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Please switch from **Serverless** compute to your **classic compute cluster**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Go to next Lab: 02_AutomaticLiquidClustering

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
