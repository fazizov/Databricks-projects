-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lab Summary
-- MAGIC
-- MAGIC In this session, we will discuss default storage for Unity Catalog. Default Storage, along with Serverless Compute, offers a SaaS Lakehouse experience for our customers, eliminating the need for cloud infrastructure management. It is built with advanced security frameworks, such as multi-key protection and storage proxy, ensuring data and storage security from the outset. As Default Storage will become the default option for new accounts, new workspaces, and new catalogs in the future, it is important for field engineers to become familiar with the hybrid deployment model and security architecture. Attendees should leave with an understanding of how Default Storage fits within the Databricks deployment architecture.
-- MAGIC
-- MAGIC Then we will learn more about Liquid Clustering, Automatic Key Selection, and Predictive Optimization. We will discuss how they work and how your customers can understand the decisions being made.
-- MAGIC
-- MAGIC
-- MAGIC ## Slack Channels
-- MAGIC - [#default-storage](https://databricks.enterprise.slack.com/archives/C04Q3SP2QH2)
-- MAGIC - [#liquid-clustering](https://databricks.enterprise.slack.com/archives/C0595BS29LJ)
-- MAGIC
-- MAGIC ## Private Preview Resources
-- MAGIC Encourage your customers to sign up!
-- MAGIC - [Databricks Managed Storage(Default Storage) Private Preview](https://home.databricks.com/sales/field-performance/products/dms/)
-- MAGIC - [Automatic Liquid Clustering Key Selection Private Preview Sign-up](https://docs.google.com/forms/d/e/1FAIpQLSedBVoC9sEs3gMydgYFngvhjlopB_MEFe4H9ckMF0YZidrxfg/viewform?usp=send_form)
-- MAGIC
-- MAGIC
-- MAGIC ## Lab Agenda
-- MAGIC The following notebooks are part of the lab. Please begin with the **01_DefaultStorage** notebook.
-- MAGIC | # |Type|  Estimated Time (mins) | Notebook| Description|
-- MAGIC | --- | --- | --- | --- | --- |
-- MAGIC | 01 |Lab| 5-7 |[Default Storage]($./01_DefaultStorage) | (Private Preview) Learn how to set up Default Storage to provide a fully managed storage location for Unity Catalog. This enables you to create catalogs without needing to configure storage credentials or external locations in Unity Catalog. |
-- MAGIC | 02 |Demo|5-7 |[Automatic Liquid Clustering]($./02_AutomaticLiquidClustering) | (Private Preview) Learn how to enable automatic Liquid Clustering key selection with predictive optimization (PO) to intelligently choose keys for your new or existing Liquid tables, enhancing query performance.|
-- MAGIC | 03 |Lab|15-20  |[Partitioning to Liquid Clustering]($./03_PartitioningToLiquidClustering) | Learn how to enhance query performance of a poorly partitioned table using Liquid Clustering with a step by step example.| 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Start Lab: Open the 01_DefaultStorage notebook

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
