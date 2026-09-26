# Databricks notebook source
# MAGIC %md
# MAGIC # Banking Data Engineering Pipeline using Databricks
# MAGIC
# MAGIC ## Project Objective
# MAGIC
# MAGIC The objective of this project is to build an end-to-end banking data engineering pipeline using Databricks, PySpark, SQL, and Delta Lake.
# MAGIC
# MAGIC The pipeline processes banking data through Bronze, Silver, and Gold layers and produces business-ready datasets for analytics and reporting.
# MAGIC
# MAGIC ## Technologies Used
# MAGIC
# MAGIC - Python
# MAGIC - PySpark
# MAGIC - SQL
# MAGIC - Databricks
# MAGIC - Delta Lake
# MAGIC - Medallion Architecture
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Pipeline Architecture
# MAGIC
# MAGIC Raw CSV / Excel Files
# MAGIC         ↓
# MAGIC Bronze Layer
# MAGIC         ↓
# MAGIC Silver Layer
# MAGIC         ↓
# MAGIC Data Quality Checks
# MAGIC         ↓
# MAGIC Data Modeling
# MAGIC         ↓
# MAGIC Gold Layer
# MAGIC         ↓
# MAGIC SQL Analytics
# MAGIC         ↓
# MAGIC Databricks Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medallion Architecture
# MAGIC
# MAGIC ### Bronze Layer
# MAGIC
# MAGIC Raw banking data is ingested into Delta tables with minimal transformation.
# MAGIC
# MAGIC Main tables:
# MAGIC
# MAGIC - customers_bronze
# MAGIC - accounts_bronze
# MAGIC - branches_bronze
# MAGIC - loans_bronze
# MAGIC - merchants_bronze
# MAGIC - cards_bronze
# MAGIC - bank_transactions_bronze
# MAGIC
# MAGIC ### Silver Layer
# MAGIC
# MAGIC Data is cleaned, standardized, deduplicated, and transformed.
# MAGIC
# MAGIC Main tables:
# MAGIC
# MAGIC - customers_silver
# MAGIC - accounts_silver
# MAGIC - branches_silver
# MAGIC - loans_silver
# MAGIC - merchants_silver
# MAGIC - cards_silver
# MAGIC - bank_transactions_silver
# MAGIC
# MAGIC ### Gold Layer
# MAGIC
# MAGIC Business-level aggregations are created for analytics and reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Engineering Challenges
# MAGIC
# MAGIC 1. Duplicate transaction records were identified and handled.
# MAGIC 2. Transaction account numbers had a different format from synthetic account IDs.
# MAGIC 3. Accounts data did not contain branch_id, so a branch-account relationship could not be established.
# MAGIC 4. Null and duplicate checks were performed on important business keys.
# MAGIC 5. Transaction types were derived as DEPOSIT and WITHDRAWAL.
# MAGIC 6. Data was transformed from raw source format into analytics-ready datasets.