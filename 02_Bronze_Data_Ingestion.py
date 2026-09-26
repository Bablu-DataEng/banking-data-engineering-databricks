# Databricks notebook source
# MAGIC %md
# MAGIC ## 02_Bronze_Data_Ingestion

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customers Bronze

# COMMAND ----------

customers_bronze = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/banking_data/customers.csv")

display(customers_bronze)

# COMMAND ----------

customers_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customers_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Accounts Bronze

# COMMAND ----------

accounts_bronze = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/banking_data/accounts.csv")

accounts_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("accounts_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Branches Bronze

# COMMAND ----------

branches_bronze = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/banking_data/branches.csv")

branches_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("branches_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loans Bronze

# COMMAND ----------

loans_bronze = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/banking_data/loans.csv")

loans_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("loans_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merchants Bronze

# COMMAND ----------

merchants_bronze = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/banking_data/merchants.csv")

merchants_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("merchants_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cards Bronze

# COMMAND ----------

cards_bronze = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/banking_data/cards.csv")

cards_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("cards_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bank Excel Bronze

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

import pandas as pd

bank_pd = pd.read_excel(
    "/Volumes/workspace/default/banking_data/bank.xlsx",
    engine="openpyxl"
)

bank_pd = bank_pd.astype(str)

bank_bronze = spark.createDataFrame(bank_pd)

# COMMAND ----------

bank_bronze = bank_bronze.toDF(
    "account_no",
    "transaction_date",
    "transaction_details",
    "cheque_no",
    "value_date",
    "withdrawal_amt",
    "deposit_amt",
    "balance_amt",
    "extra_column"
)

# COMMAND ----------

bank_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bank_transactions_bronze")

# COMMAND ----------

spark.sql("SHOW TABLES").show()