# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Modling

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

relationship_metadata = [
    (
        "customers_silver",
        "accounts_silver",
        "customer_id",
        "customer_id",
        "One customer can have multiple accounts",
        "Validated"
    ),
    (
        "customers_silver",
        "loans_silver",
        "customer_id",
        "customer_id",
        "One customer can have multiple loans",
        "Validated"
    ),
    (
        "accounts_silver",
        "cards_silver",
        "account_id",
        "account_id",
        "One account can have multiple cards",
        "Validated"
    ),
    (
        "branches_silver",
        "accounts_silver",
        "branch_id",
        "branch_id",
        "Branch-account relationship",
        "Not available: branch_id missing in accounts_silver"
    ),
    (
        "accounts_silver",
        "bank_transactions_silver",
        "account_id",
        "source_account_no",
        "Account transaction relationship",
        "Not matched: different ID formats"
    )
]

relationship_schema = StructType([
    StructField("parent_table", StringType(), True),
    StructField("child_table", StringType(), True),
    StructField("parent_key", StringType(), True),
    StructField("child_key", StringType(), True),
    StructField("relationship_description", StringType(), True),
    StructField("validation_status", StringType(), True)
])

relationship_df = spark.createDataFrame(
    relationship_metadata,
    schema=relationship_schema
)

display(relationship_df)