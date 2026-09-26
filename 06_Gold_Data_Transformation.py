# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer Data

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    date_format,
    count,
    sum as spark_sum,
    when,
    coalesce,
    lit,
    round
)

transactions = spark.table("bank_transactions_silver")

gold_monthly_transaction_summary = (
    transactions
    .withColumn(
        "transaction_month",
        date_format(col("transaction_date"), "yyyy-MM")
    )
    .groupBy("transaction_month")
    .agg(
        count("*").alias("total_transactions"),

        spark_sum(
            when(
                col("transaction_type") == "DEPOSIT",
                coalesce(col("deposit_amt"), lit(0))
            ).otherwise(lit(0))
        ).alias("total_deposit_amount"),

        spark_sum(
            when(
                col("transaction_type") == "WITHDRAWAL",
                coalesce(col("withdrawal_amt"), lit(0))
            ).otherwise(lit(0))
        ).alias("total_withdrawal_amount")
    )
    .withColumn(
        "total_deposit_amount",
        round(col("total_deposit_amount"), 2)
    )
    .withColumn(
        "total_withdrawal_amount",
        round(col("total_withdrawal_amount"), 2)
    )
    .orderBy("transaction_month")
)

display(gold_monthly_transaction_summary)

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min,
    round
)

accounts = spark.table("accounts_silver")

gold_account_type_summary = (
    accounts
    .groupBy("account_type")
    .agg(
        count("*").alias("total_accounts"),

        spark_sum("balance_usd").alias("total_balance"),

        avg("balance_usd").alias("average_balance"),

        spark_max("balance_usd").alias("highest_balance"),

        spark_min("balance_usd").alias("lowest_balance")
    )
    .withColumn(
        "total_balance",
        round(col("total_balance"), 2)
    )
    .withColumn(
        "average_balance",
        round(col("average_balance"), 2)
    )
    .withColumn(
        "highest_balance",
        round(col("highest_balance"), 2)
    )
    .withColumn(
        "lowest_balance",
        round(col("lowest_balance"), 2)
    )
    .orderBy("account_type")
)

display(gold_account_type_summary)

# COMMAND ----------

gold_account_type_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_account_type_summary")

# COMMAND ----------

display(
    spark.table("gold_account_type_summary")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Loan Summary

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min,
    round
)

loans = spark.table("loans_silver")

gold_loan_summary = (
    loans
    .agg(
        count("*").alias("total_loans"),

        spark_sum("loan_amount").alias("total_loan_amount"),

        avg("loan_amount").alias("average_loan_amount"),

        spark_max("loan_amount").alias("highest_loan_amount"),

        spark_min("loan_amount").alias("lowest_loan_amount")
    )
    .withColumn(
        "total_loan_amount",
        round(col("total_loan_amount"), 2)
    )
    .withColumn(
        "average_loan_amount",
        round(col("average_loan_amount"), 2)
    )
    .withColumn(
        "highest_loan_amount",
        round(col("highest_loan_amount"), 2)
    )
    .withColumn(
        "lowest_loan_amount",
        round(col("lowest_loan_amount"), 2)
    )
)

display(gold_loan_summary)

# COMMAND ----------

gold_loan_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_loan_summary")

# COMMAND ----------

display(
    spark.table("gold_loan_summary")
)

# COMMAND ----------

print(cards.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Card Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Card Summary

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    count,
    countDistinct
)

cards = spark.table("cards_silver")

gold_card_summary = (
    cards
    .groupBy("card_type")
    .agg(
        count("*").alias("total_cards"),
        countDistinct("account_id").alias("unique_accounts")
    )
    .orderBy("card_type")
)

display(gold_card_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Customer Summary

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    count,
    avg,
    min as spark_min,
    max as spark_max,
    round
)

customers = spark.table("customers_silver")

display(customers.limit(10))
print(customers.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Credit Score Category

# COMMAND ----------

gold_customer_summary = (
    customers
    .withColumn(
        "credit_category",
        when(col("credit_score") >= 750, "Excellent")
        .when(col("credit_score") >= 650, "Good")
        .when(col("credit_score") >= 550, "Fair")
        .otherwise("Poor")
    )
    .groupBy("credit_category")
    .agg(
        count("*").alias("total_customers"),
        round(avg("credit_score"), 2).alias("average_credit_score"),
        spark_min("credit_score").alias("minimum_credit_score"),
        spark_max("credit_score").alias("maximum_credit_score")
    )
    .orderBy("credit_category")
)

display(gold_customer_summary)

# COMMAND ----------

gold_customer_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_customer_summary")

# COMMAND ----------

display(
    spark.table("gold_customer_summary")
)

# COMMAND ----------

gold_tables = [
    "gold_monthly_transaction_summary",
    "gold_account_type_summary",
    "gold_loan_summary",
    "gold_card_summary",
    "gold_customer_summary"
]

for table_name in gold_tables:
    print(f"\nTable: {table_name}")

    if spark.catalog.tableExists(table_name):
        print("Status: Available")
        print("Rows:", spark.table(table_name).count())
        display(spark.table(table_name).limit(5))
    else:
        print("Status: Missing")

# COMMAND ----------

display(
    spark.sql("SHOW TABLES")
    .filter("tableName LIKE 'gold_%'")
)

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, when

for table_name in gold_tables:
    if spark.catalog.tableExists(table_name):
        df = spark.table(table_name)

        print(f"\nNull check for: {table_name}")

        null_check = df.select([
            spark_sum(
                when(col(column_name).isNull(), 1).otherwise(0)
            ).alias(column_name)
            for column_name in df.columns
        ])

        display(null_check)