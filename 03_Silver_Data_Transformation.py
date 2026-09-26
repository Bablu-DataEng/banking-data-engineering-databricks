# Databricks notebook source
from pyspark.sql.functions import (
    col,
    lit,
    when,
    trim,
    lower,
    to_date,
    regexp_replace,
    current_timestamp
)

# COMMAND ----------

customers_bronze = spark.table("customers_bronze")

display(customers_bronze)

# COMMAND ----------

customers_bronze.printSchema()

# COMMAND ----------

customers_silver = customers_bronze \
    .dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull())

# COMMAND ----------

customers_silver = customers_silver.select(
    col("customer_id"),
    trim(col("first_name")).alias("first_name"),
    trim(col("last_name")).alias("last_name"),
    lower(trim(col("email"))).alias("email"),
    trim(col("city")).alias("city"),
    col("credit_score"),
    col("created_at")
)

# COMMAND ----------

customers_silver = customers_silver.withColumn(
    "processed_at",
    current_timestamp()
)

# COMMAND ----------

customers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customers_silver")

# COMMAND ----------

display(
    spark.table("customers_silver")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## accounts_bronze

# COMMAND ----------

accounts_bronze = spark.table("accounts_bronze")

display(accounts_bronze)

# COMMAND ----------

accounts_bronze.printSchema()

# COMMAND ----------

accounts_silver = accounts_bronze \
    .dropDuplicates(["account_id"]) \
    .filter(col("account_id").isNotNull())

# COMMAND ----------

accounts_silver = accounts_silver.select(
    col("account_id"),
    col("customer_id"),
    trim(col("account_type")).alias("account_type"),
    col("balance_usd"),
    col("open_date")
)

# COMMAND ----------

accounts_silver = accounts_silver.join(
    customers_silver.select("customer_id").distinct(),
    on="customer_id",
    how="inner"
)

# COMMAND ----------

accounts_silver = accounts_silver.withColumn(
    "processed_at",
    current_timestamp()
)

# COMMAND ----------

accounts_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("accounts_silver")

# COMMAND ----------

print(
    "Silver account records:",
    spark.table("accounts_silver").count()
)

# COMMAND ----------

display(
    spark.table("accounts_silver").limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Branches Silver Data

# COMMAND ----------

branches_bronze = spark.table("branches_bronze")

display(branches_bronze)

# COMMAND ----------

branches_bronze.printSchema()

# COMMAND ----------

branches_silver = branches_bronze \
    .dropDuplicates(["branch_id"]) \
    .filter(col("branch_id").isNotNull())

# COMMAND ----------

branches_silver = branches_silver.select(
    col("branch_id"),
    trim(col("branch_name")).alias("branch_name"),
    trim(col("manager_name")).alias("manager_name")
)

# COMMAND ----------

branches_silver = branches_silver.withColumn(
    "processed_at",
    current_timestamp()
)

# COMMAND ----------

branches_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("branches_silver")

# COMMAND ----------

print(
    "Silver branch records:",
    spark.table("branches_silver").count()
)

# COMMAND ----------

display(
    spark.table("branches_silver")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Lone Data

# COMMAND ----------

loans_bronze = spark.table("loans_bronze")

display(loans_bronze)

# COMMAND ----------

loans_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
loans_silver = loans_bronze \
    .dropDuplicates(["loan_id"]) \
    .filter(col("loan_id").isNotNull())

# COMMAND ----------

customers_silver = spark.table("customers_silver")

# COMMAND ----------

loans_silver = loans_silver.join(
    customers_silver.select("customer_id").distinct(),
    on="customer_id",
    how="inner"
)

# COMMAND ----------

loans_silver = loans_silver.select(
    col("loan_id"),
    col("customer_id"),
    col("loan_amount"),
    col("interest_rate"),
    col("start_date")
)

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

# COMMAND ----------

loans_silver = loans_silver.withColumn(
    "processed_at",
    current_timestamp()
)

# COMMAND ----------

loans_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("loans_silver")

# COMMAND ----------

print("Silver loan records:", loans_silver.count())

display(loans_silver.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merchants Silver

# COMMAND ----------

merchants_bronze = spark.table("merchants_bronze")

display(merchants_bronze)

# COMMAND ----------

merchants_silver =merchants_bronze.printSchema()

# COMMAND ----------

merchants_silver = merchants_bronze \
    .dropDuplicates(["merchant_id"]) \
    .filter(col("merchant_id").isNotNull())

# COMMAND ----------

merchants_silver = merchants_silver.select(
    col("merchant_id"),
    trim(col("merchant_name")).alias("merchant_name"),
    trim(col("city")).alias("city")
)

# COMMAND ----------

merchants_silver = merchants_silver.withColumn(
    "processed_at",
    current_timestamp()
)

# COMMAND ----------

merchants_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("merchants_silver")

# COMMAND ----------

print(
    "Silver merchant records:",
    merchants_silver.count()
)

# COMMAND ----------

display(
    merchants_silver.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cards Silver

# COMMAND ----------

cards_bronze = spark.table("cards_bronze")

display(cards_bronze)

# COMMAND ----------

cards_silver = cards_bronze \
    .dropDuplicates(["card_id"]) \
    .filter(col("card_id").isNotNull())

# COMMAND ----------

accounts_silver = spark.table("accounts_silver")

cards_silver = cards_silver.join(
    accounts_silver.select("account_id").distinct(),
    on="account_id",
    how="inner"
)

# COMMAND ----------

cards_silver = cards_silver.select(
    col("card_id"),
    col("account_id"),
    trim(col("card_type")).alias("card_type"),
    col("expiration_date")
)

# COMMAND ----------

cards_silver = cards_silver.withColumn(
    "processed_at",
    current_timestamp()
)

# COMMAND ----------

cards_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("cards_silver")

# COMMAND ----------

print("Silver card records:", cards_silver.count())

display(cards_silver.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bank Transactions Silver

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    when,
    lower,
    trim,
    to_date,
    regexp_replace,
    coalesce,
    current_timestamp
)

# COMMAND ----------

bank_transactions_bronze = spark.table(
    "bank_transactions_bronze"
)

print(
    "Bronze transaction records:",
    bank_transactions_bronze.count()
)

print(
    "Bronze columns:",
    bank_transactions_bronze.columns
)

# COMMAND ----------

transactions_silver = bank_transactions_bronze.dropDuplicates()

print(
    "Records after duplicate removal:",
    transactions_silver.count()
)

# COMMAND ----------

transactions_silver = transactions_silver.select([
    when(
        lower(trim(col(c))).isin("nan", "null", ""),
        None
    ).otherwise(col(c)).alias(c)
    for c in transactions_silver.columns
])

# COMMAND ----------

if "extra_column" in transactions_silver.columns:
    transactions_silver = transactions_silver.drop("extra_column")

# COMMAND ----------

if "account_no" in transactions_silver.columns:
    transactions_silver = transactions_silver.withColumnRenamed(
        "account_no",
        "source_account_no"
    )

# COMMAND ----------

transactions_silver = (
    transactions_silver
    .withColumn(
        "transaction_date",
        coalesce(
            to_date(col("transaction_date"), "yyyy-MM-dd"),
            to_date(col("transaction_date"), "dd/MM/yyyy"),
            to_date(col("transaction_date"), "MM/dd/yyyy"),
            to_date(col("transaction_date"), "d/M/yyyy")
        )
    )
    .withColumn(
        "value_date",
        coalesce(
            to_date(col("value_date"), "yyyy-MM-dd"),
            to_date(col("value_date"), "dd/MM/yyyy"),
            to_date(col("value_date"), "MM/dd/yyyy"),
            to_date(col("value_date"), "d/M/yyyy")
        )
    )
)

# COMMAND ----------

transactions_silver = (
    transactions_silver
    .withColumn(
        "withdrawal_amt",
        regexp_replace(
            trim(col("withdrawal_amt")),
            ",",
            ""
        ).cast("double")
    )
    .withColumn(
        "deposit_amt",
        regexp_replace(
            trim(col("deposit_amt")),
            ",",
            ""
        ).cast("double")
    )
    .withColumn(
        "balance_amt",
        regexp_replace(
            trim(col("balance_amt")),
            ",",
            ""
        ).cast("double")
    )
)

# COMMAND ----------

transactions_silver = transactions_silver.withColumn(
    "processed_at",
    current_timestamp()
)

# COMMAND ----------

print(
    "Final Silver transaction records:",
    transactions_silver.count()
)

print(
    "Silver columns:",
    transactions_silver.columns
)

transactions_silver.printSchema()

display(transactions_silver.limit(10))

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    regexp_replace,
    trim
)

transactions_silver = transactions_silver.withColumn(
    "source_account_no",
    regexp_replace(
        trim(col("source_account_no")),
        "'",
        ""
    )
)


# COMMAND ----------

display(
    transactions_silver.select("source_account_no").distinct().limit(10)
)

# COMMAND ----------

from pyspark.sql.functions import (
    count,
    sum,
    when,
    isnan,
    isnull,
    min,
    max
)

transactions_silver.select(
    count("*").alias("total_records"),
    sum(when(col("source_account_no").isNull(), 1).otherwise(0)).alias("null_account_numbers"),
    sum(when(col("transaction_date").isNull(), 1).otherwise(0)).alias("null_transaction_dates"),
    sum(when(col("withdrawal_amt").isNull(), 1).otherwise(0)).alias("null_withdrawals"),
    sum(when(col("deposit_amt").isNull(), 1).otherwise(0)).alias("null_deposits"),
    min("balance_amt").alias("minimum_balance"),
    max("balance_amt").alias("maximum_balance")
).show()

# COMMAND ----------

from pyspark.sql.functions import when, lit, col

transactions_silver = transactions_silver.withColumn(
    "transaction_type",
    when(
        col("deposit_amt").isNotNull() &
        (col("deposit_amt") > 0),
        lit("DEPOSIT")
    )
    .when(
        col("withdrawal_amt").isNotNull() &
        (col("withdrawal_amt") > 0),
        lit("WITHDRAWAL")
    )
    .otherwise(lit("UNKNOWN"))
)

# COMMAND ----------

display(
    transactions_silver.groupBy("transaction_type").count()
)

# COMMAND ----------

transactions_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bank_transactions_silver")

# COMMAND ----------

spark.sql("""
    SELECT
        transaction_type,
        COUNT(*) AS total_transactions
    FROM bank_transactions_silver
    GROUP BY transaction_type
    ORDER BY total_transactions DESC
""").show()