# Databricks notebook source
# MAGIC %md
# MAGIC ## SQL Banking Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_customers
# MAGIC FROM customers_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Account

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_accounts
# MAGIC FROM accounts_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Account Type Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     account_type,
# MAGIC     COUNT(*) AS total_accounts,
# MAGIC     ROUND(SUM(balance_usd), 2) AS total_balance,
# MAGIC     ROUND(AVG(balance_usd), 2) AS average_balance
# MAGIC FROM accounts_silver
# MAGIC GROUP BY account_type
# MAGIC ORDER BY total_accounts DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Transaction Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_FORMAT(transaction_date, 'yyyy-MM') AS transaction_month,
# MAGIC     COUNT(*) AS total_transactions,
# MAGIC     ROUND(SUM(deposit_amt), 2) AS total_deposits,
# MAGIC     ROUND(SUM(withdrawal_amt), 2) AS total_withdrawals
# MAGIC FROM bank_transactions_silver
# MAGIC GROUP BY DATE_FORMAT(transaction_date, 'yyyy-MM')
# MAGIC ORDER BY transaction_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 Accounts by Balance

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     account_id,
# MAGIC     customer_id,
# MAGIC     account_type,
# MAGIC     ROUND(balance_usd, 2) AS balance_usd,
# MAGIC     open_date
# MAGIC FROM accounts_silver
# MAGIC ORDER BY balance_usd DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Account Type with Highest Average Balance

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     account_type,
# MAGIC     COUNT(*) AS total_accounts,
# MAGIC     ROUND(AVG(balance_usd), 2) AS average_balance
# MAGIC FROM accounts_silver
# MAGIC GROUP BY account_type
# MAGIC ORDER BY average_balance DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Total Deposit and Withdrawal Amount

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(SUM(deposit_amt), 2) AS total_deposits,
# MAGIC     ROUND(SUM(withdrawal_amt), 2) AS total_withdrawals
# MAGIC FROM bank_transactions_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaction Type Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_type,
# MAGIC     COUNT(*) AS total_transactions
# MAGIC FROM bank_transactions_silver
# MAGIC GROUP BY transaction_type
# MAGIC ORDER BY total_transactions DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Account Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.city,
# MAGIC     COUNT(a.account_id) AS total_accounts,
# MAGIC     ROUND(COALESCE(SUM(a.balance_usd), 0), 2) AS total_balance
# MAGIC FROM customers_silver c
# MAGIC LEFT JOIN accounts_silver a
# MAGIC     ON c.customer_id = a.customer_id
# MAGIC GROUP BY
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.city
# MAGIC ORDER BY total_balance DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers with Multiple Accounts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     COUNT(account_id) AS total_accounts
# MAGIC FROM accounts_silver
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(account_id) > 1
# MAGIC ORDER BY total_accounts DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 Customers by Total Loan Amount

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.city,
# MAGIC     COUNT(l.loan_id) AS total_loans,
# MAGIC     ROUND(SUM(l.loan_amount), 2) AS total_loan_amount
# MAGIC FROM customers_silver c
# MAGIC INNER JOIN loans_silver l
# MAGIC     ON c.customer_id = l.customer_id
# MAGIC GROUP BY
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.city
# MAGIC ORDER BY total_loan_amount DESC
# MAGIC LIMIT 10;

# COMMAND ----------

print(spark.table("loans_silver").columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loan Amount by Interest Rate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(interest_rate, 2) AS interest_rate,
# MAGIC     COUNT(*) AS total_loans,
# MAGIC     ROUND(SUM(loan_amount), 2) AS total_loan_amount,
# MAGIC     ROUND(AVG(loan_amount), 2) AS average_loan_amount
# MAGIC FROM loans_silver
# MAGIC GROUP BY interest_rate
# MAGIC ORDER BY interest_rate;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 Customers by Total Loan Amount

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.city,
# MAGIC     COUNT(l.loan_id) AS total_loans,
# MAGIC     ROUND(SUM(l.loan_amount), 2) AS total_loan_amount
# MAGIC FROM customers_silver c
# MAGIC INNER JOIN loans_silver l
# MAGIC     ON c.customer_id = l.customer_id
# MAGIC GROUP BY
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.city
# MAGIC ORDER BY total_loan_amount DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Loan Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_FORMAT(start_date, 'yyyy-MM') AS loan_month,
# MAGIC     COUNT(*) AS total_loans,
# MAGIC     ROUND(SUM(loan_amount), 2) AS total_loan_amount,
# MAGIC     ROUND(AVG(interest_rate), 2) AS average_interest_rate
# MAGIC FROM loans_silver
# MAGIC GROUP BY DATE_FORMAT(start_date, 'yyyy-MM')
# MAGIC ORDER BY loan_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Card Type Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     card_type,
# MAGIC     COUNT(*) AS total_cards,
# MAGIC     COUNT(DISTINCT account_id) AS unique_accounts
# MAGIC FROM cards_silver
# MAGIC GROUP BY card_type
# MAGIC ORDER BY total_cards DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cards per Account

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     account_id,
# MAGIC     COUNT(card_id) AS total_cards
# MAGIC FROM cards_silver
# MAGIC GROUP BY account_id
# MAGIC ORDER BY total_cards DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC # Credit Score Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #### Credit Score Category Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN credit_score >= 750 THEN 'Excellent'
# MAGIC         WHEN credit_score >= 650 THEN 'Good'
# MAGIC         WHEN credit_score >= 550 THEN 'Fair'
# MAGIC         ELSE 'Poor'
# MAGIC     END AS credit_category,
# MAGIC     COUNT(*) AS total_customers,
# MAGIC     ROUND(AVG(credit_score), 2) AS average_credit_score
# MAGIC FROM customers_silver
# MAGIC GROUP BY
# MAGIC     CASE
# MAGIC         WHEN credit_score >= 750 THEN 'Excellent'
# MAGIC         WHEN credit_score >= 650 THEN 'Good'
# MAGIC         WHEN credit_score >= 550 THEN 'Fair'
# MAGIC         ELSE 'Poor'
# MAGIC     END
# MAGIC ORDER BY average_credit_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top 10 Customers by Credit Score

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     city,
# MAGIC     credit_score
# MAGIC FROM customers_silver
# MAGIC ORDER BY credit_score DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Average Credit Score by City

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     city,
# MAGIC     COUNT(*) AS total_customers,
# MAGIC     ROUND(AVG(credit_score), 2) AS average_credit_score,
# MAGIC     MAX(credit_score) AS highest_credit_score,
# MAGIC     MIN(credit_score) AS lowest_credit_score
# MAGIC FROM customers_silver
# MAGIC GROUP BY city
# MAGIC ORDER BY average_credit_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # Customer Ranking

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rank Customers by Credit Score

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     city,
# MAGIC     credit_score,
# MAGIC     RANK() OVER (
# MAGIC         ORDER BY credit_score DESC
# MAGIC     ) AS credit_rank
# MAGIC FROM customers_silver
# MAGIC ORDER BY credit_rank
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rank Customers Within Each City

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     city,
# MAGIC     credit_score,
# MAGIC     RANK() OVER (
# MAGIC         PARTITION BY city
# MAGIC         ORDER BY credit_score DESC
# MAGIC     ) AS city_credit_rank
# MAGIC FROM customers_silver
# MAGIC ORDER BY city, city_credit_rank;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top 3 Customers from Each City

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ranked_customers AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         first_name,
# MAGIC         last_name,
# MAGIC         city,
# MAGIC         credit_score,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY city
# MAGIC             ORDER BY credit_score DESC
# MAGIC         ) AS row_num
# MAGIC     FROM customers_silver
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     city,
# MAGIC     credit_score
# MAGIC FROM ranked_customers
# MAGIC WHERE row_num <= 3
# MAGIC ORDER BY city, credit_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a Reusable SQL View

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW customer_credit_ranking AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     city,
# MAGIC     credit_score,
# MAGIC     RANK() OVER (
# MAGIC         ORDER BY credit_score DESC
# MAGIC     ) AS credit_rank
# MAGIC FROM customers_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM customer_credit_ranking
# MAGIC ORDER BY credit_rank
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC # Monthly Transaction Analysis View

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create View

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW monthly_transaction_analysis AS
# MAGIC SELECT
# MAGIC     DATE_FORMAT(transaction_date, 'yyyy-MM') AS transaction_month,
# MAGIC     COUNT(*) AS total_transactions,
# MAGIC     ROUND(SUM(deposit_amt), 2) AS total_deposits,
# MAGIC     ROUND(SUM(withdrawal_amt), 2) AS total_withdrawals,
# MAGIC     ROUND(
# MAGIC         SUM(deposit_amt) - SUM(withdrawal_amt),
# MAGIC         2
# MAGIC     ) AS net_transaction_amount
# MAGIC FROM bank_transactions_silver
# MAGIC GROUP BY DATE_FORMAT(transaction_date, 'yyyy-MM');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM monthly_transaction_analysis
# MAGIC ORDER BY transaction_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM monthly_transaction_analysis
# MAGIC ORDER BY total_transactions DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM monthly_transaction_analysis
# MAGIC ORDER BY transaction_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM monthly_transaction_analysis
# MAGIC ORDER BY total_deposits DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Loan Analytics View

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW loan_analysis AS 
# MAGIC SELECT 
# MAGIC     DATE_FORMAT(start_date, "yyyy-MM") AS loan_month,
# MAGIC     COUNT(*) AS total_loans,
# MAGIC     ROUND(SUM(loan_amount), 2) AS total_loan_amount,
# MAGIC     ROUND(AVG(loan_amount), 2) AS average_loan_amount,
# MAGIC     ROUND(AVG(interest_rate), 2) AS average_interest_rate,
# MAGIC     ROUND(MAX(loan_amount), 2) AS highest_loan_amount,
# MAGIC     ROUND(MIN(loan_amount), 2) AS lowest_loan_amount
# MAGIC FROM loans_silver
# MAGIC GROUP BY DATE_FORMAT(start_date, 'yyyy-MM');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC     FROM loan_analysis
# MAGIC ORDER BY loan_month    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the Month with Highest Average Interest Rate

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM loan_analysis
# MAGIC ORDER BY average_interest_rate DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the Month with Highest Loan Amount

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM loan_analysis
# MAGIC ORDER BY total_loan_amount DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Customer 360 View

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW customer_360_summary AS
# MAGIC
# MAGIC WITH account_summary AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         COUNT(account_id) AS total_accounts,
# MAGIC         ROUND(SUM(balance_usd), 2) AS total_balance
# MAGIC     FROM accounts_silver
# MAGIC     GROUP BY customer_id
# MAGIC ),
# MAGIC
# MAGIC loan_summary AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         COUNT(loan_id) AS total_loans,
# MAGIC         ROUND(SUM(loan_amount), 2) AS total_loan_amount
# MAGIC     FROM loans_silver
# MAGIC     GROUP BY customer_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.city,
# MAGIC     c.credit_score,
# MAGIC
# MAGIC     COALESCE(a.total_accounts, 0) AS total_accounts,
# MAGIC     COALESCE(a.total_balance, 0) AS total_balance,
# MAGIC
# MAGIC     COALESCE(l.total_loans, 0) AS total_loans,
# MAGIC     COALESCE(l.total_loan_amount, 0) AS total_loan_amount
# MAGIC
# MAGIC FROM customers_silver c
# MAGIC
# MAGIC LEFT JOIN account_summary a
# MAGIC     ON c.customer_id = a.customer_id
# MAGIC
# MAGIC LEFT JOIN loan_summary l
# MAGIC     ON c.customer_id = l.customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM customer_360_summary
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find Top Customers by Total Balance

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     city,
# MAGIC     total_accounts,
# MAGIC     total_balance,
# MAGIC     total_loans,
# MAGIC     total_loan_amount
# MAGIC FROM customer_360_summary
# MAGIC ORDER BY total_balance DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Find Customers with Both Accounts and Loans

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM customer_360_summary
# MAGIC WHERE total_accounts > 0
# MAGIC   AND total_loans > 0
# MAGIC ORDER BY total_balance DESC
# MAGIC LIMIT 20;