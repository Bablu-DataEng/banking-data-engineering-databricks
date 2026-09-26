# 🏦 Banking Data Engineering Pipeline

An end-to-end **Banking Data Engineering project** built using **Databricks, PySpark, SQL, and Delta Lake**. The project follows the **Medallion Architecture (Bronze, Silver, Gold)** to transform raw banking data into clean, validated, and analytics-ready datasets.

---

## 📌 Project Overview

The objective of this project is to build a scalable data engineering pipeline for banking data.

The pipeline processes multiple banking datasets such as:

- Customers
- Accounts
- Branches
- Loans
- Merchants
- Cards
- Bank Transactions

The processed data is then used for SQL-based analytics and a Databricks dashboard.

---

## 🏗️ Architecture

```text
                Raw Data
           CSV / Excel Files
                  │
                  ▼
        ┌───────────────────┐
        │   Bronze Layer    │
        │  Raw Delta Tables │
        └─────────┬─────────┘
                  │
                  ▼
        ┌───────────────────┐
        │   Silver Layer    │
        │ Clean & Transform │
        └─────────┬─────────┘
                  │
                  ▼
        ┌───────────────────┐
        │  Data Quality     │
        │ Checks & Validate │
        └─────────┬─────────┘
                  │
                  ▼
        ┌───────────────────┐
        │   Data Modeling   │
        │ Dimensions & Fact │
        └─────────┬─────────┘
                  │
                  ▼
        ┌───────────────────┐
        │    Gold Layer     │
        │ Business Summary  │
        └─────────┬─────────┘
                  │
                  ▼
        ┌───────────────────┐
        │   SQL Analytics   │
        └─────────┬─────────┘
                  │
                  ▼
        ┌───────────────────┐
        │    Dashboard      │
        │ Banking Analytics │
        └───────────────────┘
