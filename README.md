# 🏦 Banking Data Engineering Pipeline using Databricks

An end-to-end **Banking Data Engineering project** built using **Databricks, PySpark, SQL, and Delta Lake**. The project follows the **Medallion Architecture (Bronze, Silver, Gold)** to transform raw banking data into clean, validated, and analytics-ready datasets.

## 📌 Project Overview

This project demonstrates a complete banking data engineering workflow:

**Raw CSV/Excel → Bronze → Silver → Data Quality → Data Modeling → Gold → SQL Analytics → Databricks Dashboard**

The pipeline processes Customers, Accounts, Branches, Loans, Merchants, Cards, and Bank Transactions.

## 🎯 Objectives

- Build an end-to-end ETL/data engineering pipeline.
- Ingest raw banking data into Databricks.
- Implement Bronze, Silver, and Gold layers.
- Clean, standardize, and validate data with PySpark.
- Perform data quality checks.
- Build a dimensional-style data model.
- Create business-ready Gold datasets.
- Perform SQL analytics.
- Build a Databricks banking analytics dashboard.
- Document the project and maintain it in GitHub.

## 🛠️ Technologies

| Technology | Purpose |
|---|---|
| Python | Data processing |
| PySpark | Distributed transformations |
| SQL | Analytics |
| Databricks | Data engineering platform |
| Delta Lake | Reliable data storage |
| Medallion Architecture | Pipeline design |
| GitHub | Version control |

## 🏗️ Architecture

```text
Raw CSV / Excel Files
        ↓
Bronze Layer
        ↓
Silver Layer
        ↓
Data Quality Checks
        ↓
Data Modeling
        ↓
Gold Layer
        ↓
SQL Analytics
        ↓
Databricks Dashboard
```

## 🥉 Bronze Layer

Raw source data is ingested into Delta tables with minimal transformation.

- `customers_bronze`
- `accounts_bronze`
- `branches_bronze`
- `loans_bronze`
- `merchants_bronze`
- `cards_bronze`
- `bank_transactions_bronze`

## 🥈 Silver Layer

The Silver layer cleans and standardizes the source data.

### Transformations

- Column standardization
- Data type conversion
- Duplicate detection/removal
- Null validation
- Business-rule validation
- Transaction type derivation
- Account number cleaning
- Processing timestamp creation
- Relationship validation

### Silver Tables

- `customers_silver`
- `accounts_silver`
- `branches_silver`
- `loans_silver`
- `merchants_silver`
- `cards_silver`
- `bank_transactions_silver`

## 🔍 Data Quality

Checks include:

- Primary-key duplicate checks
- Null checks
- Credit-score validation
- Duplicate transaction detection
- Data-type validation
- Transaction-type validation
- Relationship validation

### Dataset Scale

| Dataset | Records |
|---|---:|
| Customers | 50,000 |
| Accounts | 75,000 |
| Branches | 500 |
| Loans | 30,000 |
| Merchants | 5,000 |
| Cards | 100,000 |
| Bank Transactions | 116,162 |

Major Silver dimension-table primary-key duplicate checks were validated at zero duplicates.

## 🧩 Data Modeling

### Dimension Tables

- Customers
- Accounts
- Branches
- Loans
- Cards
- Merchants

### Transaction Fact Data

- Bank Transactions

### Validated Relationships

```text
Customers
   │
   ├── Accounts
   │      │
   │      └── Cards
   │
   └── Loans
```

The source transaction account numbers use a different identifier format from the synthetic account IDs. The original source account number is therefore preserved instead of forcing an incorrect join.

## 🥇 Gold Layer

The Gold layer contains business-ready analytical datasets, including:

- Account Type Summary
- Customer Summary
- Loan Summary
- Monthly Transaction Analysis
- Card Analysis

## 📊 SQL Analytics

The project includes analysis for:

- Total customers and accounts
- Account type distribution
- Monthly transactions
- Deposits and withdrawals
- Customer-account summaries
- Customers with multiple accounts
- Loan analysis
- Credit-score analysis
- Card analysis
- SQL window functions
- Customer 360 analysis

## 📈 Databricks Dashboard

### KPIs

- **Total Customers:** 50,000
- **Total Accounts:** 75,000
- **Total Bank Balance:** displayed in the dashboard

### Visualizations

- Monthly Deposit vs Withdrawal
- Accounts by Account Type
- Credit Score Distribution
- Card Type Distribution

### Dashboard Screenshot

![Banking Analytics Dashboard](dashboard/screenshots/banking-analytics-dashboard.png)

## 🔄 Pipeline Workflow

```text
01 Source Data Profiling
        ↓
02 Bronze Data Ingestion
        ↓
03 Silver Data Transformation
        ↓
04 Data Quality Checks
        ↓
05 Data Modeling
        ↓
06 Gold Data Transformation
        ↓
07 SQL Banking Analysis
        ↓
08 Project Documentation
        ↓
Databricks Dashboard
```

## 🚧 Challenges Addressed

### Duplicate Transactions
Duplicate transaction records were identified during data quality validation and handled during transformation.

### Different Account ID Formats
Transaction source account numbers use a different format from the synthetic account IDs. The source identifier was retained rather than creating an incorrect relationship.

### Missing Branch Relationship
The Accounts dataset does not contain `branch_id`, so a direct branch-to-account relationship could not be established from the available source data.

### Data Quality
Null checks, duplicate checks, key validation, and business-rule validation were performed before analytics.

## 📂 Project Structure

```text
banking-data-engineering-databricks/
│
├── README.md
├── 01_Source_Data_Profiling.ipynb
├── 02_Bronze_Data_Ingestion.ipynb
├── 03_Silver_Data_Transformation.ipynb
├── 04_Data_Quality_Checks.ipynb
├── 05_Data_Modeling.ipynb
├── 06_Gold_Data_Transformation.ipynb
├── 07_SQL_Banking_Analysis.ipynb
├── 08_Project_Documentation.ipynb
│
└── dashboard/
    └── screenshots/
        └── banking-analytics-dashboard.png
```

## 📚 Key Learning Outcomes

- End-to-end ETL pipeline development
- PySpark DataFrame transformations
- Databricks notebooks
- Delta Lake
- Medallion Architecture
- Data quality engineering
- Data modeling
- SQL analytics and window functions
- Business aggregations
- Dashboard development
- GitHub version control

## 🚀 Future Improvements

- Incremental data ingestion
- Automated pipeline scheduling
- Streaming transaction ingestion
- Advanced monitoring and alerting
- Cloud object-storage integration
- Automated data-quality reporting
- CI/CD integration

## 👨‍💻 Author

**Bablu Kumar Saw**  
B.Tech – Computer Science & Engineering  
Aspiring Data Engineer

