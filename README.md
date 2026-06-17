# Azure End-to-End Card Transaction ETL Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apache-spark&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)

## Overview
Cloud-based ETL pipeline that ingests raw Master/Visa card transaction 
records, cleans and transforms them using PySpark on Azure Databricks, 
and stores processed data in Azure Data Lake Storage (ADLS Gen2).

---

## Architecture
Raw Transaction Files (CSV)

│

▼

┌─────────────────────┐

│  ADLS Gen2 (Input)  │  ← Bronze Layer (Raw Data)

└─────────────────────┘

│

▼

┌─────────────────────┐

│ Azure Data Factory  │  ← Orchestration & Automation

│       (ADF)         │

└─────────────────────┘

│

▼

┌─────────────────────┐

│ Azure Databricks    │  ← PySpark Transformation

│    (PySpark)        │

└─────────────────────┘

│

▼

┌─────────────────────┐

│  ADLS Gen2 (Output) │  ← Silver Layer (Cleaned Data)

└─────────────────────┘

---

## Tech Stack
| Layer | Technology |
|---|---|
| Storage (Input/Output) | Azure Data Lake Storage Gen2 (ADLS) |
| Orchestration | Azure Data Factory (ADF) |
| Processing | Azure Databricks |
| Transformation | PySpark |
| Language | Python 3 |
| Output Format | Parquet |

---

## Project Structure
azure-card-transaction-etl-pipeline/

├── notebooks/

│   └── card_transaction_etl.py   # PySpark transformation script

├── data/

│   └── sample_transactions.csv   # Sample Master/Visa transaction data

├── docs/

│   └── architecture.md           # Detailed pipeline documentation

└── README.md

---

## Key Transformations
1. **Deduplication** — Remove duplicate records on `transaction_id`
2. **Null Handling** — Drop records missing critical fields
3. **Standardisation** — Uppercase and trim `card_type`, `status`, `currency`
4. **Validation** — Filter only valid card types (MASTER / VISA)
5. **Date Parsing** — Convert raw date strings to `DateType`
6. **Feature Engineering** — Derive `transaction_category` (LOW / MEDIUM / HIGH)

---

## Sample Output
| transaction_id | card_type | amount | status | transaction_category |
|---|---|---|---|---|
| TXN001 | MASTER | 2500.00 | SUCCESS | MEDIUM |
| TXN004 | VISA | 15000.00 | SUCCESS | HIGH |
| TXN005 | MASTER | 320.75 | SUCCESS | LOW |

---

## How to Run Locally
```bash
# Install dependencies
pip install pyspark

# Run the ETL script
python notebooks/card_transaction_etl.py
```

> **In production:** Replace local paths with your ADLS Gen2 
> `abfss://` paths and run directly on Azure Databricks.

---

## Author
**Avanit Singh** — Data Engineer | Fintech & BFSI Domain  
[LinkedIn](https://linkedin.com/in/[your-id]) · 
[GitHub](https://github.com/avanit-singh)
