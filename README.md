# Azure End-to-End Card Transaction ETL Pipeline

## Overview
Cloud-based ETL pipeline that ingests Master/Visa card transactions 
records, processes them using PySpark on Azure Databricks, and 
stores cleaned data in Azure Data Lake Storage (ADLS Gen2).

## Architecture
ADLS Gen2 (Raw Input) → Azure Data Factory (Orchestration) 
→ Azure Databricks / PySpark (Transformation) 
→ ADLS Gen2 (Processed Output)

## Tech Stack
- Azure Data Lake Storage Gen2 (ADLS)
- Azure Data Factory (ADF)
- Azure Databricks
- PySpark
- Python

## Pipeline Steps
1. Raw Master/Visa transaction files land in the ADLS input container
2. ADF pipeline triggers automatically and picks up new files
3. PySpark scripts on Databricks clean and validate records
4. Processed data written to ADLS output container

## Key Concepts Demonstrated
- ETL Pipeline Design
- Cloud Data Orchestration
- PySpark Transformations
- Medallion Architecture (Bronze → Silver)
- Pipeline Automation
