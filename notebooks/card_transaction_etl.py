# =============================================================
# Azure Card Transaction ETL Pipeline
# Author  : Avanit Singh
# Stack   : PySpark | Azure Databricks | ADLS Gen2
# Purpose : Ingest raw Master/Visa transaction records,
#           clean and transform using PySpark,
#           write processed data back to ADLS Gen2
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, upper, trim, when,
    count, sum as spark_sum, round as spark_round
)

# ── 1. Initialise Spark Session ───────────────────────────────
spark = SparkSession.builder \
    .appName("CardTransactionETL") \
    .getOrCreate()

print("Spark Session started.")

# ── 2. BRONZE LAYER — Read Raw Data from ADLS Gen2 ───────────
# In production: replace with your ADLS Gen2 path
# abfss://input@<storage_account>.dfs.core.windows.net/transactions/

raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/sample_transactions.csv")

print(f"\nRaw record count : {raw_df.count()}")
print("\nSchema:")
raw_df.printSchema()
print("\nSample Raw Data:")
raw_df.show(5, truncate=False)

# ── 3. SILVER LAYER — Cleaning & Transformation ───────────────

# 3a. Remove duplicates on transaction_id
cleaned_df = raw_df.dropDuplicates(["transaction_id"])

# 3b. Drop records with nulls in critical columns
cleaned_df = cleaned_df.dropna(
    subset=["transaction_id", "card_type", "amount", "transaction_date"]
)

# 3c. Standardise text fields
cleaned_df = cleaned_df \
    .withColumn("card_type", upper(trim(col("card_type")))) \
    .withColumn("status",    upper(trim(col("status"))))   \
    .withColumn("currency",  upper(trim(col("currency"))))

# 3d. Filter valid card types only
cleaned_df = cleaned_df.filter(
    col("card_type").isin(["MASTER", "VISA"])
)

# 3e. Cast and parse date column
cleaned_df = cleaned_df.withColumn(
    "transaction_date",
    to_date(col("transaction_date"), "yyyy-MM-dd")
)

# 3f. Add business-derived column: transaction category by amount
cleaned_df = cleaned_df.withColumn(
    "transaction_category",
    when(col("amount") < 1000,  "LOW")
   .when(col("amount") < 10000, "MEDIUM")
   .otherwise("HIGH")
)

print(f"\nClean record count : {cleaned_df.count()}")
print("\nCleaned Schema:")
cleaned_df.printSchema()
print("\nSample Cleaned Data:")
cleaned_df.show(5, truncate=False)

# ── 4. Business Insights — Summary Aggregation ────────────────
print("\n--- Transaction Summary by Card Type ---")
cleaned_df.groupBy("card_type").agg(
    count("transaction_id").alias("total_transactions"),
    spark_round(spark_sum("amount"), 2).alias("total_amount_INR")
).show()

print("\n--- Transaction Summary by Status ---")
cleaned_df.groupBy("status").agg(
    count("transaction_id").alias("count")
).show()

print("\n--- Category Distribution ---")
cleaned_df.groupBy("transaction_category").agg(
    count("transaction_id").alias("count"),
    spark_round(spark_sum("amount"), 2).alias("total_value")
).orderBy("transaction_category").show()

# ── 5. Write Processed Data to ADLS Gen2 Output ──────────────
# In production: replace with your ADLS Gen2 output path
# abfss://output@<storage_account>.dfs.core.windows.net/transactions_cleaned/

cleaned_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("output/transactions_cleaned")

print("\nETL Pipeline completed. Processed data written to output layer.")
spark.stop()
