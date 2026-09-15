# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FinGuard - 01 Bronze Ingestion
# MAGIC Load PaySim transaction data into the logged-in student's Bronze schema.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "bootcamp_students"
USER_EMAIL = spark.sql("SELECT current_user() AS user_email").first()["user_email"]
USERNAME = USER_EMAIL.split("@")[0].replace(".", "_").replace("-", "_")

BRONZE_SCHEMA = f"{USERNAME}_bronze"
BRONZE_SCHEMA_FQN = f"{CATALOG}.{BRONZE_SCHEMA}"
RAW_VOLUME = f"{BRONZE_SCHEMA_FQN}.raw"

print(f"Logged-in Databricks user: {USER_EMAIL}")
print(f"Bronze schema: {BRONZE_SCHEMA_FQN}")
print(f"Raw Volume: {RAW_VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Schema and Raw Volume
# MAGIC Create the student-specific schema and raw volume before running ingestion.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA_FQN}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {RAW_VOLUME}")

SOURCE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/raw/paysim"
TARGET = f"{BRONZE_SCHEMA_FQN}.bronze_transactions"

# Create the paysim folder inside the raw volume
dbutils.fs.mkdirs(SOURCE_PATH)

print(f"Raw volume ready: {RAW_VOLUME}")
print(f"Source path: {SOURCE_PATH}")
print(f"Target table: {TARGET}")

# Display files inside the paysim folder
display(dbutils.fs.ls(SOURCE_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest PaySim Transactions
# MAGIC Read the source CSV, add audit fields, remove existing transaction IDs, and append new rows.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

schema = T.StructType(
    [
        T.StructField("step", T.LongType(), False),
        T.StructField("type", T.StringType(), False),
        T.StructField("amount", T.DoubleType(), False),
        T.StructField("nameOrig", T.StringType(), False),
        T.StructField("oldbalanceOrg", T.DoubleType(), True),
        T.StructField("newbalanceOrig", T.DoubleType(), True),
        T.StructField("nameDest", T.StringType(), False),
        T.StructField("oldbalanceDest", T.DoubleType(), True),
        T.StructField("newbalanceDest", T.DoubleType(), True),
        T.StructField("isFraud", T.IntegerType(), True),
        T.StructField("isFlaggedFraud", T.IntegerType(), True),
    ]
)

# Read PaySim CSV and capture the Unity Catalog source file path.
raw = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load(SOURCE_PATH)
    .select(
        "*",
        F.col("_metadata.file_path").alias("source_file"),
    )
)

# PaySim step represents elapsed hours rather than a calendar timestamp.
# Use a deterministic epoch for reproducible timestamps and dates.
base_epoch_seconds = 1_735_689_600  # 2025-01-01T00:00:00Z

enriched = (
    raw.withColumn(
        "transaction_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("step").cast("string"),
                F.col("type"),
                F.col("amount").cast("string"),
                F.col("nameOrig"),
                F.col("nameDest"),
                F.col("oldbalanceOrg").cast("string"),
                F.col("newbalanceOrig").cast("string"),
            ),
            256,
        ),
    )
    .withColumn(
        "transaction_type",
        F.upper(F.trim(F.col("type"))),
    )
    .withColumn("customer_id", F.col("nameOrig"))
    .withColumn("destination_id", F.col("nameDest"))
    .withColumn("source_currency", F.lit("USD"))
    .withColumn(
        "event_timestamp",
        F.from_unixtime(
            F.lit(base_epoch_seconds)
            + (F.col("step") * F.lit(3600))
        ).cast("timestamp"),
    )
    .withColumn(
        "event_date",
        F.to_date(F.col("event_timestamp")),
    )
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp(),
    )
    .select(
        "transaction_id",
        "step",
        "transaction_type",
        F.col("amount")
        .cast("decimal(18,2)")
        .alias("amount"),
        "customer_id",
        F.col("oldbalanceOrg")
        .cast("decimal(18,2)")
        .alias("old_balance_origin"),
        F.col("newbalanceOrig")
        .cast("decimal(18,2)")
        .alias("new_balance_origin"),
        "destination_id",
        F.col("oldbalanceDest")
        .cast("decimal(18,2)")
        .alias("old_balance_destination"),
        F.col("newbalanceDest")
        .cast("decimal(18,2)")
        .alias("new_balance_destination"),
        "source_currency",
        F.col("isFraud").alias("is_fraud"),
        F.col("isFlaggedFraud").alias("is_flagged_fraud"),
        "event_timestamp",
        "event_date",
        "source_file",
        "ingestion_timestamp",
    )
    .dropDuplicates(["transaction_id"])
)

# Prevent duplicates when the Bronze table already exists.
if spark.catalog.tableExists(TARGET):
    existing = spark.table(TARGET).select("transaction_id")

    new_rows = enriched.join(
        existing,
        on="transaction_id",
        how="left_anti",
    )
else:
    # First execution: use all enriched records.
    new_rows = enriched

rows_to_append = new_rows.count()

if rows_to_append > 0:
    (
        new_rows.write.format("delta")
        .mode("append")
        .saveAsTable(TARGET)
    )

    print(f"Bronze rows appended: {rows_to_append:,}")
else:
    print("No new Bronze rows to append.")

print(f"Bronze target table: {TARGET}")


# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS bronze_row_count
FROM bootcamp_students.premetl9_bronze.bronze_transactions
""").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT transaction_id) AS unique_transactions
# MAGIC FROM bootcamp_students.premetl9_bronze.bronze_transactions;