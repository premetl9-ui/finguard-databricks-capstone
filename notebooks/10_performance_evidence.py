# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FinGuard - 10 Performance Evidence
# MAGIC Capture reproducible scale, storage, and representative Spark query metrics
# MAGIC for the validated 6.36M-row Lakehouse.

# COMMAND ----------

import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "bootcamp_students"

USER_EMAIL = (
    spark.sql("SELECT current_user() AS user_email")
    .first()["user_email"]
)

USERNAME = (
    USER_EMAIL.split("@")[0]
    .replace(".", "_")
    .replace("-", "_")
)

BRONZE = (
    f"{CATALOG}.{USERNAME}_bronze."
    "bronze_transactions"
)

SILVER = (
    f"{CATALOG}.{USERNAME}_silver."
    "silver_transactions"
)

GOLD = (
    f"{CATALOG}.{USERNAME}_gold."
    "gold_transaction_risk"
)

RESULTS = (
    f"{CATALOG}.{USERNAME}_operations."
    "performance_evidence_results"
)

RUN_ID = str(uuid.uuid4())
MEASURED_AT = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Scale and Physical Storage

# COMMAND ----------

def table_detail(table_name: str):
    row = (
        spark.sql(f"DESCRIBE DETAIL {table_name}")
        .first()
    )
    return {
        "num_files": int(row["numFiles"] or 0),
        "size_bytes": int(row["sizeInBytes"] or 0),
        "clustering_columns": (
            str(row.asDict().get("clusteringColumns"))
            if row.asDict().get("clusteringColumns") is not None
            else ""
        ),
    }


def timed_count(table_name: str):
    started = time.perf_counter()
    count = spark.table(table_name).count()
    elapsed = time.perf_counter() - started
    return count, elapsed


def timed_query(query: str):
    started = time.perf_counter()
    rows = spark.sql(query).collect()
    elapsed = time.perf_counter() - started
    return rows, elapsed


bronze_detail = table_detail(BRONZE)
silver_detail = table_detail(SILVER)
gold_detail = table_detail(GOLD)

bronze_rows, bronze_count_seconds = timed_count(BRONZE)
silver_rows, silver_count_seconds = timed_count(SILVER)
gold_rows, gold_count_seconds = timed_count(GOLD)

_, risk_distribution_seconds = timed_query(f"""
SELECT
    risk_level,
    COUNT(*) AS transaction_count
FROM {GOLD}
GROUP BY risk_level
ORDER BY risk_level
""")

_, customer_lookup_seconds = timed_query(f"""
SELECT
    customer_id,
    COUNT(*) AS transaction_count,
    MAX(risk_score) AS max_risk_score
FROM {GOLD}
GROUP BY customer_id
ORDER BY max_risk_score DESC, transaction_count DESC
LIMIT 100
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Evidence

# COMMAND ----------

schema = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("measured_at", T.TimestampType(), False),
        T.StructField("table_name", T.StringType(), False),
        T.StructField("row_count", T.LongType(), False),
        T.StructField("num_files", T.LongType(), False),
        T.StructField("size_bytes", T.LongType(), False),
        T.StructField("clustering_columns", T.StringType(), True),
        T.StructField("count_query_seconds", T.DoubleType(), False),
        T.StructField("risk_distribution_seconds", T.DoubleType(), True),
        T.StructField("customer_lookup_seconds", T.DoubleType(), True),
    ]
)

rows = [
    (
        RUN_ID,
        MEASURED_AT,
        BRONZE,
        bronze_rows,
        bronze_detail["num_files"],
        bronze_detail["size_bytes"],
        bronze_detail["clustering_columns"],
        bronze_count_seconds,
        None,
        None,
    ),
    (
        RUN_ID,
        MEASURED_AT,
        SILVER,
        silver_rows,
        silver_detail["num_files"],
        silver_detail["size_bytes"],
        silver_detail["clustering_columns"],
        silver_count_seconds,
        None,
        None,
    ),
    (
        RUN_ID,
        MEASURED_AT,
        GOLD,
        gold_rows,
        gold_detail["num_files"],
        gold_detail["size_bytes"],
        gold_detail["clustering_columns"],
        gold_count_seconds,
        risk_distribution_seconds,
        customer_lookup_seconds,
    ),
]

(
    spark.createDataFrame(rows, schema=schema)
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(RESULTS)
)

print(f"Performance evidence run: {RUN_ID}")
print(
    f"Bronze: rows={bronze_rows:,}, "
    f"files={bronze_detail['num_files']}, "
    f"size={bronze_detail['size_bytes']:,} bytes, "
    f"count={bronze_count_seconds:.3f}s"
)
print(
    f"Silver: rows={silver_rows:,}, "
    f"files={silver_detail['num_files']}, "
    f"size={silver_detail['size_bytes']:,} bytes, "
    f"count={silver_count_seconds:.3f}s"
)
print(
    f"Gold: rows={gold_rows:,}, "
    f"files={gold_detail['num_files']}, "
    f"size={gold_detail['size_bytes']:,} bytes, "
    f"count={gold_count_seconds:.3f}s"
)
print(
    "Representative Gold queries: "
    f"risk_distribution={risk_distribution_seconds:.3f}s, "
    f"customer_lookup={customer_lookup_seconds:.3f}s"
)

display(
    spark.table(RESULTS)
    .filter(F.col("run_id") == RUN_ID)
    .orderBy("table_name")
)
