# Databricks notebook source
# MAGIC %md
# MAGIC # FinGuard - 07 Pipeline Validation
# MAGIC Run automated reconciliation, data-quality, FX, risk, CDC, and Gold-summary checks.
# MAGIC
# MAGIC The notebook appends every validation result to the Operations schema and fails the
# MAGIC Lakeflow Job when any required check fails.

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql import Row
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

BRONZE_SCHEMA = f"{USERNAME}_bronze"
SILVER_SCHEMA = f"{USERNAME}_silver"
GOLD_SCHEMA = f"{USERNAME}_gold"
OPERATIONS_SCHEMA = f"{USERNAME}_operations"
CDC_SCHEMA = f"{USERNAME}_lakebase_cdc"

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_transactions"
BRONZE_FX = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_fx_api"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}.silver_transactions"
QUARANTINE = f"{CATALOG}.{SILVER_SCHEMA}.silver_transaction_quarantine"
SILVER_FX = f"{CATALOG}.{SILVER_SCHEMA}.silver_fx_rates"
GOLD_RISK = f"{CATALOG}.{GOLD_SCHEMA}.gold_transaction_risk"
GOLD_ALERT_SUMMARY = f"{CATALOG}.{GOLD_SCHEMA}.gold_alert_summary"
VALIDATION_TABLE = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}.pipeline_validation_results"
)

CDC_TABLES = [
    f"{CATALOG}.{CDC_SCHEMA}.lb_fraud_alerts_history",
    f"{CATALOG}.{CDC_SCHEMA}.lb_alert_status_history_history",
    f"{CATALOG}.{CDC_SCHEMA}.lb_investigations_history",
    f"{CATALOG}.{CDC_SCHEMA}.lb_agent_actions_history",
]

ALERT_THRESHOLD = 50
REQUIRED_FX_PAIRS = {
    ("USD", "EUR"),
    ("USD", "GBP"),
    ("USD", "JPY"),
}

RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc)

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{OPERATIONS_SCHEMA}"
)

print(f"Validation run: {RUN_ID}")
print(f"User: {USER_EMAIL}")
print(f"Validation audit table: {VALIDATION_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Helpers

# COMMAND ----------

results = []


def add_check(
    check_name: str,
    passed: bool,
    observed,
    expected: str,
    details: str = "",
):
    status = "PASS" if passed else "FAIL"

    results.append(
        Row(
            validation_run_id=RUN_ID,
            validation_timestamp=RUN_TS,
            check_name=check_name,
            status=status,
            observed_value=str(observed),
            expected_value=expected,
            details=details,
        )
    )

    print(
        f"[{status}] {check_name}: "
        f"observed={observed}; expected={expected}"
    )


def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def table_count(table_name: str) -> int:
    return spark.table(table_name).count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Required Table Checks

# COMMAND ----------

required_tables = {
    "bronze_transactions": BRONZE,
    "bronze_fx_api": BRONZE_FX,
    "silver_transactions": SILVER,
    "silver_transaction_quarantine": QUARANTINE,
    "silver_fx_rates": SILVER_FX,
    "gold_transaction_risk": GOLD_RISK,
    "gold_alert_summary": GOLD_ALERT_SUMMARY,
}

for logical_name, table_name in required_tables.items():
    exists = table_exists(table_name)
    add_check(
        f"table_exists::{logical_name}",
        exists,
        table_name if exists else "MISSING",
        "table exists",
    )

missing_required = [
    table_name
    for table_name in required_tables.values()
    if not table_exists(table_name)
]

if missing_required:
    # Persist the table-existence failures before stopping.
    result_schema = T.StructType(
        [
            T.StructField("validation_run_id", T.StringType(), False),
            T.StructField("validation_timestamp", T.TimestampType(), False),
            T.StructField("check_name", T.StringType(), False),
            T.StructField("status", T.StringType(), False),
            T.StructField("observed_value", T.StringType(), True),
            T.StructField("expected_value", T.StringType(), True),
            T.StructField("details", T.StringType(), True),
        ]
    )

    (
        spark.createDataFrame(results, schema=result_schema)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(VALIDATION_TABLE)
    )

    raise RuntimeError(
        "Required FinGuard tables are missing: "
        + ", ".join(missing_required)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze, Silver, and Quarantine Reconciliation

# COMMAND ----------

bronze_stats = (
    spark.table(BRONZE)
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("transaction_id").alias("unique_count"),
    )
    .first()
)

silver_stats = (
    spark.table(SILVER)
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("transaction_id").alias("unique_count"),
    )
    .first()
)

quarantine_rows = table_count(QUARANTINE)

add_check(
    "bronze_has_rows",
    bronze_stats["row_count"] > 0,
    bronze_stats["row_count"],
    "> 0",
)

add_check(
    "bronze_transaction_ids_unique",
    bronze_stats["row_count"] == bronze_stats["unique_count"],
    f"{bronze_stats['unique_count']} unique / {bronze_stats['row_count']} rows",
    "unique_transactions = total_rows",
)

add_check(
    "silver_has_rows",
    silver_stats["row_count"] > 0,
    silver_stats["row_count"],
    "> 0",
)

add_check(
    "silver_transaction_ids_unique",
    silver_stats["row_count"] == silver_stats["unique_count"],
    f"{silver_stats['unique_count']} unique / {silver_stats['row_count']} rows",
    "unique_transactions = total_rows",
)

add_check(
    "bronze_equals_silver_plus_quarantine",
    bronze_stats["row_count"]
    == silver_stats["row_count"] + quarantine_rows,
    (
        f"bronze={bronze_stats['row_count']}; "
        f"silver={silver_stats['row_count']}; "
        f"quarantine={quarantine_rows}"
    ),
    "bronze_rows = silver_rows + quarantine_rows",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Risk Validation

# COMMAND ----------

gold_stats = (
    spark.table(GOLD_RISK)
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("transaction_id").alias("unique_count"),
        F.sum(
            F.when(F.col("is_alert_candidate"), 1).otherwise(0)
        ).alias("alert_candidates"),
    )
    .first()
)

add_check(
    "gold_rows_equal_silver_rows",
    gold_stats["row_count"] == silver_stats["row_count"],
    f"gold={gold_stats['row_count']}; silver={silver_stats['row_count']}",
    "gold_rows = silver_rows",
)

add_check(
    "gold_transaction_ids_unique",
    gold_stats["row_count"] == gold_stats["unique_count"],
    f"{gold_stats['unique_count']} unique / {gold_stats['row_count']} rows",
    "unique_transactions = total_rows",
)

risk_band_mismatches = (
    spark.table(GOLD_RISK)
    .filter(
        (
            (F.col("risk_score") < 30)
            & (F.col("risk_level") != "LOW")
        )
        | (
            (F.col("risk_score").between(30, 59))
            & (F.col("risk_level") != "MEDIUM")
        )
        | (
            (F.col("risk_score").between(60, 79))
            & (F.col("risk_level") != "HIGH")
        )
        | (
            (F.col("risk_score") >= 80)
            & (F.col("risk_level") != "CRITICAL")
        )
        | (F.col("risk_score") < 0)
        | (F.col("risk_score") > 100)
    )
    .count()
)

add_check(
    "risk_band_mapping",
    risk_band_mismatches == 0,
    risk_band_mismatches,
    "0 mismatched rows",
)

alert_flag_mismatches = (
    spark.table(GOLD_RISK)
    .filter(
        F.col("is_alert_candidate")
        != (F.col("risk_score") >= F.lit(ALERT_THRESHOLD))
    )
    .count()
)

add_check(
    "alert_threshold_mapping",
    alert_flag_mismatches == 0,
    alert_flag_mismatches,
    f"0 rows inconsistent with threshold {ALERT_THRESHOLD}",
)

add_check(
    "alert_candidates_exist",
    (gold_stats["alert_candidates"] or 0) > 0,
    gold_stats["alert_candidates"] or 0,
    "> 0",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FX Validation

# COMMAND ----------

fx_pairs = {
    (row["from_currency"], row["to_currency"])
    for row in (
        spark.table(SILVER_FX)
        .filter(F.col("exchange_rate") > 0)
        .select("from_currency", "to_currency")
        .distinct()
        .collect()
    )
}

missing_fx_pairs = sorted(REQUIRED_FX_PAIRS - fx_pairs)

add_check(
    "required_fx_pairs_available",
    len(missing_fx_pairs) == 0,
    sorted(fx_pairs),
    "USD/EUR, USD/GBP, USD/JPY available with positive rates",
    (
        "Missing: " + ", ".join("/".join(pair) for pair in missing_fx_pairs)
        if missing_fx_pairs
        else ""
    ),
)

successful_fx_requests = (
    spark.table(BRONZE_FX)
    .filter(
        (F.col("ingestion_status") == "SUCCESS")
        & (F.col("from_currency") == "USD")
        & (F.col("to_currency").isin("EUR", "GBP", "JPY"))
    )
    .count()
)

add_check(
    "fx_api_success_records_exist",
    successful_fx_requests >= 3,
    successful_fx_requests,
    ">= 3 successful USD FX API audit rows",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC and Operational Analytics Validation

# COMMAND ----------

cdc_missing = [
    table_name
    for table_name in CDC_TABLES
    if not table_exists(table_name)
]

add_check(
    "cdc_history_tables_exist",
    len(cdc_missing) == 0,
    "all present" if not cdc_missing else ", ".join(cdc_missing),
    "4 required Lakebase CDC history tables exist",
)

summary_rows = table_count(GOLD_ALERT_SUMMARY)

add_check(
    "gold_alert_summary_has_rows",
    summary_rows > 0,
    summary_rows,
    "> 0",
)

invalid_success_rates = (
    spark.table(GOLD_ALERT_SUMMARY)
    .filter(
        (F.col("agent_action_success_rate") < 0)
        | (F.col("agent_action_success_rate") > 1)
    )
    .count()
)

add_check(
    "agent_action_success_rate_range",
    invalid_success_rates == 0,
    invalid_success_rates,
    "0 rows outside [0, 1]",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Validation Results and Fail the Job on Errors

# COMMAND ----------

result_schema = T.StructType(
    [
        T.StructField("validation_run_id", T.StringType(), False),
        T.StructField("validation_timestamp", T.TimestampType(), False),
        T.StructField("check_name", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("observed_value", T.StringType(), True),
        T.StructField("expected_value", T.StringType(), True),
        T.StructField("details", T.StringType(), True),
    ]
)

result_df = spark.createDataFrame(
    results,
    schema=result_schema,
)

(
    result_df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(VALIDATION_TABLE)
)

display(
    result_df.orderBy(
        F.col("status").asc(),
        F.col("check_name").asc(),
    )
)

failed_checks = [
    row["check_name"]
    for row in results
    if row["status"] == "FAIL"
]

print(
    f"Validation summary: "
    f"{len(results) - len(failed_checks)} passed, "
    f"{len(failed_checks)} failed."
)

if failed_checks:
    raise RuntimeError(
        "FinGuard pipeline validation failed: "
        + ", ".join(failed_checks)
    )

print("FinGuard pipeline validation PASSED.")
