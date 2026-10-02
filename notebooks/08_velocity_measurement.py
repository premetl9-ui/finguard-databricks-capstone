# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FinGuard - 08 Velocity Measurement
# MAGIC Measure sub-minute Spark Structured Streaming latency with a repeatable probe workload.
# MAGIC
# MAGIC This notebook is intentionally isolated from production transaction tables. It inserts
# MAGIC timestamped probe events into an Operations Delta table, processes them through a
# MAGIC checkpointed Structured Streaming query, persists the processed events, calculates
# MAGIC min/avg/p95/max latency, writes an auditable result row, and fails when p95 >= 60 seconds.

# COMMAND ----------

import json
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

OPERATIONS_SCHEMA = f"{USERNAME}_operations"

SOURCE = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}."
    "velocity_probe_source"
)

SINK = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}."
    "velocity_probe_sink"
)

RESULTS = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}."
    "velocity_measurement_results"
)

CHECKPOINT_VOLUME = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}.checkpoints"
)

CHECKPOINT = (
    f"/Volumes/{CATALOG}/{OPERATIONS_SCHEMA}"
    "/checkpoints/velocity_probe_v1"
)

QUERY_NAME = "finguard_velocity_probe"
DEFAULT_PROBE_EVENTS = 10
DEFAULT_SLA_SECONDS = 60.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime Parameters

# COMMAND ----------

def runtime_int(name: str, default: int) -> int:
    try:
        dbutils.widgets.text(name, str(default))
        return int(dbutils.widgets.get(name).strip() or default)
    except Exception:
        return default


def runtime_float(name: str, default: float) -> float:
    try:
        dbutils.widgets.text(name, str(default))
        return float(dbutils.widgets.get(name).strip() or default)
    except Exception:
        return default


PROBE_EVENTS = runtime_int("probe_events", DEFAULT_PROBE_EVENTS)
SLA_SECONDS = runtime_float("sla_seconds", DEFAULT_SLA_SECONDS)

if PROBE_EVENTS < 1 or PROBE_EVENTS > 1000:
    raise ValueError("probe_events must be between 1 and 1000")

if SLA_SECONDS <= 0:
    raise ValueError("sla_seconds must be positive")

print(f"Logged-in user: {USER_EMAIL}")
print(f"Velocity probe events: {PROBE_EVENTS}")
print(f"Velocity SLA: p95 < {SLA_SECONDS:.1f} seconds")
print(f"Checkpoint: {CHECKPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Operations Tables and Checkpoint

# COMMAND ----------

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{CATALOG}.{OPERATIONS_SCHEMA}"
)

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"{CHECKPOINT_VOLUME}"
)

dbutils.fs.mkdirs(CHECKPOINT)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SOURCE} (
    event_id STRING NOT NULL,
    measurement_run_id STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    payload STRING
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SINK} (
    event_id STRING NOT NULL,
    measurement_run_id STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    payload STRING,
    processed_at TIMESTAMP NOT NULL
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RESULTS} (
    measurement_run_id STRING NOT NULL,
    measurement_timestamp TIMESTAMP NOT NULL,
    event_count BIGINT NOT NULL,
    min_latency_seconds DOUBLE,
    avg_latency_seconds DOUBLE,
    p95_latency_seconds DOUBLE,
    max_latency_seconds DOUBLE,
    checkpoint_path STRING,
    query_name STRING,
    input_rows BIGINT,
    batch_id BIGINT,
    status STRING NOT NULL,
    progress_json STRING
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert Timestamped Probe Events
# MAGIC The event timestamp is captured immediately before the source append so the measured latency
# MAGIC includes Delta source commit plus Structured Streaming discovery and processing.

# COMMAND ----------

MEASUREMENT_RUN_ID = str(uuid.uuid4())
created_at = datetime.now(timezone.utc)

probe_schema = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("measurement_run_id", T.StringType(), False),
        T.StructField("created_at", T.TimestampType(), False),
        T.StructField("payload", T.StringType(), True),
    ]
)

probe_rows = [
    (
        str(uuid.uuid4()),
        MEASUREMENT_RUN_ID,
        created_at,
        f"velocity-probe-{index + 1}",
    )
    for index in range(PROBE_EVENTS)
]

(
    spark.createDataFrame(probe_rows, schema=probe_schema)
    .write.format("delta")
    .mode("append")
    .saveAsTable(SOURCE)
)

print(f"Velocity measurement run: {MEASUREMENT_RUN_ID}")
print(f"Probe source rows appended: {PROBE_EVENTS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Probe Events with Structured Streaming
# MAGIC The checkpoint is reused between runs so only newly appended source events are processed.

# COMMAND ----------

stream = (
    spark.readStream
    .table(SOURCE)
    .withColumn(
        "processed_at",
        F.current_timestamp(),
    )
)

query = (
    stream.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT,
    )
    .trigger(availableNow=True)
    .queryName(QUERY_NAME)
    .toTable(SINK)
)

query.awaitTermination()

progress = query.lastProgress or {}
print("Structured Streaming query completed.")
print(
    "Last progress: "
    + json.dumps(progress, default=str)[:4000]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Measured Latency
# MAGIC p95 below 60 seconds demonstrates the capstone's second Big Data V: Velocity.

# COMMAND ----------

run_events = (
    spark.table(SINK)
    .filter(
        F.col("measurement_run_id")
        == F.lit(MEASUREMENT_RUN_ID)
    )
    .withColumn(
        "latency_seconds",
        F.col("processed_at").cast("double")
        - F.col("created_at").cast("double"),
    )
)

metrics = (
    run_events.agg(
        F.count("*").alias("event_count"),
        F.min("latency_seconds").alias("min_latency_seconds"),
        F.avg("latency_seconds").alias("avg_latency_seconds"),
        F.expr(
            "percentile_approx(latency_seconds, 0.95, 10000)"
        ).alias("p95_latency_seconds"),
        F.max("latency_seconds").alias("max_latency_seconds"),
    )
    .first()
)

event_count = int(metrics["event_count"] or 0)
min_latency = float(metrics["min_latency_seconds"] or 0.0)
avg_latency = float(metrics["avg_latency_seconds"] or 0.0)
p95_latency = float(metrics["p95_latency_seconds"] or 0.0)
max_latency = float(metrics["max_latency_seconds"] or 0.0)

input_rows = int(progress.get("numInputRows", 0) or 0)
batch_id = int(progress.get("batchId", -1) or -1)

passed = (
    event_count == PROBE_EVENTS
    and p95_latency < SLA_SECONDS
)

status = "PASS" if passed else "FAIL"

print(
    f"[{status}] Velocity measurement: "
    f"events={event_count}; "
    f"min={min_latency:.3f}s; "
    f"avg={avg_latency:.3f}s; "
    f"p95={p95_latency:.3f}s; "
    f"max={max_latency:.3f}s; "
    f"SLA=p95<{SLA_SECONDS:.1f}s"
)

display(
    run_events.select(
        "event_id",
        "measurement_run_id",
        "created_at",
        "processed_at",
        F.round(
            F.col("latency_seconds"),
            3,
        ).alias("latency_seconds"),
    )
    .orderBy("event_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Velocity Evidence

# COMMAND ----------

result_schema = T.StructType(
    [
        T.StructField("measurement_run_id", T.StringType(), False),
        T.StructField("measurement_timestamp", T.TimestampType(), False),
        T.StructField("event_count", T.LongType(), False),
        T.StructField("min_latency_seconds", T.DoubleType(), True),
        T.StructField("avg_latency_seconds", T.DoubleType(), True),
        T.StructField("p95_latency_seconds", T.DoubleType(), True),
        T.StructField("max_latency_seconds", T.DoubleType(), True),
        T.StructField("checkpoint_path", T.StringType(), True),
        T.StructField("query_name", T.StringType(), True),
        T.StructField("input_rows", T.LongType(), True),
        T.StructField("batch_id", T.LongType(), True),
        T.StructField("status", T.StringType(), False),
        T.StructField("progress_json", T.StringType(), True),
    ]
)

result_row = [
    (
        MEASUREMENT_RUN_ID,
        datetime.now(timezone.utc),
        event_count,
        min_latency,
        avg_latency,
        p95_latency,
        max_latency,
        CHECKPOINT,
        QUERY_NAME,
        input_rows,
        batch_id,
        status,
        json.dumps(progress, default=str),
    )
]

(
    spark.createDataFrame(
        result_row,
        schema=result_schema,
    )
    .write.format("delta")
    .mode("append")
    .saveAsTable(RESULTS)
)

display(
    spark.table(RESULTS)
    .orderBy(
        F.desc("measurement_timestamp")
    )
    .limit(10)
)

if not passed:
    raise RuntimeError(
        "FinGuard velocity validation failed: "
        f"event_count={event_count}/{PROBE_EVENTS}, "
        f"p95={p95_latency:.3f}s, "
        f"required p95<{SLA_SECONDS:.1f}s"
    )

print(
    "FinGuard Velocity validation PASSED: "
    f"p95={p95_latency:.3f}s < "
    f"{SLA_SECONDS:.1f}s."
)
