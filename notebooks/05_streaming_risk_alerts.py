# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %pip install "psycopg[binary]" "databricks-sdk>=0.81"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # FinGuard - 05 Streaming Risk Alerts
# MAGIC Process incremental Silver transactions, update Gold risk scores, and write Lakebase alerts.

# COMMAND ----------

import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F

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

SILVER_SCHEMA = f"{USERNAME}_silver"
GOLD_SCHEMA = f"{USERNAME}_gold"
OPERATIONS_SCHEMA = f"{USERNAME}_operations"

SILVER = (
    f"{CATALOG}.{SILVER_SCHEMA}.silver_transactions"
)

RISK = (
    f"{CATALOG}.{GOLD_SCHEMA}.gold_transaction_risk"
)

CHECKPOINT_VOLUME = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}.checkpoints"
)

CHECKPOINT = (
    f"/Volumes/{CATALOG}/{OPERATIONS_SCHEMA}"
    "/checkpoints/transaction_risk_v1"
)

# Match the corrected threshold used in notebook 03.
ALERT_THRESHOLD = 50

# Streaming micro-batch interval.
TRIGGER_SECONDS = 10

# Create the Operations schema and checkpoint volume.
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{CATALOG}.{OPERATIONS_SCHEMA}"
)

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"{CHECKPOINT_VOLUME}"
)

# Create the checkpoint subfolder.
dbutils.fs.mkdirs(CHECKPOINT)

print(f"Logged-in user: {USER_EMAIL}")
print(f"Silver source: {SILVER}")
print(f"Gold risk target: {RISK}")
print(f"Checkpoint: {CHECKPOINT}")
print(f"Alert threshold: {ALERT_THRESHOLD}")
print(f"Trigger interval: {TRIGGER_SECONDS} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk-Scoring Function
# MAGIC Apply deterministic fraud rules to each incremental Silver micro-batch.

# COMMAND ----------

def score_batch(df):
    amount_rule = F.col("amount_deviation") >= 3.0
    velocity_rule = F.col("recent_txn_count") >= 3
    new_destination_rule = F.col("is_new_destination")
    unusual_type_rule = F.col("is_unusual_type")
    international_rule = F.col("is_international")
    high_value_rule = F.col("is_high_value")

    score = (
        F.when(amount_rule, 30).otherwise(0)
        + F.when(velocity_rule, 20).otherwise(0)
        + F.when(new_destination_rule, 15).otherwise(0)
        + F.when(unusual_type_rule, 15).otherwise(0)
        + F.when(international_rule, 10).otherwise(0)
        + F.when(high_value_rule, 10).otherwise(0)
    )

    reasons = F.array(
        F.when(amount_rule, F.lit("amount >= 3x customer average")),
        F.when(velocity_rule, F.lit("multiple transactions in short window")),
        F.when(new_destination_rule, F.lit("new destination/account")),
        F.when(unusual_type_rule, F.lit("unusual transaction type")),
        F.when(international_rule, F.lit("international transaction")),
        F.when(high_value_rule, F.lit("high-value transaction")),
    )

    return (
        df.withColumn("risk_score", F.least(score, F.lit(100)).cast("int"))
        .withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= 80, "CRITICAL")
            .when(F.col("risk_score") >= 60, "HIGH")
            .when(F.col("risk_score") >= 30, "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn("risk_reasons", F.filter(reasons, lambda x: x.isNotNull()))
        .withColumn("is_alert_candidate", F.col("risk_score") >= ALERT_THRESHOLD)
        .withColumn("scored_at", F.current_timestamp())
        .select(
            "transaction_id",
            "customer_id",
            "destination_id",
            "event_timestamp",
            "event_date",
            "amount_home_currency",
            "risk_score",
            "risk_level",
            "risk_reasons",
            "is_alert_candidate",
            "scored_at",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Secure Lakebase Connection
# MAGIC Generate a short-lived OAuth database credential at runtime. Never commit OAuth tokens or database passwords.

# COMMAND ----------

import psycopg
from databricks.sdk import WorkspaceClient

PGHOST = os.getenv("PGHOST") or spark.conf.get("finguard.lakebase.host", "")
PGPORT = int(os.getenv("PGPORT") or spark.conf.get("finguard.lakebase.port", "5432"))
PGDATABASE = os.getenv("PGDATABASE") or spark.conf.get(
    "finguard.lakebase.database",
    "databricks_postgres",
)
PGUSER = os.getenv("PGUSER") or USER_EMAIL
LAKEBASE_ENDPOINT = os.getenv("ENDPOINT_NAME") or spark.conf.get(
    "finguard.lakebase.endpoint",
    "",
)
LAKEBASE_SCHEMA = USERNAME

missing = []
if not PGHOST:
    missing.append("finguard.lakebase.host / PGHOST")
if not LAKEBASE_ENDPOINT:
    missing.append("finguard.lakebase.endpoint / ENDPOINT_NAME")

if missing:
    raise RuntimeError(
        "Missing Lakebase runtime configuration: "
        + ", ".join(missing)
        + ". Use the Lakebase Connect dialog values; do not put credentials in source control."
    )

workspace_client = WorkspaceClient()


def open_lakebase_connection():
    credential = workspace_client.postgres.generate_database_credential(
        endpoint=LAKEBASE_ENDPOINT
    )
    return psycopg.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=credential.token,
        sslmode="require",
        connect_timeout=30,
    )


with open_lakebase_connection() as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database(), current_user")
        print(cursor.fetchone())

# COMMAND ----------

def write_alert_rows(alert_df):
    """
    Write alert candidates to Lakebase on the driver using a fresh OAuth credential.

    This driver-side pattern is appropriate for the capstone/demo alert volume.
    For larger production volumes, use a scalable sink with managed credential rotation.
    """
    customer_sql = f"""
        INSERT INTO {LAKEBASE_SCHEMA}.customers (
            customer_id
        )
        VALUES (%s)
        ON CONFLICT (customer_id)
        DO NOTHING
    """

    alert_sql = f"""
        INSERT INTO {LAKEBASE_SCHEMA}.fraud_alerts (
            transaction_id,
            customer_id,
            risk_score,
            risk_level,
            alert_reason,
            status
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            'OPEN'
        )
        ON CONFLICT (transaction_id)
        DO UPDATE SET
            risk_score = EXCLUDED.risk_score,
            risk_level = EXCLUDED.risk_level,
            alert_reason = EXCLUDED.alert_reason,
            updated_at = CURRENT_TIMESTAMP
    """

    with open_lakebase_connection() as conn:
        try:
            with conn.cursor() as cursor:
                for row in alert_df.toLocalIterator():
                    cursor.execute(
                        customer_sql,
                        (row.customer_id,),
                    )
                    cursor.execute(
                        alert_sql,
                        (
                            row.transaction_id,
                            row.customer_id,
                            int(row.risk_score),
                            row.risk_level,
                            "; ".join(row.risk_reasons or []),
                        ),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


# COMMAND ----------

# MAGIC %md
# MAGIC ## Micro-Batch Processing
# MAGIC Merge scored transactions into Gold and send qualifying alerts to Lakebase.

# COMMAND ----------

def process_microbatch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        return

    batch_df = batch_df.filter(F.col("data_quality_status").isin("VALID", "STALE_FX_RATE"))
    scored = score_batch(batch_df)

    target = DeltaTable.forName(spark, RISK)
    (
        target.alias("t")
        .merge(scored.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    alerts = scored.filter("is_alert_candidate").select(
        "transaction_id", "customer_id", "risk_score", "risk_level", "risk_reasons"
    )
    if not alerts.isEmpty():
        write_alert_rows(alerts)

    latency = scored.select(
        F.expr("percentile_approx(unix_timestamp(scored_at) - unix_timestamp(event_timestamp), 0.95)").alias(
            "p95_seconds"
        )
    ).first()["p95_seconds"]
    print(f"batch_id={batch_id}, scored={scored.count()}, alerts={alerts.count()}, p95_seconds={latency}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Streaming Query
# MAGIC Run the Silver stream using the configured checkpoint and processing interval.

# COMMAND ----------

stream = spark.readStream.table(SILVER)

query = (
    stream.writeStream
    .foreachBatch(process_microbatch)
    .option(
        "checkpointLocation",
        CHECKPOINT,
    )
    .trigger(availableNow=True)
    .queryName("finguard_transaction_risk")
    .start()
)

query.awaitTermination()

print(
    "Available Silver transactions were processed "
    "and the streaming query completed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Validate Lakebase**

# COMMAND ----------

with open_lakebase_connection() as connection:
    with connection.cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {LAKEBASE_SCHEMA}.fraud_alerts
        """)
        alert_count = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {LAKEBASE_SCHEMA}.customers
        """)
        customer_count = cursor.fetchone()[0]

print(f"Lakebase fraud alerts: {alert_count:,}")
print(f"Lakebase customers: {customer_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Alert distribution validation**

# COMMAND ----------

with open_lakebase_connection() as connection:
    with connection.cursor() as cursor:
        cursor.execute(f"""
            SELECT
                risk_level,
                status,
                COUNT(*) AS alert_count
            FROM {LAKEBASE_SCHEMA}.fraud_alerts
            GROUP BY risk_level, status
            ORDER BY alert_count DESC
        """)

        for record in cursor.fetchall():
            print(record)