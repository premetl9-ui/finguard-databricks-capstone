# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark.range(1).show()

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]"

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
            .when(F.col("risk_score") >= 50, "HIGH")
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

# Temporary runtime value only.
# Do not commit the real connection string to GitHub.
DATABASE_URL = (
    "postgresql://premetl9%40gmail.com@ep-falling-cherry-d1cu09hi.database.us-west-2.cloud.databricks.com/databricks_postgres"
    "?sslmode=require"
)

if not DATABASE_URL.startswith(
    ("postgresql://", "postgres://")
):
    raise RuntimeError(
        "A valid PostgreSQL connection string is required."
    )

print("PostgreSQL connection string configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase Alert Writer
# MAGIC Write alert candidates to PostgreSQL with idempotent transaction-based upserts.

# COMMAND ----------

import psycopg

PGHOST = (
    "ep-falling-cherry-d1cu09hi."
    "database.us-west-2.cloud.databricks.com"
)
PGPORT = 5432
PGDATABASE = "databricks_postgres"
PGUSER = "premetl9@gmail.com"

# Paste the generated OAuth database token here temporarily.
PGPASSWORD =  "eyJraWQiOiJqblJxRmciLCJhbGciOiJSUzI1NiIsInR5cCI6ImF0K2p3dCJ9.eyJpc3MiOiJodHRwczovL2RiYy03YjEwNjE1Mi1jYWYzLmNsb3VkLmRhdGFicmlja3MuY29tL29pZGMiLCJzdWIiOiJwcmVtZXRsOUBnbWFpbC5jb20iLCJhdWQiOlsiMTM1Mjc4NTA3OTIyNDk1NCJdLCJpYXQiOjE3ODk1MjQyMzcsImV4cCI6MTc4OTUyNzgzNywianRpIjoiYmQ0MzkzNDktYWFmYy00Y2ZkLWFhZDYtZTkwMThiNDRmZGU2IiwiY2xpZW50X2lkIjoiZGItZGF0YWJhc2UtY3JlZGVudGlhbCIsInNjb3BlIjoiaWFtLmN1cnJlbnQtdXNlcjpyZWFkIGlhbS5ncm91cHM6cmVhZCBpYW0uc2VydmljZS1wcmluY2lwYWxzOnJlYWQgaWFtLnVzZXJzOnJlYWQiLCJwcml2YXRlX21ldGFkYXRhIjoiQVVQNGFXazhhcTlVUnpUNkMwaE5aeGFXQ0pqbWZTbzRlTXNFZDIyTS1KSTVFaFJjcVQ0RFVqTGthMEVZRVd1YV82UnkyU0ZuSGx5bkJSN2RPVDRRR0cwMncxMzRYNS1qQXV4VTRtSnJGNWxYd0pFaEFoN1JZb3NPSnVaVXNXbnNsTFRtV2VaSS01Y2tuLUFBOVZJcDZvTGYwOTk3VE5oQ2lqNG04OU9YaW9zIiwicGN0eCI6IkN1UURDaFFJQVJvR0NPX3hwOVVHSWdZSXplS28xUVlvQWhMOEFnR29OcVk4QWs2NGxhOG1GYjE1dmkza2hycnZCR0h0b0RacWI5N3R4b2E5UXNfZnVWOGRRdnV2bjdFZmZwcER3dHR2cEt5dHo4YjNEVk40SWdoUFN3VnNqTU9maUltRldxd054YzZsQXlDNHgzV3ZWdGNldjE1eEIzeUpxdm8zTkV0aVk4YkFQVWk2Q3FoLUZES3I0WEdKNlJjX1BwLUl6RUZZNHJna0FwWHZOd3ZuTjk0WkVrSmd1aExiN0Jic2JKN19sWnlPTHNLTDloY19hTVM0Qll5QVotOHZvcG4yNmlGQ2lyaGs1Z1A5WGdneVc0Ujc0NGlXWEt5VWJycXJ4Uk9oNFBGT3dhdF9RZDV5a2k0NjhDN0xGN2hHdXF0LXJlVG0wODFJdHlYc1dEbmk0TlM2ZUczcnFOczNjTVpsX0I0dDZ6cFNRVks3NFF6d1hLMkhfdzlKaWNleVd6QWl0UmJtT2phMklnWjV3ME50aFN1ZVBzOEpIb1FydXVSa012Q1dTWTIxaDVsbm9WYTBLckpVZnZyaE5lZ2dIYjhEbDBYRGpUdkM2djYzcG1IOVFBWmNsNXFIbEVuTXIwZ1B0ZjczY0xDNktubE5GZE5kNWIyNWlzdHVDX2Voc0dRLVltV0RHRzQzNlJPWnRkV3k4U19BbDVJSWRHZEJHazBCam5fTDFUQkdBaUVBNG5VZGJPY0t4Y1Mwajk4aWhMazlmRGZadXktOWFpY2N3aml4THJDQTJUWUNJUUQyeXhTa1FIRnZjRGxLcmV5R2pqVk5PMGdHT19EY1BJUGhMcHprTWN5TG9nPT0ifQ.UXeby6cZ_5mzX33TaWBup9wg_9m4YOb5bbm490HMOc6_c7f1Q6qIyTVhKUpWvM37wOJ4hwskAOiqeCoIFGJEG1kcEfDbmLOzPuQ_uXtAOOOoTh2g07F-6izahC6eFx2ZtvuQngIy_og8PTDci4f5QAYKZd70n3UG9EQa9NuvqfqpF3fmG-6DYMAZUl6AtWA8bvyj786-h9h_iDVGR_dER2hfl_4rbqiXqkbrnlwUVgezFhZFAQF_N4GxhjAGnt4NeHR7aV4gb8szpLEbUSk9MeykT9-0CX-rmsMaUBg5Axcyiyaj1pTugnv-SDLp74at2GjKaCK0tJolK_G5YlrGeQ" 

conn = psycopg.connect(
    host=PGHOST,
    port=PGPORT,
    dbname=PGDATABASE,
    user=PGUSER,
    password=PGPASSWORD,
    sslmode="require",
    connect_timeout=30,
)

with conn.cursor() as cursor:
    cursor.execute(
        "SELECT current_database(), current_user"
    )
    print(cursor.fetchone())

conn.close()

# COMMAND ----------

def write_alert_partition(rows):
    """
    Write one Spark partition to Lakebase/PostgreSQL.

    transaction_id makes fraud-alert writes idempotent.
    """
    import psycopg

    conn = psycopg.connect(
    host=PGHOST,
    port=PGPORT,
    dbname=PGDATABASE,
    user=PGUSER,
    password=PGPASSWORD,
    sslmode="require",
    connect_timeout=30,
    )

    customer_sql = """
        INSERT INTO premetl9.customers (
            customer_id
        )
        VALUES (%s)
        ON CONFLICT (customer_id)
        DO NOTHING
    """

    alert_sql = """
        INSERT INTO premetl9.fraud_alerts (
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

    try:
        with conn.cursor() as cursor:
            for row in rows:
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

    finally:
        conn.close()

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
        alerts.foreachPartition(write_alert_partition)

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

with psycopg.connect(
    host=PGHOST,
    port=PGPORT,
    dbname=PGDATABASE,
    user=PGUSER,
    password=PGPASSWORD,
    sslmode="require",
    connect_timeout=30,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*)
            FROM premetl9.fraud_alerts
        """)
        alert_count = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*)
            FROM premetl9.customers
        """)
        customer_count = cursor.fetchone()[0]

print(f"Lakebase fraud alerts: {alert_count:,}")
print(f"Lakebase customers: {customer_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Alert distribution validation**

# COMMAND ----------

with psycopg.connect(
    host=PGHOST,
    port=PGPORT,
    dbname=PGDATABASE,
    user=PGUSER,
    password=PGPASSWORD,
    sslmode="require",
    connect_timeout=30,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                risk_level,
                status,
                COUNT(*) AS alert_count
            FROM premetl9.fraud_alerts
            GROUP BY risk_level, status
            ORDER BY alert_count DESC
        """)

        for record in cursor.fetchall():
            print(record)