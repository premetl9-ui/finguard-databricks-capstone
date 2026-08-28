# Databricks notebook source
# FinGuard - 05 Optional Velocity path
# Reads incremental Silver commits, scores them every 10 seconds, MERGEs Gold risk,
# and performs idempotent Lakebase alert upserts.

import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F

CATALOG = spark.conf.get("finguard.catalog", "finguard")
SILVER = f"{CATALOG}.silver.silver_transactions"
RISK = f"{CATALOG}.gold.gold_transaction_risk"
CHECKPOINT = spark.conf.get(
    "finguard.risk_checkpoint",
    f"/Volumes/{CATALOG}/operations/checkpoints/transaction_risk_v1",
)
ALERT_THRESHOLD = int(spark.conf.get("finguard.alert_threshold", "60"))
TRIGGER_SECONDS = int(spark.conf.get("finguard.trigger_seconds", "10"))


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


def write_alert_partition(rows):
    """One PostgreSQL connection per Spark partition; transaction_id makes writes idempotent."""
    import psycopg

    database_url = os.getenv("DATABASE_URL")
    if database_url:
        conn = psycopg.connect(database_url)
    else:
        conn = psycopg.connect(
            host=os.environ["PGHOST"],
            port=os.getenv("PGPORT", "5432"),
            dbname=os.environ["PGDATABASE"],
            user=os.environ["PGUSER"],
            password=os.getenv("PGPASSWORD"),
        )

    customer_sql = """
    INSERT INTO customers (customer_id)
    VALUES (%s)
    ON CONFLICT (customer_id) DO NOTHING
    """
    alert_sql = """
    INSERT INTO fraud_alerts (
        transaction_id, customer_id, risk_score, risk_level, alert_reason, status
    ) VALUES (%s, %s, %s, %s, %s, 'OPEN')
    ON CONFLICT (transaction_id)
    DO UPDATE SET
        risk_score = EXCLUDED.risk_score,
        risk_level = EXCLUDED.risk_level,
        alert_reason = EXCLUDED.alert_reason,
        updated_at = CURRENT_TIMESTAMP
    """

    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(customer_sql, (row.customer_id,))
                cur.execute(
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
    finally:
        conn.close()


def process_microbatch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        return

    batch_df = batch_df.filter(F.col("data_quality_status").isin("VALID", "STALE_FX_RATE"))
    scored = score_batch(batch_df).persist()

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
    scored.unpersist()


stream = spark.readStream.table(SILVER)

query = (
    stream.writeStream.foreachBatch(process_microbatch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    .queryName("finguard_transaction_risk")
    .start()
)

query.awaitTermination()
