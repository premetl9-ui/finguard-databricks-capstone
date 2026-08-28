# Databricks notebook source
# FinGuard - 06 Lakebase CDF -> Gold operational analytics
# Configure this notebook as a Databricks Workflow task every 1 minute after
# native Lakebase CDF has created the lb_*_history Delta tables.

from pyspark.sql import functions as F

CATALOG = spark.conf.get("finguard.catalog", "finguard")
CDC = f"{CATALOG}.lakebase_cdc"
GOLD = f"{CATALOG}.gold.gold_alert_summary"


def post_image(table_name: str):
    df = spark.table(f"{CDC}.{table_name}")
    # Inserts represent a complete new row; update_postimage represents the new version.
    return df.filter(F.col("_pg_change_type").isin("insert", "update_postimage"))


alerts = post_image("lb_fraud_alerts_history")
status_history = post_image("lb_alert_status_history_history")
investigations = post_image("lb_investigations_history")
actions = post_image("lb_agent_actions_history")

alert_created = (
    alerts.filter(F.col("_pg_change_type") == "insert")
    .withColumn("metric_date", F.to_date(F.coalesce(F.col("created_at"), F.col("_timestamp"))))
    .groupBy("metric_date")
    .agg(F.count("*").alias("alerts_created"))
)

status_metrics = (
    status_history.withColumn("metric_date", F.to_date(F.coalesce(F.col("changed_at"), F.col("_timestamp"))))
    .groupBy("metric_date")
    .agg(
        F.sum(F.when(F.col("new_status") == "ESCALATED", 1).otherwise(0)).alias("alerts_escalated"),
        F.sum(F.when(F.col("new_status").isin("RESOLVED", "CLOSED"), 1).otherwise(0)).alias(
            "alerts_resolved"
        ),
    )
)

resolution_metrics = (
    investigations.filter(F.col("closed_at").isNotNull())
    .withColumn("metric_date", F.to_date("closed_at"))
    .withColumn(
        "resolution_minutes",
        (F.unix_timestamp("closed_at") - F.unix_timestamp("opened_at")) / F.lit(60.0),
    )
    # An investigation can have multiple post-images. Keep the newest version per ID.
    .groupBy("metric_date", "investigation_id")
    .agg(F.max("resolution_minutes").alias("resolution_minutes"))
    .groupBy("metric_date")
    .agg(F.avg("resolution_minutes").alias("avg_resolution_minutes"))
)

agent_metrics = (
    actions.filter(F.col("_pg_change_type") == "insert")
    .withColumn("metric_date", F.to_date(F.coalesce(F.col("created_at"), F.col("_timestamp"))))
    .groupBy("metric_date")
    .agg(
        F.count("*").alias("agent_actions"),
        F.avg(F.when(F.col("action_status") == "SUCCESS", 1.0).otherwise(0.0)).alias(
            "agent_action_success_rate"
        ),
    )
)

all_dates = (
    alert_created.select("metric_date")
    .union(status_metrics.select("metric_date"))
    .union(resolution_metrics.select("metric_date"))
    .union(agent_metrics.select("metric_date"))
    .distinct()
)

summary = (
    all_dates.join(alert_created, "metric_date", "left")
    .join(status_metrics, "metric_date", "left")
    .join(resolution_metrics, "metric_date", "left")
    .join(agent_metrics, "metric_date", "left")
    .fillna(
        {
            "alerts_created": 0,
            "alerts_escalated": 0,
            "alerts_resolved": 0,
            "avg_resolution_minutes": 0.0,
            "agent_actions": 0,
            "agent_action_success_rate": 0.0,
        }
    )
    .withColumn("refreshed_at", F.current_timestamp())
)

summary.createOrReplaceTempView("finguard_alert_summary_refresh")
spark.sql(f"INSERT OVERWRITE {GOLD} SELECT * FROM finguard_alert_summary_refresh")

print(f"Operational analytics dates refreshed: {summary.count():,}")
