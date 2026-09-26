# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FinGuard - 06 Lakebase CDC Analytics
# MAGIC Refresh Gold operational analytics from native Lakebase change-data-feed history tables.

# COMMAND ----------

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

CDC_SCHEMA = f"{USERNAME}_lakebase_cdc"
GOLD_SCHEMA = f"{USERNAME}_gold"

CDC = f"{CATALOG}.{CDC_SCHEMA}"

GOLD = (
    f"{CATALOG}.{GOLD_SCHEMA}."
    "gold_alert_summary"
)

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{CATALOG}.{CDC_SCHEMA}"
)

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{CATALOG}.{GOLD_SCHEMA}"
)

print(f"Logged-in user: {USER_EMAIL}")
print(f"Lakebase CDC schema: {CDC}")
print(f"Gold alert summary: {GOLD}")

# COMMAND ----------

display(
    spark.sql(
        f"SHOW TABLES IN {CDC}"
    )
)

# COMMAND ----------

expected_cdc_tables = [
    "lb_fraud_alerts_history",
    "lb_alert_status_history_history",
    "lb_investigations_history",
    "lb_agent_actions_history",
]

for table_name in expected_cdc_tables:
    full_table_name = f"{CDC}.{table_name}"

    print(
        full_table_name,
        spark.catalog.tableExists(full_table_name),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Source Validation
# MAGIC The CDC analytics path reads replicated Lakebase history from Unity Catalog.
# MAGIC Direct PostgreSQL credentials are not required in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CDC Post-Images
# MAGIC Keep inserted rows and the latest values produced by PostgreSQL updates.

# COMMAND ----------

def post_image(table_name: str):
    df = spark.table(f"{CDC}.{table_name}")
    # Inserts represent a complete new row; update_postimage represents the new version.
    return df.filter(F.col("_pg_change_type").isin("insert", "update_postimage"))


alerts = post_image("lb_fraud_alerts_history")
status_history = post_image("lb_alert_status_history_history")
investigations = post_image("lb_investigations_history")
actions = post_image("lb_agent_actions_history")

# COMMAND ----------

for dataframe_name, dataframe in [
    ("alerts", alerts),
    ("status_history", status_history),
    ("investigations", investigations),
    ("actions", actions),
]:
    print(f"\n{dataframe_name}")

    (
        dataframe.groupBy("_pg_change_type")
        .count()
        .orderBy("_pg_change_type")
        .show()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Operational Metrics
# MAGIC Calculate daily alert, status, investigation-resolution, and agent-action metrics.

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh Gold Summary
# MAGIC Combine each operational metric and overwrite the Gold summary table.

# COMMAND ----------

all_dates = (
    alert_created.select("metric_date")
    .union(
        status_metrics.select("metric_date")
    )
    .union(
        resolution_metrics.select("metric_date")
    )
    .union(
        agent_metrics.select("metric_date")
    )
    .filter(
        F.col("metric_date").isNotNull()
    )
    .distinct()
)

summary = (
    all_dates
    .join(
        alert_created,
        on="metric_date",
        how="left",
    )
    .join(
        status_metrics,
        on="metric_date",
        how="left",
    )
    .join(
        resolution_metrics,
        on="metric_date",
        how="left",
    )
    .join(
        agent_metrics,
        on="metric_date",
        how="left",
    )
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
    .withColumn(
        "refreshed_at",
        F.current_timestamp(),
    )
    .dropDuplicates(["metric_date"])
)

summary_count = summary.count()

if summary_count > 0:
    (
        summary.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD)
    )

    print(f"Gold summary refreshed: {GOLD}")
else:
    print(
        "No CDC operational metrics were available "
        "to write."
    )

print(
    f"Operational analytics dates refreshed: "
    f"{summary_count:,}"
)

# COMMAND ----------

display(
    spark.table(GOLD)
    .orderBy("metric_date")
)