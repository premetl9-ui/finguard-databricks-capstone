# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FinGuard - 06 Lakebase CDC Analytics
# MAGIC Refresh Gold operational analytics from native Lakebase change-data-feed history tables.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]"

# COMMAND ----------

dbutils.library.restartPython()

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

import psycopg

PGHOST = (
    "ep-falling-cherry-d1cu09hi."
    "database.us-west-2.cloud.databricks.com"
)
PGPORT = 5432
PGDATABASE = "databricks_postgres"
PGUSER = "premetl9@gmail.com"

# Paste the generated OAuth database token here temporarily.
PGPASSWORD = "eyJraWQiOiJqblJxRmciLCJhbGciOiJSUzI1NiIsInR5cCI6ImF0K2p3dCJ9.eyJpc3MiOiJodHRwczovL2RiYy03YjEwNjE1Mi1jYWYzLmNsb3VkLmRhdGFicmlja3MuY29tL29pZGMiLCJzdWIiOiJwcmVtZXRsOUBnbWFpbC5jb20iLCJhdWQiOlsiMTM1Mjc4NTA3OTIyNDk1NCJdLCJpYXQiOjE3OTAzODM1NTEsImV4cCI6MTc5MDM4NzE1MSwianRpIjoiODY2NmM4ODUtOGJiNi00MjgyLWJiZjMtYTAwNTNhNzI0ZDBhIiwiY2xpZW50X2lkIjoiZGItZGF0YWJhc2UtY3JlZGVudGlhbCIsInNjb3BlIjoiaWFtLmN1cnJlbnQtdXNlcjpyZWFkIGlhbS5ncm91cHM6cmVhZCBpYW0uc2VydmljZS1wcmluY2lwYWxzOnJlYWQgaWFtLnVzZXJzOnJlYWQiLCJwcml2YXRlX21ldGFkYXRhIjoiQVVQNGFXbDRhU0F1MGN4Q0FpTDJDT1o0V1hhWXZjLTNRX3BQTkpDZVF2RFdIQk8xTnRlZkNSN0dIaHpISHZkUnhHaHRXdS1kNTJWelQ0Z2lGdUh5bTFyN2ZHdWpiQ2xMN2xPMTVzZEhiRVdtVlc1UVB0bzNBemZhaTgzYllBNkZtcnpxMFBmd0tULUYyWEZDTWc4OFJWeDN0ZmVnN0lTdkhicVNSNnF1Qi04IiwicGN0eCI6IkN1SURDaFFJQVJvR0NLR3IzTlVHSWdZSV81dmQxUVlvQWhMNkFnR29OcVk4MGFKVGxURjZlUVJFanNDM1I5Q0FIcmI0Z0ZDN1VDYXNWNHRQd0RaYnI5Xzl2RU85alZIVFhKMU85REE3MGI5RjhCMHpPMGx1SVV2alJFREJzcFdLQ1NxNzc3RHJBZlMyaC03SFJnZ0VjUDdFb3RJZ0d0NzY0VklGb2dlTERTVVU4eTU5dWhPRDhnTEM4aXEzLU4yTm96SW1nTXlCQjlJRGlMSVNBRGlTSkxxcFJpbXJRbGZnM1Z3QmtBV1VvMkFRam5uWHYtZ2x4Ql9PYkQ1Ry1CZXBFSC1OVjdPYk50MXN2X3N6b2xwR24yaUEwbkpoOWhaMzI1d0ZxVTFGQ0R5LTJyaFBVWk42c3NsZ0x3dnlBQ3BPUW1LdnhId1RyeFFoMWVYOTA1MEtSeVZSOHJ2djRtVU02dkk0VnNNQWJaUU5SWU5jNHBNUlZLNTNTSnJFS0tqYV96Ulp1ZFJNVlRkVkgtclJMRVVXUGNmWlhTRWR0ZmpzS2w4Q1FyRVJJOWZDb0FMM3hQejZka0RweTZ1YVM4alI2RUNGNzBxaGhQU2xLamMwazFCSV9lY0h1dXB3NEdNcjRWekdGQjZXN0pfRFh2blY4alF3RWdfQnVSOU5qNnpfN3BqalVLVUUzSElESlVfdTE0VkZCSVp4LW5KVFZScE5BWTVfeTlVd1JnSWhBTVd2eXUwTUZ4X0dBNEhFMEZtY2Q2ay1kX0NmYzJySU9BcURxY0NDbF9hbkFpRUE4dDRBaU9LeVFlVTZBc05jaFdINUxNakRkNjhqNlNIRTdYWENQTG9KbkNZPSJ9.kKvAs7NMJtsnuQGzYYNd5kDLhanF7n8yj8Vyrt6bkjMT6NfBK_iU3UbIxTPy6pVejmAnJ017sQdxmPFM_2FPJqKJGDA0Qgwk0UX_yzpu8xgGFZEfsMTcc0ATr7wfnbxr7EP7zQouxbYJa218RCe-1RlA7yuxhC7lLkSFJKuNoLcu2bA86P_n6vXbExBV0YGqG18jwvmURk44XOdCmaUZEnaDGUx7iyUeQXDI5iZJCPovvV4C7ggIcSbNenyA_J3cfABkIAzISs7evjn3N45mpJU1bXJ_Bb8Du2ZDx0uwI26tIeY5PaU5pjNAJ1LPoPYNnbK2UfKfsE5g-Yif6zblfQ" 

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

import psycopg

print(f"psycopg version: {psycopg.__version__}")

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
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'premetl9'
            ORDER BY table_name
        """)

        for row in cursor.fetchall():
            print(row[0])

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