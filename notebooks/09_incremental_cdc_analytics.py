# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FinGuard - 09 Incremental CDC Analytics
# MAGIC Incrementally materialize Lakebase CDC events into compact Delta fact tables,
# MAGIC then refresh Gold operational analytics from those facts.
# MAGIC
# MAGIC Design goals:
# MAGIC - Re-runnable and idempotent by business/event key.
# MAGIC - Persist a watermark per CDC source.
# MAGIC - Re-read a small lookback window to tolerate late/same-timestamp events.
# MAGIC - MERGE facts so overlap does not create duplicates.
# MAGIC - Persist run-level monitoring metrics.

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Window
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

CDC_SCHEMA = f"{USERNAME}_lakebase_cdc"
GOLD_SCHEMA = f"{USERNAME}_gold"
OPERATIONS_SCHEMA = f"{USERNAME}_operations"

CDC = f"{CATALOG}.{CDC_SCHEMA}"
GOLD = f"{CATALOG}.{GOLD_SCHEMA}.gold_alert_summary"

STATE = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}."
    "cdc_analytics_state"
)

RUN_HISTORY = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}."
    "cdc_analytics_run_history"
)

ALERT_FACT = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}."
    "cdc_alert_created_facts"
)

STATUS_FACT = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}."
    "cdc_status_event_facts"
)

INVESTIGATION_FACT = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}."
    "cdc_investigation_facts"
)

ACTION_FACT = (
    f"{CATALOG}.{OPERATIONS_SCHEMA}."
    "cdc_agent_action_facts"
)

LOOKBACK_MINUTES = 5
RUN_ID = str(uuid.uuid4())
RUN_STARTED_AT = datetime.now(timezone.utc)

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{OPERATIONS_SCHEMA}"
)
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}"
)

print(f"Logged-in user: {USER_EMAIL}")
print(f"CDC schema: {CDC}")
print(f"Gold summary: {GOLD}")
print(f"Run ID: {RUN_ID}")
print(f"Watermark lookback: {LOOKBACK_MINUTES} minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## State and Monitoring Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {STATE} (
    source_name STRING NOT NULL,
    watermark_ts TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RUN_HISTORY} (
    run_id STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NOT NULL,
    lookback_minutes INT NOT NULL,
    alerts_window_rows BIGINT NOT NULL,
    status_window_rows BIGINT NOT NULL,
    investigations_window_rows BIGINT NOT NULL,
    actions_window_rows BIGINT NOT NULL,
    alerts_fact_rows BIGINT NOT NULL,
    status_fact_rows BIGINT NOT NULL,
    investigation_fact_rows BIGINT NOT NULL,
    action_fact_rows BIGINT NOT NULL,
    gold_dates BIGINT NOT NULL,
    status STRING NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Helpers

# COMMAND ----------

def post_image(table_name: str):
    return (
        spark.table(f"{CDC}.{table_name}")
        .filter(
            F.col("_pg_change_type")
            .isin("insert", "update_postimage")
        )
    )


def get_watermark(source_name: str):
    rows = (
        spark.table(STATE)
        .filter(F.col("source_name") == source_name)
        .select("watermark_ts")
        .limit(1)
        .collect()
    )
    return rows[0]["watermark_ts"] if rows else None


def incremental_window(source_name: str, df):
    watermark = get_watermark(source_name)
    if watermark is None:
        return df

    cutoff = F.lit(watermark).cast("timestamp") - F.expr(
        f"INTERVAL {LOOKBACK_MINUTES} MINUTES"
    )
    return df.filter(F.col("_timestamp") >= cutoff)


def merge_fact(table_name: str, df, key_column: str):
    if df.limit(1).count() == 0:
        return 0

    if not spark.catalog.tableExists(table_name):
        (
            df.write.format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )
        return df.count()

    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("t")
        .merge(
            df.alias("s"),
            f"t.{key_column} = s.{key_column}",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return df.count()


def update_watermark(source_name: str, df):
    max_ts = (
        df.agg(F.max("_timestamp").alias("max_ts"))
        .first()["max_ts"]
    )
    if max_ts is None:
        return

    state_row = spark.createDataFrame(
        [
            (
                source_name,
                max_ts,
                datetime.now(timezone.utc),
            )
        ],
        schema=T.StructType(
            [
                T.StructField("source_name", T.StringType(), False),
                T.StructField("watermark_ts", T.TimestampType(), True),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        ),
    )

    if not spark.catalog.tableExists(STATE):
        state_row.write.format("delta").mode("append").saveAsTable(STATE)
        return

    target = DeltaTable.forName(spark, STATE)
    (
        target.alias("t")
        .merge(
            state_row.alias("s"),
            "t.source_name = s.source_name",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Incremental CDC Windows

# COMMAND ----------

alerts_all = post_image("lb_fraud_alerts_history")
status_all = post_image("lb_alert_status_history_history")
investigations_all = post_image("lb_investigations_history")
actions_all = post_image("lb_agent_actions_history")

alerts_window = incremental_window("fraud_alerts", alerts_all)
status_window = incremental_window("alert_status_history", status_all)
investigations_window = incremental_window("investigations", investigations_all)
actions_window = incremental_window("agent_actions", actions_all)

alerts_window_rows = alerts_window.count()
status_window_rows = status_window.count()
investigations_window_rows = investigations_window.count()
actions_window_rows = actions_window.count()

print(
    "Incremental CDC window rows: "
    f"alerts={alerts_window_rows}, "
    f"status={status_window_rows}, "
    f"investigations={investigations_window_rows}, "
    f"actions={actions_window_rows}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialize Idempotent Fact Tables

# COMMAND ----------

alert_fact_updates = (
    alerts_window
    .filter(F.col("_pg_change_type") == "insert")
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("alert_id")
            .orderBy(F.col("_timestamp").desc())
        ),
    )
    .filter(F.col("_rn") == 1)
    .select(
        F.col("alert_id").cast("string").alias("alert_id"),
        F.to_date(
            F.coalesce(
                F.col("created_at"),
                F.col("_timestamp"),
            )
        ).alias("metric_date"),
        F.coalesce(
            F.col("created_at"),
            F.col("_timestamp"),
        ).alias("event_timestamp"),
        F.col("_timestamp").alias("cdc_timestamp"),
    )
)

status_fact_updates = (
    status_window
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("history_id")
            .orderBy(F.col("_timestamp").desc())
        ),
    )
    .filter(F.col("_rn") == 1)
    .select(
        F.col("history_id").cast("string").alias("history_id"),
        F.col("alert_id").cast("string").alias("alert_id"),
        F.col("new_status"),
        F.to_date(
            F.coalesce(
                F.col("changed_at"),
                F.col("_timestamp"),
            )
        ).alias("metric_date"),
        F.coalesce(
            F.col("changed_at"),
            F.col("_timestamp"),
        ).alias("event_timestamp"),
        F.col("_timestamp").alias("cdc_timestamp"),
    )
)

investigation_fact_updates = (
    investigations_window
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("investigation_id")
            .orderBy(F.col("_timestamp").desc())
        ),
    )
    .filter(F.col("_rn") == 1)
    .select(
        F.col("investigation_id").cast("string").alias("investigation_id"),
        F.col("alert_id").cast("string").alias("alert_id"),
        F.col("opened_at"),
        F.col("closed_at"),
        F.when(
            F.col("closed_at").isNotNull(),
            (
                F.unix_timestamp("closed_at")
                - F.unix_timestamp("opened_at")
            )
            / F.lit(60.0),
        ).alias("resolution_minutes"),
        F.col("_timestamp").alias("cdc_timestamp"),
    )
)

action_fact_updates = (
    actions_window
    .filter(F.col("_pg_change_type") == "insert")
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("action_id")
            .orderBy(F.col("_timestamp").desc())
        ),
    )
    .filter(F.col("_rn") == 1)
    .select(
        F.col("action_id").cast("string").alias("action_id"),
        F.col("alert_id").cast("string").alias("alert_id"),
        F.col("tool_name"),
        F.col("action_type"),
        F.col("action_status"),
        F.to_date(
            F.coalesce(
                F.col("created_at"),
                F.col("_timestamp"),
            )
        ).alias("metric_date"),
        F.coalesce(
            F.col("created_at"),
            F.col("_timestamp"),
        ).alias("event_timestamp"),
        F.col("_timestamp").alias("cdc_timestamp"),
    )
)

merge_fact(ALERT_FACT, alert_fact_updates, "alert_id")
merge_fact(STATUS_FACT, status_fact_updates, "history_id")
merge_fact(INVESTIGATION_FACT, investigation_fact_updates, "investigation_id")
merge_fact(ACTION_FACT, action_fact_updates, "action_id")

# Advance watermarks only after successful fact merges.
update_watermark("fraud_alerts", alerts_all)
update_watermark("alert_status_history", status_all)
update_watermark("investigations", investigations_all)
update_watermark("agent_actions", actions_all)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold Summary from Incremental Fact State

# COMMAND ----------

alert_created = (
    spark.table(ALERT_FACT)
    .groupBy("metric_date")
    .agg(
        F.count("*").alias("alerts_created")
    )
)

status_metrics = (
    spark.table(STATUS_FACT)
    .groupBy("metric_date")
    .agg(
        F.sum(
            F.when(
                F.col("new_status") == "ESCALATED",
                1,
            ).otherwise(0)
        ).alias("alerts_escalated"),
        F.sum(
            F.when(
                F.col("new_status").isin("RESOLVED", "CLOSED"),
                1,
            ).otherwise(0)
        ).alias("alerts_resolved"),
    )
)

resolution_metrics = (
    spark.table(INVESTIGATION_FACT)
    .filter(F.col("closed_at").isNotNull())
    .withColumn(
        "metric_date",
        F.to_date("closed_at"),
    )
    .groupBy("metric_date")
    .agg(
        F.avg("resolution_minutes")
        .alias("avg_resolution_minutes")
    )
)

agent_metrics = (
    spark.table(ACTION_FACT)
    .groupBy("metric_date")
    .agg(
        F.count("*").alias("agent_actions"),
        F.avg(
            F.when(
                F.col("action_status") == "SUCCESS",
                1.0,
            ).otherwise(0.0)
        ).alias("agent_action_success_rate"),
    )
)

all_dates = (
    alert_created.select("metric_date")
    .union(status_metrics.select("metric_date"))
    .union(resolution_metrics.select("metric_date"))
    .union(agent_metrics.select("metric_date"))
    .filter(F.col("metric_date").isNotNull())
    .distinct()
)

summary = (
    all_dates
    .join(alert_created, "metric_date", "left")
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

gold_dates = summary.count()

if gold_dates > 0:
    (
        summary.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Run Monitoring

# COMMAND ----------

alerts_fact_rows = spark.table(ALERT_FACT).count()
status_fact_rows = spark.table(STATUS_FACT).count()
investigation_fact_rows = spark.table(INVESTIGATION_FACT).count()
action_fact_rows = spark.table(ACTION_FACT).count()

RUN_FINISHED_AT = datetime.now(timezone.utc)

run_schema = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("started_at", T.TimestampType(), False),
        T.StructField("finished_at", T.TimestampType(), False),
        T.StructField("lookback_minutes", T.IntegerType(), False),
        T.StructField("alerts_window_rows", T.LongType(), False),
        T.StructField("status_window_rows", T.LongType(), False),
        T.StructField("investigations_window_rows", T.LongType(), False),
        T.StructField("actions_window_rows", T.LongType(), False),
        T.StructField("alerts_fact_rows", T.LongType(), False),
        T.StructField("status_fact_rows", T.LongType(), False),
        T.StructField("investigation_fact_rows", T.LongType(), False),
        T.StructField("action_fact_rows", T.LongType(), False),
        T.StructField("gold_dates", T.LongType(), False),
        T.StructField("status", T.StringType(), False),
    ]
)

run_row = [
    (
        RUN_ID,
        RUN_STARTED_AT,
        RUN_FINISHED_AT,
        LOOKBACK_MINUTES,
        alerts_window_rows,
        status_window_rows,
        investigations_window_rows,
        actions_window_rows,
        alerts_fact_rows,
        status_fact_rows,
        investigation_fact_rows,
        action_fact_rows,
        gold_dates,
        "SUCCESS",
    )
]

(
    spark.createDataFrame(run_row, schema=run_schema)
    .write.format("delta")
    .mode("append")
    .saveAsTable(RUN_HISTORY)
)

print(
    "Incremental CDC analytics completed successfully: "
    f"gold_dates={gold_dates}; "
    f"alerts_fact={alerts_fact_rows}; "
    f"status_fact={status_fact_rows}; "
    f"investigation_fact={investigation_fact_rows}; "
    f"action_fact={action_fact_rows}"
)

display(
    spark.table(GOLD)
    .orderBy("metric_date")
)

display(
    spark.table(STATE)
    .orderBy("source_name")
)

display(
    spark.table(RUN_HISTORY)
    .orderBy(F.desc("finished_at"))
    .limit(10)
)
