# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FinGuard - 03 Gold Risk Scoring
# MAGIC Calculate transaction risk scores and refresh customer and daily Gold analytics.

# COMMAND ----------

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

ALERT_THRESHOLD = 50

SILVER_SCHEMA = f"{USERNAME}_silver"
GOLD_SCHEMA = f"{USERNAME}_gold"

SILVER = f"{CATALOG}.{SILVER_SCHEMA}.silver_transactions"
RISK = f"{CATALOG}.{GOLD_SCHEMA}.gold_transaction_risk"
CUSTOMER_RISK = f"{CATALOG}.{GOLD_SCHEMA}.gold_customer_risk"
DAILY = f"{CATALOG}.{GOLD_SCHEMA}.gold_daily_transaction_metrics"

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}"
)

print(f"Silver source: {SILVER}")
print(f"Gold risk target: {RISK}")
print(f"Alert threshold: {ALERT_THRESHOLD}")

silver = (
    spark.table(SILVER)
    .filter(
        F.col("data_quality_status").isin(
            "VALID",
            "STALE_FX_RATE",
        )
    )
)

amount_rule = F.col("amount_deviation") >= 3.0
velocity_rule = F.col("recent_txn_count") >= 3
new_destination_rule = F.col("is_new_destination")
unusual_type_rule = F.col("is_unusual_type")
international_rule = F.col("is_international")
high_value_rule = F.col("is_high_value")

risk_score = (
    F.when(amount_rule, 30).otherwise(0)
    + F.when(velocity_rule, 20).otherwise(0)
    + F.when(new_destination_rule, 15).otherwise(0)
    + F.when(unusual_type_rule, 15).otherwise(0)
    + F.when(international_rule, 10).otherwise(0)
    + F.when(high_value_rule, 10).otherwise(0)
)

reason_array = F.array(
    F.when(
        amount_rule,
        F.lit("amount >= 3x customer average"),
    ),
    F.when(
        velocity_rule,
        F.lit("multiple transactions in short window"),
    ),
    F.when(
        new_destination_rule,
        F.lit("new destination/account"),
    ),
    F.when(
        unusual_type_rule,
        F.lit("unusual transaction type"),
    ),
    F.when(
        international_rule,
        F.lit("international transaction"),
    ),
    F.when(
        high_value_rule,
        F.lit("high-value transaction"),
    ),
)

scored = (
    silver.withColumn(
        "risk_score",
        F.least(risk_score, F.lit(100)).cast("int"),
    )
    .withColumn(
        "risk_level",
        F.when(F.col("risk_score") >= 80, "CRITICAL")
        .when(F.col("risk_score") >= 50, "HIGH")
        .when(F.col("risk_score") >= 30, "MEDIUM")
        .otherwise("LOW"),
    )
    .withColumn(
        "risk_reasons",
        F.filter(
            reason_array,
            lambda reason: reason.isNotNull(),
        ),
    )
    .withColumn(
        "is_alert_candidate",
        F.col("risk_score") >= ALERT_THRESHOLD,
    )
    .withColumn(
        "scored_at",
        F.current_timestamp(),
    )
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
    .dropDuplicates(["transaction_id"])
)

# Create the Gold risk table on the first run.
# On later runs, update existing transactions and insert new transactions.
if spark.catalog.tableExists(RISK):
    risk_target = DeltaTable.forName(spark, RISK)

    (
        risk_target.alias("t")
        .merge(
            scored.alias("s"),
            "t.transaction_id = s.transaction_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Gold risk table merged: {RISK}")
else:
    (
        scored.write.format("delta")
        .mode("overwrite")
        .saveAsTable(RISK)
    )

    print(f"Gold risk table created: {RISK}")

print(f"Scored rows: {scored.count():,}")
print(
    "Alert candidates: "
    f"{scored.filter(F.col('is_alert_candidate')).count():,}"
)

# COMMAND ----------

display(
    scored.groupBy(
        "risk_score",
        "risk_level",
    )
    .count()
    .orderBy(F.desc("risk_score"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Risk Snapshot
# MAGIC Aggregate transaction risk into the latest customer-level risk view.

# COMMAND ----------

customer = (
    spark.table(RISK)
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount_home_currency").cast("decimal(20,2)").alias("total_amount"),
        F.avg("amount_home_currency").cast("decimal(18,2)").alias("avg_amount"),
        F.sum(F.when(F.col("risk_score") >= 60, 1).otherwise(0)).alias("high_risk_transaction_count"),
        F.max("risk_score").alias("max_risk_score"),
    )
    .withColumn(
        "customer_risk_level",
        F.when(F.col("max_risk_score") >= 80, "CRITICAL")
        .when(F.col("max_risk_score") >= 60, "HIGH")
        .when(F.col("max_risk_score") >= 30, "MEDIUM")
        .otherwise("LOW"),
    )
    .withColumn("refreshed_at", F.current_timestamp())
)
customer.createOrReplaceTempView("customer_risk_refresh")
spark.sql(f"INSERT OVERWRITE {CUSTOMER_RISK} SELECT * FROM customer_risk_refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Transaction Metrics
# MAGIC Refresh daily transaction counts, amounts, and high-risk totals.

# COMMAND ----------

daily = (
    spark.table(RISK)
    .groupBy("event_date")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount_home_currency").cast("decimal(22,2)").alias("total_amount"),
        F.avg("amount_home_currency").cast("decimal(18,2)").alias("avg_amount"),
        F.sum(F.when(F.col("risk_level") == "HIGH", 1).otherwise(0)).alias("high_risk_count"),
        F.sum(F.when(F.col("risk_level") == "CRITICAL", 1).otherwise(0)).alias("critical_risk_count"),
    )
    .withColumn("refreshed_at", F.current_timestamp())
)
daily.createOrReplaceTempView("daily_metrics_refresh")
spark.sql(f"INSERT OVERWRITE {DAILY} SELECT * FROM daily_metrics_refresh")

print(f"Gold transaction risks processed: {scored.count():,}")
print(f"Alert candidates: {scored.filter('is_alert_candidate').count():,}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS customer_count
# MAGIC FROM bootcamp_students.premetl9_gold.gold_customer_risk;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(event_date) AS earliest_date,
# MAGIC     MAX(event_date) AS latest_date,
# MAGIC     SUM(transaction_count) AS total_transactions
# MAGIC FROM bootcamp_students.premetl9_gold.gold_daily_transaction_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT risk_level, COUNT(*)
# MAGIC FROM bootcamp_students.premetl9_gold.gold_transaction_risk
# MAGIC GROUP BY risk_level;