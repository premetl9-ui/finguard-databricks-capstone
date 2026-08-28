# Databricks notebook source
# FinGuard - 02 Silver cleaning, quality contracts, FX enrichment, and features

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOG = spark.conf.get("finguard.catalog", "finguard")
HOME_CURRENCY = spark.conf.get("finguard.home_currency", "USD").upper()
HIGH_VALUE_THRESHOLD = float(spark.conf.get("finguard.high_value_threshold", "10000"))

BRONZE = f"{CATALOG}.bronze.bronze_transactions"
FX = f"{CATALOG}.silver.silver_fx_rates"
SILVER = f"{CATALOG}.silver.silver_transactions"
QUARANTINE = f"{CATALOG}.silver.silver_transaction_quarantine"

bronze = spark.table(BRONZE).dropDuplicates(["transaction_id"])

# -----------------------------
# Silver data-quality contract
# -----------------------------
quality = (
    bronze.withColumn("source_currency", F.upper(F.trim("source_currency")))
    .withColumn("transaction_type", F.upper(F.trim("transaction_type")))
    .withColumn(
        "dq_error",
        F.when(F.col("transaction_id").isNull(), F.lit("NULL_TRANSACTION_ID"))
        .when(F.col("customer_id").isNull(), F.lit("NULL_CUSTOMER_ID"))
        .when(F.col("destination_id").isNull(), F.lit("NULL_DESTINATION_ID"))
        .when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("INVALID_AMOUNT"))
        .when(
            ~F.col("source_currency").rlike("^[A-Z]{3}$"),
            F.lit("INVALID_CURRENCY_CODE"),
        )
        .when(F.col("event_timestamp").isNull(), F.lit("INVALID_EVENT_TIMESTAMP"))
        .when(
            F.col("event_timestamp") > F.current_timestamp() + F.expr("INTERVAL 5 MINUTES"),
            F.lit("FUTURE_EVENT_TIMESTAMP"),
        )
    )
)

invalid = quality.filter(F.col("dq_error").isNotNull())
valid = quality.filter(F.col("dq_error").isNull()).drop("dq_error")

if invalid.limit(1).count() > 0:
    (
        invalid.withColumn("quarantined_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(QUARANTINE)
    )

# -----------------------------
# FX enrichment
# -----------------------------
fx = (
    spark.table(FX)
    .filter(F.col("to_currency") == HOME_CURRENCY)
    .select(
        "from_currency",
        "to_currency",
        "rate_date",
        "exchange_rate",
        "provider_timestamp",
        "is_stale",
    )
)

joined = valid.join(
    fx,
    (valid.source_currency == fx.from_currency) & (valid.event_date == fx.rate_date),
    "left",
)

joined = (
    joined.withColumn("home_currency", F.lit(HOME_CURRENCY))
    .withColumn(
        "exchange_rate",
        F.when(F.col("source_currency") == HOME_CURRENCY, F.lit(1.0)).otherwise(
            F.col("exchange_rate")
        ),
    )
    .withColumn(
        "amount_home_currency",
        (F.col("amount") * F.col("exchange_rate")).cast("decimal(18,2)"),
    )
    .withColumn(
        "data_quality_status",
        F.when(F.col("exchange_rate").isNull(), F.lit("MISSING_FX_RATE"))
        .when(F.coalesce(F.col("is_stale"), F.lit(False)), F.lit("STALE_FX_RATE"))
        .otherwise(F.lit("VALID")),
    )
)

# Records with missing FX rates are not risk-scored until enrichment succeeds.
scorable = joined.filter(F.col("exchange_rate").isNotNull())

# -----------------------------
# Customer behavior features
# -----------------------------
customer_time_window = (
    Window.partitionBy("customer_id")
    .orderBy(F.col("event_timestamp").cast("long"))
    .rangeBetween(-600, 0)
)
customer_history_window = (
    Window.partitionBy("customer_id")
    .orderBy("event_timestamp")
    .rowsBetween(Window.unboundedPreceding, -1)
)
destination_window = Window.partitionBy("customer_id", "destination_id").orderBy("event_timestamp")

customer_counts = scorable.groupBy("customer_id").agg(
    F.count("*").alias("customer_txn_count")
)
type_counts = scorable.groupBy("customer_id", "transaction_type").agg(
    F.count("*").alias("customer_type_count")
)

features = (
    scorable.withColumn(
        "customer_avg_amount",
        F.avg(F.col("amount_home_currency").cast("double")).over(customer_history_window),
    )
    .withColumn("recent_txn_count", F.count("*").over(customer_time_window))
    .withColumn("destination_sequence", F.row_number().over(destination_window))
    .join(customer_counts, "customer_id", "left")
    .join(type_counts, ["customer_id", "transaction_type"], "left")
    .withColumn(
        "customer_avg_amount",
        F.coalesce(F.col("customer_avg_amount"), F.col("amount_home_currency").cast("double")),
    )
    .withColumn(
        "amount_deviation",
        F.when(F.col("customer_avg_amount") > 0,
               F.col("amount_home_currency").cast("double") / F.col("customer_avg_amount"))
        .otherwise(F.lit(1.0)),
    )
    .withColumn("is_high_value", F.col("amount_home_currency") >= F.lit(HIGH_VALUE_THRESHOLD))
    .withColumn("is_new_destination", F.col("destination_sequence") == 1)
    .withColumn(
        "is_unusual_type",
        (F.col("customer_txn_count") >= 20)
        & ((F.col("customer_type_count") / F.col("customer_txn_count")) < 0.05),
    )
    .withColumn("is_international", F.col("source_currency") != F.lit(HOME_CURRENCY))
    .withColumn(
        "merchant_category",
        F.when(F.col("transaction_type") == "PAYMENT", "PAYMENT")
        .when(F.col("transaction_type") == "TRANSFER", "TRANSFER")
        .when(F.col("transaction_type") == "CASH_OUT", "CASH")
        .when(F.col("transaction_type") == "CASH_IN", "CASH")
        .when(F.col("transaction_type") == "DEBIT", "DEBIT")
        .otherwise("OTHER"),
    )
    .withColumn("processed_at", F.current_timestamp())
    .select(
        "transaction_id",
        "customer_id",
        "destination_id",
        "transaction_type",
        "event_timestamp",
        "event_date",
        "amount",
        "source_currency",
        "home_currency",
        F.col("exchange_rate").cast("decimal(24,10)"),
        "amount_home_currency",
        F.col("customer_avg_amount").cast("decimal(18,2)"),
        "amount_deviation",
        "recent_txn_count",
        "is_high_value",
        "is_new_destination",
        "is_unusual_type",
        "is_international",
        "merchant_category",
        "data_quality_status",
        "processed_at",
    )
)

# Idempotent Silver upsert keyed by transaction_id.
target = DeltaTable.forName(spark, SILVER)
(
    target.alias("t")
    .merge(features.alias("s"), "t.transaction_id = s.transaction_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"Silver valid/scorable rows processed: {features.count():,}")
print(f"Quarantined rows this run: {invalid.count():,}")
