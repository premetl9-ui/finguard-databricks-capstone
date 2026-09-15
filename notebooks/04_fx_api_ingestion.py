# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FinGuard - 04 Alpha Vantage FX Ingestion
# MAGIC Retrieve current and daily FX rates and upsert the Bronze and Silver FX tables.

# COMMAND ----------

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from delta.tables import DeltaTable
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Allow imports from the repository src directory.
working_directory = os.getcwd()

candidate_paths = [
    os.path.join(working_directory, "src"),
    os.path.abspath(os.path.join(working_directory, "..", "src")),
]

for candidate_path in candidate_paths:
    if (
        os.path.isdir(candidate_path)
        and candidate_path not in sys.path
    ):
        sys.path.insert(0, candidate_path)

from finguard.fx import (  # noqa: E402
    AlphaVantageClient,
    AlphaVantageError,
)

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

BRONZE_SCHEMA = f"{USERNAME}_bronze"
SILVER_SCHEMA = f"{USERNAME}_silver"

BRONZE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_fx_api"
)

SILVER = (
    f"{CATALOG}.{SILVER_SCHEMA}.silver_fx_rates"
)

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{CATALOG}.{BRONZE_SCHEMA}"
)

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{CATALOG}.{SILVER_SCHEMA}"
)

print(f"Logged-in user: {USER_EMAIL}")
print(f"Bronze FX target: {BRONZE}")
print(f"Silver FX target: {SILVER}")
print("Alpha Vantage module imported successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime Parameters
# MAGIC Configure the FX retrieval mode and requested currency pairs.

# COMMAND ----------

default_mode = "current"
default_pairs = "USD:EUR,USD:GBP,USD:JPY"

try:
    dbutils.widgets.text(
        "mode",
        default_mode,
        "FX Retrieval Mode",
    )

    dbutils.widgets.text(
        "pairs",
        default_pairs,
        "Currency Pairs",
    )

    mode = (
        dbutils.widgets.get("mode")
        .strip()
        .lower()
    )

    pair_text = (
        dbutils.widgets.get("pairs")
        .strip()
        .upper()
    )

except Exception as error:
    print(f"Widgets unavailable; using defaults: {error}")

    mode = default_mode
    pair_text = default_pairs

# Validate retrieval mode.
if mode not in {"current", "daily"}:
    raise ValueError(
        "Invalid mode. Use 'current' or 'daily'."
    )

# Parse and validate currency pairs.
pairs = []

for item in pair_text.split(","):
    item = item.strip()

    if not item:
        continue

    currencies = [
        value.strip().upper()
        for value in item.split(":")
    ]

    if (
        len(currencies) != 2
        or len(currencies[0]) != 3
        or len(currencies[1]) != 3
    ):
        raise ValueError(
            f"Invalid currency pair: {item}. "
            "Expected format such as USD:EUR."
        )

    pairs.append(tuple(currencies))

if not pairs:
    raise ValueError(
        "At least one currency pair is required."
    )

print(f"FX retrieval mode: {mode}")
print(f"Currency pairs: {pairs}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## API Authentication
# MAGIC Read the Alpha Vantage API key from Databricks Secrets or the development environment.

# COMMAND ----------

SECRET_SCOPE = "finguard"
SECRET_KEY = "alpha-vantage-api-key"

api_key = None
secret_error = None

# Preferred method: Databricks secret.
try:
    api_key = dbutils.secrets.get(
        scope=SECRET_SCOPE,
        key=SECRET_KEY,
    )
except Exception as error:
    secret_error = str(error)

# Optional fallback for local development.
if not api_key:
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

if not api_key:
    raise RuntimeError(
        "Alpha Vantage API key was not found. "
        f"Expected Databricks secret "
        f"'{SECRET_SCOPE}/{SECRET_KEY}' or environment variable "
        "'ALPHA_VANTAGE_API_KEY'. "
        f"Databricks secret error: {secret_error}"
    )

print("Alpha Vantage API key loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve FX Rates
# MAGIC Call Alpha Vantage and prepare Bronze API audit rows and normalized rate rows.

# COMMAND ----------

client = AlphaVantageClient(api_key=api_key, timeout_seconds=15, max_retries=3)

bronze_rows = []
rate_rows = []
now = datetime.now(timezone.utc)

for from_currency, to_currency in pairs:
    request_id = str(uuid.uuid4())
    try:
        if mode == "daily":
            payload = client.daily_rates(from_currency, to_currency, outputsize="compact")
            function_name = "FX_DAILY"
            series = payload.get("Time Series FX (Daily)", {})
            for rate_date, values in series.items():
                close_rate = Decimal(values["4. close"])
                provider_ts = datetime.strptime(rate_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                rate_rows.append(
                    (from_currency, to_currency, rate_date, close_rate, provider_ts)
                )
                if close_rate != 0:
                    rate_rows.append(
                        (to_currency, from_currency, rate_date, Decimal(1) / close_rate, provider_ts)
                    )
        else:
            result = client.current_rate(from_currency, to_currency)
            function_name = "CURRENCY_EXCHANGE_RATE"
            payload = result.raw
            rate_date = result.provider_timestamp.date().isoformat()
            rate_rows.append(
                (
                    result.from_currency,
                    result.to_currency,
                    rate_date,
                    result.rate,
                    result.provider_timestamp,
                )
            )
            if result.rate != 0:
                rate_rows.append(
                    (
                        result.to_currency,
                        result.from_currency,
                        rate_date,
                        Decimal(1) / result.rate,
                        result.provider_timestamp,
                    )
                )

        bronze_rows.append(
            Row(
                request_id=request_id,
                function_name=function_name,
                from_currency=from_currency,
                to_currency=to_currency,
                requested_at=now,
                http_status=200,
                response_json=json.dumps(payload),
                ingestion_status="SUCCESS",
                error_message=None,
            )
        )
    except Exception as exc:
        bronze_rows.append(
            Row(
                request_id=request_id,
                function_name="FX_DAILY" if mode == "daily" else "CURRENCY_EXCHANGE_RATE",
                from_currency=from_currency,
                to_currency=to_currency,
                requested_at=now,
                http_status=None,
                response_json=None,
                ingestion_status="FAILED",
                error_message=str(exc)[:2000],
            )
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist FX Results
# MAGIC Append API audit records to Bronze and upsert normalized exchange rates into Silver.

# COMMAND ----------

# Explicit schema is required because nullable columns may contain only None.
bronze_schema = T.StructType(
    [
        T.StructField(
            "request_id",
            T.StringType(),
            False,
        ),
        T.StructField(
            "function_name",
            T.StringType(),
            False,
        ),
        T.StructField(
            "from_currency",
            T.StringType(),
            False,
        ),
        T.StructField(
            "to_currency",
            T.StringType(),
            False,
        ),
        T.StructField(
            "requested_at",
            T.TimestampType(),
            False,
        ),
        T.StructField(
            "http_status",
            T.IntegerType(),
            True,
        ),
        T.StructField(
            "response_json",
            T.StringType(),
            True,
        ),
        T.StructField(
            "ingestion_status",
            T.StringType(),
            False,
        ),
        T.StructField(
            "error_message",
            T.StringType(),
            True,
        ),
    ]
)

# Write successful and failed API request details to Bronze.
if bronze_rows:
    bronze_df = spark.createDataFrame(
        bronze_rows,
        schema=bronze_schema,
    )

    (
        bronze_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(BRONZE)
    )

    print(
        f"Bronze FX audit rows written: "
        f"{len(bronze_rows)}"
    )

# Stop if every API request failed.
if not rate_rows:
    raise AlphaVantageError(
        "No FX rates were successfully retrieved. "
        f"Inspect {BRONZE} for API error details."
    )

rate_schema = T.StructType(
    [
        T.StructField(
            "from_currency",
            T.StringType(),
            False,
        ),
        T.StructField(
            "to_currency",
            T.StringType(),
            False,
        ),
        T.StructField(
            "rate_date",
            T.StringType(),
            False,
        ),
        T.StructField(
            "exchange_rate",
            T.DecimalType(24, 10),
            False,
        ),
        T.StructField(
            "provider_timestamp",
            T.TimestampType(),
            False,
        ),
    ]
)

rates = (
    spark.createDataFrame(
        rate_rows,
        schema=rate_schema,
    )
    .withColumn(
        "rate_date",
        F.to_date(F.col("rate_date")),
    )
    .withColumn(
        "provider",
        F.lit("ALPHA_VANTAGE"),
    )
    .withColumn(
        "refreshed_at",
        F.current_timestamp(),
    )
    .withColumn(
        "is_stale",
        F.col("provider_timestamp")
        < (
            F.current_timestamp()
            - F.expr("INTERVAL 2 DAYS")
        ),
    )
    .dropDuplicates(
        [
            "from_currency",
            "to_currency",
            "rate_date",
        ]
    )
)

# Merge when the Silver table exists; otherwise create it.
if spark.catalog.tableExists(SILVER):
    target = DeltaTable.forName(
        spark,
        SILVER,
    )

    (
        target.alias("t")
        .merge(
            rates.alias("s"),
            """
            t.from_currency = s.from_currency
            AND t.to_currency = s.to_currency
            AND t.rate_date = s.rate_date
            """,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Silver FX table merged: {SILVER}")

else:
    (
        rates.write.format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER)
    )

    print(f"Silver FX table created: {SILVER}")

rates_upserted = rates.count()

print(f"FX mode: {mode}")
print(f"Currency pairs: {pairs}")
print(f"FX rates processed: {rates_upserted:,}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     from_currency,
# MAGIC     to_currency,
# MAGIC     exchange_rate,
# MAGIC     rate_date,
# MAGIC     provider,
# MAGIC     is_stale
# MAGIC FROM bootcamp_students.premetl9_silver.silver_fx_rates
# MAGIC ORDER BY from_currency, to_currency;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     function_name,
# MAGIC     from_currency,
# MAGIC     to_currency,
# MAGIC     ingestion_status,
# MAGIC     requested_at,
# MAGIC     error_message
# MAGIC FROM bootcamp_students.premetl9_bronze.bronze_fx_api
# MAGIC ORDER BY requested_at DESC
# MAGIC LIMIT 10;