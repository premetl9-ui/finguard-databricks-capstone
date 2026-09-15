# Databricks notebook source
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
from pyspark.sql import Row, functions as F, types as T

# Allow imports from repository src/ when executed from a Databricks Git folder.
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
for candidate in [os.path.join(repo_root, "src"), os.path.join(os.getcwd(), "src")]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from finguard.fx import AlphaVantageClient, AlphaVantageError  # noqa: E402

CATALOG = "bootcamp_students"
USER_EMAIL = spark.sql("SELECT current_user() AS user_email").first()["user_email"]
USERNAME = USER_EMAIL.split("@")[0].replace(".", "_").replace("-", "_")
BRONZE = f"{CATALOG}.{USERNAME}_bronze.bronze_fx_api"
SILVER = f"{CATALOG}.{USERNAME}_silver.silver_fx_rates"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime Parameters
# MAGIC Configure the FX retrieval mode and requested currency pairs.

# COMMAND ----------

try:
    dbutils.widgets.text("mode", "current")
    dbutils.widgets.text("pairs", "USD:EUR,USD:GBP,USD:JPY")
    mode = dbutils.widgets.get("mode").strip().lower()
    pair_text = dbutils.widgets.get("pairs")
except Exception:
    mode = "current"
    pair_text = "USD:EUR,USD:GBP,USD:JPY"

pairs = [tuple(item.split(":")) for item in pair_text.split(",") if ":" in item]

# COMMAND ----------

# MAGIC %md
# MAGIC ## API Authentication
# MAGIC Read the Alpha Vantage API key from Databricks Secrets or the development environment.

# COMMAND ----------

api_key = None
try:
    scope = spark.conf.get("finguard.secret_scope", "")
    key_name = spark.conf.get("finguard.alpha_vantage_secret_key", "alpha-vantage-api-key")
    if scope:
        api_key = dbutils.secrets.get(scope=scope, key=key_name)
except Exception:
    pass
api_key = api_key or os.getenv("ALPHA_VANTAGE_API_KEY")
if not api_key:
    raise RuntimeError("Configure ALPHA_VANTAGE_API_KEY or finguard.secret_scope before running FX ingestion")

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

if bronze_rows:
    spark.createDataFrame(bronze_rows).write.mode("append").format("delta").saveAsTable(BRONZE)

if not rate_rows:
    raise AlphaVantageError("No FX rates were successfully retrieved; inspect bronze_fx_api for errors")

rate_schema = T.StructType(
    [
        T.StructField("from_currency", T.StringType(), False),
        T.StructField("to_currency", T.StringType(), False),
        T.StructField("rate_date", T.StringType(), False),
        T.StructField("exchange_rate", T.DecimalType(24, 10), False),
        T.StructField("provider_timestamp", T.TimestampType(), False),
    ]
)

rates = (
    spark.createDataFrame(rate_rows, schema=rate_schema)
    .withColumn("rate_date", F.to_date("rate_date"))
    .withColumn("provider", F.lit("ALPHA_VANTAGE"))
    .withColumn("refreshed_at", F.current_timestamp())
    .withColumn(
        "is_stale",
        F.col("provider_timestamp") < F.current_timestamp() - F.expr("INTERVAL 2 DAYS"),
    )
    .dropDuplicates(["from_currency", "to_currency", "rate_date"])
)

target = DeltaTable.forName(spark, SILVER)
(
    target.alias("t")
    .merge(
        rates.alias("s"),
        "t.from_currency = s.from_currency AND t.to_currency = s.to_currency AND t.rate_date = s.rate_date",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"FX mode={mode}; pairs={pairs}; rates upserted={rates.count():,}")
