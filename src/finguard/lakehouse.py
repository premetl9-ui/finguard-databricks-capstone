from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

from databricks import sql


def _hostname() -> str:
    value = os.getenv("DATABRICKS_SERVER_HOSTNAME") or os.getenv("DATABRICKS_HOST", "")
    return value.replace("https://", "").replace("http://", "").rstrip("/")


@contextmanager
def connect() -> Iterator[Any]:
    hostname = _hostname()
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")
    if not hostname or not http_path or not token:
        raise RuntimeError(
            "Databricks SQL access requires DATABRICKS_SERVER_HOSTNAME/DATABRICKS_HOST, "
            "DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN. In a Databricks App, map these "
            "to the authorized SQL Warehouse/app identity configuration."
        )
    conn = sql.connect(server_hostname=hostname, http_path=http_path, access_token=token)
    try:
        yield conn
    finally:
        conn.close()


def query(sql_text: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text, parameters or [])
            columns = [column[0] for column in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def get_transaction(catalog: str, transaction_id: str) -> dict[str, Any] | None:
    rows = query(
        f"""
        SELECT transaction_id, customer_id, destination_id, event_timestamp,
               amount_home_currency, risk_score, risk_level, risk_reasons
        FROM {catalog}.gold.gold_transaction_risk
        WHERE transaction_id = ?
        LIMIT 1
        """,
        [transaction_id],
    )
    return rows[0] if rows else None


def get_customer_transactions(catalog: str, customer_id: str, limit: int = 50) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit), 200))
    # LIMIT is validated as an integer and interpolated; customer_id remains parameterized.
    return query(
        f"""
        SELECT transaction_id, event_timestamp, destination_id, amount_home_currency,
               risk_score, risk_level, risk_reasons
        FROM {catalog}.gold.gold_transaction_risk
        WHERE customer_id = ?
        ORDER BY event_timestamp DESC
        LIMIT {limit}
        """,
        [customer_id],
    )


def get_customer_risk_profile(catalog: str, customer_id: str) -> dict[str, Any] | None:
    rows = query(
        f"""
        SELECT customer_id, transaction_count, total_amount, avg_amount,
               high_risk_transaction_count, max_risk_score, customer_risk_level, refreshed_at
        FROM {catalog}.gold.gold_customer_risk
        WHERE customer_id = ?
        LIMIT 1
        """,
        [customer_id],
    )
    return rows[0] if rows else None


def get_exchange_rate(catalog: str, from_currency: str, to_currency: str) -> dict[str, Any] | None:
    rows = query(
        f"""
        SELECT from_currency, to_currency, rate_date, exchange_rate,
               provider_timestamp, provider, is_stale
        FROM {catalog}.silver.silver_fx_rates
        WHERE from_currency = ? AND to_currency = ?
        ORDER BY rate_date DESC, provider_timestamp DESC
        LIMIT 1
        """,
        [from_currency.upper(), to_currency.upper()],
    )
    return rows[0] if rows else None
