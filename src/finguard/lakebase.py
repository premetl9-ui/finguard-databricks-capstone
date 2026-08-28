from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row


def _connection_kwargs() -> dict[str, Any]:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return {"conninfo": database_url}

    required = {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT", "5432"),
        "dbname": os.getenv("PGDATABASE"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
    }
    missing = [name for name, value in required.items() if not value and name != "password"]
    if missing:
        raise RuntimeError(f"Missing Lakebase connection settings: {', '.join(missing)}")
    return required


@contextmanager
def connect() -> Iterator[psycopg.Connection]:
    kwargs = _connection_kwargs()
    if "conninfo" in kwargs:
        conn = psycopg.connect(kwargs["conninfo"], row_factory=dict_row)
    else:
        conn = psycopg.connect(**kwargs, row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def fetch_one(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def fetch_all(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def upsert_fraud_alert(
    transaction_id: str,
    customer_id: str,
    risk_score: int,
    risk_level: str,
    alert_reason: str,
) -> dict[str, Any]:
    sql = """
    INSERT INTO fraud_alerts (
        transaction_id, customer_id, risk_score, risk_level, alert_reason, status
    ) VALUES (%s, %s, %s, %s, %s, 'OPEN')
    ON CONFLICT (transaction_id)
    DO UPDATE SET
        risk_score = EXCLUDED.risk_score,
        risk_level = EXCLUDED.risk_level,
        alert_reason = EXCLUDED.alert_reason,
        updated_at = CURRENT_TIMESTAMP
    RETURNING *
    """
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            sql,
            (transaction_id, customer_id, risk_score, risk_level, alert_reason),
        )
        row = cur.fetchone()
        conn.commit()
        return row


def record_agent_action(
    actor_user_id: str,
    tool_name: str,
    action_type: str,
    status: str,
    alert_id: str | None = None,
    investigation_id: str | None = None,
    request_payload: dict[str, Any] | None = None,
    result_payload: dict[str, Any] | None = None,
) -> None:
    sql = """
    INSERT INTO agent_actions (
        actor_user_id, alert_id, investigation_id, agent_name, tool_name,
        action_type, request_payload, result_payload, action_status
    ) VALUES (%s, %s, %s, 'finguard-investigator', %s, %s, %s::jsonb, %s::jsonb, %s)
    """
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            sql,
            (
                actor_user_id,
                alert_id,
                investigation_id,
                tool_name,
                action_type,
                json.dumps(request_payload or {}),
                json.dumps(result_payload or {}),
                status,
            ),
        )
        conn.commit()
