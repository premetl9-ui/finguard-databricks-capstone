from __future__ import annotations

import os
import sys
from typing import Any

import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(REPO_ROOT, "src")
AGENT_DIR = os.path.join(REPO_ROOT, "agent")
for path in [SRC, AGENT_DIR]:
    if path not in sys.path:
        sys.path.insert(0, path)

from finguard import lakebase  # noqa: E402
from tools import Actor, get_user_by_email  # noqa: E402


def current_actor(email: str) -> Actor:
    return get_user_by_email(email)


def alert_queue(limit: int = 200) -> pd.DataFrame:
    rows = lakebase.fetch_all(
        """
        SELECT alert_id::text, transaction_id, customer_id, risk_score, risk_level,
               status, assigned_to::text, created_at, updated_at
        FROM fraud_alerts
        ORDER BY
          CASE risk_level WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
          created_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    return pd.DataFrame(rows)


def alert_detail(alert_id: str) -> dict[str, Any] | None:
    return lakebase.fetch_one(
        """
        SELECT alert_id::text, transaction_id, customer_id, risk_score, risk_level,
               alert_reason, status, assigned_to::text, created_at, updated_at
        FROM fraud_alerts WHERE alert_id = %s::uuid
        """,
        (alert_id,),
    )


def investigations_for_alert(alert_id: str) -> pd.DataFrame:
    rows = lakebase.fetch_all(
        """
        SELECT investigation_id::text, status, priority, summary, resolution,
               opened_at, closed_at, assigned_to::text
        FROM investigations
        WHERE alert_id = %s::uuid
        ORDER BY opened_at DESC
        """,
        (alert_id,),
    )
    return pd.DataFrame(rows)


def investigation_notes(investigation_id: str) -> pd.DataFrame:
    rows = lakebase.fetch_all(
        """
        SELECT note_id::text, author_id::text, note_text, created_at
        FROM investigation_notes
        WHERE investigation_id = %s::uuid
        ORDER BY created_at DESC
        """,
        (investigation_id,),
    )
    return pd.DataFrame(rows)


def operational_metrics() -> dict[str, Any]:
    row = lakebase.fetch_one(
        """
        SELECT
          count(*) AS total_alerts,
          count(*) FILTER (WHERE status NOT IN ('RESOLVED', 'CLOSED')) AS open_alerts,
          count(*) FILTER (WHERE risk_level = 'CRITICAL' AND status NOT IN ('RESOLVED', 'CLOSED')) AS critical_open,
          count(*) FILTER (WHERE status = 'ESCALATED') AS escalated
        FROM fraud_alerts
        """
    )
    return row or {"total_alerts": 0, "open_alerts": 0, "critical_open": 0, "escalated": 0}
