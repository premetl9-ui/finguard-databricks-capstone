from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Any, Callable

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from finguard import lakebase, lakehouse  # noqa: E402


class AuthorizationError(PermissionError):
    pass


class ValidationError(ValueError):
    pass


ROLE_ORDER = {"ANALYST": 1, "SENIOR_ANALYST": 2, "ADMIN": 3}
VALID_ALERT_STATUSES = {"OPEN", "ASSIGNED", "ESCALATED", "RESOLVED", "CLOSED"}
CATALOG = os.getenv("FINGUARD_CATALOG", "finguard")


@dataclass(frozen=True)
class Actor:
    user_id: str
    email: str
    role: str


def require_role(actor: Actor, minimum: str) -> None:
    if ROLE_ORDER.get(actor.role, 0) < ROLE_ORDER[minimum]:
        raise AuthorizationError(f"{actor.role} cannot perform an action requiring {minimum}")


def get_user_by_email(email: str) -> Actor:
    row = lakebase.fetch_one(
        "SELECT user_id::text, email, role FROM users WHERE lower(email) = lower(%s)",
        (email,),
    )
    if not row:
        raise AuthorizationError(f"No FinGuard user is registered for {email}")
    return Actor(user_id=str(row["user_id"]), email=row["email"], role=row["role"])


# -----------------------------
# Read-only tools
# -----------------------------
def get_alert(actor: Actor, alert_id: str) -> dict[str, Any]:
    row = lakebase.fetch_one(
        """
        SELECT a.alert_id::text, a.transaction_id, a.customer_id, a.risk_score,
               a.risk_level, a.alert_reason, a.status, a.assigned_to::text,
               a.created_at, a.updated_at
        FROM fraud_alerts a
        WHERE a.alert_id = %s::uuid
        """,
        (alert_id,),
    )
    if not row:
        raise ValidationError("Alert not found")
    return row


def list_open_alerts(actor: Actor, limit: int = 50) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit), 200))
    return lakebase.fetch_all(
        """
        SELECT alert_id::text, transaction_id, customer_id, risk_score, risk_level,
               status, assigned_to::text, created_at
        FROM fraud_alerts
        WHERE status NOT IN ('RESOLVED', 'CLOSED')
        ORDER BY risk_score DESC, created_at DESC
        LIMIT %s
        """,
        (limit,),
    )


def get_transaction(actor: Actor, transaction_id: str) -> dict[str, Any]:
    row = lakehouse.get_transaction(CATALOG, transaction_id)
    if not row:
        raise ValidationError("Transaction not found")
    return row


def get_customer_transactions(actor: Actor, customer_id: str, limit: int = 50) -> list[dict[str, Any]]:
    return lakehouse.get_customer_transactions(CATALOG, customer_id, limit)


def get_customer_risk_profile(actor: Actor, customer_id: str) -> dict[str, Any]:
    row = lakehouse.get_customer_risk_profile(CATALOG, customer_id)
    if not row:
        raise ValidationError("Customer risk profile not found")
    return row


def get_exchange_rate(actor: Actor, from_currency: str, to_currency: str = "USD") -> dict[str, Any]:
    row = lakehouse.get_exchange_rate(CATALOG, from_currency, to_currency)
    if not row:
        raise ValidationError("Exchange rate not found")
    return row


def get_investigation_notes(actor: Actor, alert_id: str) -> list[dict[str, Any]]:
    return lakebase.fetch_all(
        """
        SELECT i.investigation_id::text, n.note_id::text, n.note_text,
               n.author_id::text, n.created_at
        FROM investigations i
        JOIN investigation_notes n ON n.investigation_id = i.investigation_id
        WHERE i.alert_id = %s::uuid
        ORDER BY n.created_at DESC
        """,
        (alert_id,),
    )


# -----------------------------
# Controlled write tools
# -----------------------------
def create_investigation(actor: Actor, alert_id: str, summary: str = "") -> dict[str, Any]:
    require_role(actor, "ANALYST")
    if len(summary) > 4000:
        raise ValidationError("Investigation summary is too long")

    with lakebase.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM fraud_alerts WHERE alert_id = %s::uuid FOR UPDATE", (alert_id,))
        alert = cur.fetchone()
        if not alert:
            raise ValidationError("Alert not found")
        if alert["status"] in {"RESOLVED", "CLOSED"}:
            raise ValidationError("Cannot open an investigation for a closed alert")

        cur.execute(
            """
            INSERT INTO investigations (alert_id, opened_by, assigned_to, status, summary)
            VALUES (%s::uuid, %s::uuid, %s::uuid, 'OPEN', %s)
            RETURNING investigation_id::text, alert_id::text, status, opened_at
            """,
            (alert_id, actor.user_id, actor.user_id, summary),
        )
        result = cur.fetchone()
        conn.commit()

    lakebase.record_agent_action(
        actor.user_id,
        "create_investigation",
        "WRITE",
        "SUCCESS",
        alert_id=alert_id,
        investigation_id=result["investigation_id"],
        request_payload={"summary": summary},
        result_payload=result,
    )
    return result


def assign_alert(actor: Actor, alert_id: str, analyst_id: str) -> dict[str, Any]:
    require_role(actor, "ANALYST")
    if actor.role == "ANALYST" and analyst_id != actor.user_id:
        raise AuthorizationError("Analysts may only assign alerts to themselves")

    with lakebase.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM fraud_alerts WHERE alert_id = %s::uuid FOR UPDATE", (alert_id,))
        current = cur.fetchone()
        if not current:
            raise ValidationError("Alert not found")
        if current["status"] in {"RESOLVED", "CLOSED"}:
            raise ValidationError("Closed alerts cannot be reassigned")

        cur.execute(
            """
            UPDATE fraud_alerts
            SET assigned_to = %s::uuid, status = 'ASSIGNED', updated_at = CURRENT_TIMESTAMP
            WHERE alert_id = %s::uuid
            RETURNING alert_id::text, assigned_to::text, status
            """,
            (analyst_id, alert_id),
        )
        result = cur.fetchone()
        cur.execute(
            """
            INSERT INTO alert_status_history (alert_id, old_status, new_status, changed_by, change_source, reason)
            VALUES (%s::uuid, %s, 'ASSIGNED', %s::uuid, 'AGENT', 'Alert assignment')
            """,
            (alert_id, current["status"], actor.user_id),
        )
        conn.commit()

    lakebase.record_agent_action(
        actor.user_id,
        "assign_alert",
        "WRITE",
        "SUCCESS",
        alert_id=alert_id,
        request_payload={"analyst_id": analyst_id},
        result_payload=result,
    )
    return result


def add_investigation_note(actor: Actor, investigation_id: str, note: str) -> dict[str, Any]:
    require_role(actor, "ANALYST")
    note = note.strip()
    if not note or len(note) > 8000:
        raise ValidationError("Note must contain 1-8000 characters")

    with lakebase.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT alert_id::text FROM investigations WHERE investigation_id = %s::uuid",
            (investigation_id,),
        )
        investigation = cur.fetchone()
        if not investigation:
            raise ValidationError("Investigation not found")
        cur.execute(
            """
            INSERT INTO investigation_notes (investigation_id, author_id, note_text)
            VALUES (%s::uuid, %s::uuid, %s)
            RETURNING note_id::text, investigation_id::text, created_at
            """,
            (investigation_id, actor.user_id, note),
        )
        result = cur.fetchone()
        conn.commit()

    lakebase.record_agent_action(
        actor.user_id,
        "add_investigation_note",
        "WRITE",
        "SUCCESS",
        alert_id=investigation["alert_id"],
        investigation_id=investigation_id,
        request_payload={"note": note},
        result_payload=result,
    )
    return result


def update_alert_status(actor: Actor, alert_id: str, new_status: str, reason: str) -> dict[str, Any]:
    require_role(actor, "SENIOR_ANALYST")
    new_status = new_status.upper().strip()
    if new_status not in VALID_ALERT_STATUSES:
        raise ValidationError(f"Invalid alert status: {new_status}")
    if not reason.strip():
        raise ValidationError("A reason is required for status changes")

    allowed_transitions = {
        "OPEN": {"ASSIGNED", "ESCALATED", "RESOLVED"},
        "ASSIGNED": {"ESCALATED", "RESOLVED", "OPEN"},
        "ESCALATED": {"ASSIGNED", "RESOLVED"},
        "RESOLVED": {"CLOSED", "OPEN"},
        "CLOSED": set(),
    }

    with lakebase.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM fraud_alerts WHERE alert_id = %s::uuid FOR UPDATE", (alert_id,))
        current = cur.fetchone()
        if not current:
            raise ValidationError("Alert not found")
        old_status = current["status"]
        if new_status not in allowed_transitions.get(old_status, set()):
            raise ValidationError(f"Transition {old_status} -> {new_status} is not allowed")

        cur.execute(
            """
            UPDATE fraud_alerts
            SET status = %s, updated_at = CURRENT_TIMESTAMP
            WHERE alert_id = %s::uuid
            RETURNING alert_id::text, status, updated_at
            """,
            (new_status, alert_id),
        )
        result = cur.fetchone()
        cur.execute(
            """
            INSERT INTO alert_status_history (alert_id, old_status, new_status, changed_by, change_source, reason)
            VALUES (%s::uuid, %s, %s, %s::uuid, 'AGENT', %s)
            """,
            (alert_id, old_status, new_status, actor.user_id, reason),
        )
        conn.commit()

    lakebase.record_agent_action(
        actor.user_id,
        "update_alert_status",
        "WRITE",
        "SUCCESS",
        alert_id=alert_id,
        request_payload={"new_status": new_status, "reason": reason},
        result_payload=result,
    )
    return result


def escalate_alert(actor: Actor, alert_id: str, reason: str) -> dict[str, Any]:
    return update_alert_status(actor, alert_id, "ESCALATED", reason)


def resolve_alert(actor: Actor, alert_id: str, resolution: str) -> dict[str, Any]:
    return update_alert_status(actor, alert_id, "RESOLVED", resolution)


TOOL_REGISTRY: dict[str, Callable[..., Any]] = {
    "get_alert": get_alert,
    "list_open_alerts": list_open_alerts,
    "get_transaction": get_transaction,
    "get_customer_transactions": get_customer_transactions,
    "get_customer_risk_profile": get_customer_risk_profile,
    "get_exchange_rate": get_exchange_rate,
    "get_investigation_notes": get_investigation_notes,
    "create_investigation": create_investigation,
    "assign_alert": assign_alert,
    "add_investigation_note": add_investigation_note,
    "update_alert_status": update_alert_status,
    "escalate_alert": escalate_alert,
    "resolve_alert": resolve_alert,
}
