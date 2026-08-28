from __future__ import annotations

import json
import os
from typing import Any

from openai import OpenAI

from tools import Actor, TOOL_REGISTRY


MODEL = os.getenv("FINGUARD_MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
HIGH_IMPACT_TOOLS = {"escalate_alert", "resolve_alert", "update_alert_status"}


def _schema(name: str, description: str, properties: dict[str, Any], required: list[str] | None = None):
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required or [],
                "additionalProperties": False,
            },
        },
    }


TOOL_SCHEMAS = [
    _schema(
        "get_alert",
        "Retrieve one FinGuard fraud alert by alert ID.",
        {"alert_id": {"type": "string"}},
        ["alert_id"],
    ),
    _schema(
        "list_open_alerts",
        "List open, assigned, or escalated FinGuard alerts.",
        {"limit": {"type": "integer", "minimum": 1, "maximum": 200}},
    ),
    _schema(
        "get_transaction",
        "Retrieve a risk-scored transaction from the governed Gold Delta table.",
        {"transaction_id": {"type": "string"}},
        ["transaction_id"],
    ),
    _schema(
        "get_customer_transactions",
        "Retrieve recent risk-scored transactions for one customer.",
        {
            "customer_id": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 200},
        },
        ["customer_id"],
    ),
    _schema(
        "get_customer_risk_profile",
        "Retrieve the customer's Gold risk profile and behavioral summary.",
        {"customer_id": {"type": "string"}},
        ["customer_id"],
    ),
    _schema(
        "get_exchange_rate",
        "Retrieve the latest cached FX rate from Silver. Do not call the third-party API directly.",
        {
            "from_currency": {"type": "string", "minLength": 3, "maxLength": 3},
            "to_currency": {"type": "string", "minLength": 3, "maxLength": 3},
        },
        ["from_currency"],
    ),
    _schema(
        "get_investigation_notes",
        "Retrieve existing investigation notes for an alert.",
        {"alert_id": {"type": "string"}},
        ["alert_id"],
    ),
    _schema(
        "create_investigation",
        "Open an investigation for an alert.",
        {
            "alert_id": {"type": "string"},
            "summary": {"type": "string", "maxLength": 4000},
        },
        ["alert_id"],
    ),
    _schema(
        "assign_alert",
        "Assign an alert to an analyst. Analysts can only self-assign.",
        {"alert_id": {"type": "string"}, "analyst_id": {"type": "string"}},
        ["alert_id", "analyst_id"],
    ),
    _schema(
        "add_investigation_note",
        "Add an audit-trailed note to an existing investigation.",
        {
            "investigation_id": {"type": "string"},
            "note": {"type": "string", "minLength": 1, "maxLength": 8000},
        },
        ["investigation_id", "note"],
    ),
    _schema(
        "escalate_alert",
        "Escalate an alert. This high-impact action requires explicit analyst confirmation.",
        {"alert_id": {"type": "string"}, "reason": {"type": "string", "minLength": 1}},
        ["alert_id", "reason"],
    ),
    _schema(
        "resolve_alert",
        "Resolve an alert. This high-impact action requires explicit analyst confirmation.",
        {"alert_id": {"type": "string"}, "resolution": {"type": "string", "minLength": 1}},
        ["alert_id", "resolution"],
    ),
]

SYSTEM_PROMPT = """You are the FinGuard investigation assistant.
Ground conclusions in tool results. Never invent transaction, alert, customer, investigation, or market data.
Use only the provided allowlisted tools. Never generate or request arbitrary SQL.
Use cached Silver FX data rather than calling a third-party API from the chat path.
Explain risk factors clearly and keep financial-monitoring language factual.
For escalation or resolution, request confirmation rather than claiming the action happened unless the tool result confirms success.
"""


def _client() -> OpenAI:
    base_url = os.getenv("DATABRICKS_OPENAI_BASE_URL")
    token = os.getenv("DATABRICKS_TOKEN")
    if not base_url or not token:
        raise RuntimeError(
            "Set DATABRICKS_OPENAI_BASE_URL and DATABRICKS_TOKEN, or map the equivalent "
            "Databricks App/AI Gateway resource credentials to these variables."
        )
    return OpenAI(api_key=token, base_url=base_url)


def _execute_tool(actor: Actor, name: str, arguments: dict[str, Any], confirm_high_impact: bool):
    if name not in TOOL_REGISTRY:
        raise ValueError(f"Tool is not allowlisted: {name}")
    if name in HIGH_IMPACT_TOOLS and not confirm_high_impact:
        return {
            "confirmation_required": True,
            "tool": name,
            "arguments": arguments,
            "message": "Explicit analyst confirmation is required before this write action executes.",
        }
    return TOOL_REGISTRY[name](actor, **arguments)


def chat(
    actor: Actor,
    user_message: str,
    history: list[dict[str, Any]] | None = None,
    confirm_high_impact: bool = False,
) -> dict[str, Any]:
    client = _client()
    messages: list[dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.extend(history or [])
    messages.append({"role": "user", "content": user_message})

    first = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        tools=TOOL_SCHEMAS,
        tool_choice="auto",
        temperature=0.1,
    )
    assistant = first.choices[0].message
    messages.append(assistant.model_dump(exclude_none=True))

    tool_events: list[dict[str, Any]] = []
    for call in assistant.tool_calls or []:
        args = json.loads(call.function.arguments or "{}")
        try:
            result = _execute_tool(actor, call.function.name, args, confirm_high_impact)
            event = {"tool": call.function.name, "arguments": args, "result": result}
        except Exception as exc:
            event = {"tool": call.function.name, "arguments": args, "error": str(exc)}
        tool_events.append(event)
        messages.append(
            {
                "role": "tool",
                "tool_call_id": call.id,
                "content": json.dumps(event, default=str),
            }
        )

    if assistant.tool_calls:
        final = client.chat.completions.create(model=MODEL, messages=messages, temperature=0.1)
        text = final.choices[0].message.content or ""
    else:
        text = assistant.content or ""

    return {"answer": text, "tool_events": tool_events}
