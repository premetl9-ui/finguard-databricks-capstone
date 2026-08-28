from __future__ import annotations

import json
import os
from typing import Any

from openai import OpenAI

from tools import Actor, TOOL_REGISTRY


MODEL = os.getenv("FINGUARD_MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
HIGH_IMPACT_TOOLS = {"escalate_alert", "resolve_alert", "update_alert_status"}

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "get_alert",
            "description": "Retrieve one FinGuard fraud alert by alert ID.",
            "parameters": {
                "type": "object",
                "properties": {"alert_id": {"type": "string"}},
                "required": ["alert_id"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_open_alerts",
            "description": "List open, assigned, or escalated FinGuard alerts.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 200}},
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_investigation",
            "description": "Open an investigation for an alert.",
            "parameters": {
                "type": "object",
                "properties": {
                    "alert_id": {"type": "string"},
                    "summary": {"type": "string", "maxLength": 4000},
                },
                "required": ["alert_id"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "assign_alert",
            "description": "Assign an alert to an analyst. Analysts can only self-assign.",
            "parameters": {
                "type": "object",
                "properties": {
                    "alert_id": {"type": "string"},
                    "analyst_id": {"type": "string"},
                },
                "required": ["alert_id", "analyst_id"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_investigation_note",
            "description": "Add an audit-trailed note to an existing investigation.",
            "parameters": {
                "type": "object",
                "properties": {
                    "investigation_id": {"type": "string"},
                    "note": {"type": "string", "minLength": 1, "maxLength": 8000},
                },
                "required": ["investigation_id", "note"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "escalate_alert",
            "description": "Escalate an alert. This is a high-impact action and requires user confirmation.",
            "parameters": {
                "type": "object",
                "properties": {
                    "alert_id": {"type": "string"},
                    "reason": {"type": "string", "minLength": 1},
                },
                "required": ["alert_id", "reason"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "resolve_alert",
            "description": "Resolve an alert. This is a high-impact action and requires user confirmation.",
            "parameters": {
                "type": "object",
                "properties": {
                    "alert_id": {"type": "string"},
                    "resolution": {"type": "string", "minLength": 1},
                },
                "required": ["alert_id", "resolution"],
                "additionalProperties": False,
            },
        },
    },
]

SYSTEM_PROMPT = """You are the FinGuard investigation assistant.
Ground conclusions in tool results. Never invent transaction, alert, customer, or investigation data.
Use only the provided allowlisted tools. Never generate or request arbitrary SQL.
Explain risk factors clearly and keep financial-monitoring language factual.
For escalation, resolution, or general status mutation, request confirmation rather than claiming the action happened unless the tool result confirms success.
"""


def _client() -> OpenAI:
    base_url = os.getenv("DATABRICKS_OPENAI_BASE_URL")
    token = os.getenv("DATABRICKS_TOKEN")
    if not base_url or not token:
        raise RuntimeError(
            "Set DATABRICKS_OPENAI_BASE_URL and DATABRICKS_TOKEN (or map your Databricks App resource to these variables)."
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
        final = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            temperature=0.1,
        )
        text = final.choices[0].message.content or ""
    else:
        text = assistant.content or ""

    return {"answer": text, "tool_events": tool_events}
