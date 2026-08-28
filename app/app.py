from __future__ import annotations

import os
import sys

import streamlit as st

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(REPO_ROOT, "src")
AGENT_DIR = os.path.join(REPO_ROOT, "agent")
for path in [SRC, AGENT_DIR, os.path.dirname(__file__)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from agent import chat as agent_chat  # noqa: E402
from services import (  # noqa: E402
    alert_detail,
    alert_queue,
    current_actor,
    investigation_notes,
    investigations_for_alert,
    operational_metrics,
)

st.set_page_config(page_title="FinGuard", page_icon="🛡️", layout="wide")


def authenticated_email() -> str:
    # Databricks Apps sits behind Databricks authentication. Header names can vary by
    # deployment, so keep local fallback explicit instead of silently assuming identity.
    try:
        headers = st.context.headers
        for key in ["X-Forwarded-Email", "X-Databricks-User-Email", "X-Forwarded-User"]:
            value = headers.get(key)
            if value:
                return value
    except Exception:
        pass

    local = os.getenv("FINGUARD_LOCAL_USER_EMAIL")
    if local:
        return local
    st.error(
        "Authenticated email was not available. Configure the Databricks App identity header mapping "
        "or set FINGUARD_LOCAL_USER_EMAIL for local development."
    )
    st.stop()


email = authenticated_email()
try:
    actor = current_actor(email)
except Exception as exc:
    st.error(f"FinGuard authorization failed for {email}: {exc}")
    st.stop()

st.title("FinGuard")
st.caption("Real-Time Financial Transaction Intelligence & AI Investigation Platform")
st.sidebar.write(f"**User:** {actor.email}")
st.sidebar.write(f"**Role:** {actor.role}")

if "selected_alert" not in st.session_state:
    st.session_state.selected_alert = None
if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []

page = st.sidebar.radio("Navigation", ["Dashboard", "Alerts & Investigations", "AI Investigator"])

if page == "Dashboard":
    metrics = operational_metrics()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Alerts", metrics.get("total_alerts", 0))
    c2.metric("Open Alerts", metrics.get("open_alerts", 0))
    c3.metric("Critical Open", metrics.get("critical_open", 0))
    c4.metric("Escalated", metrics.get("escalated", 0))

    st.subheader("Highest-Risk Alerts")
    queue = alert_queue(25)
    if queue.empty:
        st.info("No Lakebase alerts are available yet. Run the risk pipeline first.")
    else:
        st.dataframe(queue, use_container_width=True, hide_index=True)

elif page == "Alerts & Investigations":
    st.subheader("Alert Investigation Queue")
    queue = alert_queue(200)
    if queue.empty:
        st.info("No alerts found.")
        st.stop()

    st.dataframe(queue, use_container_width=True, hide_index=True)
    alert_ids = queue["alert_id"].astype(str).tolist()
    default_index = 0
    if st.session_state.selected_alert in alert_ids:
        default_index = alert_ids.index(st.session_state.selected_alert)
    selected = st.selectbox("Select alert", alert_ids, index=default_index)
    st.session_state.selected_alert = selected

    detail = alert_detail(selected)
    if detail:
        c1, c2, c3 = st.columns(3)
        c1.metric("Risk Score", detail["risk_score"])
        c2.metric("Risk Level", detail["risk_level"])
        c3.metric("Status", detail["status"])
        st.write("**Customer:**", detail["customer_id"])
        st.write("**Transaction:**", detail["transaction_id"])
        st.write("**Why flagged:**", detail.get("alert_reason") or "No reason recorded")

    investigations = investigations_for_alert(selected)
    st.subheader("Investigations")
    if investigations.empty:
        st.caption("No investigation has been opened for this alert.")
    else:
        st.dataframe(investigations, use_container_width=True, hide_index=True)
        investigation_id = st.selectbox(
            "Investigation notes",
            investigations["investigation_id"].astype(str).tolist(),
        )
        notes = investigation_notes(investigation_id)
        if notes.empty:
            st.caption("No notes yet.")
        else:
            st.dataframe(notes, use_container_width=True, hide_index=True)

elif page == "AI Investigator":
    st.subheader("AI Investigator")
    selected = st.session_state.selected_alert
    if selected:
        st.info(f"Current alert context: {selected}")

    confirm_high_impact = st.checkbox(
        "Confirm high-impact agent actions for this request",
        help="Required before the agent can escalate, resolve, or perform a high-impact status mutation.",
    )

    for message in st.session_state.chat_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    prompt = st.chat_input("Ask why an alert was flagged or request an investigation action...")
    if prompt:
        if selected and "alert" not in prompt.lower():
            effective_prompt = f"Current alert_id is {selected}. User request: {prompt}"
        else:
            effective_prompt = prompt

        st.session_state.chat_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        history = [
            {"role": item["role"], "content": item["content"]}
            for item in st.session_state.chat_messages[:-1]
            if item["role"] in {"user", "assistant"}
        ]

        with st.chat_message("assistant"):
            try:
                with st.spinner("Investigating..."):
                    result = agent_chat(
                        actor=actor,
                        user_message=effective_prompt,
                        history=history,
                        confirm_high_impact=confirm_high_impact,
                    )
                answer = result["answer"]
                st.markdown(answer)
                if result.get("tool_events"):
                    with st.expander("Agent tool activity"):
                        st.json(result["tool_events"])
            except Exception as exc:
                answer = f"Agent request failed: {exc}"
                st.error(answer)

        st.session_state.chat_messages.append({"role": "assistant", "content": answer})
