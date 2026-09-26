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
    alert_risk_distribution,
    alert_status_distribution,
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


# ------------------------------------------------------------
# FinGuard Login / Welcome Screen
# ------------------------------------------------------------
if "finguard_logged_in" not in st.session_state:
    st.session_state.finguard_logged_in = False

if not st.session_state.finguard_logged_in:
    st.markdown(
        """
        <div style="
            max-width: 520px;
            margin: 80px auto 28px auto;
            padding: 40px;
            border-radius: 16px;
            border: 1px solid #e5e7eb;
            text-align: center;
        ">
            <h1>🛡️ FinGuard</h1>
            <p style="color: gray;">
                Real-Time Financial Transaction Intelligence
                & AI Investigation Platform
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    _, center, _ = st.columns([1, 1.3, 1])

    with center:
        login_username = st.text_input(
            "Username",
            placeholder="Enter your username",
            key="finguard_login_username",
        )

        if st.button(
            "Login",
            use_container_width=True,
            type="primary",
        ):
            entered_username = login_username.strip().lower()
            authenticated_usernames = {
                actor.email.lower(),
                actor.email.split("@")[0].lower(),
            }

            if entered_username in authenticated_usernames:
                st.session_state.finguard_logged_in = True
                st.rerun()
            else:
                st.error(
                    "Username does not match your authenticated Databricks account."
                )

    st.stop()


st.title("FinGuard")
st.caption("Real-Time Financial Transaction Intelligence & AI Investigation Platform")
st.sidebar.write(f"**User:** {actor.email}")
st.sidebar.write(f"**Role:** {actor.role}")

if "selected_alert" not in st.session_state:
    st.session_state.selected_alert = None
if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []

if st.sidebar.button("Exit FinGuard", use_container_width=True):
    st.session_state.finguard_logged_in = False
    st.session_state.selected_alert = None
    st.session_state.chat_messages = []
    st.rerun()

page = st.sidebar.radio("Navigation", ["Dashboard", "Alerts & Investigations", "AI Investigator"])

if page == "Dashboard":
    metrics = operational_metrics()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Alerts", metrics.get("total_alerts", 0))
    c2.metric("Open Alerts", metrics.get("open_alerts", 0))
    c3.metric("Critical Open", metrics.get("critical_open", 0))
    c4.metric("Escalated", metrics.get("escalated", 0))

    st.subheader("Alert Overview")

    status_distribution = alert_status_distribution()
    risk_distribution = alert_risk_distribution()

    chart_left, chart_right = st.columns(2)

    with chart_left:
        st.markdown("**Alerts by Status**")
        if status_distribution.empty:
            st.caption("No alert status data available.")
        else:
            status_chart = status_distribution.copy()
            status_chart["alert_count"] = (
                status_chart["alert_count"].astype(int)
            )
            st.bar_chart(
                status_chart.set_index("status")[["alert_count"]],
                use_container_width=True,
            )

    with chart_right:
        st.markdown("**Alerts by Risk Level**")
        if risk_distribution.empty:
            st.caption("No risk-level data available.")
        else:
            risk_chart = risk_distribution.copy()
            risk_chart["alert_count"] = (
                risk_chart["alert_count"].astype(int)
            )
            st.bar_chart(
                risk_chart.set_index("risk_level")[["alert_count"]],
                use_container_width=True,
            )

    st.subheader("Alert Queue")

    filter_risk, filter_status, filter_search = st.columns([1, 1, 2])

    with filter_risk:
        selected_risk = st.selectbox(
            "Risk level",
            ["ALL", "CRITICAL", "HIGH", "MEDIUM", "LOW"],
            key="dashboard_risk_filter",
        )

    with filter_status:
        selected_status = st.selectbox(
            "Status",
            [
                "ALL",
                "OPEN",
                "ASSIGNED",
                "ESCALATED",
                "RESOLVED",
                "CLOSED",
            ],
            key="dashboard_status_filter",
        )

    with filter_search:
        search_text = st.text_input(
            "Search",
            placeholder="Transaction, customer, or alert ID",
            key="dashboard_alert_search",
        )

    queue = alert_queue(
        limit=200,
        risk_level=selected_risk,
        status=selected_status,
        search=search_text,
    )

    if queue.empty:
        st.info("No alerts match the selected filters.")
    else:
        st.caption(
            f"Showing {len(queue):,} matching alerts "
            "(maximum 200 rows)."
        )
        st.dataframe(
            queue,
            use_container_width=True,
            hide_index=True,
        )

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
        if selected:
            effective_prompt = (
                f"Current selected alert_id is exactly {selected}. "
                "Use this exact UUID for any alert-specific tool call. "
                f"User request: {prompt}"
            )
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
