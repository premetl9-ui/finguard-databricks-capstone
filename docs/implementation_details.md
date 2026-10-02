# FinGuard Implementation Details

This document describes the final implemented FinGuard capstone rather than the original proposal.

## Frontend and Service Layer

FinGuard uses **Streamlit with Python** as a deployed Databricks App. The browser interacts with the Streamlit/Python backend; it does not connect directly to Lakebase or Delta tables.

The application code is organized under:

- `app/app.py` for the Streamlit UI.
- `app/services.py` for dashboard and investigation service calls.
- `agent/agent.py` for model orchestration and tool calling.
- `agent/tools.py` for allowlisted read/write tools.
- `src/finguard/lakebase.py` for Lakebase connectivity.
- `src/finguard/lakehouse.py` for governed Lakehouse reads.

The implemented agent tool surface includes:

### Read tools
- `get_alert(alert_id)`
- `list_open_alerts(limit)`
- `get_transaction(transaction_id)`
- `get_customer_transactions(customer_id, limit)`
- `get_customer_risk_profile(customer_id)`
- `get_exchange_rate(from_currency, to_currency)`
- `get_investigation_notes(alert_id)`

### Write/action tools
- `create_investigation(alert_id, summary)`
- `assign_alert(alert_id, analyst_id)`
- `add_investigation_note(investigation_id, note)`
- `update_alert_status(alert_id, new_status, reason)`
- `escalate_alert(alert_id, reason)`
- `resolve_alert(alert_id, resolution)`

Write-capable tools validate the authenticated user, role, arguments, and current alert state. High-impact status changes require explicit confirmation in the app before execution.

## Lakebase Operational Model

The implemented Lakebase schema contains:

- `users`
- `customers`
- `fraud_alerts`
- `investigations`
- `investigation_notes`
- `agent_actions`
- `alert_status_history`

The schema defines primary keys, foreign keys, CHECK constraints, unique transaction IDs, timestamps, updated-at triggers, and indexes for alert queues, assignments, investigations, notes, agent actions, and status history.

The application reads from and writes to these tables through server-side `psycopg` connections.

## Lakebase Authentication

Notebook 05 and the Databricks App do not store database passwords in source code.

Notebook 05 accepts Lakebase connection metadata as Job parameters and generates a short-lived OAuth database credential at runtime through the Databricks SDK. The App receives its Lakebase endpoint through the `postgres` App resource and also uses short-lived OAuth credentials.

## Lakebase Change Data Feed and Gold Analytics

Native Lakebase Change Data Feed is enabled for the operational tables required by the analytics workflow:

- `fraud_alerts`
- `investigations`
- `investigation_notes`
- `agent_actions`
- `alert_status_history`

The corresponding Unity Catalog history tables are stored under the user-specific `_lakebase_cdc` schema.

`notebooks/06_cdc_analytics.py` reads insert and update-postimage records from the CDC history tables and refreshes `gold_alert_summary` with:

- alerts created
- alerts escalated
- alerts resolved
- average investigation-resolution minutes
- agent action count
- agent action success rate

The deployed CDC Analytics Refresh job is configured every **5 minutes** and is committed in a PAUSED state for cost control. It has also been run successfully manually.

## Transaction Streaming Runtime

`notebooks/05_streaming_risk_alerts.py` uses Spark Structured Streaming against the Silver transaction table.

The final implementation uses:

- Source: `silver_transactions`
- Persistent checkpoint: `/Volumes/bootcamp_students/<username>_operations/checkpoints/transaction_risk_v1`
- Trigger: `availableNow=True`
- Historical compatibility: `skipChangeCommits=true` to move past earlier Silver MERGE history
- Current Silver transaction writes: append-only and idempotent by `transaction_id`
- Gold sink: idempotent Delta MERGE keyed by `transaction_id`
- Lakebase sink: `INSERT ... ON CONFLICT DO UPDATE` with a unique `fraud_alerts.transaction_id`

This design supports reruns without creating duplicate business transactions or alerts.

## Velocity Measurement

The repository includes `notebooks/08_velocity_measurement.py` and a manual bundle job named **FinGuard - Velocity Validation**.

The measurement harness:

1. Appends timestamped probe events to an Operations Delta source table.
2. Processes only new events using Spark Structured Streaming and a persistent checkpoint.
3. Adds a processing timestamp in the streaming query.
4. Calculates min, average, p95, and max event-processing latency.
5. Persists each run to `bootcamp_students.<username>_operations.velocity_measurement_results`.
6. Fails the job when the event count is incomplete or p95 latency is not below 60 seconds.

Measured Databricks evidence is now available. Two runs completed with `status = PASS`. The latest clean run processed **10 events / 10 source input rows**, reused the persistent checkpoint at `velocity_probe_v1`, advanced to **batch_id = 1**, and measured **p95 latency = 11.582 seconds**. The previous run measured **9.242 seconds p95**. This demonstrates sub-minute event processing with checkpoint reuse.

## Delta Data Pipeline

The implemented pipeline is:

```text
PaySim
  -> Bronze transaction ingestion
  -> Silver data quality + FX enrichment + behavioral features
  -> Gold risk scoring
  -> incremental Structured Streaming risk processing
  -> Lakebase fraud alerts
```

Alpha Vantage FX data follows:

```text
Alpha Vantage REST API
  -> Bronze API request audit
  -> Silver normalized FX rates
  -> transaction enrichment
```

Bronze ingestion, Silver transactions, and quarantine handling are designed to be rerunnable and idempotent.

## AI Model, Tool Calling, and Guardrails

FinGuard uses the Databricks-hosted model endpoint configured through the App resource `llm`. The validated endpoint configuration is `databricks-meta-llama-3-3-70b-instruct`.

The agent uses an OpenAI-compatible function/tool-calling interface. The model may request only functions exposed in the allowlist, and application code executes the call after validation.

Implemented guardrails:

1. No arbitrary SQL tool is exposed to the model.
2. Only predefined tool names are accepted.
3. Tool arguments use strict JSON schemas.
4. Application roles are checked server-side.
5. Current alert state and allowed status transitions are validated.
6. Escalation and resolution require explicit analyst confirmation.
7. Writes execute through controlled service functions and database transactions.
8. Write actions are recorded in `agent_actions`.
9. Alert lifecycle changes are recorded in `alert_status_history`.

## Final Automation

The final Databricks bundle defines four jobs:

- **FinGuard - Main Transaction Pipeline**
- **FinGuard - FX Refresh**
- **FinGuard - CDC Analytics Refresh**
- **FinGuard - Velocity Validation**

All four jobs have now been deployed. The Main Transaction Pipeline, FX Refresh, CDC Analytics Refresh, and Velocity Validation workflows have been exercised. The Velocity Validation job produced repeatable PASS evidence with p95 latency below 60 seconds.

## Final Validated Results

The final end-to-end pipeline validation recorded:

- Bronze rows: **6,362,620**
- Unique Bronze transaction IDs: **6,362,620**
- Silver valid transactions: **6,362,604**
- Distinct quarantined transactions: **16**
- Gold risk-scored transactions: **6,362,604**
- Alert candidates at score >= 50: **3,021**
- Latest pipeline validation: **22 PASS / 0 FAIL / ALL PASS**
- Demo operational changes captured by CDC: **1 escalation, 1 resolution**
- Agent actions captured in the validated demo: **6**
- Agent action success rate: **100%**

## Final End-to-End Path

```text
PaySim + Alpha Vantage
  -> Bronze Delta
  -> Silver DQ / FX / behavioral features
  -> Gold risk scoring
  -> checkpointed Structured Streaming
  -> Lakebase operational alerts
  -> Streamlit Databricks App
  -> controlled AI tool calls
  -> Lakebase operational changes
  -> Lakebase CDF
  -> Unity Catalog CDC history
  -> Gold operational analytics
  -> automated validation
```
