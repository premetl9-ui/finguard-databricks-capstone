# FinGuard Implementation Details

This document captures the implementation-level clarifications requested after the capstone proposal review.

## Frontend and Service Layer

FinGuard will use **Streamlit with Python** for the Databricks App frontend. The browser will communicate only with the Streamlit/Python backend; it will not connect directly to Lakebase or Delta tables.

The backend service layer will contain modules such as:

- `agent_service.py`
- `alert_service.py`
- `investigation_service.py`
- `authorization_service.py`

Lakebase reads and writes will be performed server-side through the PostgreSQL Python driver (`psycopg`) using the Databricks App service-principal identity. Delta/Gold analytics will be accessed through Databricks SQL using the app identity and Unity Catalog grants.

The main application tool/API surface will include:

- `get_alert(alert_id)`
- `get_transaction(transaction_id)`
- `get_customer_transactions(customer_id)`
- `get_customer_risk_profile(customer_id)`
- `assign_alert(alert_id, analyst_id)`
- `escalate_alert(alert_id)`
- `resolve_alert(alert_id, resolution)`
- `add_investigation_note(alert_id, note)`
- `update_alert_status(alert_id, status)`

Before any write-capable tool executes, the backend will validate the authenticated user, role, arguments, and current alert/investigation state.

## Lakebase to Delta CDC

FinGuard will use **native Lakebase Change Data Feed (CDF)** as the preferred Lakebase-to-Delta CDC path, subject to workspace availability. Operational tables will use `REPLICA IDENTITY FULL` so updates can expose complete before/after row information.

Planned source tables:

- `fraud_alerts`
- `investigations`
- `investigation_notes`
- `agent_actions`
- `alert_status_history`

The CDF destination will be a Unity Catalog schema such as `finguard.lakebase_cdc`, with history tables such as:

- `lb_fraud_alerts_history`
- `lb_investigations_history`
- `lb_investigation_notes_history`
- `lb_agent_actions_history`
- `lb_alert_status_history_history`

CDF capture will run continuously. A downstream incremental Spark/Lakeflow analytics task will process newly captured changes approximately every **1 minute** and populate Gold application analytics such as alert summaries, agent activity, and investigation metrics.

If native Lakebase CDF is unavailable in the course workspace, Lakeflow Connect PostgreSQL CDC will be used as the fallback.

## Streaming Runtime and Operations

The optional Velocity implementation will use **Spark Structured Streaming** with a **10-second processing-time trigger**.

Initial runtime configuration:

- Compute: dedicated Databricks Jobs compute
- Driver: 1
- Workers: 2 fixed workers
- Autoscaling: disabled initially for predictable streaming behavior
- Trigger: 10 seconds
- Scale-up test target: 4 fixed workers if the initial cluster cannot sustain the p95 latency objective

Each streaming query will use a separate Unity Catalog Volume checkpoint path, for example:

```text
/Volumes/finguard/operations/checkpoints/transaction_risk_v1
/Volumes/finguard/operations/checkpoints/cdc_alert_analytics_v1
```

For transaction velocity calculations:

- Risk window: 10 minutes
- Event watermark: 30 minutes
- Stateful processing: RocksDB state store where available

The end-to-end p95 objective remains **under 60 seconds** from transaction arrival to fraud-alert persistence.

### Idempotent Alert Strategy

`transaction_id` will be the business key for transaction risk results and fraud alerts. Delta writes will use `MERGE`, while Lakebase will enforce a unique constraint on `fraud_alerts.transaction_id` and use PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` behavior. Replaying a micro-batch therefore will not create duplicate alerts for the same transaction.

## Delta Physical Design

For new Delta tables, FinGuard will use liquid clustering where supported rather than relying on legacy static partitioning plus Z-ORDER.

| Delta Table | Physical Design |
| --- | --- |
| `bronze_transactions` | `CLUSTER BY (event_date)` |
| `silver_transactions` | `CLUSTER BY (event_date, customer_id)` |
| `gold_transaction_risk` | `CLUSTER BY (event_date, customer_id)` |
| `gold_customer_risk` | `CLUSTER BY (customer_id)` |
| `gold_daily_transaction_metrics` | `CLUSTER BY (event_date)` |
| `gold_alert_summary` | `CLUSTER BY (event_date)` |

Predictive optimization/`OPTIMIZE` will be used where supported. If liquid clustering is unavailable in the course environment, the fallback is partitioning large transaction tables by `event_date` and using Z-ORDER/optimization on common filtering keys such as `customer_id`.

## Lakebase Index Strategy

Primary keys provide the base indexes. Additional indexes will support the alert queue, customer investigation history, case detail, AI context retrieval, and audit screens.

```sql
CREATE UNIQUE INDEX idx_fraud_alert_transaction
ON fraud_alerts(transaction_id);

CREATE INDEX idx_fraud_alert_customer
ON fraud_alerts(customer_id);

CREATE INDEX idx_fraud_alert_status
ON fraud_alerts(status, created_at DESC);

CREATE INDEX idx_fraud_alert_assignment
ON fraud_alerts(assigned_to, status);

CREATE INDEX idx_investigation_alert
ON investigations(alert_id);

CREATE INDEX idx_investigation_notes_case
ON investigation_notes(investigation_id, created_at DESC);

CREATE INDEX idx_agent_actions_alert
ON agent_actions(alert_id, created_at DESC);

CREATE INDEX idx_alert_history_alert
ON alert_status_history(alert_id, changed_at DESC);
```

## AI Model, Tool Calling, and Guardrails

FinGuard will use a Databricks-hosted instruction model through Foundation Model APIs / Unity AI Gateway. The initial plan is **Meta Llama 3.3 70B Instruct**, subject to model availability in the course workspace.

The agent will use an OpenAI-compatible function/tool-calling pattern. The model may request an approved function, but FinGuard application code performs the actual call after validation.

Write-action guardrails:

1. No arbitrary SQL execution tool is exposed to the model.
2. Only predefined and allowlisted tools are available.
3. Tool arguments are validated against strict schemas.
4. The authenticated user's role is checked server-side.
5. The current alert/investigation state is validated before mutation.
6. High-impact actions such as escalation or resolution require confirmation.
7. Writes execute through controlled service functions and database transactions.
8. Every write is recorded in `agent_actions` for auditability.

The audit record will include the actor, model/agent, tool name, parameters, affected alert or investigation, execution status, and timestamp.

## Updated End-to-End Path

```text
Transactions
  -> 10-second Structured Streaming
  -> Bronze
  -> Silver validation/enrichment
  -> Risk scoring
  -> gold_transaction_risk
  -> idempotent Lakebase fraud_alert upsert
  -> analyst investigation in Streamlit Databricks App
  -> controlled AI tool calls
  -> Lakebase operational updates
  -> Lakebase CDF
  -> Delta CDC history
  -> 1-minute incremental analytics
  -> Gold application/agent analytics
```
