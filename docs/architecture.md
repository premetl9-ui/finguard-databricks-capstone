# FinGuard Architecture

## Data Sources

- PaySim synthetic financial transactions: **6,362,620 Bronze records** in the validated run.
- Alpha Vantage FX REST API for USD/EUR, USD/GBP, and USD/JPY.
- Analyst and AI-agent operational activity stored in Lakebase.

## Databricks Lakehouse

### Bronze

- `bronze_transactions`: deterministic transaction IDs, raw financial attributes, source metadata, ingestion timestamp.
- `bronze_fx_api`: auditable Alpha Vantage requests, responses, status, and errors.
- Bronze transaction ingestion is append-only and idempotent by `transaction_id`.

### Silver

- Data cleaning and normalization.
- Explicit data-quality contract and quarantine.
- FX enrichment.
- Customer behavioral features.
- Merchant/transaction categorization.
- Append-only immutable transaction events for downstream streaming compatibility.

Validated results:

- **6,362,604** valid Silver transactions.
- **16** distinct quarantined transactions.

### Gold

- `gold_transaction_risk`: transaction risk score, risk level, reasons, and alert-candidate flag.
- `gold_alert_summary`: CDC-derived operational analytics.

Validated risk-scored rows: **6,362,604**.

## Risk Scoring

The final deterministic score uses:

- amount >= 3x customer average: +30
- multiple transactions in a short window: +20
- new destination/account: +15
- unusual transaction type: +15
- international transaction: +10
- high-value transaction: +10

Risk bands:

```text
0-29    LOW
30-59   MEDIUM
60-79   HIGH
80-100  CRITICAL
```

The capstone alert threshold is **50**, producing **3,021** alert candidates in the validated dataset.

## Lakebase

Lakebase provides operational application state for:

- users
- customers
- fraud alerts
- investigations
- investigation notes
- agent actions
- alert status history

The schema includes relational keys, constraints, timestamps, triggers, and indexes. Fraud alerts enforce a unique business key on `transaction_id`.

## AI Investigator

The Databricks App uses a Databricks-hosted model endpoint and an allowlisted tool layer.

The agent can retrieve:

- alerts
- open-alert queues
- transactions
- customer transaction history
- customer risk profiles
- cached FX rates
- investigation notes

The agent can perform controlled writes:

- create investigation
- assign alert
- add investigation note
- escalate alert
- resolve alert
- validated status transitions

High-impact actions require explicit confirmation. Write activity is audited.

## CDC / Operational Analytics

Lakebase Change Data Feed captures operational changes into Unity Catalog history tables. `notebooks/06_cdc_analytics.py` transforms post-images into Gold metrics including escalations, resolutions, investigation duration, agent action count, and tool success rate.

The CDC Analytics Refresh bundle job is configured every **5 minutes** and is intentionally committed PAUSED.

## Structured Streaming

`notebooks/05_streaming_risk_alerts.py` reads Silver incrementally with a persistent checkpoint and an `availableNow` trigger. Current Silver writes are append-only; `skipChangeCommits` is used only to advance past historical MERGE commits created before the final design was adopted.

## Big Data Evidence

### Volume

FinGuard clearly demonstrates Volume:

- **6,362,620** Bronze rows
- **6,362,604** Silver rows
- **6,362,604** Gold risk rows

### Velocity

`notebooks/08_velocity_measurement.py` is a dedicated repeatable measurement harness. It records p95 processing latency for timestamped probe events processed through checkpointed Structured Streaming.

Velocity should be claimed in the final submission only after a Databricks run shows **p95 < 60 seconds** and the result is captured as evidence.

## End-to-End Flow

```text
PaySim + Alpha Vantage
        |
        v
Bronze Delta
        |
        v
Silver DQ + FX + Behavioral Features
        |
        v
Gold Risk Scoring
        |
        v
Checkpointed Structured Streaming
        |
        v
Lakebase Fraud Alerts
        |
        +--> Databricks App --> AI Investigator
        |                         |
        |                         +--> controlled write actions
        |                                  |
        +----------------------------------+
        |
        v
Lakebase Change Data Feed
        |
        v
Unity Catalog CDC History
        |
        v
Gold Operational Analytics
        |
        v
Automated Pipeline Validation
```
