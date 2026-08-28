# FinGuard Architecture

## Data Sources

- PaySim synthetic financial transaction dataset (6M+ records target).
- Alpha Vantage FX/market REST API.
- User and operational interactions from analysts and the AI agent.

## Databricks Lakehouse

### Bronze
- Raw transactions.
- Raw API JSON.
- Append-only ingestion for replay and traceability.

### Silver
- Data cleaning and standardization.
- FX enrichment.
- Derived risk features.
- Merchant categorization.
- Data-quality validation and anomaly checks.

### Gold
- Transaction risk scores.
- Customer 360 / customer risk profile.
- Alert candidates.
- Aggregations and summary metrics.

## AI/ML

Risk scoring uses transparent rules initially, with the option to extend to an ML model. Model serving can provide real-time scoring for application use.

## Lakebase

Operational tables include users, customers, fraud alerts, investigations, investigation notes, agent actions, and alert status history.

## CDC / Analytics

Lakebase changes are captured into Delta history tables. Spark/Lakeflow transforms these records into agent activity, alert lifecycle, analyst/user, and data-change metrics.

## Databricks App

The frontend provides a real-time dashboard, alert/investigation workflow, AI investigator, analytics/reports, and user management.

## Velocity Target

If Velocity is claimed, the target is under 60 seconds p95 from transaction arrival to fraud-alert persistence using Spark Structured Streaming with a 10-second micro-batch trigger. Alpha Vantage enrichment is cached and is not part of the streaming latency SLA.
