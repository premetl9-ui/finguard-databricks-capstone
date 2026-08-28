# FinGuard — Databricks Capstone

FinGuard is a real-time financial transaction intelligence and AI-assisted investigation platform built on Databricks. It demonstrates an end-to-end workflow using Apache Spark, Delta Lake, Unity Catalog, Lakebase, CDC/Change Data Feed, Databricks Apps, an external financial API, and an action-taking AI agent.

## Project Goals

- Process 1M+ financial transaction records with Spark.
- Build a Bronze / Silver / Gold medallion architecture.
- Enrich transactions with Alpha Vantage FX data.
- Calculate transaction risk indicators and generate alert candidates.
- Store operational workflow state in Lakebase.
- Capture Lakebase changes into Delta history tables for analytics.
- Provide a Databricks App with dashboards, alerts, investigations, and an AI investigator.
- Demonstrate Volume and Variety, with optional sub-minute Velocity using Structured Streaming.

## Repository Structure

```text
finguard-databricks-capstone/
├── README.md
├── .gitignore
├── requirements.txt
├── docs/
│   ├── FinGuard_Detailed_Capstone_Project_Document_Updated.docx
│   ├── FinGuard_Architecture_Diagram.png
│   ├── architecture.md
│   ├── implementation_details.md
│   └── project_setup.md
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformations.py
│   ├── 03_gold_risk_scoring.py
│   ├── 04_fx_api_ingestion.py
│   └── 05_cdc_analytics.py
├── src/
│   ├── ingestion/
│   ├── transformations/
│   ├── risk_scoring/
│   ├── api/
│   └── utils/
├── sql/
│   ├── create_lakebase_tables.sql
│   ├── create_delta_tables.sql
│   └── analytics_queries.sql
├── app/
│   ├── app.py
│   ├── app.yaml
│   ├── requirements.txt
│   └── pages/
├── agent/
│   ├── agent.py
│   └── tools.py
├── tests/
└── config/
    └── config.example.yaml
```

## Required Revision Implementation Choices

The implementation details requested after proposal review are documented in [`docs/implementation_details.md`](docs/implementation_details.md). They define the selected Streamlit frontend/service layer, Lakebase-to-Delta CDC plan, Structured Streaming runtime configuration, checkpointing and idempotent alert writes, Delta clustering and Lakebase indexes, and the Databricks-hosted LLM/tool-calling guardrail approach.

## High-Level Workflow

1. Ingest PaySim transactions and Alpha Vantage API JSON into Bronze Delta tables.
2. Clean, standardize, validate, and enrich data in Silver.
3. Build risk features and Gold analytics/risk tables.
4. Create high-risk alerts in Lakebase.
5. Use an AI investigation agent for grounded retrieval and controlled workflow actions.
6. Capture Lakebase changes with CDC/CDF into Delta history tables.
7. Build application, agent, analyst, and alert-lifecycle analytics.
8. Surface the workflow in a deployed Databricks App.

## Security

Do not commit API keys, Databricks tokens, passwords, or Lakebase credentials. Use Databricks secret resources/scopes and environment configuration for runtime secrets.

## GitHub Repository

Target repository: `premetl9-ui/finguard-databricks-capstone`
