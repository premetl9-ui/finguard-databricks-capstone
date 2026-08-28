# FinGuard — Databricks Capstone

FinGuard is a real-time financial transaction intelligence and AI-assisted investigation platform built on Databricks.

## Architecture

- **Bronze**: raw PaySim transactions and Alpha Vantage JSON
- **Silver**: cleaned, validated, standardized, FX-enriched transactions and derived risk features
- **Gold**: transaction risk, customer risk, alert candidates, and analytics
- **Lakebase**: operational users, customers, alerts, investigations, notes, agent actions, and alert status history
- **CDC/CDF**: Lakebase operational changes replicated to Delta history tables and aggregated for analytics
- **Databricks App**: Streamlit analyst dashboard and investigation UI
- **AI Agent**: Databricks-hosted model with allowlisted read/write tools and server-side authorization

## Repository Layout

```text
.
├── agent/                  # AI agent and tool definitions
├── app/                    # Streamlit Databricks App
├── config/                 # Example non-secret configuration
├── docs/                   # Proposal, architecture, implementation notes
├── notebooks/              # Databricks ingestion/transformation/streaming jobs
├── sql/                    # Delta and Lakebase DDL / analytics SQL
├── src/finguard/           # Reusable Python services
├── tests/                  # Unit tests
├── .gitignore
├── README.md
└── requirements.txt
```

## Implementation Order

1. Run `sql/create_delta_tables.sql` in Databricks SQL after creating the target catalog.
2. Run `notebooks/01_bronze_ingestion.py` for the PaySim dataset.
3. Run `notebooks/04_fx_api_ingestion.py` to cache Alpha Vantage FX rates.
4. Run `notebooks/02_silver_transformations.py`.
5. Run `notebooks/03_gold_risk_scoring.py`.
6. Create Lakebase tables with `sql/create_lakebase_tables.sql`.
7. Run `notebooks/05_streaming_risk_alerts.py` for the optional sub-minute path.
8. Configure Lakebase CDF and run `notebooks/06_cdc_analytics.py`.
9. Deploy the `app/` folder as a Databricks App.

## Required Environment Variables

Do **not** commit secrets. Configure these through Databricks App resources, secret scopes, or environment settings.

```text
ALPHA_VANTAGE_API_KEY
FINGUARD_CATALOG=finguard
FINGUARD_HOME_CURRENCY=USD
DATABASE_URL=postgresql://...       # local/dev option
PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD
DATABRICKS_OPENAI_BASE_URL
DATABRICKS_TOKEN
FINGUARD_MODEL_ENDPOINT
```

## Development

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pytest
```

The code is intentionally configuration-driven so the same repository can be adapted to the exact catalog, Lakebase project, SQL warehouse, and model endpoint available in the course workspace.
