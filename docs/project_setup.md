# FinGuard Project Setup

## Databricks Data Plane

Use catalog `bootcamp_students`. The notebooks derive the logged-in username and use the following schema suffixes:

- `_bronze`
- `_silver`
- `_gold`
- `_operations`
- `_lakebase_cdc`

Run the implementation in this order:

1. `sql/create_delta_tables.sql`
2. `notebooks/01_bronze_ingestion.py`
3. `notebooks/04_fx_api_ingestion.py`
4. `notebooks/02_silver_transformations.py`
5. `notebooks/03_gold_risk_scoring.py`
6. `sql/create_lakebase_tables.sql`
7. `notebooks/05_streaming_risk_alerts.py`
8. `notebooks/06_cdc_analytics.py`
9. Deploy the repository root as the Databricks App.

## Lakebase

Validated project settings:

```text
Project:  FinGuard-Lakebase
Branch:   production
Database: databricks_postgres
```

The Lakebase schema is derived from the Databricks user prefix (for example, `premetl9`).

Notebook 05 requires Lakebase host metadata and the full endpoint resource name at runtime. It generates a short-lived OAuth database credential through the Databricks SDK. Do not paste database tokens into notebook source.

## Databricks App Resources

Configure:

- Database resource key `postgres` -> FinGuard-Lakebase / production / databricks_postgres
- Model serving resource key `llm` -> the configured Foundation Model API endpoint

The root `app.yaml` maps those resources to the application.

Grant the FinGuard App service-principal Postgres role `USAGE` on the application schema plus the required table privileges. Register application users in the `users` table with an `ANALYST`, `SENIOR_ANALYST`, or `ADMIN` role.

## Secrets

Store `ALPHA_VANTAGE_API_KEY` in the Databricks secret scope configured by notebook 04. Lakebase and model access use OAuth/App resources. Never commit tokens, passwords, connection strings containing credentials, or personal access tokens.

## Local Development

```bash
git clone https://github.com/premetl9-ui/finguard-databricks-capstone.git
cd finguard-databricks-capstone
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pytest
```
