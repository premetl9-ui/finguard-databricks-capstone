# FinGuard Project Setup

## Databricks Data Plane

Use catalog `bootcamp_students`. Notebooks derive the logged-in username and use these schema suffixes:

- `_bronze`
- `_silver`
- `_gold`
- `_operations`
- `_lakebase_cdc`

For the validated account, the schemas are:

```text
bootcamp_students.premetl9_bronze
bootcamp_students.premetl9_silver
bootcamp_students.premetl9_gold
bootcamp_students.premetl9_operations
bootcamp_students.premetl9_lakebase_cdc
```

## One-Time Setup and Initial Execution

1. Run `sql/create_delta_tables.sql`.
2. Upload PaySim under `/Volumes/bootcamp_students/<username>_bronze/raw/paysim`.
3. Run `notebooks/01_bronze_ingestion.py`.
4. Configure the Alpha Vantage secret and run `notebooks/04_fx_api_ingestion.py`.
5. Run `notebooks/02_silver_transformations.py`.
6. Run `notebooks/03_gold_risk_scoring.py`.
7. Create Lakebase tables with `sql/create_lakebase_tables.sql`.
8. Run `notebooks/05_streaming_risk_alerts.py` with Lakebase runtime parameters.
9. Configure Lakebase Change Data Feed.
10. Run `notebooks/06_cdc_analytics.py`.
11. Run `notebooks/07_pipeline_validation.py`.
12. Deploy the repository root as the Databricks App.
13. Deploy the Databricks bundle from `databricks.yml`.
14. Run `notebooks/08_velocity_measurement.py` or the **FinGuard - Velocity Validation** job to capture measured latency evidence.

## Lakebase

Validated settings:

```text
Project:   FinGuard-Lakebase
Branch:    production
Database:  databricks_postgres
Schema:    premetl9
Endpoint:  projects/finguard-lakebase/branches/production/endpoints/primary
```

Notebook 05 receives the Lakebase host, database, port, and endpoint resource name through Job parameters from `databricks.yml`. It generates a short-lived OAuth database credential using the Databricks SDK.

Do not place OAuth tokens or database passwords in source control.

## Alpha Vantage Secret

The final FX notebook expects:

```text
Scope: premetl9-dataexpert
Key:   alpha-vantage-api-key
```

The secret value remains in Databricks Secrets and is never committed.

## Databricks App Resources

Configure:

- Database resource key `postgres` -> FinGuard-Lakebase / production / databricks_postgres
- Model-serving resource key `llm` -> the configured Databricks Foundation Model endpoint

The root `app.yaml` maps these resources to environment variables used by the application.

Register authorized FinGuard users in Lakebase with one of:

- `ANALYST`
- `SENIOR_ANALYST`
- `ADMIN`

## Bundle Jobs

The final bundle defines four jobs:

### FinGuard - Main Transaction Pipeline

```text
bronze_ingestion + fx_refresh
          |
          v
silver_transformations
          |
          v
gold_risk_scoring
          |
          v
streaming_risk_alerts
          |
          v
pipeline_validation
```

Schedule in source: daily at 02:15 UTC, **PAUSED**.

### FinGuard - FX Refresh

Refreshes USD/EUR, USD/GBP, and USD/JPY.

Schedule in source: every 6 hours, **PAUSED**.

### FinGuard - CDC Analytics Refresh

Refreshes Gold operational analytics from Lakebase CDC.

Schedule in source: every 5 minutes, **PAUSED**.

### FinGuard - Velocity Validation

Manual evidence job that runs `notebooks/08_velocity_measurement.py`.

Default parameters:

```text
probe_events = 10
sla_seconds  = 60
```

The job persists results to:

```text
bootcamp_students.<username>_operations.velocity_measurement_results
```

## Bundle Deployment

From the Databricks bundle editor or CLI:

```bash
databricks bundle validate
databricks bundle deploy
```

The validated project has already deployed and successfully executed the Main Transaction Pipeline, FX Refresh, and CDC Analytics Refresh jobs. After adding notebook 08, redeploy once more before running the Velocity Validation job.

## Velocity Evidence Capture

Run:

```text
FinGuard - Velocity Validation
  -> Run now
```

The notebook succeeds only when all probe events are processed and:

```text
p95_latency_seconds < 60
```

For submission, capture:

1. The successful Velocity Validation job run.
2. The notebook result table showing event count and p95 latency.
3. The latest row from `velocity_measurement_results` with `status = PASS`.

Do not claim Velocity until the measured run passes.

## Final Pipeline Validation

`notebooks/07_pipeline_validation.py` writes audit results to:

```text
bootcamp_students.<username>_operations.pipeline_validation_results
```

The latest validated run reported:

```text
22 total checks
22 PASS
0 FAIL
ALL PASS
```

## Databricks App Deployment Evidence

Before submission, capture the Databricks **Apps > FinGuard** deployment page showing:

- App name: FinGuard
- Status: Running
- Deployment URL
- Latest successful deployment

Also capture the running application dashboard. Add the deployment URL to the final submission document only from the Databricks UI; do not guess the URL from the workspace host.

## Local Development

```bash
git clone https://github.com/premetl9-ui/finguard-databricks-capstone.git
cd finguard-databricks-capstone
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pytest
```
