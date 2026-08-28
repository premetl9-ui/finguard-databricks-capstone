# FinGuard Project Setup

## Clone

```bash
git clone https://github.com/premetl9-ui/finguard-databricks-capstone.git
cd finguard-databricks-capstone
```

## Suggested Development Order

1. Bronze transaction ingestion.
2. Silver cleaning, validation, and enrichment.
3. Gold risk scoring and analytics.
4. Alpha Vantage API ingestion.
5. Lakebase operational schema.
6. CDC/CDF analytics pipeline.
7. AI agent tools.
8. Databricks App.
9. Structured Streaming stretch goal.

## Secrets

Store `ALPHA_VANTAGE_API_KEY`, Lakebase credentials, and any Databricks tokens in Databricks secrets or approved environment configuration. Never commit them to Git.

## Branching

Use `main` for stable work and feature branches such as:

- `feature/bronze-ingestion`
- `feature/silver-quality`
- `feature/risk-scoring`
- `feature/lakebase`
- `feature/ai-agent`
- `feature/databricks-app`
