# FinGuard Submission Evidence Checklist

This file maps the final capstone implementation to evidence that should be included with the submission.

## Already Captured

- Final Databricks App dashboard with alert counts, status distribution, risk distribution, and filters.
- Alert investigation queue with selected alert details and risk reasons.
- AI Investigator creating an investigation through an allowlisted tool.
- Lakebase Change Data Feed configuration for operational tables.
- Gold operational analytics showing escalation, resolution, agent-action count, and success rate.
- Databricks bundle deployment success.
- Main Transaction Pipeline successful end-to-end run.
- CDC Analytics Refresh successful run.
- Latest automated validation detail rows.
- Latest automated validation summary: **22 total / 22 PASS / 0 FAIL / ALL PASS**.

## Databricks App Deployment Evidence — Captured

Captured from:

```text
Databricks
  -> Apps
  -> FinGuard
```

The captured deployment evidence visibly includes:

- **App name:** finguard
- **Status:** Running
- **Deployment URL:** `https://finguard-1352785079224954.aws.databricksapps.com`
- Latest deployment marked **Active**

A second screenshot captures the running FinGuard dashboard at the same URL.

## Big Data V #1 — Volume

Already demonstrated:

```text
Bronze rows:                 6,362,620
Unique Bronze transactions: 6,362,620
Silver valid transactions:  6,362,604
Gold risk-scored rows:       6,362,604
Distinct quarantined rows:          16
```

## Big Data V #2 — Velocity Measurement — Captured

The repository includes:

```text
notebooks/08_velocity_measurement.py
Bundle job: FinGuard - Velocity Validation
```

The test:

1. Adds timestamped probe events to `velocity_probe_source`.
2. Processes them with Spark Structured Streaming.
3. Uses a persistent checkpoint at `velocity_probe_v1`.
4. Writes processed events to `velocity_probe_sink`.
5. Calculates min/avg/p95/max latency.
6. Persists the result to `velocity_measurement_results`.
7. Fails unless all probe events are processed and p95 latency is below 60 seconds.

### Captured Velocity Evidence

Two runs are present in `bootcamp_students.premetl9_operations.velocity_measurement_results`.

Latest clean run:

```text
event_count          = 10
min_latency_seconds  = 11.582
avg_latency_seconds  = 11.582
p95_latency_seconds  = 11.582
max_latency_seconds  = 11.582
input_rows           = 10
batch_id             = 1
status               = PASS
```

Previous run:

```text
event_count          = 10
p95_latency_seconds  = 9.242
status               = PASS
```

The latest run reuses the persistent checkpoint and advances the Structured Streaming batch ID, strengthening the evidence for reliable incremental processing.

Suggested SQL:

```sql
SELECT
    measurement_run_id,
    measurement_timestamp,
    event_count,
    ROUND(min_latency_seconds, 3) AS min_latency_seconds,
    ROUND(avg_latency_seconds, 3) AS avg_latency_seconds,
    ROUND(p95_latency_seconds, 3) AS p95_latency_seconds,
    ROUND(max_latency_seconds, 3) AS max_latency_seconds,
    input_rows,
    batch_id,
    status
FROM bootcamp_students.premetl9_operations.velocity_measurement_results
ORDER BY measurement_timestamp DESC
LIMIT 10;
```

## Final Evidence Rule

Velocity can now be claimed as demonstrated: the measured Databricks runs passed with p95 well under 60 seconds, and the clean rerun records 10 source input rows with batch_id 1 on the persistent checkpoint.


## Incremental CDC Evidence — Captured

The deployed CDC Analytics Refresh job now uses `notebooks/09_incremental_cdc_analytics.py`.

Captured evidence shows **two SUCCESS runs**.

```text
Initialization run window:
alerts=3025, status=4, investigations=1, actions=6

Second incremental run window:
alerts=4, status=4, investigations=1, actions=4

Stable fact counts after both runs:
alerts_fact_rows=3021
status_fact_rows=4
investigation_fact_rows=1
action_fact_rows=6
gold_dates=2
```

The second run re-read only the watermark overlap and did not grow the fact counts, demonstrating idempotent incremental processing. `cdc_analytics_state` contains persisted watermarks for all four sources.

## Performance Evidence — Captured

Captured Performance Evidence results:

```text
Bronze:
rows=6,362,620
files=4
size_bytes=391,953,413
clustering=['event_date']
count_query_seconds=1.256

Silver:
rows=6,362,604
files=6
size_bytes=370,860,036
clustering=['event_date','customer_id']
count_query_seconds=0.498

Gold risk:
rows=6,362,604
files=5
size_bytes=323,735,556
clustering=['event_date','customer_id']
count_query_seconds=0.449
risk_distribution_seconds=2.714
customer_lookup_seconds=1.836
```

Results are persisted in `bootcamp_students.premetl9_operations.performance_evidence_results`.

## Production Streaming Monitoring — Added

When notebook 05 processes a non-empty Silver micro-batch, it appends one row to:

```text
bootcamp_students.premetl9_operations.streaming_batch_metrics
```

with batch ID, scored rows, alert rows, Silver-to-score p95 latency, Lakebase write duration, batch duration, checkpoint path, and status.
