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
