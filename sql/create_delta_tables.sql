DECLARE OR REPLACE VARIABLE username STRING;

SET VAR username =
  regexp_replace(
    split(session_user(), '@')[0],
    '[.-]',
    '_'
  );

-- ============================================================
-- SCHEMAS
-- ============================================================

EXECUTE IMMEDIATE
  'CREATE SCHEMA IF NOT EXISTS bootcamp_students.' ||
  username || '_bronze';

EXECUTE IMMEDIATE
  'CREATE SCHEMA IF NOT EXISTS bootcamp_students.' ||
  username || '_silver';

EXECUTE IMMEDIATE
  'CREATE SCHEMA IF NOT EXISTS bootcamp_students.' ||
  username || '_gold';

EXECUTE IMMEDIATE
  'CREATE SCHEMA IF NOT EXISTS bootcamp_students.' ||
  username || '_operations';

EXECUTE IMMEDIATE
  'CREATE SCHEMA IF NOT EXISTS bootcamp_students.' ||
  username || '_lakebase_cdc';


-- ============================================================
-- BRONZE
-- ============================================================

EXECUTE IMMEDIATE
  'CREATE TABLE IF NOT EXISTS bootcamp_students.' ||
  username || '_bronze.bronze_transactions (
      transaction_id STRING,
      step BIGINT,
      transaction_type STRING,
      amount DECIMAL(18,2),
      customer_id STRING,
      old_balance_origin DECIMAL(18,2),
      new_balance_origin DECIMAL(18,2),
      destination_id STRING,
      old_balance_destination DECIMAL(18,2),
      new_balance_destination DECIMAL(18,2),
      source_currency STRING,
      is_fraud INT,
      is_flagged_fraud INT,
      event_timestamp TIMESTAMP,
      event_date DATE,
      source_file STRING,
      ingestion_timestamp TIMESTAMP
   )
   USING DELTA
   CLUSTER BY (event_date)';


EXECUTE IMMEDIATE
  'CREATE TABLE IF NOT EXISTS bootcamp_students.' ||
  username || '_bronze.bronze_fx_api (
      request_id STRING,
      function_name STRING,
      from_currency STRING,
      to_currency STRING,
      requested_at TIMESTAMP,
      http_status INT,
      response_json STRING,
      ingestion_status STRING,
      error_message STRING
   )
   USING DELTA';


-- ============================================================
-- SILVER
-- ============================================================

EXECUTE IMMEDIATE
  'CREATE TABLE IF NOT EXISTS bootcamp_students.' ||
  username || '_silver.silver_fx_rates (
      from_currency STRING,
      to_currency STRING,
      rate_date DATE,
      exchange_rate DECIMAL(24,10),
      provider_timestamp TIMESTAMP,
      provider STRING,
      refreshed_at TIMESTAMP,
      is_stale BOOLEAN
   )
   USING DELTA
   CLUSTER BY (rate_date, from_currency, to_currency)';


EXECUTE IMMEDIATE
  'CREATE TABLE IF NOT EXISTS bootcamp_students.' ||
  username || '_silver.silver_transactions (
      transaction_id STRING,
      customer_id STRING,
      destination_id STRING,
      transaction_type STRING,
      event_timestamp TIMESTAMP,
      event_date DATE,
      amount DECIMAL(18,2),
      source_currency STRING,
      home_currency STRING,
      exchange_rate DECIMAL(24,10),
      amount_home_currency DECIMAL(18,2),
      customer_avg_amount DECIMAL(18,2),
      amount_deviation DOUBLE,
      recent_txn_count BIGINT,
      is_high_value BOOLEAN,
      is_new_destination BOOLEAN,
      is_unusual_type BOOLEAN,
      is_international BOOLEAN,
      merchant_category STRING,
      data_quality_status STRING,
      processed_at TIMESTAMP
   )
   USING DELTA
   CLUSTER BY (event_date, customer_id)';


-- ============================================================
-- GOLD
-- ============================================================

EXECUTE IMMEDIATE
  'CREATE TABLE IF NOT EXISTS bootcamp_students.' ||
  username || '_gold.gold_transaction_risk (
      transaction_id STRING,
      customer_id STRING,
      destination_id STRING,
      event_timestamp TIMESTAMP,
      event_date DATE,
      amount_home_currency DECIMAL(18,2),
      risk_score INT,
      risk_level STRING,
      risk_reasons ARRAY<STRING>,
      is_alert_candidate BOOLEAN,
      scored_at TIMESTAMP
   )
   USING DELTA
   CLUSTER BY (event_date, customer_id)';


EXECUTE IMMEDIATE
  'CREATE TABLE IF NOT EXISTS bootcamp_students.' ||
  username || '_gold.gold_customer_risk (
      customer_id STRING,
      transaction_count BIGINT,
      total_amount DECIMAL(20,2),
      avg_amount DECIMAL(18,2),
      high_risk_transaction_count BIGINT,
      max_risk_score INT,
      customer_risk_level STRING,
      refreshed_at TIMESTAMP
   )
   USING DELTA
   CLUSTER BY (customer_id)';


EXECUTE IMMEDIATE
  'CREATE TABLE IF NOT EXISTS bootcamp_students.' ||
  username || '_gold.gold_daily_transaction_metrics (
      event_date DATE,
      transaction_count BIGINT,
      total_amount DECIMAL(22,2),
      avg_amount DECIMAL(18,2),
      high_risk_count BIGINT,
      critical_risk_count BIGINT,
      refreshed_at TIMESTAMP
   )
   USING DELTA
   CLUSTER BY (event_date)';


EXECUTE IMMEDIATE
  'CREATE TABLE IF NOT EXISTS bootcamp_students.' ||
  username || '_gold.gold_alert_summary (
      metric_date DATE,
      alerts_created BIGINT,
      alerts_escalated BIGINT,
      alerts_resolved BIGINT,
      avg_resolution_minutes DOUBLE,
      agent_actions BIGINT,
      agent_action_success_rate DOUBLE,
      refreshed_at TIMESTAMP
   )
   USING DELTA
   CLUSTER BY (metric_date)';


-- ============================================================
-- CHECKPOINT VOLUME
-- ============================================================

EXECUTE IMMEDIATE
  'CREATE VOLUME IF NOT EXISTS bootcamp_students.' ||
  username || '_operations.checkpoints';