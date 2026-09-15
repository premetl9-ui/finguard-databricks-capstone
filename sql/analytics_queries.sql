-- ============================================================
-- FinGuard Demonstration / Validation Queries
-- Dynamic for current Databricks user
-- ============================================================

DECLARE OR REPLACE VARIABLE username STRING;

SET VAR username =
  regexp_replace(
    split(session_user(), '@')[0],
    '[.-]',
    '_'
  );

SELECT
  session_user() AS user_email,
  username AS username;


-- ============================================================
-- 1. Risk distribution
-- ============================================================

SELECT
    risk_level,
    COUNT(*) AS transactions
FROM IDENTIFIER(
    'bootcamp_students.' || username || '_gold.gold_transaction_risk'
)
GROUP BY risk_level
ORDER BY
    CASE risk_level
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        ELSE 4
    END;


-- ============================================================
-- 2. Highest-risk transactions
-- ============================================================

SELECT
    transaction_id,
    customer_id,
    event_timestamp,
    amount_home_currency,
    risk_score,
    risk_level,
    risk_reasons
FROM IDENTIFIER(
    'bootcamp_students.' || username || '_gold.gold_transaction_risk'
)
ORDER BY
    risk_score DESC,
    event_timestamp DESC
LIMIT 100;


-- ============================================================
-- 3. Daily transaction / risk metrics
-- ============================================================

SELECT *
FROM IDENTIFIER(
    'bootcamp_students.' || username || '_gold.gold_daily_transaction_metrics'
)
ORDER BY event_date DESC;


-- ============================================================
-- 4. Customers with greatest high-risk activity
-- ============================================================

SELECT
    customer_id,
    transaction_count,
    total_amount,
    high_risk_transaction_count,
    max_risk_score,
    customer_risk_level
FROM IDENTIFIER(
    'bootcamp_students.' || username || '_gold.gold_customer_risk'
)
ORDER BY
    high_risk_transaction_count DESC,
    max_risk_score DESC
LIMIT 100;


-- ============================================================
-- 5. Lakebase CDC / Application Analytics
-- ============================================================

SELECT *
FROM IDENTIFIER(
    'bootcamp_students.' || username || '_gold.gold_alert_summary'
)
ORDER BY metric_date DESC;


-- ============================================================
-- 6. Data Quality / Quarantine
-- Run after silver_transaction_quarantine has been created
-- ============================================================

SELECT
    dq_error,
    COUNT(*) AS rejected_rows
FROM IDENTIFIER(
    'bootcamp_students.' || username || '_silver.silver_transaction_quarantine'
)
GROUP BY dq_error
ORDER BY rejected_rows DESC;


-- ============================================================
-- 7. Alpha Vantage ingestion audit
-- ============================================================

SELECT
    function_name,
    from_currency,
    to_currency,
    ingestion_status,
    COUNT(*) AS requests,
    MAX(requested_at) AS latest_request
FROM IDENTIFIER(
    'bootcamp_students.' || username || '_bronze.bronze_fx_api'
)
GROUP BY
    function_name,
    from_currency,
    to_currency,
    ingestion_status
ORDER BY latest_request DESC;