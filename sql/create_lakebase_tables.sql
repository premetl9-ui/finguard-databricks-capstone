-- ============================================================
-- FinGuard Lakebase / PostgreSQL Operational Schema
-- Dynamic schema based on current Lakebase user
-- ============================================================

-- ------------------------------------------------------------
-- 1. Resolve schema dynamically
-- Example:
--   premetl9@gmail.com -> premetl9
-- ------------------------------------------------------------

DO $$
DECLARE
    v_schema TEXT :=
        regexp_replace(
            split_part(session_user, '@', 1),
            '[.-]',
            '_',
            'g'
        );
BEGIN
    EXECUTE format(
        'CREATE SCHEMA IF NOT EXISTS %I',
        v_schema
    );

    PERFORM set_config(
        'search_path',
        format('%I, public', v_schema),
        false
    );

    RAISE NOTICE 'FinGuard Lakebase schema: %', v_schema;
END
$$;


-- Verify the current user/schema.
SELECT
    session_user AS lakebase_user,
    current_schema() AS active_schema;


-- ============================================================
-- 2. USERS
-- ============================================================

CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    email TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,

    role TEXT NOT NULL
        CHECK (
            role IN (
                'ANALYST',
                'SENIOR_ANALYST',
                'ADMIN'
            )
        ),

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- 3. CUSTOMERS
-- ============================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,

    customer_name TEXT,

    home_currency CHAR(3)
        NOT NULL
        DEFAULT 'USD',

    risk_segment TEXT
        NOT NULL
        DEFAULT 'STANDARD'
        CHECK (
            risk_segment IN (
                'LOW',
                'STANDARD',
                'HIGH',
                'CRITICAL'
            )
        ),

    status TEXT
        NOT NULL
        DEFAULT 'ACTIVE'
        CHECK (
            status IN (
                'ACTIVE',
                'INACTIVE',
                'BLOCKED'
            )
        ),

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- 4. FRAUD ALERTS
-- ============================================================

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id UUID PRIMARY KEY
        DEFAULT gen_random_uuid(),

    transaction_id TEXT NOT NULL UNIQUE,

    customer_id TEXT NOT NULL,

    risk_score INTEGER NOT NULL
        CHECK (
            risk_score BETWEEN 0 AND 100
        ),

    risk_level TEXT NOT NULL
        CHECK (
            risk_level IN (
                'LOW',
                'MEDIUM',
                'HIGH',
                'CRITICAL'
            )
        ),

    alert_reason TEXT,

    status TEXT
        NOT NULL
        DEFAULT 'OPEN'
        CHECK (
            status IN (
                'OPEN',
                'ASSIGNED',
                'ESCALATED',
                'RESOLVED',
                'CLOSED'
            )
        ),

    assigned_to UUID
        REFERENCES users(user_id),

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fraud_alert_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
);


-- ============================================================
-- 5. INVESTIGATIONS
-- ============================================================

CREATE TABLE IF NOT EXISTS investigations (
    investigation_id UUID PRIMARY KEY
        DEFAULT gen_random_uuid(),

    alert_id UUID NOT NULL
        REFERENCES fraud_alerts(alert_id),

    opened_by UUID
        REFERENCES users(user_id),

    assigned_to UUID
        REFERENCES users(user_id),

    priority TEXT
        NOT NULL
        DEFAULT 'NORMAL'
        CHECK (
            priority IN (
                'LOW',
                'NORMAL',
                'HIGH',
                'URGENT'
            )
        ),

    status TEXT
        NOT NULL
        DEFAULT 'OPEN'
        CHECK (
            status IN (
                'OPEN',
                'IN_PROGRESS',
                'ESCALATED',
                'RESOLVED',
                'CLOSED'
            )
        ),

    summary TEXT,
    resolution TEXT,

    opened_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    closed_at TIMESTAMPTZ
);


-- ============================================================
-- 6. INVESTIGATION NOTES
-- ============================================================

CREATE TABLE IF NOT EXISTS investigation_notes (
    note_id UUID PRIMARY KEY
        DEFAULT gen_random_uuid(),

    investigation_id UUID NOT NULL
        REFERENCES investigations(investigation_id),

    author_id UUID
        REFERENCES users(user_id),

    note_text TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- 7. AI AGENT ACTION AUDIT
-- ============================================================

CREATE TABLE IF NOT EXISTS agent_actions (
    action_id UUID PRIMARY KEY
        DEFAULT gen_random_uuid(),

    alert_id UUID
        REFERENCES fraud_alerts(alert_id),

    investigation_id UUID
        REFERENCES investigations(investigation_id),

    actor_user_id UUID
        REFERENCES users(user_id),

    agent_name TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    action_type TEXT NOT NULL,

    request_payload JSONB
        NOT NULL
        DEFAULT '{}'::jsonb,

    result_payload JSONB
        NOT NULL
        DEFAULT '{}'::jsonb,

    action_status TEXT NOT NULL
        CHECK (
            action_status IN (
                'SUCCESS',
                'FAILED',
                'DENIED'
            )
        ),

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- 8. ALERT STATUS HISTORY
-- ============================================================

CREATE TABLE IF NOT EXISTS alert_status_history (
    history_id UUID PRIMARY KEY
        DEFAULT gen_random_uuid(),

    alert_id UUID NOT NULL
        REFERENCES fraud_alerts(alert_id),

    old_status TEXT,

    new_status TEXT NOT NULL
        CHECK (
            new_status IN (
                'OPEN',
                'ASSIGNED',
                'ESCALATED',
                'RESOLVED',
                'CLOSED'
            )
        ),

    changed_by UUID
        REFERENCES users(user_id),

    change_source TEXT
        NOT NULL
        DEFAULT 'APP'
        CHECK (
            change_source IN (
                'APP',
                'AGENT',
                'SYSTEM'
            )
        ),

    reason TEXT,

    changed_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- 9. UPDATED_AT TRIGGER
-- ============================================================

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;


DROP TRIGGER IF EXISTS trg_users_updated_at
ON users;

CREATE TRIGGER trg_users_updated_at
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();


DROP TRIGGER IF EXISTS trg_customers_updated_at
ON customers;

CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();


DROP TRIGGER IF EXISTS trg_fraud_alerts_updated_at
ON fraud_alerts;

CREATE TRIGGER trg_fraud_alerts_updated_at
BEFORE UPDATE ON fraud_alerts
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();


-- ============================================================
-- 10. PERFORMANCE INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_fraud_alert_customer
    ON fraud_alerts(customer_id);

CREATE INDEX IF NOT EXISTS idx_fraud_alert_status
    ON fraud_alerts(
        status,
        created_at DESC
    );

CREATE INDEX IF NOT EXISTS idx_fraud_alert_assignment
    ON fraud_alerts(
        assigned_to,
        status
    );

CREATE INDEX IF NOT EXISTS idx_investigation_alert
    ON investigations(alert_id);

CREATE INDEX IF NOT EXISTS idx_investigation_assignment
    ON investigations(
        assigned_to,
        status
    );

CREATE INDEX IF NOT EXISTS idx_investigation_notes_case
    ON investigation_notes(
        investigation_id,
        created_at DESC
    );

CREATE INDEX IF NOT EXISTS idx_agent_actions_alert
    ON agent_actions(
        alert_id,
        created_at DESC
    );

CREATE INDEX IF NOT EXISTS idx_agent_actions_investigation
    ON agent_actions(
        investigation_id,
        created_at DESC
    );

CREATE INDEX IF NOT EXISTS idx_alert_history_alert
    ON alert_status_history(
        alert_id,
        changed_at DESC
    );


-- ============================================================
-- 11. LAKEBASE CDC / CDF SUPPORT
-- ============================================================

ALTER TABLE fraud_alerts
    REPLICA IDENTITY FULL;

ALTER TABLE investigations
    REPLICA IDENTITY FULL;

ALTER TABLE investigation_notes
    REPLICA IDENTITY FULL;

ALTER TABLE agent_actions
    REPLICA IDENTITY FULL;

ALTER TABLE alert_status_history
    REPLICA IDENTITY FULL;


-- ============================================================
-- 12. VALIDATION
-- ============================================================

SELECT
    current_schema() AS schema_name,
    table_name
FROM information_schema.tables
WHERE table_schema = current_schema()
  AND table_type = 'BASE TABLE'
ORDER BY table_name;


-- ============================================================
-- Optional demo user
-- Replace values if needed.
-- ============================================================

-- INSERT INTO users (
--     email,
--     display_name,
--     role
-- )
-- VALUES (
--     'premetl9@gmail.com',
--     'Prema Veerapaneni',
--     'ADMIN'
-- )
-- ON CONFLICT (email) DO NOTHING;