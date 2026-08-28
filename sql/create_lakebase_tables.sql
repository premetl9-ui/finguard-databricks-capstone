-- FinGuard Lakebase (PostgreSQL) operational schema.
-- Execute against the FinGuard Lakebase database.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('ANALYST', 'SENIOR_ANALYST', 'ADMIN')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT,
    home_currency CHAR(3) NOT NULL DEFAULT 'USD',
    risk_segment TEXT NOT NULL DEFAULT 'STANDARD',
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id TEXT NOT NULL UNIQUE,
    customer_id TEXT NOT NULL,
    risk_score INTEGER NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
    risk_level TEXT NOT NULL CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    alert_reason TEXT,
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'ASSIGNED', 'ESCALATED', 'RESOLVED', 'CLOSED')),
    assigned_to UUID REFERENCES users(user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fraud_alert_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS investigations (
    investigation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id UUID NOT NULL REFERENCES fraud_alerts(alert_id),
    opened_by UUID REFERENCES users(user_id),
    assigned_to UUID REFERENCES users(user_id),
    priority TEXT NOT NULL DEFAULT 'NORMAL' CHECK (priority IN ('LOW', 'NORMAL', 'HIGH', 'URGENT')),
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'IN_PROGRESS', 'ESCALATED', 'RESOLVED', 'CLOSED')),
    summary TEXT,
    resolution TEXT,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS investigation_notes (
    note_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    investigation_id UUID NOT NULL REFERENCES investigations(investigation_id),
    author_id UUID REFERENCES users(user_id),
    note_text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agent_actions (
    action_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id UUID REFERENCES fraud_alerts(alert_id),
    investigation_id UUID REFERENCES investigations(investigation_id),
    actor_user_id UUID REFERENCES users(user_id),
    agent_name TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    action_type TEXT NOT NULL,
    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    action_status TEXT NOT NULL CHECK (action_status IN ('SUCCESS', 'FAILED', 'DENIED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alert_status_history (
    history_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id UUID NOT NULL REFERENCES fraud_alerts(alert_id),
    old_status TEXT,
    new_status TEXT NOT NULL,
    changed_by UUID REFERENCES users(user_id),
    change_source TEXT NOT NULL DEFAULT 'APP',
    reason TEXT,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Query/write performance indexes.
CREATE INDEX IF NOT EXISTS idx_fraud_alert_customer
    ON fraud_alerts(customer_id);
CREATE INDEX IF NOT EXISTS idx_fraud_alert_status
    ON fraud_alerts(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_alert_assignment
    ON fraud_alerts(assigned_to, status);
CREATE INDEX IF NOT EXISTS idx_investigation_alert
    ON investigations(alert_id);
CREATE INDEX IF NOT EXISTS idx_investigation_notes_case
    ON investigation_notes(investigation_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_actions_alert
    ON agent_actions(alert_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_history_alert
    ON alert_status_history(alert_id, changed_at DESC);

-- Required for complete before/after images in Lakebase CDF.
ALTER TABLE fraud_alerts REPLICA IDENTITY FULL;
ALTER TABLE investigations REPLICA IDENTITY FULL;
ALTER TABLE investigation_notes REPLICA IDENTITY FULL;
ALTER TABLE agent_actions REPLICA IDENTITY FULL;
ALTER TABLE alert_status_history REPLICA IDENTITY FULL;

-- Seed a local/demo administrator only when desired; replace the email before use.
-- INSERT INTO users (email, display_name, role)
-- VALUES ('analyst@example.com', 'Demo Analyst', 'ADMIN')
-- ON CONFLICT (email) DO NOTHING;
