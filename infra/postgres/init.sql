-- =============================================================
-- RealTimeFraudGuard — PostgreSQL Schema
-- Audit store for decisions, feature snapshots, LLM notes,
-- and analyst resolutions. Optimized for write-heavy workload
-- during real-time scoring and read queries for the dashboard.
-- =============================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- =============================================================
-- decisions
-- Written synchronously on every transaction score.
-- Partitioned by decision_time for efficient time-range queries.
-- =============================================================
CREATE TABLE IF NOT EXISTS decisions (
    id               UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id   VARCHAR(64) NOT NULL UNIQUE,
    account_id       VARCHAR(64) NOT NULL,
    amount           NUMERIC(12, 4) NOT NULL,
    merchant_id      VARCHAR(128) NOT NULL,
    device_fingerprint VARCHAR(128) NOT NULL,
    geo_country      VARCHAR(8) NOT NULL,
    billing_country  VARCHAR(8) NOT NULL,
    shipping_country VARCHAR(8) NOT NULL,
    timestamp_ms     BIGINT NOT NULL,
    risk_score       NUMERIC(6, 5) NOT NULL,         -- 0.00000 – 1.00000
    decision         VARCHAR(8) NOT NULL              -- 'allow' | 'flag' | 'block'
                     CHECK (decision IN ('allow', 'flag', 'block')),
    triggered_rules  JSONB NOT NULL DEFAULT '[]',    -- list of rule names triggered
    model_version    VARCHAR(32) NOT NULL DEFAULT 'v1',
    decision_time    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    latency_ms       INTEGER,                         -- end-to-end scoring latency
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_decisions_account_id    ON decisions (account_id);
CREATE INDEX idx_decisions_decision_time ON decisions (decision_time DESC);
CREATE INDEX idx_decisions_decision      ON decisions (decision);
CREATE INDEX idx_decisions_risk_score    ON decisions (risk_score DESC);

-- =============================================================
-- feature_snapshots
-- Full feature vector recorded at scoring time for audit and
-- future model retraining. One row per decision.
-- =============================================================
CREATE TABLE IF NOT EXISTS feature_snapshots (
    id                     UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id         VARCHAR(64) NOT NULL REFERENCES decisions(transaction_id),
    -- Rolling window features (computed by Flink)
    txn_count_5min         INTEGER NOT NULL DEFAULT 0,
    avg_amount_1hr         NUMERIC(12, 4) NOT NULL DEFAULT 0,
    distinct_merchants_24hr INTEGER NOT NULL DEFAULT 0,
    distinct_countries_10min INTEGER NOT NULL DEFAULT 0,
    -- Raw transaction features (from the event itself)
    amount                 NUMERIC(12, 4) NOT NULL,
    billing_eq_shipping    BOOLEAN NOT NULL,
    geo_country            VARCHAR(8) NOT NULL,
    billing_country        VARCHAR(8) NOT NULL,
    shipping_country       VARCHAR(8) NOT NULL,
    -- Full feature vector as JSON for schema evolution
    feature_vector         JSONB NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_feature_snapshots_txn_id ON feature_snapshots (transaction_id);

-- =============================================================
-- investigation_notes
-- LLM-generated investigation summaries for flagged/blocked
-- transactions. Written asynchronously by the LLM worker.
-- =============================================================
CREATE TABLE IF NOT EXISTS investigation_notes (
    id                UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id    VARCHAR(64) NOT NULL REFERENCES decisions(transaction_id),
    note_text         TEXT        NOT NULL,
    model_used        VARCHAR(64) NOT NULL,          -- e.g. 'llama3-70b-8192'
    prompt_tokens     INTEGER,
    completion_tokens INTEGER,
    llm_latency_ms    INTEGER,
    cache_hit         BOOLEAN     NOT NULL DEFAULT FALSE,
    triggered_rules   JSONB       NOT NULL DEFAULT '[]',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_investigation_notes_txn_id    ON investigation_notes (transaction_id);
CREATE INDEX idx_investigation_notes_created   ON investigation_notes (created_at DESC);

-- =============================================================
-- analyst_resolutions
-- Records analyst decisions: resolved or false_positive.
-- These labels feed back into future model retraining.
-- =============================================================
CREATE TABLE IF NOT EXISTS analyst_resolutions (
    id             UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id VARCHAR(64) NOT NULL REFERENCES decisions(transaction_id),
    analyst_id     VARCHAR(64),                       -- optional analyst identifier
    resolution     VARCHAR(16) NOT NULL
                   CHECK (resolution IN ('resolved', 'false_positive')),
    notes          TEXT,
    resolved_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_analyst_resolutions_txn ON analyst_resolutions (transaction_id);
CREATE INDEX idx_analyst_resolutions_resolved   ON analyst_resolutions (resolved_at DESC);

-- =============================================================
-- llm_worker_dlq
-- Dead-letter queue entries for LLM calls that exhausted retries.
-- Separate from the Kafka DLQ for audit purposes.
-- =============================================================
CREATE TABLE IF NOT EXISTS llm_worker_dlq (
    id             UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id VARCHAR(64),
    raw_event      JSONB       NOT NULL,
    error_message  TEXT        NOT NULL,
    retry_count    INTEGER     NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================
-- Materialized view: analyst dashboard queue
-- Pre-joins decisions + notes + resolution status.
-- Refreshed by the LLM worker on note insertion.
-- =============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS analyst_queue AS
SELECT
    d.transaction_id,
    d.account_id,
    d.amount,
    d.merchant_id,
    d.geo_country,
    d.billing_country,
    d.shipping_country,
    d.risk_score,
    d.decision,
    d.triggered_rules,
    d.decision_time,
    n.note_text,
    n.created_at AS note_ready_at,
    r.resolution,
    r.resolved_at,
    CASE
        WHEN r.resolution IS NOT NULL THEN 'resolved'
        WHEN n.id IS NOT NULL         THEN 'pending_review'
        ELSE                               'awaiting_note'
    END AS queue_status
FROM decisions d
LEFT JOIN investigation_notes n ON n.transaction_id = d.transaction_id
LEFT JOIN analyst_resolutions  r ON r.transaction_id = d.transaction_id
WHERE d.decision IN ('flag', 'block')
ORDER BY d.decision_time DESC;

CREATE UNIQUE INDEX ON analyst_queue (transaction_id);

-- =============================================================
-- Grant minimal privileges (production pattern)
-- =============================================================
-- The scoring service only needs to INSERT into decisions + feature_snapshots
-- The LLM worker only needs to INSERT into investigation_notes + SELECT from decisions
-- The dashboard only needs SELECT + UPDATE on resolutions
-- In production, create separate roles. For local dev, a single user is fine.
