-- Minecraft Auto Miner – Telemetry Schema
-- schema_telemetry.sql – v0.6.0 – 2025-12-07

CREATE SCHEMA IF NOT EXISTS telemetry;

-- Episodes: one mining session / arm→disarm window
CREATE TABLE IF NOT EXISTS telemetry.episode (
    episode_id      BIGSERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    miner_version   TEXT NOT NULL,
    profile_name    TEXT,
    notes           TEXT
);

-- Decision windows: each state→action→reward window
CREATE TABLE IF NOT EXISTS telemetry.decision_window (
    window_id       BIGSERIAL PRIMARY KEY,
    episode_id      BIGINT NOT NULL REFERENCES telemetry.episode(episode_id) ON DELETE CASCADE,
    start_ts        TIMESTAMPTZ NOT NULL,
    end_ts          TIMESTAMPTZ NOT NULL,
    state_code      INTEGER NOT NULL,
    action_code     INTEGER NOT NULL,
    reward          DOUBLE PRECISION NOT NULL DEFAULT 0,
    blocks_broken   INTEGER,
    mining_ratio    REAL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_decision_window_episode
    ON telemetry.decision_window (episode_id);

CREATE INDEX IF NOT EXISTS idx_decision_window_state
    ON telemetry.decision_window (state_code);

-- Miner events: stuck, reset, profile switches, etc.
CREATE TABLE IF NOT EXISTS telemetry.miner_event (
    event_id        BIGSERIAL PRIMARY KEY,
    episode_id      BIGINT NOT NULL REFERENCES telemetry.episode(episode_id) ON DELETE CASCADE,
    ts              TIMESTAMPTZ NOT NULL,
    event_type      TEXT NOT NULL,
    payload_json    JSONB
);

CREATE INDEX IF NOT EXISTS idx_miner_event_episode_ts
    ON telemetry.miner_event (episode_id, ts);

-- Snapshots of control_tuning.json / strategy_stats.json for later analysis
CREATE TABLE IF NOT EXISTS telemetry.control_tuning_snapshot (
    snapshot_id     BIGSERIAL PRIMARY KEY,
    episode_id      BIGINT NOT NULL REFERENCES telemetry.episode(episode_id) ON DELETE CASCADE,
    ts              TIMESTAMPTZ NOT NULL,
    control_tuning  JSONB,
    strategy_stats  JSONB
);

CREATE INDEX IF NOT EXISTS idx_control_tuning_episode_ts
    ON telemetry.control_tuning_snapshot (episode_id, ts);
