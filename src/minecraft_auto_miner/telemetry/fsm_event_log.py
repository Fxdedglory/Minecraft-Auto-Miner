"""
fsm_event_log.py v0.1.1 – 2025-12-10
Tiny FSM event logger for Minecraft Auto Miner.

Purpose:
- Log high-level FSM state/action events into a small table:
    silver.fsm_event_log

This table is then used by episodes_from_silver.py to derive
"dominant" state_name / action_name for:
- silver.episode
- silver.decision_window
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Optional, Dict

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

load_dotenv()


def _get_conn():
    """
    Build a Postgres connection using .env variables.

    Prefers the MAM_TELEMETRY_* vars, falls back to POSTGRES_*,
    then to sane defaults if not set.
    """
    db_name = os.getenv("MAM_TELEMETRY_DB") or os.getenv("POSTGRES_DB", "mam_telemetry")
    user = os.getenv("MAM_TELEMETRY_USER") or os.getenv("POSTGRES_USER", "mam_user")
    password = os.getenv("MAM_TELEMETRY_PASSWORD") or os.getenv(
        "POSTGRES_PASSWORD", "postgres"
    )
    host = os.getenv("MAM_TELEMETRY_HOST") or os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("MAM_TELEMETRY_PORT") or os.getenv("POSTGRES_PORT", "5432"))

    conn = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    conn.autocommit = False
    return conn


FSM_DDL = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.fsm_event_log (
    fsm_event_id   BIGSERIAL PRIMARY KEY,
    event_ts_utc   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    state_name     TEXT        NOT NULL,
    action_name    TEXT        NOT NULL,
    source         TEXT,
    extra_json     JSONB,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fsm_event_ts
    ON silver.fsm_event_log (event_ts_utc);
"""

_DDL_READY = False
_DDL_LOCK = threading.Lock()
_INSERT_LOCK = threading.Lock()


def ensure_table() -> None:
    """
    Idempotently ensure silver.fsm_event_log exists.
    Safe to call often.
    """
    global _DDL_READY
    if _DDL_READY:
        return
    with _DDL_LOCK:
        if _DDL_READY:
            return
        conn = _get_conn()
        try:
            with conn:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(FSM_DDL)
            _DDL_READY = True
        finally:
            conn.close()


def _insert_event_once(
    state_name: str,
    action_name: str,
    source: str,
    extra: Optional[Dict],
) -> None:
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO silver.fsm_event_log (
                        state_name,
                        action_name,
                        source,
                        extra_json
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        state_name,
                        action_name,
                        source,
                        json.dumps(extra) if extra is not None else None,
                    ),
                )
    finally:
        conn.close()


def log_fsm_event(
    state_name: str,
    action_name: str,
    source: str = "miner",
    extra: Optional[Dict] = None,
) -> None:
    """
    Insert a single FSM event row.

    This is intended to be called on *transitions* only,
    not every tick, so opening/closing a connection is fine.
    """
    ensure_table()
    last_exc: Optional[Exception] = None
    for attempt in range(4):
        try:
            with _INSERT_LOCK:
                _insert_event_once(
                    state_name=state_name,
                    action_name=action_name,
                    source=source,
                    extra=extra,
                )
            return
        except psycopg2.Error as exc:
            last_exc = exc
            pgcode = getattr(exc, "pgcode", None)
            if pgcode != "40P01" and attempt >= 1:
                raise
            time.sleep(0.05 * (attempt + 1))
    if last_exc is not None:
        raise last_exc
