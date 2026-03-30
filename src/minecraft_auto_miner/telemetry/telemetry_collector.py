"""
minecraft_auto_miner.telemetry.telemetry_collector v0.7.2 – 2025-12-08
Generated with ChatGPT (GPT-5.1 Thinking).

Changelog:
- 2025-12-08 v0.7.0:
    * Initial Phase 1 TelemetryCollector with reward wiring.
- 2025-12-08 v0.7.1:
    * Added idempotent DDL bootstrap (schema + tables + indexes).
    * Added .env support via python-dotenv (if available).
- 2025-12-08 v0.7.2:
    * Aligned schema + handlers with actual TELEMETRY_JSON (UUID episode_id, flat event format).
    * Fixed type mismatches (episode_id TEXT, no int() casts).
    * Added debug logging for raw telemetry events.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import psycopg

# Optional .env support
try:
    from dotenv import load_dotenv  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - optional dep
    load_dotenv = None

from ..learning import compute_reward_from_raw


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class TelemetryCollectorConfig:
    """
    Configuration for the telemetry collector.

    Attributes
    ----------
    db_dsn : str
        Postgres connection string, e.g.
        "postgresql://mam_user:postgres@localhost:5432/mam_telemetry"
    log_path : Path
        Path to the `mining_helper.log` file that contains TELEMETRY_JSON lines.
    poll_interval_sec : float
        How often to poll the log file for new lines (currently unused but kept
        for future tuning).
    batch_size : int
        How many events to buffer before flushing to the database (not yet used
        in v0.7.x; we commit per event for simplicity).
    """

    db_dsn: str
    log_path: Path
    poll_interval_sec: float = 0.25
    batch_size: int = 64


def load_config_from_env() -> TelemetryCollectorConfig:
    """
    Load telemetry collector config from `.env` + environment variables.

    We first call python-dotenv's load_dotenv() (if available), so that
    a project-level `.env` file is respected, then pull values from os.environ.

    Required environment variables
    ------------------------------
    MAM_TELEMETRY_DB       : str
    MAM_TELEMETRY_USER     : str
    MAM_TELEMETRY_PASSWORD : str
    MAM_TELEMETRY_HOST     : str
    MAM_TELEMETRY_PORT     : str/int
    MAM_MINING_LOG_PATH    : str (path to mining_helper.log)
    """
    if load_dotenv is not None:
        # Load .env from CWD / project root if present.
        load_dotenv()

    db_name = os.environ.get("MAM_TELEMETRY_DB")
    db_user = os.environ.get("MAM_TELEMETRY_USER")
    db_password = os.environ.get("MAM_TELEMETRY_PASSWORD")
    db_host = os.environ.get("MAM_TELEMETRY_HOST")
    db_port = os.environ.get("MAM_TELEMETRY_PORT")
    log_path = os.environ.get("MAM_MINING_LOG_PATH")

    missing = [
        name
        for name, value in [
            ("MAM_TELEMETRY_DB", db_name),
            ("MAM_TELEMETRY_USER", db_user),
            ("MAM_TELEMETRY_PASSWORD", db_password),
            ("MAM_TELEMETRY_HOST", db_host),
            ("MAM_TELEMETRY_PORT", db_port),
            ("MAM_MINING_LOG_PATH", log_path),
        ]
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    # Build a standard Postgres DSN for psycopg
    db_dsn = (
        f"postgresql://{db_user}:{db_password}"
        f"@{db_host}:{db_port}/{db_name}"
    )

    return TelemetryCollectorConfig(
        db_dsn=db_dsn,
        log_path=Path(log_path),
    )


# ---------------------------------------------------------------------------
# Idempotent DDL bootstrap
# ---------------------------------------------------------------------------


DDL_STATEMENTS = [
    # Schema
    """
    CREATE SCHEMA IF NOT EXISTS telemetry;
    """,
    # Episodes (one row per episode run)
    """
    CREATE TABLE IF NOT EXISTS telemetry.episode (
        episode_id          TEXT PRIMARY KEY,
        episode_index       INTEGER,
        profile_name        TEXT,
        started_at          TIMESTAMPTZ,
        finished_at         TIMESTAMPTZ,
        duration_seconds    DOUBLE PRECISION,
        total_ticks         INTEGER,
        total_mining_ticks  INTEGER,
        total_block_breaks  INTEGER,
        blocks_per_minute   DOUBLE PRECISION,
        mining_ratio        DOUBLE PRECISION,
        stuck_flag          BOOLEAN,
        reset_flag          BOOLEAN,
        invalid_flag        BOOLEAN,
        wallhug_flag        BOOLEAN,
        low_progress_flag   BOOLEAN,
        reward_raw          DOUBLE PRECISION,
        reward              DOUBLE PRECISION
    );
    """,
    # Decision windows (one row per WINDOW_SUMMARY)
    """
    CREATE TABLE IF NOT EXISTS telemetry.decision_window (
        window_id         BIGSERIAL PRIMARY KEY,
        episode_id        TEXT NOT NULL,
        episode_index     INTEGER,
        window_index      INTEGER,
        profile_name      TEXT,
        start_utc         TIMESTAMPTZ NOT NULL,
        end_utc           TIMESTAMPTZ NOT NULL,
        duration_seconds  DOUBLE PRECISION,
        blocks_broken     INTEGER,
        blocks_per_minute DOUBLE PRECISION,
        mining_ratio      DOUBLE PRECISION,
        reward            DOUBLE PRECISION,
        stuck_flag        BOOLEAN,
        reset_flag        BOOLEAN,
        low_progress_flag BOOLEAN,
        ts_ingested       TIMESTAMPTZ DEFAULT now()
    );
    """,
    # Miner events (raw event log)
    """
    CREATE TABLE IF NOT EXISTS telemetry.miner_event (
        id          BIGSERIAL PRIMARY KEY,
        episode_id  TEXT,
        event_type  TEXT NOT NULL,
        payload     JSONB,
        ts          TIMESTAMPTZ
    );
    """,
    # Indexes
    """
    CREATE INDEX IF NOT EXISTS ix_decision_window_episode_start
        ON telemetry.decision_window (episode_id, start_utc);
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_miner_event_episode_ts
        ON telemetry.miner_event (episode_id, ts);
    """,
]


def ensure_schema(cur) -> None:
    """
    Run idempotent DDL to ensure telemetry schema/tables/indexes exist.

    This is safe to run at every startup and will be a no-op once the schema
    is already in place.
    """
    logging.info("Ensuring telemetry schema and tables exist...")
    for stmt in DDL_STATEMENTS:
        cur.execute(stmt)
    logging.info("Telemetry schema/bootstrap complete.")


# ---------------------------------------------------------------------------
# Log tailing & TELEMETRY_JSON parsing
# ---------------------------------------------------------------------------


def tail_log(path: Path) -> Iterator[str]:
    """
    Tail a log file, yielding new lines as they arrive.

    This is a simple, blocking tail implementation that seeks to the end
    of the file and then yields new lines as they appear.
    """
    logging.info("Tailing log file: %s", path)
    with path.open("r", encoding="utf-8", errors="replace") as f:
        # Seek to end so we only process new lines going forward.
        f.seek(0, os.SEEK_END)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.1)
                continue
            yield line.rstrip("\n")


def extract_telemetry_json(line: str) -> Optional[Dict[str, Any]]:
    """
    Extract and parse TELEMETRY_JSON payload from a log line.

    Example format from your logs:

        2025-12-08 ... | INFO | minecraft_auto_miner |
        TELEMETRY_JSON {"type":"TELEMETRY","event":"WINDOW_SUMMARY", ...}

    We:
    - Look for the substring "TELEMETRY_JSON".
    - Find the first '{' after it.
    - Parse the JSON object.

    Returns
    -------
    dict or None
        Parsed JSON object if present, otherwise None.
    """
    marker = "TELEMETRY_JSON"
    idx = line.find(marker)
    if idx == -1:
        return None

    brace_idx = line.find("{", idx)
    if brace_idx == -1:
        logging.warning("Found TELEMETRY_JSON marker but no JSON object: %s", line)
        return None

    json_str = line[brace_idx:]
    try:
        obj = json.loads(json_str)
        if not isinstance(obj, dict):
            logging.warning("TELEMETRY_JSON is not a JSON object: %s", json_str)
            return None
        return obj
    except json.JSONDecodeError:
        logging.exception("Failed to parse TELEMETRY_JSON: %s", json_str)
        return None


# ---------------------------------------------------------------------------
# Event handling: EPISODE, WINDOW, MINER_EVENT
# ---------------------------------------------------------------------------


def handle_episode_start(cur, payload: Dict[str, Any]) -> None:
    """
    Insert an EPISODE_START event into telemetry.episode and telemetry.miner_event.

    This handler is defensive: if your miner does not emit EPISODE_START yet,
    it will simply never be called.

    Expected payload fields (if emitted):
        - episode_id : str (UUID-like string)
        - episode_index : int
        - profile_name : str
        - ts / started_at : str (ISO timestamp)
    """
    episode_id = payload.get("episode_id")
    episode_index = payload.get("episode_index")
    profile_name = payload.get("profile_name")
    ts = payload.get("ts") or payload.get("started_at")

    cur.execute(
        """
        INSERT INTO telemetry.episode (episode_id, episode_index, profile_name, started_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (episode_id) DO UPDATE
            SET episode_index = EXCLUDED.episode_index,
                profile_name  = EXCLUDED.profile_name,
                started_at    = COALESCE(EXCLUDED.started_at, telemetry.episode.started_at)
        """,
        (episode_id, episode_index, profile_name, ts),
    )

    cur.execute(
        """
        INSERT INTO telemetry.miner_event (episode_id, event_type, payload, ts)
        VALUES (%s, %s, %s::jsonb, %s)
        """,
        (
            episode_id,
            "EPISODE_START",
            json.dumps(payload),
            ts,
        ),
    )


def handle_episode_end(cur, payload: Dict[str, Any]) -> None:
    """
    Insert an EPISODE_END event and upsert into telemetry.episode.

    Example payload (from your log):

        {
          "type": "TELEMETRY",
          "event": "EPISODE_END",
          "ts": "2025-12-08T23:09:47.101511+00:00",
          "episode_id": "...",
          "episode_index": 1,
          "profile_id": 0,
          "profile_name": "straight_lane_default",
          "duration_seconds": 53.241692,
          "total_ticks": 489,
          "total_mining_ticks": 488,
          "total_block_breaks": 0,
          "blocks_per_minute": 0.0,
          "mining_ratio": 0.9979,
          "reason": "program_exit",
          "stuck_flag": false,
          "reset_flag": false,
          "invalid_flag": false,
          "wallhug_flag": false,
          "low_progress_flag": true,
          "reward_raw": 0.0,
          "reward": 0.0
        }
    """
    episode_id = payload.get("episode_id")
    episode_index = payload.get("episode_index")
    profile_name = payload.get("profile_name")
    ts = payload.get("ts")

    duration_seconds = payload.get("duration_seconds")
    total_ticks = payload.get("total_ticks")
    total_mining_ticks = payload.get("total_mining_ticks")
    total_block_breaks = payload.get("total_block_breaks")
    blocks_per_minute = payload.get("blocks_per_minute")
    mining_ratio = payload.get("mining_ratio")

    stuck_flag = payload.get("stuck_flag")
    reset_flag = payload.get("reset_flag")
    invalid_flag = payload.get("invalid_flag")
    wallhug_flag = payload.get("wallhug_flag")
    low_progress_flag = payload.get("low_progress_flag")
    reward_raw = payload.get("reward_raw")
    reward = payload.get("reward")

    # Upsert into telemetry.episode
    cur.execute(
        """
        INSERT INTO telemetry.episode (
            episode_id,
            episode_index,
            profile_name,
            finished_at,
            duration_seconds,
            total_ticks,
            total_mining_ticks,
            total_block_breaks,
            blocks_per_minute,
            mining_ratio,
            stuck_flag,
            reset_flag,
            invalid_flag,
            wallhug_flag,
            low_progress_flag,
            reward_raw,
            reward
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (episode_id) DO UPDATE
        SET
            episode_index      = EXCLUDED.episode_index,
            profile_name       = EXCLUDED.profile_name,
            finished_at        = EXCLUDED.finished_at,
            duration_seconds   = EXCLUDED.duration_seconds,
            total_ticks        = EXCLUDED.total_ticks,
            total_mining_ticks = EXCLUDED.total_mining_ticks,
            total_block_breaks = EXCLUDED.total_block_breaks,
            blocks_per_minute  = EXCLUDED.blocks_per_minute,
            mining_ratio       = EXCLUDED.mining_ratio,
            stuck_flag         = EXCLUDED.stuck_flag,
            reset_flag         = EXCLUDED.reset_flag,
            invalid_flag       = EXCLUDED.invalid_flag,
            wallhug_flag       = EXCLUDED.wallhug_flag,
            low_progress_flag  = EXCLUDED.low_progress_flag,
            reward_raw         = EXCLUDED.reward_raw,
            reward             = EXCLUDED.reward
        """,
        (
            episode_id,
            episode_index,
            profile_name,
            ts,
            duration_seconds,
            total_ticks,
            total_mining_ticks,
            total_block_breaks,
            blocks_per_minute,
            mining_ratio,
            stuck_flag,
            reset_flag,
            invalid_flag,
            wallhug_flag,
            low_progress_flag,
            reward_raw,
            reward,
        ),
    )

    # Also store raw event
    handle_miner_side_event(cur, "EPISODE_END", payload)


def handle_miner_side_event(cur, kind: str, payload: Dict[str, Any]) -> None:
    """
    Generic handler for RESET_EVENT, STUCK_EVENT, WATCHDOG_* etc.

    We simply append a row into telemetry.miner_event.
    """
    episode_id = payload.get("episode_id")
    ts = payload.get("ts")  # string timestamp, as emitted by app.py

    cur.execute(
        """
        INSERT INTO telemetry.miner_event (episode_id, event_type, payload, ts)
        VALUES (%s, %s, %s::jsonb, %s)
        """,
        (
            episode_id,
            kind,
            json.dumps(payload),
            ts,
        ),
    )


def handle_window_summary(cur, payload: Dict[str, Any]) -> None:
    """
    Handle a WINDOW_SUMMARY TELEMETRY_JSON event.

    Example payload (from mining_helper.log):

        {
          "type": "TELEMETRY",
          "event": "WINDOW_SUMMARY",
          "ts": "2025-12-08T23:09:47.099505+00:00",
          "episode_id": "ee5357...",
          "episode_index": 1,
          "window_index": 0,
          "start_utc": "...",
          "end_utc": "...",
          "ticks": 211,
          "mining_ticks": 211,
          "block_breaks": 0,
          "duration_seconds": 30.021472,
          "blocks_per_minute": 0.0,
          "mining_ratio": 1.0
        }
    """
    episode_id = payload.get("episode_id")
    episode_index = payload.get("episode_index")
    window_index = payload.get("window_index")
    profile_name = payload.get("profile_name")  # may be None for now

    start_utc = payload.get("start_utc")
    end_utc = payload.get("end_utc")

    duration_seconds = payload.get("duration_seconds")
    if duration_seconds is not None:
        try:
            duration_seconds = float(duration_seconds)
        except (TypeError, ValueError):
            duration_seconds = None

    blocks_broken = int(payload.get("block_breaks", 0))

    blocks_per_minute = payload.get("blocks_per_minute")
    if blocks_per_minute is not None:
        try:
            blocks_per_minute = float(blocks_per_minute)
        except (TypeError, ValueError):
            blocks_per_minute = None

    mining_ratio = payload.get("mining_ratio")
    if mining_ratio is not None:
        try:
            mining_ratio = float(mining_ratio)
        except (TypeError, ValueError):
            mining_ratio = None

    # Flags not present on WINDOW_SUMMARY; default False.
    had_stuck_event = bool(payload.get("stuck_flag", False))
    had_low_progress_watchdog = bool(payload.get("low_progress_flag", False))
    had_reset_event = bool(payload.get("reset_flag", False))

    # Use reward engine (even if blocks_per_minute is already given).
    reward = compute_reward_from_raw(
        blocks_broken=blocks_broken,
        duration_sec=duration_seconds or 1.0,
        mining_ratio=mining_ratio,
        had_stuck_event=had_stuck_event,
        had_low_progress_watchdog=had_low_progress_watchdog,
        had_reset_event=had_reset_event,
    )

    cur.execute(
        """
        INSERT INTO telemetry.decision_window (
            episode_id,
            episode_index,
            window_index,
            profile_name,
            start_utc,
            end_utc,
            duration_seconds,
            blocks_broken,
            blocks_per_minute,
            mining_ratio,
            reward,
            stuck_flag,
            reset_flag,
            low_progress_flag
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            episode_id,
            episode_index,
            window_index,
            profile_name,
            start_utc,
            end_utc,
            duration_seconds,
            blocks_broken,
            blocks_per_minute,
            mining_ratio,
            reward,
            had_stuck_event,
            had_reset_event,
            had_low_progress_watchdog,
        ),
    )


# ---------------------------------------------------------------------------
# Main collector loop
# ---------------------------------------------------------------------------


def process_telemetry_event(cur, obj: Dict[str, Any]) -> None:
    """
    Route a TELEMETRY_JSON object to the appropriate handler.

    Supports the current flat shape:

        {
          "type": "TELEMETRY",
          "event": "WINDOW_SUMMARY",
          "ts": "...",
          ...
        }
    """
    # Prefer explicit "event" field; fall back to "kind" for older shapes.
    kind = obj.get("event") or obj.get("kind")
    payload = obj.get("payload") or obj

    logging.debug("Processing TELEMETRY_JSON event=%s payload=%s", kind, payload)

    if kind is None:
        logging.debug(
            "TELEMETRY_JSON without event/kind; storing as generic miner_event: %s", obj
        )
        handle_miner_side_event(cur, "UNKNOWN", payload)
        return

    if kind == "EPISODE_START":
        handle_episode_start(cur, payload)
    elif kind == "EPISODE_END":
        handle_episode_end(cur, payload)
    elif kind == "WINDOW_SUMMARY":
        handle_window_summary(cur, payload)
    elif kind in {"RESET_EVENT", "STUCK_EVENT"} or str(kind).startswith("WATCHDOG_"):
        handle_miner_side_event(cur, str(kind), payload)
    else:
        logging.debug(
            "Unhandled TELEMETRY_JSON event=%s; storing as miner_event", kind
        )
        handle_miner_side_event(cur, str(kind), payload)


def run_telemetry_collector(cfg: TelemetryCollectorConfig) -> None:
    """
    Run the telemetry collector until interrupted.

    Steps:
    - Connect to Postgres.
    - Run idempotent DDL to ensure schema/tables/indexes exist.
    - Tail the mining_helper.log file.
    - For each TELEMETRY_JSON line, route it to handlers.
    - Commit after each event (simple & safe – can batch later if desired).
    """
    logging.info("Connecting to Postgres: %s", cfg.db_dsn)
    conn = psycopg.connect(cfg.db_dsn)

    try:
        # First: ensure schema & tables exist with autocommit on.
        conn.autocommit = True
        with conn.cursor() as cur:
            ensure_schema(cur)

        # Then switch to manual commit mode for event ingestion.
        conn.autocommit = False
        with conn.cursor() as cur:
            for line in tail_log(cfg.log_path):
                obj = extract_telemetry_json(line)
                if obj is None:
                    continue

                logging.debug("RAW TELEMETRY_JSON: %s", obj)

                try:
                    process_telemetry_event(cur, obj)
                    conn.commit()
                except Exception:
                    logging.exception("Error processing TELEMETRY_JSON event")
                    conn.rollback()
                    # Continue; collector should not die on a single bad event.
    finally:
        conn.close()


def main() -> None:
    """
    Entry point when running as a script:

        uv run -m minecraft_auto_miner.telemetry.telemetry_collector

    Uses:
      - .env (if python-dotenv installed)
      - MAM_TELEMETRY_DB / MAM_TELEMETRY_USER / ... / MAM_MINING_LOG_PATH
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)-7s | telemetry_collector | %(message)s",
    )

    cfg = load_config_from_env()
    logging.info("Starting TelemetryCollector with config: %s", cfg)
    run_telemetry_collector(cfg)


# ---------------------------------------------------------------------------
# Backwards-compat shims for older app.py integrations
# ---------------------------------------------------------------------------


class TelemetryDBConfig(TelemetryCollectorConfig):
    """
    Backwards-compat alias for older app.py code that imported
    TelemetryDBConfig from this module.

    New code should use TelemetryCollectorConfig instead.
    """
    pass


class TelemetryCollector:
    """
    Backwards-compat no-op TelemetryCollector.

    Older versions of app.py may try to construct a TelemetryCollector and call
    methods like .start() / .stop() / .join().

    In the new architecture, telemetry is handled by running this module as an
    external process:

        uv run -m minecraft_auto_miner.telemetry.telemetry_collector

    So this class is intentionally a no-op; it only exists to keep app.py
    imports and calls from crashing.
    """

    def __init__(self, *args, **kwargs):
        logging.warning(
            "Legacy TelemetryCollector stub created. "
            "Telemetry is now handled by the external telemetry_collector "
            "process; this in-process stub does nothing."
        )

    def start(self) -> None:
        # No-op: external process should be running separately.
        logging.debug("TelemetryCollector.start() called (no-op).")

    def stop(self) -> None:
        # No-op
        logging.debug("TelemetryCollector.stop() called (no-op).")

    def join(self, timeout: float | None = None) -> None:
        # No-op
        logging.debug("TelemetryCollector.join() called (no-op).")

    def run(self) -> None:
        # Some older patterns might call .run() directly; make it a no-op too.
        logging.debug("TelemetryCollector.run() called (no-op).")


if __name__ == "__main__":
    main()
