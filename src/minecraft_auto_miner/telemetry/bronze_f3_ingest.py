"""
bronze_f3_ingest.py v0.2.0 – 2025-12-09
Generated with ChatGPT (GPT-5.1 Thinking).

Purpose:
- Bronze layer ingest for Forge F3 telemetry.
- Idempotently:
  - Ensures `bronze` schema exists.
  - Ensures `bronze.f3_tick_raw` table exists.
- On each run:
  - Reads mam_f3_stream.log (Forge mod output).
  - Inserts only *new* F3_TICK records (ts_utc > max(ts_utc) in bronze).

This makes it safe to call repeatedly from:
- app.py telemetry loop (batch mode).
- CLI (manual one-shot ingest).
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, List, Dict

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Load .env from project root or parent
load_dotenv()


# ----------------------------------------------------------------------
#  PATHS
# ----------------------------------------------------------------------


def get_ingest_state_path() -> Path:
    project_root = Path(__file__).resolve().parents[3]
    return project_root / "data" / "bronze_ingest_state.json"


def get_forge_log_path() -> Path:
    """
    Resolve the Forge F3 telemetry log path.

    Matches what app.py is using:
        %APPDATA%\\.minecraft\\mam_telemetry\\mam_f3_stream.log
    """
    override = os.getenv("MAM_FORGE_LOG_PATH")
    if override:
        return Path(override)

    appdata = os.environ.get("APPDATA")
    if appdata:
        root = Path(appdata)
    else:
        # Fallback: standard Roaming path
        root = Path.home() / "AppData" / "Roaming"

    return root / ".minecraft" / "mam_telemetry" / "mam_f3_stream.log"


def load_ingest_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_ingest_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


# ----------------------------------------------------------------------
#  DB HELPERS + DDL (IDEMPOTENT)
# ----------------------------------------------------------------------


def get_pg_conn():
    """
    Open a Postgres connection using MAM_TELEMETRY_* / POSTGRES_* env vars.

    .env examples:
      MAM_TELEMETRY_DB=mam_telemetry
      MAM_TELEMETRY_USER=mam_user
      MAM_TELEMETRY_PASSWORD=postgres
      MAM_TELEMETRY_HOST=localhost
      MAM_TELEMETRY_PORT=5432
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


BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "f3_tick_raw"  # full name: bronze.f3_tick_raw

BRONZE_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA};

CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.{BRONZE_TABLE} (
    f3_tick_id      BIGSERIAL PRIMARY KEY,
    ts_utc          TIMESTAMPTZ NOT NULL,
    x               DOUBLE PRECISION NOT NULL,
    y               DOUBLE PRECISION NOT NULL,
    z               DOUBLE PRECISION NOT NULL,
    yaw             DOUBLE PRECISION NOT NULL,
    pitch           DOUBLE PRECISION NOT NULL,
    look_x          DOUBLE PRECISION,
    look_y          DOUBLE PRECISION,
    look_z          DOUBLE PRECISION,
    look_block      TEXT,
    raw_json        JSONB NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_{BRONZE_SCHEMA}_{BRONZE_TABLE}_ts
    ON {BRONZE_SCHEMA}.{BRONZE_TABLE}(ts_utc);

CREATE INDEX IF NOT EXISTS idx_{BRONZE_SCHEMA}_{BRONZE_TABLE}_look_block
    ON {BRONZE_SCHEMA}.{BRONZE_TABLE}(look_block);
"""


def ensure_bronze_objects() -> None:
    """
    Idempotently create the bronze schema + f3_tick_raw table + basic indexes.
    Safe to call on every startup.
    """
    conn = get_pg_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(BRONZE_DDL)
        print(f"[bronze] Ensured {BRONZE_SCHEMA}.{BRONZE_TABLE} exists.")
    finally:
        conn.close()


# ----------------------------------------------------------------------
#  DOMAIN MODEL
# ----------------------------------------------------------------------


@dataclass
class F3Tick:
    ts_utc: datetime
    x: float
    y: float
    z: float
    yaw: float
    pitch: float
    look_x: Optional[float]
    look_y: Optional[float]
    look_z: Optional[float]
    look_block: Optional[str]
    raw_json: Dict


def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _normalize_yaw(yaw: float) -> float:
    value = yaw
    while value <= -180.0:
        value += 360.0
    while value > 180.0:
        value -= 360.0
    return value


def parse_f3_json_line(line: str) -> Optional[F3Tick]:
    """
    Parse one line of Forge JSON into an F3Tick.
    Returns None if the line is not valid FORGE_F3 telemetry.
    """
    line = line.strip()
    if not line:
        return None

    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return None

    event_type = obj.get("type")
    if event_type == "FORGE_F3":
        ts_str = obj.get("ts_utc")
        if not ts_str:
            return None
        ts_utc = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        pose = obj.get("pose") or {}
        target = obj.get("target") or {}
    elif event_type == "F3_TICK":
        ts_str = obj.get("ts")
        if not ts_str:
            return None
        ts_utc = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        pose = obj
        target = {
            "block_id": obj.get("look_block"),
            "x": obj.get("look_x"),
            "y": obj.get("look_y"),
            "z": obj.get("look_z"),
        }
    else:
        return None

    return F3Tick(
        ts_utc=ts_utc,
        x=float(pose.get("x", 0.0)),
        y=float(pose.get("y", 0.0)),
        z=float(pose.get("z", 0.0)),
        yaw=_normalize_yaw(float(pose.get("yaw", 0.0))),
        pitch=float(pose.get("pitch", 0.0)),
        look_x=_safe_float(target.get("x")),
        look_y=_safe_float(target.get("y")),
        look_z=_safe_float(target.get("z")),
        look_block=target.get("block_id"),
        raw_json=obj,
    )


# ----------------------------------------------------------------------
#  BRONZE INGEST (ONE-SHOT)
# ----------------------------------------------------------------------


INSERT_SQL = f"""
INSERT INTO {BRONZE_SCHEMA}.{BRONZE_TABLE} (
    ts_utc,
    x, y, z,
    yaw, pitch,
    look_x, look_y, look_z,
    look_block,
    raw_json
)
VALUES (
    %(ts_utc)s,
    %(x)s, %(y)s, %(z)s,
    %(yaw)s, %(pitch)s,
    %(look_x)s, %(look_y)s, %(look_z)s,
    %(look_block)s,
    %(raw_json)s
);
"""


def _flush_batch(cur, batch: List[Dict]) -> None:
    if not batch:
        return
    print(f"[bronze] Inserting {len(batch)} rows into bronze…")
    psycopg2.extras.execute_batch(cur, INSERT_SQL, batch, page_size=len(batch))


def ingest_new_ticks_once(max_rows: Optional[int] = None) -> int:
    """
    One-shot ingest:
    - Follow only new appended bytes from mam_f3_stream.log.
    - Filter by ts_utc > max(ts_utc) as a safety backstop.
    - On first run, bootstrap from only the recent tail of the file instead of
      scanning the entire history.

    Returns number of rows inserted.
    """
    log_path = get_forge_log_path()
    state_path = get_ingest_state_path()
    print(f"[bronze] Using Forge F3 log: {log_path}")

    if not log_path.exists():
        print("[bronze] WARNING: F3 log does not exist yet.")
        return 0

    conn = get_pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # 1) Get last ingested timestamp
            cur.execute(f"SELECT max(ts_utc) AS last_ts FROM {BRONZE_SCHEMA}.{BRONZE_TABLE}")
            row = cur.fetchone()
            last_ts = row["last_ts"] if row and row["last_ts"] is not None else None

            if last_ts:
                print(f"[bronze] Last ts_utc in bronze: {last_ts.isoformat()}")
            else:
                print("[bronze] No existing rows in bronze; full log scan.")

            # 2) Read only new appended bytes from the Forge log
            state = load_ingest_state(state_path)
            current_size = log_path.stat().st_size
            bootstrap_bytes = int(os.getenv("MAM_BRONZE_BOOTSTRAP_BYTES", str(1024 * 1024)))

            offset = state.get("offset")
            if not isinstance(offset, int):
                offset = max(0, current_size - bootstrap_bytes)
            elif offset > current_size:
                offset = max(0, current_size - bootstrap_bytes)

            print(f"[bronze] Reading from byte offset {offset} of {current_size}.")

            batch: List[Dict] = []
            with log_path.open("r", encoding="utf-8") as f:
                if offset > 0:
                    f.seek(offset)
                    # If we start in the middle of a line, discard the partial prefix.
                    f.readline()
                end_offset = f.tell()
                while True:
                    line = f.readline()
                    if not line:
                        break
                    tick = parse_f3_json_line(line)
                    if tick is None:
                        end_offset = f.tell()
                        continue
                    if last_ts and tick.ts_utc <= last_ts:
                        end_offset = f.tell()
                        continue

                    batch.append(
                        {
                            "ts_utc": tick.ts_utc,
                            "x": tick.x,
                            "y": tick.y,
                            "z": tick.z,
                            "yaw": tick.yaw,
                            "pitch": tick.pitch,
                            "look_x": tick.look_x,
                            "look_y": tick.look_y,
                            "look_z": tick.look_z,
                            "look_block": tick.look_block,
                            "raw_json": json.dumps(tick.raw_json),
                        }
                    )

                    end_offset = f.tell()

                    if max_rows is not None and len(batch) >= max_rows:
                        break

            if not batch:
                save_ingest_state(
                    state_path,
                    {
                        "log_path": str(log_path),
                        "offset": end_offset,
                        "updated_at": datetime.utcnow().isoformat() + "Z",
                    },
                )
                print("[bronze] No new ticks to insert.")
                conn.rollback()
                return 0

            _flush_batch(cur, batch)
            conn.commit()
            save_ingest_state(
                state_path,
                {
                    "log_path": str(log_path),
                    "offset": end_offset,
                    "updated_at": datetime.utcnow().isoformat() + "Z",
                },
            )
            print(f"[bronze] Committed {len(batch)} new rows.")
            return len(batch)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ----------------------------------------------------------------------
#  MAIN (one-shot, suitable for app.py telemetry loop)
# ----------------------------------------------------------------------


def main(max_rows: Optional[int] = None) -> int:
    """
    Public entrypoint used by app.py and CLI.

    - Ensures schema/table/index.
    - Runs one-shot ingest of new ticks.
    """
    ensure_bronze_objects()
    return ingest_new_ticks_once(max_rows=max_rows)


if __name__ == "__main__":
    inserted = main()
    print(f"[bronze] Done. Inserted {inserted} new rows.")
