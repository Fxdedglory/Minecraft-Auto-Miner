"""
policy_bootstrap.py v0.6.0 – 2025-12-08
Generated with ChatGPT (GPT-5.1 Thinking) on 2025-12-08.

Bootstrap mining strategy policy from telemetry DB.

Purpose
-------
Read window-level rewards from Postgres:

  telemetry.decision_window
  telemetry.miner_event  (for EPISODE_END + profile_name)

Aggregate per profile_name and write:

  data/strategy_stats.json

so that StrategyManager can start with a prior that reflects
actual performance (blocks/min, mining ratio) rather than the
hard-coded initial JSON.

Sources
-------
- telemetry.decision_window:
    episode_id, start_ts, end_ts, reward, blocks_broken, mining_ratio

- telemetry.miner_event:
    event_type = 'EPISODE_END'
    payload_json ->> 'profile_name'

Notes
-----
- We learn purely from decision windows here.
- Reward = decision_window.reward (currently blocks_per_minute).
- total_duration_seconds = sum of (end_ts - start_ts) over all windows.
- total_blocks = sum(blocks_broken) over all windows.
- total_mining_ratio = sum(mining_ratio) over all windows.

This keeps the JSON layout consistent with StrategyManager:

  {
    "straight_lane_default": {
      "runs": 7,
      "total_duration_seconds": ...,
      "total_blocks": ...,
      "total_mining_ratio": ...
    },
    "straight_sprint": { ... },
    ...
  }
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any

from datetime import datetime


# DSN is built the same way as in app.py so docker-compose + .env work.
TELEMETRY_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'mam_user')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'postgres')}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'mam_telemetry')}"
)

STRATEGY_STATE_PATH = Path("data") / "strategy_stats.json"


@dataclass
class ProfileAggregate:
    profile_name: str
    runs: int = 0
    total_duration_seconds: float = 0.0
    total_blocks: int = 0
    total_mining_ratio: float = 0.0
    avg_reward: float = 0.0


def _load_existing_state(path: Path) -> Dict[str, Any]:
    """
    Best-effort load of existing strategy_stats.json so that
    profiles with no telemetry yet are preserved.

    If the file is missing or invalid JSON, returns {}.
    """
    if not path.exists():
        print(f"[policy_bootstrap] No existing strategy state at {path}; starting fresh.")
        return {}

    try:
        txt = path.read_text(encoding="utf-8")
        data = json.loads(txt)
        if not isinstance(data, dict):
            raise ValueError("strategy_stats.json is not a dict")
        print(f"[policy_bootstrap] Loaded existing strategy state from {path}.")
        return data
    except Exception as exc:
        print(f"[policy_bootstrap] Failed to read/parse {path}: {exc}")
        return {}


def _connect():
    """
    Connect to Postgres using psycopg3.
    """
    try:
        import psycopg  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover
        print(
            "[policy_bootstrap] psycopg is not installed. "
            "Install 'psycopg[binary]' in your env."
        )
        raise SystemExit(1) from exc

    print(f"[policy_bootstrap] Using DSN: {TELEMETRY_DSN}")
    try:
        conn = psycopg.connect(TELEMETRY_DSN)
        conn.autocommit = True
        return conn
    except Exception as exc:
        print(f"[policy_bootstrap] Failed to connect to telemetry DB: {exc}")
        raise SystemExit(1) from exc


def _fetch_profile_aggregates(conn) -> Dict[str, ProfileAggregate]:
    """
    Query telemetry.decision_window + telemetry.miner_event to compute
    per-profile aggregates.

    We join decision_window with EPISODE_END miner_event on (episode_id)
    and take profile_name from the JSON payload.
    """
    sql = """
        SELECT
            ev.payload_json->>'profile_name' AS profile_name,
            COUNT(DISTINCT dw.episode_id)   AS runs,
            COALESCE(
                SUM(EXTRACT(EPOCH FROM (dw.end_ts - dw.start_ts))),
                0
            ) AS total_duration_seconds,
            COALESCE(SUM(dw.blocks_broken), 0) AS total_blocks,
            COALESCE(SUM(dw.mining_ratio), 0)  AS total_mining_ratio,
            COALESCE(AVG(dw.reward), 0)        AS avg_reward
        FROM telemetry.decision_window dw
        JOIN telemetry.miner_event ev
          ON ev.episode_id = dw.episode_id
         AND ev.event_type = 'EPISODE_END'
        WHERE ev.payload_json->>'profile_name' IS NOT NULL
        GROUP BY ev.payload_json->>'profile_name'
        ORDER BY ev.payload_json->>'profile_name';
    """

    aggregates: Dict[str, ProfileAggregate] = {}

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    if not rows:
        print(
            "[policy_bootstrap] No joined decision_window + EPISODE_END rows found; "
            "nothing to learn yet."
        )
        return aggregates

    for row in rows:
        (
            profile_name,
            runs,
            total_duration_seconds,
            total_blocks,
            total_mining_ratio,
            avg_reward,
        ) = row

        if profile_name is None:
            continue

        agg = ProfileAggregate(
            profile_name=str(profile_name),
            runs=int(runs or 0),
            total_duration_seconds=float(total_duration_seconds or 0.0),
            total_blocks=int(total_blocks or 0),
            total_mining_ratio=float(total_mining_ratio or 0.0),
            avg_reward=float(avg_reward or 0.0),
        )
        aggregates[agg.profile_name] = agg

    print("[policy_bootstrap] Aggregated profiles from telemetry:")
    for name, agg in aggregates.items():
        print(
            f"  - {name}: runs={agg.runs}, "
            f"duration={agg.total_duration_seconds:.1f}s, "
            f"blocks={agg.total_blocks}, "
            f"total_mining_ratio={agg.total_mining_ratio:.3f}, "
            f"avg_reward(bpm)={agg.avg_reward:.2f}"
        )

    return aggregates


def _merge_into_state(
    existing: Dict[str, Any],
    aggregates: Dict[str, ProfileAggregate],
) -> Dict[str, Any]:
    """
    Merge telemetry-derived aggregates into the existing JSON state.

    For any profile_name present in aggregates, we overwrite the stats.
    Profiles with no telemetry yet are left as-is.
    """
    state = dict(existing)  # shallow copy is fine: values are simple dicts

    for name, agg in aggregates.items():
        state[name] = {
            "runs": agg.runs,
            "total_duration_seconds": agg.total_duration_seconds,
            "total_blocks": agg.total_blocks,
            "total_mining_ratio": agg.total_mining_ratio,
        }

    return state


def main() -> None:
    print("[policy_bootstrap] === Policy bootstrap from telemetry DB ===")
    print(f"[policy_bootstrap] Output path: {STRATEGY_STATE_PATH}")

    conn = _connect()
    try:
        aggregates = _fetch_profile_aggregates(conn)
    finally:
        conn.close()

    if not aggregates:
        print(
            "[policy_bootstrap] No telemetry-derived aggregates available yet. "
            "Existing strategy_stats.json (if any) is unchanged."
        )
        return

    existing = _load_existing_state(STRATEGY_STATE_PATH)
    merged = _merge_into_state(existing, aggregates)

    STRATEGY_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STRATEGY_STATE_PATH.write_text(
        json.dumps(merged, indent=2),
        encoding="utf-8",
    )

    print(
        f"[policy_bootstrap] Wrote updated strategy state to "
        f"{STRATEGY_STATE_PATH} at {datetime.now().isoformat()}."
    )


if __name__ == "__main__":
    main()
