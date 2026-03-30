"""
episodes_from_silver.py v0.2.0 – 2025-12-10
Generated with ChatGPT (GPT-5.1 Thinking) on 2025-12-10.

Purpose:
- Take compressed F3 ticks from silver.f3_tick_ds
- Segment them into:
    * Episodes (continuous motion segments)
    * Decision windows (fixed-length windows within episodes)
- Write into:
    * silver.episode
    * silver.decision_window

Design:
- Uses 100ms buckets from silver.f3_tick_ds.bucket_100ms
- Episode break: large gap between consecutive buckets.
- Decision window: fixed N buckets within each episode.

v0.2.0:
- Adds FSM label population with better logic:
    * episodes / windows get state_name/action_name from silver.fsm_event_log
      based on majority-of-events in time range, tie-broken by recency.
    * Fallback: if no events inside range, use latest event before range.
- Adds per-window feature_json + reward_value computation using silver.f3_tick_ds:
    * progress_blocks (distance between first and last pose)
    * stone / red_wool / air counts and fractions
    * basic stuck flag / low-motion flag
    * simple scalar reward that rewards stone, penalises red wool + low motion.
"""

import sys
from typing import List, Dict, Any, Tuple, Optional
import os
import math
import json

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

# Load .env from project root or any parent directory
load_dotenv()

# -----------------------------
# Configuration (tunable knobs)
# -----------------------------

# Gap (in 100ms buckets) that ends an episode.
# e.g., 50 => 5 seconds with no ticks = episode boundary.
EPISODE_GAP_BUCKETS = int(os.getenv("MAM_EPISODE_GAP_BUCKETS", "50"))

# Decision window length (in 100ms buckets).
# e.g., 30 => 3 seconds per decision window.
DECISION_WINDOW_BUCKETS = int(os.getenv("MAM_DECISION_WINDOW_BUCKETS", "30"))

# Minimum buckets for an episode to be considered valid.
MIN_EPISODE_BUCKETS = int(os.getenv("MAM_MIN_EPISODE_BUCKETS", "5"))

# Reward weights (very simple bandit-style reward for now).
REWARD_W_PROGRESS = float(os.getenv("MAM_REWARD_W_PROGRESS", "1.0"))
REWARD_W_STONE = float(os.getenv("MAM_REWARD_W_STONE", "2.0"))
REWARD_W_RED_WOOL = float(os.getenv("MAM_REWARD_W_RED_WOOL", "5.0"))
REWARD_W_LOW_MOTION = float(os.getenv("MAM_REWARD_W_LOW_MOTION", "2.0"))

# Distance threshold below which a window is considered "low motion" / stuck-ish.
LOW_MOTION_PROGRESS_THRESHOLD = float(os.getenv("MAM_LOW_MOTION_THRESH", "0.5"))


# -----------------------------
# DB Connection
# -----------------------------


def get_conn():
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


# -----------------------------
# DDL – schemas & tables
# -----------------------------


def ensure_episode_schema(cur: DictCursor) -> None:
    """
    Create episode / decision_window tables in `silver` schema (idempotent),
    and ensure FSM-related tables/columns exist.
    """
    ddl = """
    CREATE SCHEMA IF NOT EXISTS silver;

    -- Episodes are continuous segments of F3 data from the compressed stream.
    CREATE TABLE IF NOT EXISTS silver.episode (
        episode_id           BIGSERIAL PRIMARY KEY,
        start_bucket_100ms   BIGINT       NOT NULL,
        end_bucket_100ms     BIGINT       NOT NULL,
        start_ts_utc         TIMESTAMPTZ  NOT NULL,
        end_ts_utc           TIMESTAMPTZ  NOT NULL,
        start_f3_tick_id     BIGINT       NOT NULL,
        end_f3_tick_id       BIGINT       NOT NULL,
        tick_count           INTEGER      NOT NULL,
        created_at_utc       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_episode_start_bucket
        ON silver.episode (start_bucket_100ms);

    CREATE INDEX IF NOT EXISTS idx_episode_end_bucket
        ON silver.episode (end_bucket_100ms);


    -- Decision windows slice each episode into fixed-size windows.
    CREATE TABLE IF NOT EXISTS silver.decision_window (
        decision_window_id   BIGSERIAL PRIMARY KEY,
        episode_id           BIGINT       NOT NULL
                             REFERENCES silver.episode(episode_id)
                             ON DELETE CASCADE,
        start_bucket_100ms   BIGINT       NOT NULL,
        end_bucket_100ms     BIGINT       NOT NULL,
        start_ts_utc         TIMESTAMPTZ  NOT NULL,
        end_ts_utc           TIMESTAMPTZ  NOT NULL,
        start_f3_tick_id     BIGINT       NOT NULL,
        end_f3_tick_id       BIGINT       NOT NULL,
        tick_count           INTEGER      NOT NULL,

        -- Placeholder for features / labels we compute later.
        feature_json         JSONB,
        reward_value         DOUBLE PRECISION,

        created_at_utc       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_decision_window_episode
        ON silver.decision_window (episode_id);

    CREATE INDEX IF NOT EXISTS idx_decision_window_start_bucket
        ON silver.decision_window (start_bucket_100ms);


    -- FSM scaffolding: one "dominant" state/action label per episode or window.
    ALTER TABLE silver.episode
        ADD COLUMN IF NOT EXISTS state_name  TEXT,
        ADD COLUMN IF NOT EXISTS action_name TEXT;

    ALTER TABLE silver.decision_window
        ADD COLUMN IF NOT EXISTS state_name  TEXT,
        ADD COLUMN IF NOT EXISTS action_name TEXT;


    -- FSM event log (also ensured by fsm_event_log.py; safe to repeat here).
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
    cur.execute(ddl)


# -----------------------------
# Episode segmentation logic
# -----------------------------


def fetch_new_silver_ticks(cur: DictCursor) -> List[Dict[str, Any]]:
    """
    Fetch all silver.f3_tick_ds rows that are newer than the last processed bucket.

    We consider the "last processed" bucket as the maximum end_bucket_100ms
    present in silver.episode. If no episodes exist, we start from the very
    beginning of silver.f3_tick_ds.

    Returns list of dict rows ordered by bucket_100ms.
    """
    # Find last processed bucket
    cur.execute(
        """
        SELECT COALESCE(MAX(end_bucket_100ms), -1) AS last_bucket
        FROM silver.episode
        """
    )
    row = cur.fetchone()
    last_bucket = row["last_bucket"] if row else -1

    # Fetch new compressed ticks
    cur.execute(
        """
        SELECT
            bucket_100ms,
            f3_tick_id,
            ts_utc,
            x, y, z,
            block_x, block_y, block_z,
            yaw, pitch,
            look_block,
            raw_json
        FROM silver.f3_tick_ds
        WHERE bucket_100ms > %s
        ORDER BY bucket_100ms ASC
        """,
        (last_bucket,),
    )
    rows = cur.fetchall()
    print(f"[episodes] Found {len(rows)} new silver ticks after bucket {last_bucket}")
    return [dict(r) for r in rows]


def segment_into_episodes(
    ticks: List[Dict[str, Any]],
    gap_buckets: int,
    min_episode_buckets: int,
) -> List[List[Dict[str, Any]]]:
    """
    Split ticks into episodes based on gaps in bucket_100ms.

    - New episode starts when gap between consecutive buckets > gap_buckets.
    - Very short segments (< min_episode_buckets buckets) are discarded.

    Returns list of episodes; each episode is a list of tick dicts.
    """
    episodes: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []

    prev_bucket: Optional[int] = None

    for t in ticks:
        bucket = t["bucket_100ms"]

        if prev_bucket is None:
            # First tick overall
            current = [t]
        else:
            gap = bucket - prev_bucket
            if gap > gap_buckets:
                # Close current episode
                if current:
                    bucket_span = current[-1]["bucket_100ms"] - current[0]["bucket_100ms"] + 1
                    if bucket_span >= min_episode_buckets:
                        episodes.append(current)
                # Start new episode
                current = [t]
            else:
                current.append(t)

        prev_bucket = bucket

    # Final episode
    if current:
        bucket_span = current[-1]["bucket_100ms"] - current[0]["bucket_100ms"] + 1
        if bucket_span >= min_episode_buckets:
            episodes.append(current)

    print(f"[episodes] Segmented into {len(episodes)} new episodes")
    return episodes


def create_episodes_in_db(
    cur: DictCursor,
    episodes: List[List[Dict[str, Any]]],
) -> List[Tuple[int, List[Dict[str, Any]]]]:
    """
    Insert new episodes into silver.episode and return a list of
    (episode_id, ticks_for_episode).
    """
    results: List[Tuple[int, List[Dict[str, Any]]]] = []

    for ep_ticks in episodes:
        first = ep_ticks[0]
        last = ep_ticks[-1]
        tick_count = len(ep_ticks)

        cur.execute(
            """
            INSERT INTO silver.episode (
                start_bucket_100ms,
                end_bucket_100ms,
                start_ts_utc,
                end_ts_utc,
                start_f3_tick_id,
                end_f3_tick_id,
                tick_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING episode_id
            """,
            (
                first["bucket_100ms"],
                last["bucket_100ms"],
                first["ts_utc"],
                last["ts_utc"],
                first["f3_tick_id"],
                last["f3_tick_id"],
                tick_count,
            ),
        )
        new_id = cur.fetchone()[0]
        results.append((new_id, ep_ticks))

    print(f"[episodes] Inserted {len(results)} rows into silver.episode")
    return results


# -----------------------------
# Decision window logic
# -----------------------------


def create_decision_windows_for_episode(
    cur: DictCursor,
    episode_id: int,
    ep_ticks: List[Dict[str, Any]],
    window_buckets: int,
) -> int:
    """
    Slice a single episode into fixed-size decision windows by bucket_100ms.

    Strategy:
    - Use bucket_100ms of the episode ticks.
    - Slide non-overlapping windows of length window_buckets.
    - If the last partial window is shorter than half the window size, drop it;
      otherwise keep it.

    Writes into silver.decision_window, returns number of rows inserted.
    """
    if not ep_ticks:
        return 0

    buckets = [t["bucket_100ms"] for t in ep_ticks]
    min_bucket = buckets[0]
    max_bucket = buckets[-1]

    windows_inserted = 0

    # Non-overlapping windows
    current_start = min_bucket
    while current_start <= max_bucket:
        current_end = current_start + window_buckets - 1

        # Collect ticks within this window
        window_ticks = [
            t
            for t in ep_ticks
            if current_start <= t["bucket_100ms"] <= current_end
        ]
        if not window_ticks:
            # No data in this window; move on
            current_start = current_end + 1
            continue

        actual_span = window_ticks[-1]["bucket_100ms"] - window_ticks[0]["bucket_100ms"] + 1

        # Drop small trailing window (optional rule)
        if (current_end >= max_bucket) and (actual_span < window_buckets / 2):
            break

        first = window_ticks[0]
        last = window_ticks[-1]

        cur.execute(
            """
            INSERT INTO silver.decision_window (
                episode_id,
                start_bucket_100ms,
                end_bucket_100ms,
                start_ts_utc,
                end_ts_utc,
                start_f3_tick_id,
                end_f3_tick_id,
                tick_count,
                feature_json,
                reward_value
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL)
            """,
            (
                episode_id,
                first["bucket_100ms"],
                last["bucket_100ms"],
                first["ts_utc"],
                last["ts_utc"],
                first["f3_tick_id"],
                last["f3_tick_id"],
                len(window_ticks),
            ),
        )
        windows_inserted += 1

        current_start = current_end + 1

    return windows_inserted


def create_decision_windows(
    cur: DictCursor,
    episode_rows: List[Tuple[int, List[Dict[str, Any]]]],
    window_buckets: int,
) -> int:
    """
    For each episode, generate decision windows.

    Returns total number of windows inserted.
    """
    total = 0
    for episode_id, ep_ticks in episode_rows:
        n = create_decision_windows_for_episode(cur, episode_id, ep_ticks, window_buckets)
        total += n
    print(f"[episodes] Inserted {total} rows into silver.decision_window")
    return total


# -----------------------------
# FSM label logic
# -----------------------------


def update_episode_and_window_labels(cur: DictCursor) -> None:
    """
    Use silver.fsm_event_log to fill state_name / action_name on:
      - silver.episode
      - silver.decision_window

    Strategy (episodes/windows separately):
    1) For rows with NULL labels, look for FSM events whose event_ts_utc lies
       within [start_ts_utc, end_ts_utc]. For each episode/window:
         - Count events by (state_name, action_name)
         - Choose the pair with highest count; tie-break by latest event_ts_utc.
    2) For any rows still NULL, fall back to "latest prior event":
         - event_ts_utc <= start_ts_utc
         - Pick the one with greatest event_ts_utc.
    """

    # Episodes: in-range majority + recency
    cur.execute(
        """
        WITH unlabeled AS (
            SELECT episode_id, start_ts_utc, end_ts_utc
            FROM silver.episode
            WHERE state_name IS NULL OR action_name IS NULL
        ),
        event_counts AS (
            SELECT
                u.episode_id,
                f.state_name,
                f.action_name,
                COUNT(*) AS evt_count,
                MAX(f.event_ts_utc) AS last_evt_ts
            FROM unlabeled u
            JOIN silver.fsm_event_log f
              ON f.event_ts_utc BETWEEN u.start_ts_utc AND u.end_ts_utc
            GROUP BY u.episode_id, f.state_name, f.action_name
        ),
        best AS (
            SELECT DISTINCT ON (episode_id)
                episode_id,
                state_name,
                action_name
            FROM event_counts
            ORDER BY episode_id, evt_count DESC, last_evt_ts DESC
        )
        UPDATE silver.episode e
        SET
            state_name  = b.state_name,
            action_name = b.action_name
        FROM best b
        WHERE e.episode_id = b.episode_id
          AND (e.state_name IS NULL OR e.action_name IS NULL);
        """
    )
    ep_updated_in_range = cur.rowcount
    print(f"[episodes] Updated {ep_updated_in_range} episode labels from in-range FSM events.")

    # Episodes: fallback to latest prior event if still NULL
    cur.execute(
        """
        WITH unlabeled AS (
            SELECT episode_id, start_ts_utc
            FROM silver.episode
            WHERE state_name IS NULL OR action_name IS NULL
        ),
        prior_events AS (
            SELECT
                u.episode_id,
                f.state_name,
                f.action_name,
                f.event_ts_utc,
                ROW_NUMBER() OVER (
                    PARTITION BY u.episode_id
                    ORDER BY f.event_ts_utc DESC
                ) AS rn
            FROM unlabeled u
            JOIN silver.fsm_event_log f
              ON f.event_ts_utc <= u.start_ts_utc
        ),
        best_prior AS (
            SELECT episode_id, state_name, action_name
            FROM prior_events
            WHERE rn = 1
        )
        UPDATE silver.episode e
        SET
            state_name  = b.state_name,
            action_name = b.action_name
        FROM best_prior b
        WHERE e.episode_id = b.episode_id
          AND (e.state_name IS NULL OR e.action_name IS NULL);
        """
    )
    ep_updated_prior = cur.rowcount
    print(f"[episodes] Updated {ep_updated_prior} episode labels from prior FSM events.")

    # Decision windows: in-range majority + recency
    cur.execute(
        """
        WITH unlabeled AS (
            SELECT decision_window_id, start_ts_utc, end_ts_utc
            FROM silver.decision_window
            WHERE state_name IS NULL OR action_name IS NULL
        ),
        event_counts AS (
            SELECT
                u.decision_window_id,
                f.state_name,
                f.action_name,
                COUNT(*) AS evt_count,
                MAX(f.event_ts_utc) AS last_evt_ts
            FROM unlabeled u
            JOIN silver.fsm_event_log f
              ON f.event_ts_utc BETWEEN u.start_ts_utc AND u.end_ts_utc
            GROUP BY u.decision_window_id, f.state_name, f.action_name
        ),
        best AS (
            SELECT DISTINCT ON (decision_window_id)
                decision_window_id,
                state_name,
                action_name
            FROM event_counts
            ORDER BY decision_window_id, evt_count DESC, last_evt_ts DESC
        )
        UPDATE silver.decision_window dw
        SET
            state_name  = b.state_name,
            action_name = b.action_name
        FROM best b
        WHERE dw.decision_window_id = b.decision_window_id
          AND (dw.state_name IS NULL OR dw.action_name IS NULL);
        """
    )
    dw_updated_in_range = cur.rowcount
    print(f"[episodes] Updated {dw_updated_in_range} decision window labels from in-range FSM events.")

    # Decision windows: fallback to latest prior event
    cur.execute(
        """
        WITH unlabeled AS (
            SELECT decision_window_id, start_ts_utc
            FROM silver.decision_window
            WHERE state_name IS NULL OR action_name IS NULL
        ),
        prior_events AS (
            SELECT
                u.decision_window_id,
                f.state_name,
                f.action_name,
                f.event_ts_utc,
                ROW_NUMBER() OVER (
                    PARTITION BY u.decision_window_id
                    ORDER BY f.event_ts_utc DESC
                ) AS rn
            FROM unlabeled u
            JOIN silver.fsm_event_log f
              ON f.event_ts_utc <= u.start_ts_utc
        ),
        best_prior AS (
            SELECT decision_window_id, state_name, action_name
            FROM prior_events
            WHERE rn = 1
        )
        UPDATE silver.decision_window dw
        SET
            state_name  = b.state_name,
            action_name = b.action_name
        FROM best_prior b
        WHERE dw.decision_window_id = b.decision_window_id
          AND (dw.state_name IS NULL OR dw.action_name IS NULL);
        """
    )
    dw_updated_prior = cur.rowcount
    print(f"[episodes] Updated {dw_updated_prior} decision window labels from prior FSM events.")


# -----------------------------
# Feature + reward computation
# -----------------------------


def _compute_progress_and_counts(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Helper: given a list of silver.f3_tick_ds rows for a single window,
    compute simple progress + block-type counts.
    """
    if not rows:
        return {
            "start_x": 0.0,
            "start_y": 0.0,
            "start_z": 0.0,
            "end_x": 0.0,
            "end_y": 0.0,
            "end_z": 0.0,
            "dx": 0.0,
            "dy": 0.0,
            "dz": 0.0,
            "avg_y": 0.0,
            "min_y": 0.0,
            "max_y": 0.0,
            "progress_blocks": 0.0,
            "horizontal_progress_blocks": 0.0,
            "duration_sec": 0.0,
            "avg_speed_bps": 0.0,
            "avg_horizontal_speed_bps": 0.0,
            "stone_hits": 0,
            "wool_hits": 0,
            "red_wool_hits": 0,
            "air_hits": 0,
            "other_hits": 0,
            "total_ticks": 0,
            "frac_stone": 0.0,
            "frac_wool": 0.0,
            "frac_red_wool": 0.0,
            "frac_air": 0.0,
        }

    # Sort by ts_utc to make sure we handle first/last correctly
    rows_sorted = sorted(rows, key=lambda r: r["ts_utc"])
    first = rows_sorted[0]
    last = rows_sorted[-1]

    dx = float(last["x"]) - float(first["x"])
    dy = float(last["y"]) - float(first["y"])
    dz = float(last["z"]) - float(first["z"])
    progress = math.sqrt(dx * dx + dy * dy + dz * dz)
    horizontal_progress = math.sqrt(dx * dx + dz * dz)

    try:
        duration_sec = max(
            0.0,
            (last["ts_utc"] - first["ts_utc"]).total_seconds(),
        )
    except Exception:
        duration_sec = max(0.0, (len(rows_sorted) - 1))

    avg_speed_bps = (progress / duration_sec) if duration_sec > 0 else 0.0
    avg_horizontal_speed_bps = (horizontal_progress / duration_sec) if duration_sec > 0 else 0.0
    y_values = [float(r["y"]) for r in rows_sorted]

    stone_hits = 0
    wool_hits = 0
    air_hits = 0
    other_hits = 0

    for r in rows_sorted:
        lb = r.get("look_block")
        if lb == "minecraft:stone":
            stone_hits += 1
        elif isinstance(lb, str) and lb.startswith("minecraft:") and lb.endswith("_wool"):
            wool_hits += 1
        elif lb == "minecraft:air":
            air_hits += 1
        else:
            other_hits += 1

    total_ticks = len(rows_sorted)
    if total_ticks > 0:
        frac_stone = stone_hits / total_ticks
        frac_wool = wool_hits / total_ticks
        frac_air = air_hits / total_ticks
    else:
        frac_stone = frac_wool = frac_air = 0.0

    return {
        "start_x": float(first["x"]),
        "start_y": float(first["y"]),
        "start_z": float(first["z"]),
        "end_x": float(last["x"]),
        "end_y": float(last["y"]),
        "end_z": float(last["z"]),
        "dx": dx,
        "dy": dy,
        "dz": dz,
        "avg_y": sum(y_values) / len(y_values),
        "min_y": min(y_values),
        "max_y": max(y_values),
        "progress_blocks": progress,
        "horizontal_progress_blocks": horizontal_progress,
        "duration_sec": duration_sec,
        "avg_speed_bps": avg_speed_bps,
        "avg_horizontal_speed_bps": avg_horizontal_speed_bps,
        "stone_hits": stone_hits,
        "wool_hits": wool_hits,
        "red_wool_hits": wool_hits,
        "air_hits": air_hits,
        "other_hits": other_hits,
        "total_ticks": total_ticks,
        "frac_stone": frac_stone,
        "frac_wool": frac_wool,
        "frac_red_wool": frac_wool,
        "frac_air": frac_air,
    }


def _compute_reward_from_features(feat: Dict[str, Any]) -> float:
    """
    Simple scalar reward:

        reward = w_progress * progress_blocks
               + w_stone    * frac_stone
               - w_red_wool * frac_red_wool
               - w_low      * I[progress_blocks < threshold]
    """
    progress = float(feat.get("progress_blocks", 0.0))
    frac_stone = float(feat.get("frac_stone", 0.0))
    frac_red = float(feat.get("frac_red_wool", 0.0))

    low_motion_flag = 1.0 if progress < LOW_MOTION_PROGRESS_THRESHOLD else 0.0

    reward = (
        REWARD_W_PROGRESS * progress
        + REWARD_W_STONE * frac_stone
        - REWARD_W_RED_WOOL * frac_red
        - REWARD_W_LOW_MOTION * low_motion_flag
    )
    return reward


def compute_window_features_and_rewards(
    cur: DictCursor,
    episode_ids: List[int],
) -> None:
    """
    For all decision windows belonging to the specified episodes and
    having NULL feature_json or reward_value, compute:

      - feature_json (progress + block-type fractions)
      - reward_value (scalar)

    Uses Python-side aggregation over silver.f3_tick_ds for clarity.
    This is not optimised for massive volumes but is fine for v0.1.
    """
    if not episode_ids:
        return

    # 1) Fetch windows that need computation
    cur.execute(
        """
        SELECT
            decision_window_id,
            episode_id,
            start_bucket_100ms,
            end_bucket_100ms
        FROM silver.decision_window
        WHERE episode_id = ANY(%s)
          AND (feature_json IS NULL OR reward_value IS NULL)
        """,
        (episode_ids,),
    )
    windows = cur.fetchall()
    if not windows:
        print("[episodes] No decision windows need feature/reward computation.")
        return

    # 2) For each window, pull its f3 ticks, compute features + reward, update row.
    updated = 0
    for w in windows:
        dw_id = w["decision_window_id"]
        b_start = w["start_bucket_100ms"]
        b_end = w["end_bucket_100ms"]

        # Fetch all compressed ticks in this bucket range
        cur.execute(
            """
            SELECT
                bucket_100ms,
                ts_utc,
                x, y, z,
                block_x, block_y, block_z,
                yaw, pitch,
                look_block
            FROM silver.f3_tick_ds
            WHERE bucket_100ms BETWEEN %s AND %s
            ORDER BY ts_utc ASC
            """,
            (b_start, b_end),
        )
        tick_rows = [dict(r) for r in cur.fetchall()]
        feat = _compute_progress_and_counts(tick_rows)
        reward = _compute_reward_from_features(feat)

        cur.execute(
            """
            UPDATE silver.decision_window
            SET
                feature_json = %s,
                reward_value = %s
            WHERE decision_window_id = %s
            """,
            (json.dumps(feat), reward, dw_id),
        )
        updated += 1

    print(f"[episodes] Computed features + reward for {updated} decision windows.")


# -----------------------------
# Main entrypoint
# -----------------------------


def main():
    print("[episodes] Starting episodes_from_silver.py")
    print(
        f"[episodes] Config: EPISODE_GAP_BUCKETS={EPISODE_GAP_BUCKETS}, "
        f"DECISION_WINDOW_BUCKETS={DECISION_WINDOW_BUCKETS}, "
        f"MIN_EPISODE_BUCKETS={MIN_EPISODE_BUCKETS}"
    )

    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # 1) Ensure DDL
            ensure_episode_schema(cur)

            # 2) Fetch new silver ticks
            ticks = fetch_new_silver_ticks(cur)
            if not ticks:
                print("[episodes] No new silver ticks to process. Nothing to do.")
                conn.commit()
                return

            # 3) Segment into episodes
            episodes = segment_into_episodes(
                ticks=ticks,
                gap_buckets=EPISODE_GAP_BUCKETS,
                min_episode_buckets=MIN_EPISODE_BUCKETS,
            )
            if not episodes:
                print("[episodes] No episodes found in new data. Nothing to insert.")
                conn.commit()
                return

            # 4) Insert episodes
            ep_rows = create_episodes_in_db(cur, episodes)
            new_episode_ids = [ep_id for ep_id, _ in ep_rows]

            # 5) Insert decision windows
            create_decision_windows(cur, ep_rows, DECISION_WINDOW_BUCKETS)

            # 6) Fill state_name / action_name from FSM event log
            update_episode_and_window_labels(cur)

            # 7) Compute features + reward for new windows
            compute_window_features_and_rewards(cur, new_episode_ids)

        conn.commit()
        print("[episodes] Done.")
    except Exception as e:
        print(f"[episodes] ERROR: {e}", file=sys.stderr)
        if "conn" in locals():
            conn.rollback()
        raise
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
