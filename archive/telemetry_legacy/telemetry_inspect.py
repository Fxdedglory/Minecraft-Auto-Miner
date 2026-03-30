"""
telemetry_inspect.py v0.6.0 – 2025-12-08
Generated with ChatGPT (GPT-5.1 Thinking).

Streamlit dashboard for Minecraft Auto Miner telemetry:
- Episode timeline
- Per-profile average reward (blocks/min)
- Recent decision windows + events

Reward currently = decision_window.reward = blocks_per_minute per window.
"""

from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import streamlit as st

try:
    import psycopg  # type: ignore[import-untyped]
except ImportError as exc:  # pragma: no cover
    st.error(
        "psycopg is not installed in this environment. "
        "Run `uv sync` to install dependencies."
    )
    raise

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


# ---------------------------------------------------------------------------
# DSN construction (matches app / collector)
# ---------------------------------------------------------------------------


def build_telemetry_dsn() -> str:
    """
    Build a Postgres DSN from environment variables.

    Uses the same env vars as docker-compose:
      POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
    """
    if load_dotenv is not None:
        # Load .env from project root if present
        load_dotenv()

    user = os.getenv("POSTGRES_USER", "mam_user")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "mam_telemetry")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def get_connection(dsn: str):
    return psycopg.connect(dsn)


def fetch_one(conn, sql: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        if row is None:
            return None
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))


def fetch_all(conn, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


def get_episode_summary(conn) -> Dict[str, Any]:
    sql = """
        SELECT
            COUNT(*) AS episode_count,
            MIN(started_at) AS first_episode,
            MAX(started_at) AS last_episode
        FROM telemetry.episode;
    """
    row = fetch_one(conn, sql) or {}
    return {
        "episode_count": row.get("episode_count", 0),
        "first_episode": row.get("first_episode"),
        "last_episode": row.get("last_episode"),
    }


def get_profile_stats(conn) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            COALESCE(e.profile_name, '<none>') AS profile_name,
            COUNT(DISTINCT e.episode_id)      AS episode_count,
            COALESCE(AVG(d.reward), 0)        AS avg_reward_bpm,
            COALESCE(AVG(d.blocks_broken), 0) AS avg_blocks_broken,
            COALESCE(AVG(d.mining_ratio), 0)  AS avg_mining_ratio
        FROM telemetry.episode e
        LEFT JOIN telemetry.decision_window d
               ON d.episode_id = e.episode_id
        GROUP BY COALESCE(e.profile_name, '<none>')
        ORDER BY avg_reward_bpm DESC;
    """
    return fetch_all(conn, sql)


def get_recent_windows(conn, limit: int = 200) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            d.window_id,
            d.episode_id,
            e.profile_name,
            d.start_ts,
            d.end_ts,
            d.state_code,
            d.action_code,
            d.reward,
            d.blocks_broken,
            d.mining_ratio
        FROM telemetry.decision_window d
        JOIN telemetry.episode e
          ON e.episode_id = d.episode_id
        ORDER BY d.window_id DESC
        LIMIT %s;
    """
    return fetch_all(conn, sql, (limit,))


def get_recent_events(conn, limit: int = 200) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            event_id,
            episode_id,
            ts,
            event_type,
            payload_json
        FROM telemetry.miner_event
        ORDER BY event_id DESC
        LIMIT %s;
    """
    return fetch_all(conn, sql, (limit,))


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------


def fmt_dt(dt: Any) -> str:
    if dt is None:
        return "-"
    if isinstance(dt, datetime):
        return dt.isoformat(sep=" ", timespec="seconds")
    return str(dt)


# ---------------------------------------------------------------------------
# Streamlit app
# ---------------------------------------------------------------------------


def main() -> None:
    st.set_page_config(
        page_title="Minecraft Auto Miner – Telemetry Dashboard",
        layout="wide",
    )

    st.title("🪓 Minecraft Auto Miner – Telemetry Dashboard")
    st.caption(
        "Episodes, decision windows, rewards and events from the telemetry DB.\n"
        "Reward = `decision_window.reward` (blocks/min per window)."
    )

    dsn = build_telemetry_dsn()
    with st.sidebar:
        st.subheader("Database connection")
        st.code(dsn, language="bash")
        st.write("Env vars: POSTGRES_USER / PASSWORD / HOST / PORT / DB")

    try:
        conn = get_connection(dsn)
    except Exception as exc:
        st.error(f"Failed to connect to telemetry DB:\n\n{exc}")
        return

    with conn:
        summary = get_episode_summary(conn)

        # Top-level metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Episodes", summary["episode_count"])
        col2.metric(
            "First episode",
            fmt_dt(summary["first_episode"]),
        )
        col3.metric(
            "Last episode",
            fmt_dt(summary["last_episode"]),
        )

        if summary["episode_count"] == 0:
            st.info("No episodes found in telemetry. Run the miner for a bit first.")
            return

        profile_stats = get_profile_stats(conn)

        st.markdown("### 📊 Per-profile performance")
        if not profile_stats:
            st.info("No decision_window rows yet – telemetry still warming up.")
        else:
            st.dataframe(
                profile_stats,
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("### 🧩 Recent decision windows")
        limit_windows = st.slider(
            "Number of recent windows to show",
            min_value=20,
            max_value=500,
            value=200,
            step=20,
        )
        windows = get_recent_windows(conn, limit_windows)
        if windows:
            st.dataframe(
                windows,
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No decision_window rows yet.")

        st.markdown("### 🧠 Recent miner events (WATCHDOG / STUCK / RESET / EPISODE_END)")
        limit_events = st.slider(
            "Number of recent events to show",
            min_value=20,
            max_value=500,
            value=200,
            step=20,
            key="events_slider",
        )
        events = get_recent_events(conn, limit_events)

        # Lightly prettify payload_json
        for ev in events:
            payload = ev.get("payload_json")
            if isinstance(payload, dict):
                ev["payload_json"] = json.dumps(payload, indent=2, default=str)

        if events:
            st.dataframe(
                events,
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No miner_event rows yet.")

    st.caption("telemetry_inspect.py v0.6.0 – reward = blocks/min per decision window.")


if __name__ == "__main__":
    main()
