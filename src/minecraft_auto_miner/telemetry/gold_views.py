"""
gold_views.py v0.2.0 – 2025-12-09
Gold views for Minecraft Auto Miner Forge Telemetry.

- Ensures `gold` schema.
- Creates/updates views that aggregate Silver compressed ticks
  for dashboards (e.g., Streamlit).

Silver assumptions:
- silver.f3_tick_ds has 1 row per second:
    bucket_100ms  BIGINT  (1-second bucket key)
    ts_utc        TIMESTAMPTZ (representative tick)
    x, y, z, block_x, block_y, block_z, yaw, pitch, look_block, raw_json
    tick_count    INTEGER
    raw_json_array JSONB
"""

import os
import sys

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

# Load .env for MAM_TELEMETRY_* or POSTGRES_* vars
load_dotenv()


def get_conn():
    """
    Open a psycopg2 connection using the same convention as Bronze/Silver.

    Prefers MAM_TELEMETRY_*; falls back to POSTGRES_*; then defaults.
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


def ensure_gold_schema_and_views(cur: DictCursor) -> None:
    """
    Create gold schema + dashboard views (idempotent).

    Existing views:
      - gold.f3_pose_ds
      - gold.block_dwell_summary_ds
      - gold.tick_rate_per_minute_ds

    New spatial views for 3D cube:
      - gold.block_state_latest
      - gold.block_state_latest_with_meta
    """
    ddl = """
    CREATE SCHEMA IF NOT EXISTS gold;

    --------------------------------------------------------------------
    -- Pose-level view over compressed ticks
    -- Mirrors silver.f3_tick_ds so dashboards can see all fields.
    --------------------------------------------------------------------
    DROP VIEW IF EXISTS gold.f3_pose_ds CASCADE;

    CREATE VIEW gold.f3_pose_ds AS
    SELECT
        bucket_100ms,
        f3_tick_id,
        ts_utc,
        x, y, z,
        block_x, block_y, block_z,
        yaw, pitch,
        look_block,
        tick_count,
        raw_json,
        raw_json_array
    FROM silver.f3_tick_ds;



    --------------------------------------------------------------------
    -- Simple “dwell time” style aggregation per block
    --------------------------------------------------------------------
    CREATE OR REPLACE VIEW gold.block_dwell_summary_ds AS
    SELECT
        block_x,
        block_y,
        block_z,
        COUNT(*)                       AS tick_count,
        MIN(ts_utc)                    AS first_seen_utc,
        MAX(ts_utc)                    AS last_seen_utc
    FROM silver.f3_tick_ds
    GROUP BY block_x, block_y, block_z;

    --------------------------------------------------------------------
    -- Per-minute sample count (sanity checks)
    --------------------------------------------------------------------
    CREATE OR REPLACE VIEW gold.tick_rate_per_minute_ds AS
    SELECT
        date_trunc('minute', ts_utc) AS minute_utc,
        COUNT(*)                     AS tick_count
    FROM silver.f3_tick_ds
    GROUP BY date_trunc('minute', ts_utc)
    ORDER BY minute_utc;

    --------------------------------------------------------------------
    -- Spatial: latest known state for each block (by block_x/y/z)
    --
    -- NOTE:
    --  - This is derived from silver.f3_tick_ds (1-second buckets).
    --  - We treat the most recent row for each block as “current state”.
    --------------------------------------------------------------------
    CREATE OR REPLACE VIEW gold.block_state_latest AS
    WITH latest_per_block AS (
        SELECT
            block_x,
            block_y,
            block_z,
            MAX(ts_utc) AS latest_ts_utc
        FROM silver.f3_tick_ds
        GROUP BY block_x, block_y, block_z
    )
    SELECT
        l.block_x,
        l.block_y,
        l.block_z,
        l.latest_ts_utc,
        s.f3_tick_id,
        s.ts_utc,
        s.x,
        s.y,
        s.z,
        s.yaw,
        s.pitch,
        s.look_block,
        s.raw_json
    FROM latest_per_block l
    JOIN silver.f3_tick_ds s
      ON s.block_x = l.block_x
     AND s.block_y = l.block_y
     AND s.block_z = l.block_z
     AND s.ts_utc = l.latest_ts_utc;

    --------------------------------------------------------------------
    -- Spatial: latest block state with derived status label.
    --
    -- status:
    --   - 'EMPTY'   : look_block is 'minecraft:air'
    --   - 'BARRIER' : known barrier / glass / wool types (expand later)
    --   - 'SOLID'   : any other block id
    --   - 'UNKNOWN' : look_block is NULL
    --------------------------------------------------------------------
    CREATE OR REPLACE VIEW gold.block_state_latest_with_meta AS
    SELECT
        g.block_x,
        g.block_y,
        g.block_z,
        g.latest_ts_utc,
        g.f3_tick_id,
        g.ts_utc,
        g.x,
        g.y,
        g.z,
        g.yaw,
        g.pitch,
        g.look_block,
        CASE
            WHEN g.look_block IS NULL THEN 'UNKNOWN'
            WHEN g.look_block = 'minecraft:air' THEN 'EMPTY'
            WHEN g.look_block IN ('minecraft:barrier', 'minecraft:glass', 'minecraft:glass_pane')
                 OR g.look_block LIKE 'minecraft:%_wool' THEN 'BARRIER'
            ELSE 'SOLID'
        END AS status,
        -- Simple bucket key for time-slicing in dashboards (1s buckets)
        floor(extract(epoch FROM g.ts_utc))::bigint AS bucket_1s
    FROM gold.block_state_latest g;
    """
    cur.execute(ddl)



def main():
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            print("[gold] Ensuring schema + views...")
            ensure_gold_schema_and_views(cur)

        conn.commit()
        print("[gold] Done. Gold views are ready.")
    except Exception as e:
        print(f"[gold] ERROR: {e}", file=sys.stderr)
        if "conn" in locals():
            conn.rollback()
        raise
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
