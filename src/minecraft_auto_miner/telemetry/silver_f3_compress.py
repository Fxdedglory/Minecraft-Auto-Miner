"""
silver_f3_compress.py v0.2.0 – 2025-12-09
Compress Forge F3 Bronze ticks into a Silver downsampled stream.

New behavior:
- 1 row per *second* in `silver.f3_tick_ds`.
- Aggregates all Bronze ticks in that second into:
    * Representative pose from the earliest tick in that second.
    * tick_count
    * raw_json_array (jsonb array of all raw_json in that second)

Notes:
- Column `bucket_100ms` now represents a 1-second bucket:
    bucket_100ms = floor(epoch(ts_utc))::bigint
- This keeps episodes_from_silver.py working without changes.
"""

import sys
import os

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

# Load .env from project root or any parent directory
load_dotenv()


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


def ensure_silver_schema_and_table(cur: DictCursor) -> None:
    """
    Create silver schema + compressed F3 table + indexes (idempotent).

    Important:
    - bucket_100ms is now a 1-second bucket:
        floor(extract(epoch FROM ts_utc))::bigint
    """
    ddl = """
    -- Schemas
    CREATE SCHEMA IF NOT EXISTS silver;

    -- Compressed F3 ticks – 1 row per second
    CREATE TABLE IF NOT EXISTS silver.f3_tick_ds (
        bucket_100ms    BIGINT PRIMARY KEY,        -- actually 1-second bucket
        f3_tick_id      BIGINT       NOT NULL,     -- representative raw tick
        ts_utc          TIMESTAMPTZ  NOT NULL,     -- ts of representative tick

        x               DOUBLE PRECISION NOT NULL,
        y               DOUBLE PRECISION NOT NULL,
        z               DOUBLE PRECISION NOT NULL,
        block_x         INTEGER        NOT NULL,
        block_y         INTEGER        NOT NULL,
        block_z         INTEGER        NOT NULL,

        yaw             DOUBLE PRECISION NOT NULL,
        pitch           DOUBLE PRECISION NOT NULL,
        look_block      TEXT,
        raw_json        JSONB          NOT NULL,

        tick_count      INTEGER        NOT NULL,
        raw_json_array  JSONB          NOT NULL
    );

    -- Backward-compatible ALTERs if table already existed
    ALTER TABLE silver.f3_tick_ds
        ADD COLUMN IF NOT EXISTS tick_count     INTEGER,
        ADD COLUMN IF NOT EXISTS raw_json_array JSONB;

    CREATE INDEX IF NOT EXISTS idx_f3_tick_ds_ts
        ON silver.f3_tick_ds (ts_utc);

    CREATE INDEX IF NOT EXISTS idx_f3_tick_ds_block_xyz
        ON silver.f3_tick_ds (block_x, block_y, block_z);
    """
    cur.execute(ddl)


def compress_bronze_to_silver(cur: DictCursor) -> int:
    """
    Compress bronze.f3_tick_raw into silver.f3_tick_ds.

    - 1-second buckets:
        bucket_100ms = floor(extract(epoch FROM ts_utc))::bigint
    - For each bucket:
        * Representative tick = earliest f3_tick_id in that second.
        * tick_count = number of raw ticks in that second.
        * raw_json_array = jsonb array of all raw_json ordered by f3_tick_id.
    - Only reads bronze rows newer than the latest bucket already present in Silver.
    - Returns number of rows inserted.
    """
    sql = """
    WITH last_silver AS (
        SELECT COALESCE(MAX(bucket_100ms), -1) AS last_bucket
        FROM silver.f3_tick_ds
    ),
    bucketed AS (
        SELECT
            floor(extract(epoch FROM b.ts_utc))::bigint AS bucket_100ms,
            b.f3_tick_id,
            b.ts_utc,
            b.x,
            b.y,
            b.z,
            COALESCE(floor(b.look_x)::int, floor(b.x)::int) AS block_x,
            COALESCE(floor(b.look_y)::int, floor(b.y)::int) AS block_y,
            COALESCE(floor(b.look_z)::int, floor(b.z)::int) AS block_z,
            b.yaw,
            b.pitch,
            b.look_block,
            b.raw_json
        FROM bronze.f3_tick_raw b
        CROSS JOIN last_silver ls
        WHERE floor(extract(epoch FROM b.ts_utc))::bigint > ls.last_bucket
    ),
    agg AS (
        SELECT
            bucket_100ms,

            -- Representative tick: earliest f3_tick_id in that second
            (array_agg(f3_tick_id ORDER BY f3_tick_id))[1]      AS f3_tick_id,
            (array_agg(ts_utc ORDER BY f3_tick_id))[1]          AS ts_utc,
            (array_agg(x ORDER BY f3_tick_id))[1]               AS x,
            (array_agg(y ORDER BY f3_tick_id))[1]               AS y,
            (array_agg(z ORDER BY f3_tick_id))[1]               AS z,
            (array_agg(block_x ORDER BY f3_tick_id))[1]         AS block_x,
            (array_agg(block_y ORDER BY f3_tick_id))[1]         AS block_y,
            (array_agg(block_z ORDER BY f3_tick_id))[1]         AS block_z,
            (array_agg(yaw ORDER BY f3_tick_id))[1]             AS yaw,
            (array_agg(pitch ORDER BY f3_tick_id))[1]           AS pitch,
            (array_agg(look_block ORDER BY f3_tick_id))[1]      AS look_block,
            (array_agg(raw_json ORDER BY f3_tick_id))[1]        AS raw_json,

            COUNT(*)                                           AS tick_count,
            jsonb_agg(raw_json ORDER BY f3_tick_id)            AS raw_json_array
        FROM bucketed
        GROUP BY bucket_100ms
    ),
    new_buckets AS (
        SELECT a.*
        FROM agg a
        LEFT JOIN silver.f3_tick_ds d
               ON d.bucket_100ms = a.bucket_100ms
        WHERE d.bucket_100ms IS NULL
    )
    INSERT INTO silver.f3_tick_ds (
        bucket_100ms,
        f3_tick_id,
        ts_utc,
        x, y, z,
        block_x, block_y, block_z,
        yaw, pitch,
        look_block,
        raw_json,
        tick_count,
        raw_json_array
    )
    SELECT
        bucket_100ms,
        f3_tick_id,
        ts_utc,
        x, y, z,
        block_x, block_y, block_z,
        yaw, pitch,
        look_block,
        raw_json,
        tick_count,
        raw_json_array
    FROM new_buckets
    ON CONFLICT (bucket_100ms) DO NOTHING;
    """
    cur.execute(sql)
    return cur.rowcount  # number of rows actually inserted


def main():
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            print("[silver] Ensuring schema + table...")
            ensure_silver_schema_and_table(cur)

            print("[silver] Compressing Bronze -> Silver (1 sec buckets)...")
            inserted = compress_bronze_to_silver(cur)

        conn.commit()
        print(f"[silver] Done. Inserted {inserted} new compressed ticks.")
    except Exception as e:
        print(f"[silver] ERROR: {e}", file=sys.stderr)
        if "conn" in locals():
            conn.rollback()
        raise
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
