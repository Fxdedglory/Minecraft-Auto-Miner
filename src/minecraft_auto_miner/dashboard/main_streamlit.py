"""
main_streamlit.py v0.4.0 – 2025-12-10
Minecraft Auto Miner – Single-Page Telemetry HUD

Purpose
-------
Single-page, live HUD over the telemetry pipeline:

- Live Pose HUD:
    * Latest compressed tick from gold.f3_pose_ds.
- Episodes & Decision Windows:
    * silver.episode
    * silver.decision_window
- 3D Space:
    * gold.block_state_latest_with_meta around the end of a selected episode.

This dashboard shows telemetry and can queue runtime commands for the live miner.

Layout (v0.4.0)
---------------
Single page, top-to-bottom:
1. Live Pose HUD (auto-refresh)
2. Episodes & Decision Windows
3. 3D Space voxel view

Raw Bronze has been removed to keep the page focused.
"""

from __future__ import annotations

import hashlib
import os
import json
import math
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor
import streamlit as st
from dotenv import load_dotenv
import plotly.graph_objects as go

# Optional auto-refresh component; if not installed, st_autorefresh will be None.
try:
    from streamlit_autorefresh import st_autorefresh  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    st_autorefresh = None

# ---------------------------------------------------------------------
# Env + DB helpers
# ---------------------------------------------------------------------

# Load .env from project root (uv usually runs from project root)
load_dotenv()


def get_project_data_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "data"


def get_dashboard_command_path() -> Path:
    return get_project_data_dir() / "dashboard_control_command.json"


def get_dashboard_status_path() -> Path:
    return get_project_data_dir() / "dashboard_control_status.json"


def get_dashboard_runtime_status_path() -> Path:
    return get_project_data_dir() / "dashboard_runtime_status.json"


def get_perimeter_scout_report_path() -> Path:
    return get_project_data_dir() / "perimeter_scout_last_run.json"


def get_perimeter_scout_memory_path() -> Path:
    return get_project_data_dir() / "perimeter_scout_memory.json"


def get_voxel_world_memory_path() -> Path:
    return get_project_data_dir() / "voxel_world_memory.json"


def load_json_file(path: Path) -> Optional[dict[str, Any]]:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2)
    last_exc: Optional[Exception] = None
    for attempt in range(8):
        temp_path = Path(f"{path}.{attempt}.tmp")
        try:
            temp_path.write_text(text, encoding="utf-8")
            temp_path.replace(path)
            return
        except PermissionError as exc:
            last_exc = exc
            try:
                path.write_text(text, encoding="utf-8")
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                return
            except PermissionError as inner_exc:
                last_exc = inner_exc
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass
        time.sleep(0.03 * (attempt + 1))
    raise RuntimeError(f"Failed to write dashboard control file {path}: {last_exc}")


def parse_iso_datetime(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _scout_report_matches_region(
    report: Optional[dict[str, Any]],
    region_snapshot: Optional[dict[str, Any]],
) -> bool:
    if report is None:
        return False
    if region_snapshot is None:
        return True
    configured_region = report.get("configured_region")
    if not isinstance(configured_region, dict):
        return True
    region_name = str(configured_region.get("name") or "").strip()
    return not region_name or region_name == str(region_snapshot.get("name") or "")


def load_latest_scout_visual_report(region_snapshot: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    report = load_json_file(get_perimeter_scout_report_path())
    if _scout_report_matches_region(report, region_snapshot):
        return report

    memory = load_json_file(get_perimeter_scout_memory_path())
    if memory is None or region_snapshot is None:
        return None
    regions = memory.get("regions")
    if not isinstance(regions, dict):
        return None
    entry = regions.get(str(region_snapshot.get("name") or ""))
    if not isinstance(entry, dict):
        return None
    template = entry.get("report_template")
    if not isinstance(template, dict):
        return None
    if not _scout_report_matches_region(template, region_snapshot):
        return None
    return template


def _iter_grid_line_cells(
    start_x: float,
    start_z: float,
    end_x: float,
    end_z: float,
) -> list[tuple[int, int]]:
    dx = end_x - start_x
    dz = end_z - start_z
    steps = max(1, int(math.ceil(max(abs(dx), abs(dz)))))
    cells: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for step in range(steps + 1):
        ratio = step / steps
        cell = (
            int(round(start_x + dx * ratio)),
            int(round(start_z + dz * ratio)),
        )
        if cell in seen:
            continue
        seen.add(cell)
        cells.append(cell)
    return cells


def get_block_display_color(block_id: str, block_class: str) -> str:
    bid = str(block_id or "").strip().lower()
    if block_class == "mineable":
        return "#4f88ff"
    overrides = {
        "minecraft:air": "#8ecdf5",
        "minecraft:stone": "#4f88ff",
        "minecraft:red_wool": "#c83737",
        "minecraft:granite": "#b7856e",
        "minecraft:dripstone_block": "#8b6a3f",
        "minecraft:tuff": "#74706a",
        "minecraft:polished_deepslate": "#59606a",
        "minecraft:polished_deepslate_stairs": "#59606a",
        "minecraft:light_gray_concrete_powder": "#cfcfd1",
        "minecraft:gray_concrete_powder": "#87888d",
        "minecraft:chiseled_stone_bricks": "#9a9b9f",
        "minecraft:nether_wart_block": "#7a2431",
        "minecraft:nether_bricks": "#4b3138",
        "minecraft:nether_brick_stairs": "#4b3138",
        "minecraft:clay": "#9aa4b3",
        "minecraft:ancient_debris": "#5f4740",
        "minecraft:brown_terracotta": "#8d5a3d",
        "minecraft:dark_oak_planks": "#4e3827",
        "minecraft:dark_oak_stairs": "#4e3827",
        "minecraft:polished_basalt": "#65656b",
        "minecraft:stone_brick_stairs": "#8f9094",
    }
    if bid in overrides:
        return overrides[bid]
    digest = hashlib.md5(bid.encode("utf-8")).hexdigest()
    if block_class == "air":
        return "#8ecdf5"
    if block_class == "mineable":
        return f"#{digest[0:2]}{digest[4:6]}{digest[8:10]}"
    if block_class == "non_mineable":
        return f"#{digest[2:4]}{digest[6:8]}{digest[10:12]}"
    return f"#{digest[12:18]}"


def queue_dashboard_command(command: str, delay_sec: float = 2.0) -> dict[str, Any]:
    execute_after = datetime.now(timezone.utc) + timedelta(seconds=max(0.0, delay_sec))
    payload = {
        "command_id": uuid.uuid4().hex,
        "command": command,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "execute_after_utc": execute_after.isoformat(),
    }
    write_json_atomic(get_dashboard_command_path(), payload)
    return payload


def try_queue_dashboard_command(command: str, delay_sec: float = 2.0) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    try:
        return queue_dashboard_command(command, delay_sec=delay_sec), None
    except Exception as exc:
        return None, str(exc)


def _get_db_params() -> Tuple[str, str, str, str, int]:
    """
    Build a Postgres connection config using .env variables.

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
    return db_name, user, password, host, port


def get_conn() -> psycopg2.extensions.connection:
    db_name, user, password, host, port = _get_db_params()
    conn = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    conn.autocommit = False
    return conn


def run_query(sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
    """
    Run a SELECT and return a DataFrame.
    Any errors are surfaced in the UI via st.error().
    """
    try:
        conn = get_conn()
    except Exception as e:
        st.error(f"DB connection failed: {e}")
        return pd.DataFrame()

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows, columns=[c.name for c in cur.description])
        conn.commit()
        return df
    except Exception as e:
        conn.rollback()
        st.error(f"Query failed: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


# ---------------------------------------------------------------------
# Time + formatting helpers
# ---------------------------------------------------------------------


def get_time_range(option: str, now_utc: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """
    Convert a UI option into a [start, end] UTC range.
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    if option == "Last 1 minute":
        start = now_utc - timedelta(minutes=1)
    elif option == "Last 5 minutes":
        start = now_utc - timedelta(minutes=5)
    elif option == "Last 15 minutes":
        start = now_utc - timedelta(minutes=15)
    else:
        # Fallback – last 5 minutes
        start = now_utc - timedelta(minutes=5)

    return start, now_utc


def format_ts_for_metric(v) -> str:
    """
    Streamlit st.metric only likes str / number.
    Convert pandas.Timestamp or datetime to iso-string.
    """
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)


def get_forge_log_path() -> Path:
    override = os.getenv("MAM_FORGE_LOG_PATH")
    if override:
        return Path(override)
    appdata = os.getenv("APPDATA")
    if appdata:
        return Path(appdata) / ".minecraft" / "mam_telemetry" / "mam_f3_stream.log"
    return Path.home() / "AppData" / "Roaming" / ".minecraft" / "mam_telemetry" / "mam_f3_stream.log"


def get_learned_blocking_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    return root / "data" / "learned_blocking_block_ids.json"


def load_learned_blocking_snapshot() -> dict[str, Any]:
    path = get_learned_blocking_path()
    if not path.exists():
        return {"version": "1.0", "regions": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": "1.0", "regions": {}}


def load_region_snapshot() -> Optional[dict[str, Any]]:
    root = Path(__file__).resolve().parents[3]
    path = root / "data" / "mine_bounds.json"
    if not path.exists():
        return None

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        st.warning(f"Failed to load mine bounds for voxel view: {exc}")
        return None

    regions = raw.get("regions") or []
    if not regions:
        return None

    region = regions[0]
    min_obj = region.get("min") or {}
    max_obj = region.get("max") or {}
    configured_blocking = {str(v).lower() for v in region.get("blocking_block_ids", [])}
    learned_snapshot = load_learned_blocking_snapshot()
    learned_region = (learned_snapshot.get("regions") or {}).get(str(region.get("name", "region0"))) or {}
    learned_blocking = {
        str(v).lower() for v in learned_region.get("learned_blocking_block_ids", [])
    }
    return {
        "name": str(region.get("name", "region0")),
        "min_x": int(min_obj.get("x", 0)),
        "min_y": int(min_obj.get("y", 0)),
        "min_z": int(min_obj.get("z", 0)),
        "max_x": int(max_obj.get("x", 0)),
        "max_y": int(max_obj.get("y", 0)),
        "max_z": int(max_obj.get("z", 0)),
        "allowed_block_ids": {str(v).lower() for v in region.get("allowed_block_ids", [])},
        "configured_blocking_block_ids": configured_blocking,
        "learned_blocking_block_ids": learned_blocking,
        "blocking_block_ids": configured_blocking | learned_blocking,
    }


def classify_voxel_block(block_id: object, allowed_block_ids: set[str]) -> str:
    bid = str(block_id or "").strip().lower()
    if not bid:
        return "unknown"
    if bid == "minecraft:air":
        return "air"
    if allowed_block_ids:
        return "mineable" if bid in allowed_block_ids else "non_mineable"
    if bid.endswith(":stone") or bid.endswith("_stone"):
        return "mineable"
    return "non_mineable"


@st.cache_data(ttl=15, show_spinner=False)
def run_query_cached(sql: str, params: tuple, refresh_bucket: int) -> pd.DataFrame:
    return run_query(sql, params)


def batch_anchor_utc(seconds: int = 15) -> datetime:
    now_ts = datetime.now(timezone.utc).timestamp()
    bucket_ts = int(now_ts // seconds) * seconds
    return datetime.fromtimestamp(bucket_ts, tz=timezone.utc)


def add_cube_shell(fig: go.Figure, region_snapshot: dict[str, Any]) -> None:
    x0, x1 = region_snapshot["min_x"], region_snapshot["max_x"]
    y0, y1 = region_snapshot["min_y"], region_snapshot["max_y"]
    z0, z1 = region_snapshot["min_z"], region_snapshot["max_z"]

    fig.add_trace(
        go.Mesh3d(
            x=[x0, x1, x1, x0, x0, x1, x1, x0],
            y=[z0, z0, z0, z0, z1, z1, z1, z1],
            z=[y0, y0, y1, y1, y0, y0, y1, y1],
            i=[0, 0, 0, 4, 4, 1, 2, 3, 0, 1, 0, 2],
            j=[1, 2, 4, 5, 6, 5, 6, 7, 3, 2, 4, 6],
            k=[2, 3, 5, 6, 7, 2, 3, 4, 7, 6, 1, 7],
            color="#909090",
            opacity=0.06,
            hoverinfo="skip",
            name="Mine volume",
            showscale=False,
        )
    )

    corners = {
        "a": (x0, z0, y0),
        "b": (x1, z0, y0),
        "c": (x1, z0, y1),
        "d": (x0, z0, y1),
        "e": (x0, z1, y0),
        "f": (x1, z1, y0),
        "g": (x1, z1, y1),
        "h": (x0, z1, y1),
    }
    edges = [
        ("a", "b"), ("b", "c"), ("c", "d"), ("d", "a"),
        ("e", "f"), ("f", "g"), ("g", "h"), ("h", "e"),
        ("a", "e"), ("b", "f"), ("c", "g"), ("d", "h"),
    ]
    for start_key, end_key in edges:
        start = corners[start_key]
        end = corners[end_key]
        fig.add_trace(
            go.Scatter3d(
                x=[start[0], end[0]],
                y=[start[1], end[1]],
                z=[start[2], end[2]],
                mode="lines",
                line=dict(color="rgba(160,160,160,0.45)", width=3),
                hoverinfo="skip",
                showlegend=False,
            )
        )


def normalize_yaw(yaw: float) -> float:
    value = yaw
    while value <= -180.0:
        value += 360.0
    while value > 180.0:
        value -= 360.0
    return value


def parse_tick_ts(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_forge_tick_record(obj: object) -> Optional[dict[str, Any]]:
    if not isinstance(obj, dict):
        return None

    event_type = str(obj.get("type", ""))
    if event_type not in {"F3_TICK", "FORGE_F3"}:
        return None

    if event_type == "FORGE_F3":
        pose_raw = obj.get("pose") or {}
        target_raw = obj.get("target") or {}
        ts_utc = obj.get("ts_utc")
        record = {
            "ts_utc": ts_utc,
            "x": pose_raw.get("x"),
            "y": pose_raw.get("y"),
            "z": pose_raw.get("z"),
            "yaw": pose_raw.get("yaw"),
            "pitch": pose_raw.get("pitch"),
            "dimension": pose_raw.get("dimension"),
            "look_block": target_raw.get("block_id"),
            "block_x": target_raw.get("x"),
            "block_y": target_raw.get("y"),
            "block_z": target_raw.get("z"),
        }
    else:
        ts_utc = obj.get("ts") or obj.get("ts_utc")
        record = {
            "ts_utc": ts_utc,
            "x": obj.get("x"),
            "y": obj.get("y"),
            "z": obj.get("z"),
            "yaw": obj.get("yaw"),
            "pitch": obj.get("pitch"),
            "dimension": obj.get("dimension"),
            "look_block": obj.get("look_block"),
            "block_x": obj.get("look_x"),
            "block_y": obj.get("look_y"),
            "block_z": obj.get("look_z"),
        }

    parsed_ts = parse_tick_ts(record.get("ts_utc"))
    if parsed_ts is None:
        return None

    try:
        record["x"] = float(record.get("x", 0.0))
        record["y"] = float(record.get("y", 0.0))
        record["z"] = float(record.get("z", 0.0))
        record["yaw"] = float(record.get("yaw", 0.0))
        record["pitch"] = float(record.get("pitch", 0.0))
    except (TypeError, ValueError):
        return None

    for key in ("block_x", "block_y", "block_z"):
        value = record.get(key)
        try:
            record[key] = None if value is None else int(value)
        except (TypeError, ValueError):
            record[key] = None

    record["look_block"] = str(record.get("look_block") or "minecraft:air")
    record["ts_dt"] = parsed_ts
    record["normalized_yaw"] = round(normalize_yaw(float(record["yaw"])), 3)
    return record


def read_recent_forge_ticks(max_bytes: int = 8_000_000) -> list[dict[str, Any]]:
    path = get_forge_log_path()
    if not path.exists():
        return []

    try:
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            offset = max(0, size - max_bytes)
            f.seek(offset)
            text = f.read().decode("utf-8", errors="replace")
    except Exception:
        return []

    records: list[dict[str, Any]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        record = parse_forge_tick_record(obj)
        if record is not None:
            records.append(record)
    return records


def load_scout_observation_records(
    region_snapshot: Optional[dict[str, Any]],
    *,
    boundary_margin: int = 2,
) -> list[dict[str, Any]]:
    report = load_latest_scout_visual_report(region_snapshot)
    if report is None:
        return []
    raw_samples = report.get("observation_samples")

    results: list[dict[str, Any]] = []
    if isinstance(raw_samples, list):
        for sample in raw_samples:
            if not isinstance(sample, dict):
                continue
            try:
                block_x = sample.get("block_x")
                block_y = sample.get("block_y")
                block_z = sample.get("block_z")
                if block_x is None or block_y is None or block_z is None:
                    continue
                record = {
                    "ts_utc": sample.get("ts_utc"),
                    "ts_dt": parse_iso_datetime(sample.get("ts_utc")),
                    "x": float(sample.get("x", 0.0)),
                    "y": float(sample.get("y", 0.0)),
                    "z": float(sample.get("z", 0.0)),
                    "yaw": float(sample.get("yaw", 0.0)),
                    "pitch": float(sample.get("pitch", 0.0)),
                    "look_block": str(sample.get("look_block") or "minecraft:air"),
                    "block_x": int(block_x),
                    "block_y": int(block_y),
                    "block_z": int(block_z),
                    "block_class": str(sample.get("look_block_class") or "unknown"),
                    "observation_state": str(sample.get("observation_state") or "unclassified"),
                    "source": "scout",
                }
            except (TypeError, ValueError):
                continue

            if region_snapshot is not None:
                if not (
                    region_snapshot["min_x"] - boundary_margin <= record["block_x"] <= region_snapshot["max_x"] + boundary_margin
                    and region_snapshot["min_y"] - boundary_margin <= record["block_y"] <= region_snapshot["max_y"] + boundary_margin
                    and region_snapshot["min_z"] - boundary_margin <= record["block_z"] <= region_snapshot["max_z"] + boundary_margin
                ):
                    continue
            results.append(record)

    if region_snapshot is None:
        return results

    trace_points = report.get("trace_points")
    trace_cells: set[tuple[int, int, int]] = set()
    if isinstance(trace_points, list):
        usable_trace_points: list[dict[str, Any]] = [
            point
            for point in trace_points
            if isinstance(point, dict)
            and isinstance(point.get("x"), (int, float))
            and isinstance(point.get("z"), (int, float))
        ]
        coverage_y = int(region_snapshot["max_y"])
        for idx, point in enumerate(usable_trace_points):
            next_point = usable_trace_points[idx + 1] if idx + 1 < len(usable_trace_points) else None
            if next_point is None:
                line_cells = _iter_grid_line_cells(float(point["x"]), float(point["z"]), float(point["x"]), float(point["z"]))
            else:
                line_cells = _iter_grid_line_cells(
                    float(point["x"]),
                    float(point["z"]),
                    float(next_point["x"]),
                    float(next_point["z"]),
                )
            for block_x, block_z in line_cells:
                if not (
                    region_snapshot["min_x"] <= block_x <= region_snapshot["max_x"]
                    and region_snapshot["min_z"] <= block_z <= region_snapshot["max_z"]
                ):
                    continue
                trace_cells.add((block_x, coverage_y, block_z))

    generated_ts = report.get("generated_at_utc")
    generated_dt = parse_iso_datetime(generated_ts)
    for block_x, block_y, block_z in sorted(trace_cells):
        results.append(
            {
                "ts_utc": generated_ts,
                "ts_dt": generated_dt,
                "x": float(block_x) + 0.5,
                "y": float(block_y),
                "z": float(block_z) + 0.5,
                "yaw": 0.0,
                "pitch": 0.0,
                "look_block": "minecraft:stone",
                "block_x": block_x,
                "block_y": block_y,
                "block_z": block_z,
                "block_class": "mineable",
                "observation_state": "scout_path_mineable",
                "source": "scout_coverage",
            }
        )

    border_y = max(int(region_snapshot["min_y"]), int(region_snapshot["max_y"]) - 1)
    border_cells: set[tuple[int, int, int]] = set()
    outer_x_min = int(region_snapshot["min_x"]) - 1
    outer_x_max = int(region_snapshot["max_x"]) + 1
    outer_z_min = int(region_snapshot["min_z"]) - 1
    outer_z_max = int(region_snapshot["max_z"]) + 1
    for block_x in range(outer_x_min, outer_x_max + 1):
        border_cells.add((block_x, border_y, outer_z_min))
        border_cells.add((block_x, border_y, outer_z_max))
    for block_z in range(outer_z_min, outer_z_max + 1):
        border_cells.add((outer_x_min, border_y, block_z))
        border_cells.add((outer_x_max, border_y, block_z))

    for block_x, block_y, block_z in sorted(border_cells):
        results.append(
            {
                "ts_utc": generated_ts,
                "ts_dt": generated_dt,
                "x": float(block_x) + 0.5,
                "y": float(block_y),
                "z": float(block_z) + 0.5,
                "yaw": 0.0,
                "pitch": 0.0,
                "look_block": "minecraft:red_wool",
                "block_x": block_x,
                "block_y": block_y,
                "block_z": block_z,
                "block_class": "non_mineable",
                "observation_state": "scout_memory_border",
                "source": "scout_border",
            }
        )
    return results


def load_world_memory_records(
    region_snapshot: Optional[dict[str, Any]],
    *,
    boundary_margin: int = 2,
) -> list[dict[str, Any]]:
    if region_snapshot is None:
        return []
    memory = load_json_file(get_voxel_world_memory_path())
    if memory is None:
        return []
    regions = memory.get("regions")
    if not isinstance(regions, dict):
        return []
    entry = regions.get(str(region_snapshot.get("name") or "")) or regions.get("__default__")
    if not isinstance(entry, dict):
        return []
    voxels = entry.get("voxels")
    if not isinstance(voxels, dict):
        return []

    allowed_block_ids = set(region_snapshot.get("allowed_block_ids") or [])
    results: list[dict[str, Any]] = []
    for key, voxel in voxels.items():
        if not isinstance(voxel, dict):
            continue
        try:
            block_x = int(voxel.get("x"))
            block_y = int(voxel.get("y"))
            block_z = int(voxel.get("z"))
        except (TypeError, ValueError):
            if isinstance(key, str) and key.count(",") == 2:
                try:
                    sx, sy, sz = key.split(",")
                    block_x, block_y, block_z = int(sx), int(sy), int(sz)
                except (TypeError, ValueError):
                    continue
            else:
                continue
        if not (
            region_snapshot["min_x"] - boundary_margin <= block_x <= region_snapshot["max_x"] + boundary_margin
            and region_snapshot["min_y"] - boundary_margin <= block_y <= region_snapshot["max_y"] + boundary_margin
            and region_snapshot["min_z"] - boundary_margin <= block_z <= region_snapshot["max_z"] + boundary_margin
        ):
            continue

        state = str(voxel.get("state") or "").strip().lower()
        block_id = str(voxel.get("block_id") or "minecraft:air").strip().lower()
        if state == "mineable_unmined":
            block_class = "mineable"
            if block_id == "minecraft:air":
                block_id = next(iter(sorted(allowed_block_ids)), "minecraft:stone")
        elif state == "mineable_mined_air":
            block_class = "air"
            block_id = "minecraft:air"
        elif state in {"border", "non_mineable"}:
            block_class = "non_mineable"
        else:
            block_class = classify_voxel_block(block_id, allowed_block_ids)

        updated_at_utc = voxel.get("updated_at_utc")
        results.append(
            {
                "ts_utc": updated_at_utc,
                "ts_dt": parse_iso_datetime(updated_at_utc),
                "x": float(block_x) + 0.5,
                "y": float(block_y),
                "z": float(block_z) + 0.5,
                "yaw": 0.0,
                "pitch": 0.0,
                "look_block": block_id,
                "block_x": block_x,
                "block_y": block_y,
                "block_z": block_z,
                "block_class": block_class,
                "observation_state": state or "voxel_memory",
                "source": "world_memory",
            }
        )
    return results


def load_world_memory_reset_ts(region_snapshot: Optional[dict[str, Any]]) -> Optional[datetime]:
    if region_snapshot is None:
        return None
    memory = load_json_file(get_voxel_world_memory_path())
    if memory is None:
        return None
    regions = memory.get("regions")
    if not isinstance(regions, dict):
        return None
    entry = regions.get(str(region_snapshot.get("name") or "")) or regions.get("__default__")
    if not isinstance(entry, dict):
        return None
    return parse_iso_datetime(entry.get("last_reset_at_utc"))


def select_current_cycle_ticks(
    records: list[dict[str, Any]],
    region_snapshot: Optional[dict[str, Any]],
    reset_vertical_distance: float = 16.0,
    max_cycle_age_sec: float = 300.0,
) -> list[dict[str, Any]]:
    if not records:
        return []

    latest_reset_index = 0
    top_y_threshold = None
    if region_snapshot is not None:
        top_y_threshold = float(region_snapshot["max_y"] - 3)

    for idx in range(1, len(records)):
        dy = float(records[idx]["y"]) - float(records[idx - 1]["y"])
        if dy < reset_vertical_distance:
            continue
        if top_y_threshold is not None and float(records[idx]["y"]) < top_y_threshold:
            continue
        latest_reset_index = idx

    current_cycle = records[latest_reset_index:]
    if current_cycle and max_cycle_age_sec > 0:
        latest_ts = current_cycle[-1].get("ts_dt")
        if isinstance(latest_ts, datetime):
            cutoff = latest_ts - timedelta(seconds=max_cycle_age_sec)
            current_cycle = [
                record
                for record in current_cycle
                if isinstance(record.get("ts_dt"), datetime) and record["ts_dt"] >= cutoff
            ]
    return current_cycle


def read_latest_forge_tick() -> Optional[dict]:
    path = get_forge_log_path()
    if not path.exists():
        return None

    try:
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            offset = max(0, size - 8192)
            f.seek(offset)
            lines = f.read().decode("utf-8", errors="replace").splitlines()
        recent_ticks: list[dict] = []
        for line in reversed(lines):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            record = parse_forge_tick_record(obj)
            if record is not None:
                recent_ticks.append(record)
                if len(recent_ticks) >= 2:
                    break
        if not recent_ticks:
            return None
        latest = recent_ticks[0]
        if len(recent_ticks) >= 2:
            previous = recent_ticks[1]
            current_ts = latest.get("ts_dt")
            previous_ts = previous.get("ts_dt")
            if current_ts is not None and previous_ts is not None:
                dt_sec = (current_ts - previous_ts).total_seconds()
                if dt_sec > 0:
                    try:
                        dx = float(latest.get("x", 0.0)) - float(previous.get("x", 0.0))
                        dy = float(latest.get("y", 0.0)) - float(previous.get("y", 0.0))
                        dz = float(latest.get("z", 0.0)) - float(previous.get("z", 0.0))
                        latest["speed_bps"] = round(math.sqrt(dx * dx + dy * dy + dz * dz) / dt_sec, 3)
                        latest["horizontal_speed_bps"] = round(math.sqrt(dx * dx + dz * dz) / dt_sec, 3)
                        latest["dt_sec"] = round(dt_sec, 4)
                    except (TypeError, ValueError):
                        pass
        if isinstance(latest.get("ts_dt"), datetime):
            latest["ts_dt"] = latest["ts_dt"].isoformat()
        return latest
    except Exception:
        return None
    return None


# ---------------------------------------------------------------------
# Runtime controls
# ---------------------------------------------------------------------


def section_runtime_controls() -> None:
    st.header("Runtime Controls")
    st.caption(
        "Queue commands for the live miner app. Each button starts about 2 seconds after click, "
        "so you can refocus Minecraft before movement begins."
    )

    queued_payload: Optional[dict[str, Any]] = None
    queue_error: Optional[str] = None
    button_cols = st.columns(5)

    with button_cols[0]:
        if st.button("Start Mine", use_container_width=True):
            queued_payload, queue_error = try_queue_dashboard_command("start_mine")
    with button_cols[1]:
        if st.button("Start Scout", use_container_width=True):
            queued_payload, queue_error = try_queue_dashboard_command("start_scout")
    with button_cols[2]:
        if st.button("Start Calibrate", use_container_width=True):
            queued_payload, queue_error = try_queue_dashboard_command("start_calibrate")
    with button_cols[3]:
        if st.button("Manual Record", use_container_width=True):
            queued_payload, queue_error = try_queue_dashboard_command("start_manual_record")
    with button_cols[4]:
        if st.button("Stop", use_container_width=True):
            queued_payload, queue_error = try_queue_dashboard_command("stop", delay_sec=0.5)

    if queued_payload is not None:
        st.success(
            "Queued "
            f"`{queued_payload['command']}` for {queued_payload['execute_after_utc']}."
        )
    elif queue_error:
        st.error(f"Failed to queue dashboard command: {queue_error}")

    pending = load_json_file(get_dashboard_command_path())
    status = load_json_file(get_dashboard_status_path())
    runtime = load_json_file(get_dashboard_runtime_status_path())
    runtime_updated = parse_iso_datetime((runtime or {}).get("updated_at_utc"))
    runtime_is_live = False
    if runtime_updated is not None:
        runtime_is_live = (datetime.now(timezone.utc) - runtime_updated) <= timedelta(seconds=5)
    mode_label = "offline"
    if runtime is not None and runtime_is_live:
        mode_label = "idle"
        if bool(runtime.get("user_paused")):
            mode_label = "paused"
        if bool(runtime.get("manual_recording_enabled")):
            mode_label = "manual_record"
        elif bool(runtime.get("calibration_enabled")):
            mode_label = "calibrate"
        elif bool(runtime.get("mapping_enabled")):
            mode_label = "scout"
        elif bool(runtime.get("mining_enabled")):
            mode_label = "mine"

    if pending is not None:
        st.info(
            "Pending command: "
            f"`{pending.get('command', 'unknown')}` at {pending.get('execute_after_utc', 'n/a')}"
        )

    metric_cols = st.columns(4)
    with metric_cols[0]:
        st.metric("App state", mode_label)
    with metric_cols[1]:
        st.metric("Last action", str((runtime or {}).get("last_action") or "n/a"))
    with metric_cols[2]:
        st.metric("Last command", str((status or {}).get("command") or "n/a"))
    with metric_cols[3]:
        st.metric("Command result", str((status or {}).get("result") or "n/a"))

    if runtime is None or not runtime_is_live:
        st.warning(
            "No live miner heartbeat yet. Start `python -m minecraft_auto_miner.app` and leave it running "
            "while you use these controls."
        )
        return

    pose = runtime.get("pose")
    if isinstance(pose, dict):
        st.caption(
            "Live runtime pose: "
            f"x={pose.get('x', 'n/a')} y={pose.get('y', 'n/a')} z={pose.get('z', 'n/a')} "
            f"yaw={pose.get('yaw', 'n/a')} pitch={pose.get('pitch', 'n/a')}"
        )

    status_message = (status or {}).get("message")
    if status_message:
        st.caption(f"Last control status: {status_message}")


def section_scout_intelligence() -> None:
    st.header("Scout Intelligence")
    st.caption(
        "Perimeter scout now classifies `look_block` observations the same way as the literal cube view: "
        "mineable, non-mineable, air, or unknown."
    )

    report = load_json_file(get_perimeter_scout_report_path())
    if report is None:
        st.info("No saved perimeter scout report yet. Run `Start Scout` to populate boundary and material intelligence.")
        return

    intelligence = report.get("look_block_intelligence")
    if not isinstance(intelligence, dict):
        st.info("The latest perimeter scout report does not include look-block intelligence yet.")
        return

    if bool(report.get("memory_reused")):
        st.info("This scout view was populated from previously verified scout memory, so the live perimeter walk was skipped.")

    class_counts = intelligence.get("class_counts") if isinstance(intelligence.get("class_counts"), dict) else {}
    metric_cols = st.columns(6)
    metric_cols[0].metric("Scout status", str(report.get("status") or "n/a"))
    metric_cols[1].metric("Scout phase", str(report.get("current_phase") or "n/a"))
    metric_cols[2].metric("Samples", int(intelligence.get("sample_count") or 0))
    metric_cols[3].metric("Mineable", int(class_counts.get("mineable", 0) or 0))
    metric_cols[4].metric("Non-mineable", int(class_counts.get("non_mineable", 0) or 0))
    metric_cols[5].metric("Source", "memory" if bool(report.get("memory_reused")) else "live")

    state_counts = intelligence.get("state_counts")
    if isinstance(state_counts, dict) and state_counts:
        st.caption("Observed scout states inferred from inward and outward mine-facing observation sweeps.")
        state_rows = [
            {"state": state_name, "count": int(count or 0)}
            for state_name, count in state_counts.items()
        ]
        st.dataframe(pd.DataFrame(state_rows), use_container_width=True, height=180)

    region_verification = report.get("region_verification")
    if isinstance(region_verification, dict):
        configured_match = (
            region_verification.get("configured_region_match")
            if isinstance(region_verification.get("configured_region_match"), dict)
            else None
        )
        if configured_match is not None:
            st.caption("Configured region verification from coordinate-aware scout observations.")
            verify_cols = st.columns(5)
            verify_cols[0].metric("Interior mineable", int(configured_match.get("interior_mineable_hits", 0) or 0))
            verify_cols[1].metric("Interior air", int(configured_match.get("interior_air_hits", 0) or 0))
            verify_cols[2].metric("Exterior non-mineable", int(configured_match.get("exterior_non_mineable_hits", 0) or 0))
            verify_cols[3].metric("Exterior mineable", int(configured_match.get("exterior_mineable_hits", 0) or 0))
            verify_cols[4].metric("Bounds match", "yes" if bool(configured_match.get("matches_expectation")) else "no")

        observed_mineable_bounds = region_verification.get("observed_mineable_bounds")
        observed_non_mineable_bounds = region_verification.get("observed_non_mineable_bounds")
        mineable_delta = region_verification.get("mineable_bounds_delta_vs_configured")
        non_mineable_delta = region_verification.get("non_mineable_bounds_delta_vs_configured")
        bounds_rows: list[dict[str, Any]] = []
        if isinstance(observed_mineable_bounds, dict):
            bounds_rows.append({"observed_bounds": "mineable", **observed_mineable_bounds})
        if isinstance(observed_non_mineable_bounds, dict):
            bounds_rows.append({"observed_bounds": "non_mineable", **observed_non_mineable_bounds})
        if bounds_rows:
            st.caption("Observed block-coordinate bounds collected during scout.")
            st.dataframe(pd.DataFrame(bounds_rows), use_container_width=True, height=160)

        delta_rows: list[dict[str, Any]] = []
        if isinstance(mineable_delta, dict):
            delta_rows.append({"delta_type": "mineable_vs_configured", **mineable_delta})
        if isinstance(non_mineable_delta, dict):
            delta_rows.append({"delta_type": "non_mineable_vs_configured", **non_mineable_delta})
        if delta_rows:
            st.caption("Observed bound deltas versus the configured mine cube.")
            st.dataframe(pd.DataFrame(delta_rows), use_container_width=True, height=160)

    block_catalog = report.get("block_catalog")
    if isinstance(block_catalog, dict) and block_catalog:
        catalog_rows: list[dict[str, Any]] = []
        for block_id, summary in block_catalog.items():
            if not isinstance(summary, dict):
                continue
            catalog_rows.append(
                {
                    "look_block": block_id,
                    "classification": summary.get("last_classification", "unknown"),
                    "samples": int(summary.get("sample_count", 0) or 0),
                    "interior_hits": int(summary.get("interior_hits", 0) or 0),
                    "exterior_hits": int(summary.get("exterior_hits", 0) or 0),
                    "mineable_hits": int(summary.get("mineable_hits", 0) or 0),
                    "non_mineable_hits": int(summary.get("non_mineable_hits", 0) or 0),
                    "air_hits": int(summary.get("air_hits", 0) or 0),
                }
            )
        if catalog_rows:
            catalog_df = pd.DataFrame(catalog_rows).sort_values(
                by=["samples", "look_block"],
                ascending=[False, True],
            )
            st.caption("Observed block catalog aggregated from scout look-block samples.")
            st.dataframe(catalog_df, use_container_width=True, height=260)

    learned_blocking = intelligence.get("learned_blocking_block_ids")
    if isinstance(learned_blocking, list) and learned_blocking:
        st.caption("Learned blocking materials currently associated with the configured region.")
        st.dataframe(
            pd.DataFrame({"learned_blocking_block_id": learned_blocking}),
            use_container_width=True,
            height=min(220, 35 * (len(learned_blocking) + 1)),
        )

    top_cols = st.columns(2)
    top_mineable = intelligence.get("top_mineable_blocks")
    if isinstance(top_mineable, list) and top_mineable:
        with top_cols[0]:
            st.caption("Top mineable `look_block` observations from scout.")
            st.dataframe(pd.DataFrame(top_mineable), use_container_width=True, height=220)
    else:
        with top_cols[0]:
            st.caption("No mineable `look_block` observations were summarized yet.")

    top_non_mineable = intelligence.get("top_non_mineable_blocks")
    if isinstance(top_non_mineable, list) and top_non_mineable:
        with top_cols[1]:
            st.caption("Top non-mineable `look_block` observations from scout.")
            st.dataframe(pd.DataFrame(top_non_mineable), use_container_width=True, height=220)
    else:
        with top_cols[1]:
            st.caption("No non-mineable `look_block` observations were summarized yet.")

    phase_summaries = intelligence.get("phase_summaries")
    if isinstance(phase_summaries, dict) and phase_summaries:
        phase_rows: list[dict[str, Any]] = []
        for phase_name, summary in phase_summaries.items():
            if not isinstance(summary, dict):
                continue
            class_counts = summary.get("class_counts") if isinstance(summary.get("class_counts"), dict) else {}
            phase_rows.append(
                {
                    "phase": phase_name,
                    "sample_count": int(summary.get("sample_count") or 0),
                    "mineable": int(class_counts.get("mineable", 0) or 0),
                    "non_mineable": int(class_counts.get("non_mineable", 0) or 0),
                    "air": int(class_counts.get("air", 0) or 0),
                    "unknown": int(class_counts.get("unknown", 0) or 0),
                }
            )
        if phase_rows:
            st.caption("Per-leg scout material/state summary derived from saved `look_block` observations.")
            st.dataframe(pd.DataFrame(phase_rows), use_container_width=True, height=240)


# ---------------------------------------------------------------------
# Live pose HUD
# ---------------------------------------------------------------------


def section_live_pose():
    """
    Top-level Live Pose HUD.

    - Optional auto-refresh every 1–2 seconds (if streamlit-autorefresh installed).
    - Shows latest compressed tick from gold.f3_pose_ds in a time window.
    """
    st.header("Live Pose HUD")

    col_range, col_info = st.columns([2, 1])

    with col_range:
        range_option = st.selectbox(
            "Time window (UTC)",
            ["Last 1 minute", "Last 5 minutes", "Last 15 minutes"],
            index=0,
            key="live_time_window",
        )
    with col_info:
        st.caption("Batch section; refreshes on a 15-second cadence.")

    batch_anchor = batch_anchor_utc(15)
    batch_bucket = int(batch_anchor.timestamp() // 15)
    start_utc, end_utc = get_time_range(range_option, now_utc=batch_anchor)

    st.caption(
        f"Showing compressed Silver ticks from **gold.f3_pose_ds** between "
        f"{start_utc.isoformat()} and {end_utc.isoformat()}."
    )

    df_live = run_query_cached(
        """
        SELECT
            ts_utc,
            x, y, z,
            block_x, block_y, block_z,
            yaw, pitch,
            look_block,
            tick_count
        FROM gold.f3_pose_ds
        WHERE ts_utc BETWEEN %s AND %s
        ORDER BY ts_utc
        """,
        (start_utc, end_utc),
        batch_bucket,
    )

    if df_live.empty:
        st.info("No Silver/Gold pose rows in this selected window yet.")

        latest_db = run_query_cached(
            """
            SELECT
                ts_utc,
                x, y, z,
                block_x, block_y, block_z,
                yaw, pitch,
                look_block,
                tick_count
            FROM gold.f3_pose_ds
            ORDER BY ts_utc DESC
            LIMIT 1
            """,
            (),
            batch_bucket,
        )
        if not latest_db.empty:
            row = latest_db.iloc[0]
            st.caption(
                "Latest DB-backed pose is older than the selected window: "
                f"{format_ts_for_metric(row['ts_utc'])}"
            )
            st.dataframe(latest_db, use_container_width=True, height=120)

        latest_forge = read_latest_forge_tick()
        if latest_forge is not None:
            st.caption("Latest raw Forge log tick")
            st.json(latest_forge)
        else:
            st.caption("Forge log could not be read from the dashboard process.")
        return

    latest = df_live.iloc[-1]

    st.subheader("Current Pose (latest compressed tick)")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Time (UTC)", format_ts_for_metric(latest["ts_utc"]))
        st.metric(
            "Block (x,y,z)",
            f"{latest['block_x']}, {latest['block_y']}, {latest['block_z']}",
        )

    with col2:
        st.metric(
            "Exact (x,y,z)",
            f"{latest['x']:.2f}, {latest['y']:.2f}, {latest['z']:.2f}",
        )
        st.metric("Look block", latest.get("look_block", "n/a") or "n/a")

    with col3:
        st.metric("Yaw / Pitch", f"{latest['yaw']:.1f}°, {latest['pitch']:.1f}°")
        st.metric("Ticks / sec (raw)", latest.get("tick_count", "n/a"))

    st.markdown("#### Recent compressed ticks (tail)")
    st.dataframe(df_live.tail(50), use_container_width=True, height=220)


# ---------------------------------------------------------------------
# Episodes & Decision Windows
# ---------------------------------------------------------------------


def section_episodes_and_windows() -> Optional[int]:
    """
    Middle section:

    - Latest episodes (silver.episode)
    - Decision windows for selected episode (silver.decision_window)

    Returns
    -------
    selected_episode_id or None
    """
    st.header("Episodes & Decision Windows")

    st.caption(
        "Episode + decision_window metadata from **silver.episode** "
        "and **silver.decision_window** (read-only view)."
    )

    batch_bucket = int(batch_anchor_utc(15).timestamp() // 15)

    df_ep = run_query_cached(
        """
        SELECT
            episode_id,
            start_ts_utc,
            end_ts_utc,
            tick_count,
            (end_ts_utc - start_ts_utc) AS duration,
            state_name,
            action_name
        FROM silver.episode
        ORDER BY end_ts_utc DESC, episode_id DESC
        LIMIT 200
        """,
        (),
        batch_bucket,
    )

    if df_ep.empty:
        st.info("No episodes found yet. Make sure the telemetry pipeline is running.")
        return None

    st.subheader("Episodes (latest first)")
    st.dataframe(df_ep, use_container_width=True, height=250)

    ep_ids = df_ep["episode_id"].tolist()
    default_ep = ep_ids[0] if ep_ids else None

    selected_ep = st.selectbox(
        "Select episode_id",
        ep_ids,
        index=0 if default_ep is not None else 0,
        key="selected_episode_id",
    )

    if selected_ep is None:
        return None

    ep_row = df_ep[df_ep["episode_id"] == selected_ep].iloc[0]

    st.subheader("Episode summary")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Episode ID", int(ep_row["episode_id"]))
        st.metric("Ticks", int(ep_row["tick_count"]))
    with col2:
        st.metric("Start (UTC)", format_ts_for_metric(ep_row["start_ts_utc"]))
        st.metric("End (UTC)", format_ts_for_metric(ep_row["end_ts_utc"]))
    with col3:
        st.metric("Duration", str(ep_row["duration"]))
        st.metric("State", ep_row.get("state_name") or "n/a")
        st.metric("Action", ep_row.get("action_name") or "n/a")

    st.subheader("Decision windows for this episode")

    df_dw = run_query_cached(
        """
        SELECT
            decision_window_id,
            episode_id,
            start_ts_utc,
            end_ts_utc,
            tick_count,
            state_name,
            action_name,
            feature_json,
            reward_value
        FROM silver.decision_window
        WHERE episode_id = %s
        ORDER BY start_ts_utc
        """,
        (selected_ep,),
        batch_bucket,
    )

    if df_dw.empty:
        st.info("No decision_windows yet for this episode (or DDL not updated).")
        return int(selected_ep)

    st.dataframe(df_dw, use_container_width=True, height=300)

    if "reward_value" in df_dw.columns and df_dw["reward_value"].notna().any():
        st.subheader("Reward per decision window")
        df_plot = df_dw[["start_ts_utc", "reward_value"]].dropna()
        df_plot = df_plot.set_index("start_ts_utc")
        st.line_chart(df_plot, use_container_width=True)
    else:
        st.caption("Reward values not populated yet – will appear here once computed.")

    return int(selected_ep)


# ---------------------------------------------------------------------
# Spatial 3D section helpers
# ---------------------------------------------------------------------


def fetch_spatial_blocks_for_episode(
    episode_id: int,
    seconds_before_end: int = 10,
    max_blocks: int = 5000,
    region_snapshot: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Fetch latest block states near the end of a given episode.

    v0.4.0:
      - Use silver.episode to find [start_ts_utc, end_ts_utc].
      - Consider blocks whose latest_ts_utc is within a window near the end.
      - Use gold.block_state_latest_with_meta as the spatial source.
    """
    try:
        conn = get_conn()
    except Exception as e:
        st.error(f"DB connection failed (3D space): {e}")
        return pd.DataFrame()

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT
                    episode_id,
                    start_ts_utc,
                    end_ts_utc
                FROM silver.episode
                WHERE episode_id = %s
                """,
                (episode_id,),
            )
            row = cur.fetchone()
            if not row:
                return pd.DataFrame()

            start_ts = row["start_ts_utc"]
            end_ts = row["end_ts_utc"]

            if isinstance(start_ts, str):
                start_ts = datetime.fromisoformat(start_ts)
            if isinstance(end_ts, str):
                end_ts = datetime.fromisoformat(end_ts)

            if seconds_before_end > 0:
                window_start = end_ts - timedelta(seconds=seconds_before_end)
                if window_start < start_ts:
                    window_start = start_ts
            else:
                window_start = start_ts

            where_clauses = ["latest_ts_utc BETWEEN %s AND %s"]
            params: list[object] = [window_start, end_ts]

            if region_snapshot is not None:
                where_clauses.extend(
                    [
                        "block_x BETWEEN %s AND %s",
                        "block_y BETWEEN %s AND %s",
                        "block_z BETWEEN %s AND %s",
                    ]
                )
                params.extend(
                    [
                        region_snapshot["min_x"],
                        region_snapshot["max_x"],
                        region_snapshot["min_y"],
                        region_snapshot["max_y"],
                        region_snapshot["min_z"],
                        region_snapshot["max_z"],
                    ]
                )

            query = f"""
                SELECT
                    block_x,
                    block_y,
                    block_z,
                    latest_ts_utc,
                    f3_tick_id,
                    ts_utc,
                    x, y, z,
                    yaw, pitch,
                    look_block,
                    status,
                    bucket_1s
                FROM gold.block_state_latest_with_meta
                WHERE {" AND ".join(where_clauses)}
                ORDER BY latest_ts_utc DESC
                LIMIT %s
                """
            params.append(max_blocks)
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame()

        data = [dict(r) for r in rows]
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"3D spatial query failed: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def fetch_latest_pose_for_episode(
    episode_id: int,
    seconds_before_end: int = 10,
) -> Optional[dict[str, Any]]:
    try:
        conn = get_conn()
    except Exception as e:
        st.error(f"DB connection failed (3D pose lookup): {e}")
        return None

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT
                    start_ts_utc,
                    end_ts_utc
                FROM silver.episode
                WHERE episode_id = %s
                """,
                (episode_id,),
            )
            row = cur.fetchone()
            if not row:
                return None

            start_ts = row["start_ts_utc"]
            end_ts = row["end_ts_utc"]
            if isinstance(start_ts, str):
                start_ts = datetime.fromisoformat(start_ts)
            if isinstance(end_ts, str):
                end_ts = datetime.fromisoformat(end_ts)

            if seconds_before_end > 0:
                window_start = end_ts - timedelta(seconds=seconds_before_end)
                if window_start < start_ts:
                    window_start = start_ts
            else:
                window_start = start_ts

            cur.execute(
                """
                SELECT
                    ts_utc,
                    x, y, z,
                    yaw, pitch,
                    look_block
                FROM gold.f3_pose_ds
                WHERE ts_utc BETWEEN %s AND %s
                ORDER BY ts_utc DESC
                LIMIT 1
                """,
                (window_start, end_ts),
            )
            pose_row = cur.fetchone()
            if not pose_row:
                return None
            return dict(pose_row)
    except Exception as e:
        st.error(f"3D pose query failed: {e}")
        return None
    finally:
        conn.close()


def section_3d_space(selected_episode_id: Optional[int]) -> None:
    """
    Bottom section: 3D spatial view of the miner's environment.

    - If an episode is already selected above, we use that as default.
    - Otherwise, we let the user choose any recent episode.
    """
    st.header("3D Space – Voxel View (WIP)")

    if st_autorefresh is not None:
        st_autorefresh(interval=1500, key="mam_space_autorefresh")

    chart_slot = st.container()

    region_snapshot = load_region_snapshot()
    if region_snapshot is not None:
        st.caption(
            "Voxel view is now driven from the live raw Forge log and clipped to the calibrated mine volume "
            f"`{region_snapshot['name']}`: "
            f"x={region_snapshot['min_x']}..{region_snapshot['max_x']}, "
            f"y={region_snapshot['min_y']}..{region_snapshot['max_y']}, "
            f"z={region_snapshot['min_z']}..{region_snapshot['max_z']}. "
            "Current mine stage treats only `minecraft:stone` as mineable."
        )
    all_raw_records = read_recent_forge_ticks()
    raw_records = select_current_cycle_ticks(all_raw_records, region_snapshot)
    scout_report = load_latest_scout_visual_report(region_snapshot)
    scout_records = load_scout_observation_records(region_snapshot)
    world_memory_records = load_world_memory_records(region_snapshot)
    latest_episode_pose = raw_records[-1] if raw_records else (all_raw_records[-1] if all_raw_records else None)
    if not raw_records and not scout_records and not world_memory_records and latest_episode_pose is None:
        st.info("No recent live Forge ticks, voxel world memory, or saved scout observations were available for the literal mine cube yet.")
        return

    boundary_margin = 2
    live_blocks_df = pd.DataFrame(raw_records)
    live_blocks_df = live_blocks_df.dropna(subset=["block_x", "block_y", "block_z", "look_block"]).copy()
    if not live_blocks_df.empty:
        live_blocks_df["source"] = "live"
    scout_blocks_df = pd.DataFrame(scout_records)
    world_memory_df = pd.DataFrame(world_memory_records)
    frames = [frame for frame in (scout_blocks_df, world_memory_df, live_blocks_df) if not frame.empty]
    df_blocks_all = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df_blocks_all.empty:
        st.info("No live target-block samples, voxel memory, or scout observations inside or near the calibrated region were available yet.")
        return

    if "ts_dt" not in df_blocks_all.columns:
        df_blocks_all["ts_dt"] = None
    df_blocks_all["source_rank"] = df_blocks_all["source"].map(
        {
            "scout_coverage": 0,
            "scout_border": 1,
            "scout": 2,
            "world_memory": 3,
            "live": 4,
        }
    ).fillna(0)

    df_blocks = df_blocks_all.copy()
    if region_snapshot is not None and not df_blocks.empty:
        df_blocks = df_blocks[
            df_blocks["block_x"].between(region_snapshot["min_x"], region_snapshot["max_x"])
            & df_blocks["block_y"].between(region_snapshot["min_y"], region_snapshot["max_y"])
            & df_blocks["block_z"].between(region_snapshot["min_z"], region_snapshot["max_z"])
        ].copy()

    if df_blocks.empty:
        st.info("No live target-block samples inside the calibrated region were available yet.")

    df_blocks = df_blocks.sort_values(["source_rank", "ts_dt"], ascending=[True, True])
    df_blocks = df_blocks.drop_duplicates(subset=["block_x", "block_y", "block_z"], keep="last")

    display_blocks_df = df_blocks_all.sort_values(["source_rank", "ts_dt"], ascending=[True, True])
    if region_snapshot is not None and not display_blocks_df.empty:
        display_blocks_df = display_blocks_df[
            display_blocks_df["block_x"].between(
                region_snapshot["min_x"] - boundary_margin,
                region_snapshot["max_x"] + boundary_margin,
            )
            & display_blocks_df["block_y"].between(
                region_snapshot["min_y"] - boundary_margin,
                region_snapshot["max_y"] + boundary_margin,
            )
            & display_blocks_df["block_z"].between(
                region_snapshot["min_z"] - boundary_margin,
                region_snapshot["max_z"] + boundary_margin,
            )
        ].copy()
    display_blocks_df = display_blocks_df.drop_duplicates(
        subset=["block_x", "block_y", "block_z"], keep="last"
    )

    allowed_block_ids = set()
    if region_snapshot is not None:
        allowed_block_ids = set(region_snapshot["allowed_block_ids"])
    df_blocks["block_class"] = df_blocks["look_block"].apply(
        lambda bid: classify_voxel_block(bid, allowed_block_ids)
    )
    display_blocks_df["block_class"] = display_blocks_df["look_block"].apply(
        lambda bid: classify_voxel_block(bid, allowed_block_ids)
    )

    observed_voxels = int(len(df_blocks))
    total_volume = None
    if region_snapshot is not None:
        total_volume = (
            (region_snapshot["max_x"] - region_snapshot["min_x"] + 1)
            * (region_snapshot["max_y"] - region_snapshot["min_y"] + 1)
            * (region_snapshot["max_z"] - region_snapshot["min_z"] + 1)
        )

    class_counts = df_blocks["block_class"].value_counts()
    mineable_count = int(class_counts.get("mineable", 0))
    air_count = int(class_counts.get("air", 0))
    non_mineable_count = int(class_counts.get("non_mineable", 0))
    boundary_non_mineable_count = int(
        (display_blocks_df["block_class"] == "non_mineable").sum() if not display_blocks_df.empty else 0
    )
    scout_observed_voxels = (
        int(display_blocks_df["source"].astype(str).str.startswith("scout").sum())
        if not display_blocks_df.empty
        else 0
    )
    memory_observed_voxels = int((display_blocks_df["source"] == "world_memory").sum()) if not display_blocks_df.empty else 0
    live_observed_voxels = int((display_blocks_df["source"] == "live").sum()) if not display_blocks_df.empty else 0
    coverage_pct = (
        (observed_voxels / total_volume) * 100.0 if total_volume and total_volume > 0 else None
    )

    metric_cols = st.columns(6)
    metric_cols[0].metric("Observed voxels", observed_voxels)
    metric_cols[1].metric("Observed stone", mineable_count)
    metric_cols[2].metric("Observed air", air_count)
    metric_cols[3].metric("Observed non-mineable", boundary_non_mineable_count)
    metric_cols[4].metric(
        "Observed coverage",
        f"{coverage_pct:.2f}%" if coverage_pct is not None else "n/a",
    )
    metric_cols[5].metric("Memory voxels", memory_observed_voxels)

    st.caption(
        "Rendering the literal mine cube from persistent voxel world memory plus the latest live Forge stream. "
        "Scout observations seed the voxel memory, mining updates it live, and an upward mine reset restores mined-air cells "
        "back to mineable stone while keeping discovered borders and non-mineable barriers. "
        "Unobserved cells inside the configured mine still default to unmined stone for this mine stage."
    )

    inferred_unmined = None
    inferred_mined = air_count
    inferred_non_mineable = boundary_non_mineable_count
    if total_volume is not None:
        inferred_unmined = max(0, int(total_volume - inferred_mined - inferred_non_mineable))

    cube_metric_cols = st.columns(4)
    cube_metric_cols[0].metric("Cube blocks", int(total_volume) if total_volume is not None else "n/a")
    cube_metric_cols[1].metric("Inferred unmined", inferred_unmined if inferred_unmined is not None else "n/a")
    cube_metric_cols[2].metric("Observed mined", inferred_mined)
    cube_metric_cols[3].metric("Observed non-mineable", inferred_non_mineable)

    top_non_mineable = (
        display_blocks_df[display_blocks_df["block_class"] == "non_mineable"]["look_block"].value_counts().head(8)
    )
    learned_blocking = sorted(region_snapshot.get("learned_blocking_block_ids", set())) if region_snapshot else []
    if learned_blocking:
        st.caption("Learned blocking materials promoted from repeated non-mineable observations.")
        st.dataframe(
            pd.DataFrame({"learned_blocking_block_id": learned_blocking}),
            use_container_width=True,
            height=min(240, 35 * (len(learned_blocking) + 1)),
        )
    if not top_non_mineable.empty:
        st.caption("Recent non-mineable boundary/material blocks seen in `look_block`.")
        st.dataframe(
            top_non_mineable.rename_axis("look_block").reset_index(name="count"),
            use_container_width=True,
            height=220,
        )

    voxel_display_df = display_blocks_df.copy()
    path_df = pd.DataFrame(raw_records)
    if not path_df.empty:
        step = max(1, len(path_df) // 250)
        sampled_indices = list(range(0, len(path_df), step))
        if sampled_indices[-1] != len(path_df) - 1:
            sampled_indices.append(len(path_df) - 1)
        path_df = path_df.iloc[sampled_indices].copy()
        path_df["path_idx"] = range(len(path_df))

    fig = go.Figure()
    if region_snapshot is not None:
        add_cube_shell(fig, region_snapshot)
    if scout_report is not None and region_snapshot is not None:
        border_x = [
            region_snapshot["min_x"] - 1,
            region_snapshot["max_x"] + 1,
            region_snapshot["max_x"] + 1,
            region_snapshot["min_x"] - 1,
            region_snapshot["min_x"] - 1,
        ]
        border_z = [
            region_snapshot["min_z"] - 1,
            region_snapshot["min_z"] - 1,
            region_snapshot["max_z"] + 1,
            region_snapshot["max_z"] + 1,
            region_snapshot["min_z"] - 1,
        ]
        border_y = [max(region_snapshot["min_y"], region_snapshot["max_y"] - 1)] * len(border_x)
        fig.add_trace(
            go.Scatter3d(
                x=border_x,
                y=border_z,
                z=border_y,
                mode="lines",
                name="Scout memory border",
                line=dict(color="rgba(255,180,70,0.95)", width=8),
                hoverinfo="skip",
            )
        )

    if not voxel_display_df.empty:
        grouped = voxel_display_df.groupby(["look_block", "block_class"], dropna=False)
        for (block_id, block_class), group in grouped:
            block_name = str(block_id or "minecraft:unknown")
            class_name = str(block_class or "unknown")
            color = get_block_display_color(block_name, class_name)
            marker_size = 2.5
            trace_opacity = 0.65
            if class_name == "air":
                marker_size = 3.5
                trace_opacity = 0.75
            elif class_name == "mineable":
                marker_size = 3.0
                trace_opacity = 0.65
            elif class_name == "non_mineable":
                marker_size = 3.0
                trace_opacity = 0.80
            hover_text = [
                f"{block_name}<br>x={int(x)} y={int(y)} z={int(z)}<br>source={src}"
                for x, y, z, src in zip(
                    group["block_x"],
                    group["block_y"],
                    group["block_z"],
                    group["source"],
                )
            ]
            fig.add_trace(
                go.Scatter3d(
                    x=group["block_x"],
                    y=group["block_z"],
                    z=group["block_y"],
                    mode="markers",
                    name=block_name,
                    marker=dict(size=marker_size, color=color),
                    opacity=trace_opacity,
                    hovertext=hover_text,
                    hoverinfo="text",
                )
            )

    if not path_df.empty:
        fig.add_trace(
            go.Scatter3d(
                x=path_df["x"],
                y=path_df["z"],
                z=path_df["y"],
                mode="lines+markers",
                name="Miner path",
                line=dict(color="rgba(170,20,45,0.55)", width=4),
                marker=dict(
                    size=4,
                    color=path_df["path_idx"],
                    colorscale=[[0.0, "#7f0000"], [1.0, "#ff7ab8"]],
                    opacity=0.9,
                ),
                hoverinfo="skip",
            )
        )

    if latest_episode_pose is not None:
        fig.add_scatter3d(
            x=[float(latest_episode_pose["x"])],
            y=[float(latest_episode_pose["z"])],
            z=[float(latest_episode_pose["y"])],
            mode="markers",
            name="Player",
            marker=dict(size=8, color="#d62728", symbol="circle"),
            hovertext=[
                "Player: "
                f"{float(latest_episode_pose['x']):.2f}, "
                f"{float(latest_episode_pose['y']):.2f}, "
                f"{float(latest_episode_pose['z']):.2f}"
            ],
            hoverinfo="text",
        )

    fig.update_layout(
        scene=dict(
            xaxis_title="X",
            yaxis_title="Z",
            zaxis_title="Y",
            aspectmode="data",
            camera=dict(
                up=dict(x=0, y=0, z=1),
                eye=dict(x=1.45, y=1.45, z=0.95),
            ),
        ),
        margin=dict(l=0, r=0, b=0, t=30),
    )

    latest_ts = latest_episode_pose.get("ts_utc") if latest_episode_pose is not None else None
    with chart_slot:
        st.subheader("Literal Mine Cube")
        if latest_ts:
            st.caption(f"Latest live cube tick: {latest_ts}")
        if memory_observed_voxels > 0:
            st.caption(
                f"Persistent voxel memory contributes {memory_observed_voxels} observed voxels; "
                f"live cycle contributes {live_observed_voxels} voxels."
            )
        elif scout_observed_voxels > 0:
            st.caption(
                f"Scout baseline contributes {scout_observed_voxels} observed voxels; "
                f"live cycle contributes {live_observed_voxels} voxels."
            )
        if scout_report is not None:
            st.caption("Scout memory border remains highlighted so the discovered perimeter stays visible in the cube.")
        if not path_df.empty:
            st.caption("Path is kept until an upward reset is detected or the last 5 minutes of live telemetry expire.")
        st.plotly_chart(fig, use_container_width=True)


def section_literal_mine_cube_compact(selected_episode_id: Optional[int]) -> None:
    """Compact top-of-page cube section: title + actual plot only."""
    _ = selected_episode_id
    st.subheader("Literal Mine Cube")

    region_snapshot = load_region_snapshot()
    all_raw_records = read_recent_forge_ticks()
    raw_records = select_current_cycle_ticks(all_raw_records, region_snapshot)
    memory_reset_ts = load_world_memory_reset_ts(region_snapshot)
    if memory_reset_ts is not None:
        raw_records = [
            record
            for record in raw_records
            if isinstance(record.get("ts_dt"), datetime) and record["ts_dt"] >= memory_reset_ts
        ]
    scout_report = load_latest_scout_visual_report(region_snapshot)
    scout_records = load_scout_observation_records(region_snapshot)
    world_memory_records = load_world_memory_records(region_snapshot)
    latest_pose = raw_records[-1] if raw_records else (all_raw_records[-1] if all_raw_records else None)

    if not raw_records and not scout_records and not world_memory_records and latest_pose is None:
        st.info("No recent live Forge ticks, voxel memory, or scout observations are available for the Literal Mine Cube yet.")
        return

    live_blocks_df = pd.DataFrame(raw_records)
    live_blocks_df = live_blocks_df.dropna(subset=["block_x", "block_y", "block_z", "look_block"]).copy()
    if not live_blocks_df.empty:
        live_blocks_df["source"] = "live"
    scout_blocks_df = pd.DataFrame(scout_records)
    world_memory_df = pd.DataFrame(world_memory_records)
    frames = [frame for frame in (scout_blocks_df, world_memory_df, live_blocks_df) if not frame.empty]
    if not frames:
        st.info("No block observations are available for the Literal Mine Cube yet.")
        return

    df_blocks = pd.concat(frames, ignore_index=True)
    if "ts_dt" not in df_blocks.columns:
        df_blocks["ts_dt"] = None
    df_blocks["source_rank"] = df_blocks["source"].map(
        {
            "scout_coverage": 0,
            "scout_border": 1,
            "scout": 2,
            "world_memory": 3,
            "live": 4,
        }
    ).fillna(0)

    boundary_margin = 2
    if region_snapshot is not None and not df_blocks.empty:
        df_blocks = df_blocks[
            df_blocks["block_x"].between(region_snapshot["min_x"] - boundary_margin, region_snapshot["max_x"] + boundary_margin)
            & df_blocks["block_y"].between(region_snapshot["min_y"] - boundary_margin, region_snapshot["max_y"] + boundary_margin)
            & df_blocks["block_z"].between(region_snapshot["min_z"] - boundary_margin, region_snapshot["max_z"] + boundary_margin)
        ].copy()
    if df_blocks.empty:
        st.info("No in-bounds block observations are available for the Literal Mine Cube yet.")
        return

    allowed_block_ids = set(region_snapshot["allowed_block_ids"]) if region_snapshot is not None else set()
    df_blocks["block_class"] = df_blocks["look_block"].apply(
        lambda bid: classify_voxel_block(bid, allowed_block_ids)
    )
    df_blocks = df_blocks.sort_values(["source_rank", "ts_dt"], ascending=[True, True])
    df_blocks = df_blocks.drop_duplicates(subset=["block_x", "block_y", "block_z"], keep="last")

    path_df = pd.DataFrame(raw_records)
    if not path_df.empty:
        step = max(1, len(path_df) // 250)
        sampled_indices = list(range(0, len(path_df), step))
        if sampled_indices[-1] != len(path_df) - 1:
            sampled_indices.append(len(path_df) - 1)
        path_df = path_df.iloc[sampled_indices].copy()
        path_df["path_idx"] = range(len(path_df))

    fig = go.Figure()
    if region_snapshot is not None:
        add_cube_shell(fig, region_snapshot)
    if scout_report is not None and region_snapshot is not None:
        border_x = [
            region_snapshot["min_x"] - 1,
            region_snapshot["max_x"] + 1,
            region_snapshot["max_x"] + 1,
            region_snapshot["min_x"] - 1,
            region_snapshot["min_x"] - 1,
        ]
        border_z = [
            region_snapshot["min_z"] - 1,
            region_snapshot["min_z"] - 1,
            region_snapshot["max_z"] + 1,
            region_snapshot["max_z"] + 1,
            region_snapshot["min_z"] - 1,
        ]
        border_y = [max(region_snapshot["min_y"], region_snapshot["max_y"] - 1)] * len(border_x)
        fig.add_trace(
            go.Scatter3d(
                x=border_x,
                y=border_z,
                z=border_y,
                mode="lines",
                name="Scout memory border",
                line=dict(color="rgba(255,180,70,0.95)", width=8),
                hoverinfo="skip",
            )
        )

    grouped = df_blocks.groupby(["look_block", "block_class"], dropna=False)
    for (block_id, block_class), group in grouped:
        block_name = str(block_id or "minecraft:unknown")
        class_name = str(block_class or "unknown")
        color = get_block_display_color(block_name, class_name)
        marker_size = 2.5
        trace_opacity = 0.65
        if class_name == "air":
            marker_size = 3.5
            trace_opacity = 0.75
        elif class_name == "mineable":
            marker_size = 3.0
            trace_opacity = 0.65
        elif class_name == "non_mineable":
            marker_size = 3.0
            trace_opacity = 0.80
        hover_text = [
            f"{block_name}<br>x={int(x)} y={int(y)} z={int(z)}<br>source={src}"
            for x, y, z, src in zip(group["block_x"], group["block_y"], group["block_z"], group["source"])
        ]
        fig.add_trace(
            go.Scatter3d(
                x=group["block_x"],
                y=group["block_z"],
                z=group["block_y"],
                mode="markers",
                name=block_name,
                marker=dict(size=marker_size, color=color),
                opacity=trace_opacity,
                hovertext=hover_text,
                hoverinfo="text",
            )
        )

    if not path_df.empty:
        fig.add_trace(
            go.Scatter3d(
                x=path_df["x"],
                y=path_df["z"],
                z=path_df["y"],
                mode="lines+markers",
                name="Miner path",
                line=dict(color="rgba(170,20,45,0.55)", width=4),
                marker=dict(
                    size=4,
                    color=path_df["path_idx"],
                    colorscale=[[0.0, "#7f0000"], [1.0, "#ff7ab8"]],
                    opacity=0.9,
                ),
                hoverinfo="skip",
            )
        )

    if latest_pose is not None:
        fig.add_scatter3d(
            x=[float(latest_pose["x"])],
            y=[float(latest_pose["z"])],
            z=[float(latest_pose["y"])],
            mode="markers",
            name="Player",
            marker=dict(size=8, color="#d62728", symbol="circle"),
            hovertext=[
                "Player: "
                f"{float(latest_pose['x']):.2f}, "
                f"{float(latest_pose['y']):.2f}, "
                f"{float(latest_pose['z']):.2f}"
            ],
            hoverinfo="text",
        )

    fig.update_layout(
        scene=dict(
            xaxis_title="X",
            yaxis_title="Z",
            zaxis_title="Y",
            aspectmode="data",
            camera=dict(
                up=dict(x=0, y=0, z=1),
                eye=dict(x=1.45, y=1.45, z=0.95),
            ),
        ),
        margin=dict(l=0, r=0, b=0, t=30),
    )

    st.plotly_chart(fig, use_container_width=True)


def section_literal_mine_cube_live(selected_episode_id: Optional[int]) -> None:
    fragment_fn = getattr(st, "fragment", None)
    if callable(fragment_fn):
        @fragment_fn(run_every=1.5)
        def _render_cube() -> None:
            section_literal_mine_cube_compact(selected_episode_id)

        _render_cube()
        return
    section_literal_mine_cube_compact(selected_episode_id)


# ---------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------


def main():
    st.set_page_config(
        page_title="Minecraft Auto Miner – Controls & Telemetry",
        layout="wide",
    )

    st.title("Minecraft Auto Miner – Controls & Telemetry")
    st.caption(
        "Single-page view over Forge F3 telemetry with runtime controls for mine, scout, calibrate, and manual recording."
    )

    selected_episode_id = st.session_state.get("selected_episode_id")

    # 1) Literal Mine Cube visual
    section_literal_mine_cube_live(selected_episode_id)

    st.markdown("---")

    # 2) Runtime controls
    section_runtime_controls()

    st.markdown("---")

    # 3) Scout intelligence
    section_scout_intelligence()

    st.markdown("---")

    # 4) Live HUD
    section_live_pose()

    st.markdown("---")

    # 5) Episodes & Decision Windows
    section_episodes_and_windows()


if __name__ == "__main__":
    main()
