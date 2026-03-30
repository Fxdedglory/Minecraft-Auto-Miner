"""
minecraft_auto_miner.app v0.7.71 - 2026-03-23

Forge-driven autonomous miner with:
- Fail-closed telemetry handling.
- Deterministic lane-style mining inside configured bounds.
- Recovery learning only at barrier / low-progress decisions.
- Safe user pause when inventory/menu keys are pressed.
- Two-stage entry: walk in, then mine into the region if the mine face stalls progress.
"""

from __future__ import annotations

import argparse
import atexit
from collections import Counter
import ctypes
import json
import logging
import math
import os
import random
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Set, Tuple

import keyboard
import mouse
import psycopg2
from psycopg2.extras import DictCursor

try:
    import pyautogui  # noqa: F401
except Exception:  # pragma: no cover
    pyautogui = None  # type: ignore[assignment]

try:
    import pydirectinput  # type: ignore[import]  # noqa: F401
except Exception:  # pragma: no cover
    pydirectinput = None  # type: ignore[assignment]

from .forge import ForgePoseProvider, Pose
from .world_model_forge import ObstacleInfo, WorldModelForge
from .telemetry import (
    bronze_f3_ingest,
    episodes_from_silver,
    fsm_event_log,
    gold_views,
    silver_f3_compress,
)


@dataclass
class HotkeyConfig:
    start_stop: str = "f8"
    panic_stop: str = "f9"
    perimeter_map: str = "f10"
    manual_record: str = "o"


@dataclass
class AppConfig:
    hotkeys: HotkeyConfig
    tick_interval_sec: float = 0.05
    telemetry_interval_sec: float = 2.0
    control_interval_sec: float = 0.45
    control_calibration_duration_sec: float = 30.0
    stale_pose_timeout_sec: float = 1.5
    navigation_pitch: float = 8.0
    desired_pitch: float = 58.0
    pitch_tolerance: float = 4.0
    heading_tolerance_deg: float = 12.0
    entry_break_in_distance: float = 8.0
    entry_stall_window_sec: float = 0.75
    entry_stall_progress_distance: float = 0.45
    reset_vertical_teleport_distance: float = 16.0
    top_reset_reorient_sec: float = 1.5
    top_reset_activation_margin: float = 3.0
    top_reset_clear_y_drop: float = 4.0
    top_reset_pitch: float = 8.0
    low_progress_window_sec: float = 1.5
    low_progress_distance: float = 0.35
    lane_step_blocks: float = 1.0
    strafe_timeout_sec: float = 1.2
    rotate_timeout_sec: float = 1.6
    recovery_grace_sec: float = 1.5
    recovery_epsilon: float = 0.12
    max_region_mismatch_distance: float = 256.0
    scout_stall_window_sec: float = 0.9
    scout_stall_progress_distance: float = 0.35
    scout_stall_speed_bps: float = 0.35
    scout_min_leg_progress: float = 1.25
    scout_sample_distance: float = 0.75
    deterministic_recovery_scan: bool = True
    blocking_learn_threshold: int = 3
    reacquire_failures_before_shift: int = 5
    floor_guard_blocks: int = 1
    lane_drift_tolerance: float = 0.75
    strict_east_lane_mode: bool = True
    strict_yaw_tolerance_deg: float = 12.0
    strict_calibration_retry_sec: float = 20.0
    strict_reset_command_cooldown_sec: float = 2.5
    strict_soak_timeout_sec: float = 9.0
    strict_soak_correction_timeout_sec: float = 5.5
    strict_soak_pose_distance: float = 0.85
    mining_use_sprint: bool = False
    camera_step_delay_sec: float = 0.03
    camera_settle_delay_sec: float = 0.03
    lane_strafe_pulse_sec: float = 0.08
    lane_strafe_settle_sec: float = 0.03
    manual_record_interval_sec: float = 0.05
    use_sprint: bool = False
    tap_jump_on_shift: bool = True
    inventory_pause_keys: Tuple[str, ...] = ("e", "esc")
    log_level: int = logging.INFO


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if not v:
        return default
    return v.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_key_tuple(name: str, default: Sequence[str]) -> Tuple[str, ...]:
    raw = os.getenv(name)
    if not raw:
        return tuple(default)
    keys = [part.strip().lower() for part in raw.split(",") if part.strip()]
    return tuple(keys) if keys else tuple(default)


def load_basic_config() -> AppConfig:
    return AppConfig(
        hotkeys=HotkeyConfig(
            start_stop="f8",
            panic_stop="f9",
            perimeter_map="f10",
            manual_record="o",
        ),
        tick_interval_sec=_env_float("MAM_TICK_INTERVAL_SEC", 0.05),
        telemetry_interval_sec=_env_float("MAM_TELEMETRY_INTERVAL_SEC", 2.0),
        control_interval_sec=_env_float("MAM_CONTROL_INTERVAL_SEC", 0.45),
        control_calibration_duration_sec=_env_float("MAM_CONTROL_CALIBRATION_DURATION_SEC", 30.0),
        stale_pose_timeout_sec=_env_float("MAM_STALE_POSE_TIMEOUT_SEC", 1.5),
        navigation_pitch=_env_float("MAM_NAVIGATION_PITCH", 8.0),
        desired_pitch=_env_float("MAM_DESIRED_PITCH", 58.0),
        pitch_tolerance=_env_float("MAM_PITCH_TOLERANCE", 4.0),
        heading_tolerance_deg=_env_float("MAM_HEADING_TOLERANCE_DEG", 12.0),
        entry_break_in_distance=_env_float("MAM_ENTRY_BREAK_IN_DISTANCE", 8.0),
        entry_stall_window_sec=_env_float("MAM_ENTRY_STALL_WINDOW_SEC", 0.75),
        entry_stall_progress_distance=_env_float("MAM_ENTRY_STALL_PROGRESS_DISTANCE", 0.45),
        reset_vertical_teleport_distance=_env_float("MAM_RESET_VERTICAL_TELEPORT_DISTANCE", 16.0),
        top_reset_reorient_sec=_env_float("MAM_TOP_RESET_REORIENT_SEC", 1.5),
        top_reset_activation_margin=_env_float("MAM_TOP_RESET_ACTIVATION_MARGIN", 3.0),
        top_reset_clear_y_drop=_env_float("MAM_TOP_RESET_CLEAR_Y_DROP", 4.0),
        top_reset_pitch=_env_float("MAM_TOP_RESET_PITCH", 8.0),
        low_progress_window_sec=_env_float("MAM_LOW_PROGRESS_WINDOW_SEC", 1.5),
        low_progress_distance=_env_float("MAM_LOW_PROGRESS_DISTANCE", 0.35),
        lane_step_blocks=_env_float("MAM_LANE_STEP_BLOCKS", 1.0),
        strafe_timeout_sec=_env_float("MAM_STRAFE_TIMEOUT_SEC", 1.2),
        rotate_timeout_sec=_env_float("MAM_ROTATE_TIMEOUT_SEC", 1.6),
        recovery_grace_sec=_env_float("MAM_RECOVERY_GRACE_SEC", 1.5),
        recovery_epsilon=_env_float("MAM_RECOVERY_EPSILON", 0.12),
        max_region_mismatch_distance=_env_float("MAM_MAX_REGION_MISMATCH_DISTANCE", 256.0),
        scout_stall_window_sec=_env_float("MAM_SCOUT_STALL_WINDOW_SEC", 0.9),
        scout_stall_progress_distance=_env_float("MAM_SCOUT_STALL_PROGRESS_DISTANCE", 0.35),
        scout_stall_speed_bps=_env_float("MAM_SCOUT_STALL_SPEED_BPS", 0.35),
        scout_min_leg_progress=_env_float("MAM_SCOUT_MIN_LEG_PROGRESS", 1.25),
        scout_sample_distance=_env_float("MAM_SCOUT_SAMPLE_DISTANCE", 0.75),
        deterministic_recovery_scan=_env_bool("MAM_DETERMINISTIC_RECOVERY_SCAN", True),
        blocking_learn_threshold=_env_int("MAM_BLOCKING_LEARN_THRESHOLD", 3),
        reacquire_failures_before_shift=_env_int("MAM_REACQUIRE_FAILURES_BEFORE_SHIFT", 5),
        floor_guard_blocks=_env_int("MAM_FLOOR_GUARD_BLOCKS", 1),
        lane_drift_tolerance=_env_float("MAM_LANE_DRIFT_TOLERANCE", 0.75),
        strict_east_lane_mode=_env_bool("MAM_STRICT_EAST_LANE_MODE", True),
        strict_yaw_tolerance_deg=_env_float("MAM_STRICT_YAW_TOLERANCE_DEG", 12.0),
        strict_calibration_retry_sec=_env_float("MAM_STRICT_CALIBRATION_RETRY_SEC", 20.0),
        strict_reset_command_cooldown_sec=_env_float("MAM_STRICT_RESET_COMMAND_COOLDOWN_SEC", 2.5),
        strict_soak_timeout_sec=_env_float("MAM_STRICT_SOAK_TIMEOUT_SEC", 9.0),
        strict_soak_correction_timeout_sec=_env_float("MAM_STRICT_SOAK_CORRECTION_TIMEOUT_SEC", 5.5),
        strict_soak_pose_distance=_env_float("MAM_STRICT_SOAK_POSE_DISTANCE", 0.85),
        mining_use_sprint=_env_bool("MAM_MINING_USE_SPRINT", False),
        camera_step_delay_sec=_env_float("MAM_CAMERA_STEP_DELAY_SEC", 0.03),
        camera_settle_delay_sec=_env_float("MAM_CAMERA_SETTLE_DELAY_SEC", 0.03),
        lane_strafe_pulse_sec=_env_float("MAM_LANE_STRAFE_PULSE_SEC", 0.08),
        lane_strafe_settle_sec=_env_float("MAM_LANE_STRAFE_SETTLE_SEC", 0.03),
        manual_record_interval_sec=_env_float("MAM_MANUAL_RECORD_INTERVAL_SEC", 0.05),
        use_sprint=_env_bool("MAM_USE_SPRINT", False),
        tap_jump_on_shift=_env_bool("MAM_TAP_JUMP_ON_SHIFT", True),
        inventory_pause_keys=_env_key_tuple("MAM_INVENTORY_PAUSE_KEYS", ("e", "esc")),
        log_level=logging.INFO,
    )


def configure_logging(level: int) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | minecraft_auto_miner | %(message)s",
    )


_RUNTIME_CONSOLE_STREAM: Optional[object] = None
_RUNTIME_CONSOLE_CAPTURE_INSTALLED = False
_RUNTIME_CONSOLE_PRIMARY_STDOUT = sys.__stdout__
_RUNTIME_CONSOLE_PRIMARY_STDERR = sys.__stderr__


class TeeTextStream:
    def __init__(self, primary, mirror) -> None:
        self.primary = primary
        self.mirror = mirror

    @property
    def encoding(self) -> str:
        return getattr(self.primary, "encoding", "utf-8")

    def write(self, data: str) -> int:
        text = "" if data is None else str(data)
        if text:
            try:
                self.primary.write(text)
            except Exception:
                pass
            try:
                self.mirror.write(text)
                self.mirror.flush()
            except Exception:
                pass
        return len(text)

    def flush(self) -> None:
        try:
            self.primary.flush()
        except Exception:
            pass
        try:
            self.mirror.flush()
        except Exception:
            pass

    def isatty(self) -> bool:
        return bool(getattr(self.primary, "isatty", lambda: False)())

    def fileno(self) -> int:
        return int(getattr(self.primary, "fileno")())


def install_runtime_console_capture() -> Path:
    global _RUNTIME_CONSOLE_STREAM, _RUNTIME_CONSOLE_CAPTURE_INSTALLED
    log_path = runtime_console_log_path()
    if _RUNTIME_CONSOLE_CAPTURE_INSTALLED:
        return log_path
    log_path.parent.mkdir(parents=True, exist_ok=True)
    mirror = log_path.open("w", encoding="utf-8", buffering=1)
    _RUNTIME_CONSOLE_STREAM = mirror
    sys.stdout = TeeTextStream(_RUNTIME_CONSOLE_PRIMARY_STDOUT, mirror)
    sys.stderr = TeeTextStream(_RUNTIME_CONSOLE_PRIMARY_STDERR, mirror)

    def _close_runtime_console_capture() -> None:
        global _RUNTIME_CONSOLE_STREAM
        stream = _RUNTIME_CONSOLE_STREAM
        if stream is None:
            return
        try:
            sys.stdout = _RUNTIME_CONSOLE_PRIMARY_STDOUT
            sys.stderr = _RUNTIME_CONSOLE_PRIMARY_STDERR
        except Exception:
            pass
        try:
            stream.flush()
            stream.close()
        except Exception:
            pass
        _RUNTIME_CONSOLE_STREAM = None

    atexit.register(_close_runtime_console_capture)
    _RUNTIME_CONSOLE_CAPTURE_INSTALLED = True
    return log_path


@dataclass
class RegionConfig:
    name: str
    dimension: Optional[str]
    min_x: int
    min_y: int
    min_z: int
    max_x: int
    max_y: int
    max_z: int
    allowed_block_ids: Set[str]
    blocking_block_ids: Set[str]
    configured_blocking_block_ids: Set[str] = field(default_factory=set)
    learned_blocking_block_ids: Set[str] = field(default_factory=set)


@dataclass(frozen=True)
class MiningPlan:
    name: str
    pitch_sequence: Tuple[float, ...]
    preferred_shift_direction: int
    reacquire_failures_before_shift: int
    rationale: str


def learned_blocking_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "learned_blocking_block_ids.json"


def control_calibration_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "control_calibration"


def control_calibration_last_run_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "control_calibration_last_run.json"


def control_calibration_profile_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "control_calibration_profile.json"


def control_calibration_memory_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "control_calibration_memory.json"


def perimeter_scout_last_run_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "perimeter_scout_last_run.json"


def perimeter_scout_memory_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "perimeter_scout_memory.json"


def voxel_world_memory_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "voxel_world_memory.json"


def strategy_stats_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "strategy_stats.json"


def runtime_console_log_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "runtime_console_latest.log"


def dashboard_control_command_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "dashboard_control_command.json"


def dashboard_control_status_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "dashboard_control_status.json"


def dashboard_runtime_status_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "dashboard_runtime_status.json"


def _load_json_dict(path: Path) -> Optional[dict[str, object]]:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


def _empty_region_memory_snapshot() -> dict[str, object]:
    return {"version": "1.0", "regions": {}}


def _region_memory_key(region: Optional[RegionConfig]) -> str:
    if region is not None and region.name:
        return region.name
    return "__default__"


def load_control_calibration_memory() -> dict[str, object]:
    raw = _load_json_dict(control_calibration_memory_path())
    if not isinstance(raw, dict):
        return _empty_region_memory_snapshot()
    regions = raw.get("regions")
    if not isinstance(regions, dict):
        raw["regions"] = {}
    return raw


def load_perimeter_scout_memory() -> dict[str, object]:
    raw = _load_json_dict(perimeter_scout_memory_path())
    if not isinstance(raw, dict):
        return _empty_region_memory_snapshot()
    regions = raw.get("regions")
    if not isinstance(regions, dict):
        raw["regions"] = {}
    return raw


def load_voxel_world_memory() -> dict[str, object]:
    raw = _load_json_dict(voxel_world_memory_path())
    if not isinstance(raw, dict):
        return _empty_region_memory_snapshot()
    regions = raw.get("regions")
    if not isinstance(regions, dict):
        raw["regions"] = {}
    return raw


def load_strategy_stats_snapshot() -> dict[str, object]:
    raw = _load_json_dict(strategy_stats_path())
    if not isinstance(raw, dict):
        return {"version": "2.0", "patterns": {}, "episodes": []}
    if "patterns" in raw and isinstance(raw.get("patterns"), dict):
        if not isinstance(raw.get("episodes"), list):
            raw["episodes"] = []
        return raw
    legacy_summary = {
        str(name): value
        for name, value in raw.items()
        if isinstance(name, str) and isinstance(value, dict)
    }
    return {
        "version": "2.0",
        "patterns": {},
        "episodes": [],
        "legacy_summary": legacy_summary,
    }


def save_region_memory_snapshot(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_control_calibration_profile(
    logger: logging.Logger,
    *,
    region: Optional[RegionConfig] = None,
) -> Optional[dict[str, object]]:
    path = control_calibration_profile_path()
    if not path.exists():
        raw = None
    else:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to load control calibration profile: %s", exc)
            raw = None
    if isinstance(raw, dict):
        validation = raw.get("validation")
        if not (isinstance(validation, dict) and not bool(validation.get("passed", False))):
            logger.info("Loaded control calibration profile from %s", path)
            return raw
        logger.warning(
            "Ignoring invalid control calibration profile at %s because validation.passed is false.",
            path,
        )
    elif raw is not None:
        logger.warning("Ignoring malformed control calibration profile at %s", path)

    memory = load_control_calibration_memory()
    regions = memory.get("regions")
    if not isinstance(regions, dict):
        return None
    entry = regions.get(_region_memory_key(region))
    if not isinstance(entry, dict):
        entry = regions.get("__default__")
    if not isinstance(entry, dict):
        return None
    fallback_profile = entry.get("last_success_profile")
    if not isinstance(fallback_profile, dict):
        return None
    validation = fallback_profile.get("validation")
    if isinstance(validation, dict) and not bool(validation.get("passed", False)):
        return None
    try:
        logger.info(
            "Loaded control calibration profile fallback from %s for region '%s'.",
            control_calibration_memory_path(),
            _region_memory_key(region),
        )
    except Exception:
        pass
    return fallback_profile


def load_region_memory_entry(path: Path, region: Optional[RegionConfig]) -> Optional[dict[str, object]]:
    snapshot = _load_json_dict(path)
    if not isinstance(snapshot, dict):
        return None
    regions = snapshot.get("regions")
    if not isinstance(regions, dict):
        return None
    entry = regions.get(_region_memory_key(region))
    return entry if isinstance(entry, dict) else None


def load_control_segment_averages(region: Optional[RegionConfig]) -> dict[str, dict[str, object]]:
    entry = load_region_memory_entry(control_calibration_memory_path(), region)
    if not isinstance(entry, dict):
        return {}
    segment_averages = entry.get("segment_averages")
    if not isinstance(segment_averages, dict):
        return {}
    return {
        str(name): meta
        for name, meta in segment_averages.items()
        if isinstance(name, str) and isinstance(meta, dict)
    }


def _voxel_point_key(x: int, y: int, z: int) -> str:
    return f"{int(x)},{int(y)},{int(z)}"


def _default_region_mineable_block_id(region: Optional[RegionConfig]) -> str:
    if region is not None and region.allowed_block_ids:
        if "minecraft:stone" in region.allowed_block_ids:
            return "minecraft:stone"
        return sorted(region.allowed_block_ids)[0]
    return "minecraft:stone"


def _iter_xz_line_cells(start_x: float, start_z: float, end_x: float, end_z: float) -> list[tuple[int, int]]:
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


class VoxelWorldMemory:
    def __init__(self, logger: logging.Logger, region: Optional[RegionConfig]) -> None:
        self.logger = logger.getChild("voxel_world_memory")
        self.region = region
        self._path = voxel_world_memory_path()
        self._snapshot = load_voxel_world_memory()
        self._dirty = False
        self._last_save_mono = 0.0
        self._save_interval_sec = 0.75

    def _configured_region_payload(self) -> Optional[dict[str, object]]:
        if self.region is None:
            return None
        return {
            "name": self.region.name,
            "dimension": self.region.dimension,
            "min": {"x": self.region.min_x, "y": self.region.min_y, "z": self.region.min_z},
            "max": {"x": self.region.max_x, "y": self.region.max_y, "z": self.region.max_z},
            "allowed_block_ids": sorted(self.region.allowed_block_ids),
            "blocking_block_ids": sorted(self.region.blocking_block_ids),
        }

    def _entry(self) -> dict[str, object]:
        regions = self._snapshot.setdefault("regions", {})
        if not isinstance(regions, dict):
            self._snapshot["regions"] = {}
            regions = self._snapshot["regions"]
        region_key = _region_memory_key(self.region)
        entry = regions.get(region_key)
        if not isinstance(entry, dict):
            entry = {}
            regions[region_key] = entry
        if not isinstance(entry.get("voxels"), dict):
            entry["voxels"] = {}
        if not isinstance(entry.get("configured_region"), dict):
            entry["configured_region"] = self._configured_region_payload()
        entry["default_mineable_block_id"] = str(
            entry.get("default_mineable_block_id") or _default_region_mineable_block_id(self.region)
        )
        entry["region_name"] = region_key
        return entry

    def maybe_flush(self, *, force: bool = False) -> None:
        if not self._dirty:
            return
        now = time.monotonic()
        if not force and (now - self._last_save_mono) < self._save_interval_sec:
            return
        entry = self._entry()
        entry["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        try:
            save_region_memory_snapshot(self._path, self._snapshot)
        except Exception as exc:
            self.logger.warning("Failed to save voxel world memory: %s", exc)
            return
        self._dirty = False
        self._last_save_mono = now

    def _classify_state(
        self,
        *,
        block_id: str,
        block_class: str,
        block_x: int,
        block_y: int,
        block_z: int,
    ) -> Optional[str]:
        in_bounds = is_point_in_region(block_x, block_y, block_z, self.region)
        on_shell = False
        if self.region is not None:
            on_shell = (
                block_x in {self.region.min_x, self.region.max_x}
                or block_y in {self.region.min_y, self.region.max_y}
                or block_z in {self.region.min_z, self.region.max_z}
            )
        if not in_bounds:
            if block_class == "non_mineable" or block_id.endswith("_wool") or "barrier" in block_id:
                return "border"
            return None
        if block_class == "mineable":
            return "mineable_unmined"
        if block_class == "air":
            return "mineable_mined_air"
        if block_class == "non_mineable":
            return "border" if on_shell or block_id.endswith("_wool") or "barrier" in block_id else "non_mineable"
        return None

    def _upsert_voxel(
        self,
        *,
        block_x: int,
        block_y: int,
        block_z: int,
        state: str,
        block_id: str,
        source: str,
    ) -> None:
        entry = self._entry()
        voxels = entry.get("voxels")
        if not isinstance(voxels, dict):
            entry["voxels"] = {}
            voxels = entry["voxels"]
        key = _voxel_point_key(block_x, block_y, block_z)
        existing = voxels.get(key)
        existing_state = str(existing.get("state") or "") if isinstance(existing, dict) else ""
        if existing_state == "border" and state != "border":
            resolved_state = "border"
            resolved_block_id = str(existing.get("block_id") or block_id) if isinstance(existing, dict) else block_id
        else:
            resolved_state = state
            if state == "mineable_unmined":
                resolved_block_id = str(entry.get("default_mineable_block_id") or _default_region_mineable_block_id(self.region))
            elif state == "mineable_mined_air":
                resolved_block_id = "minecraft:air"
            else:
                resolved_block_id = block_id
        payload = {
            "x": int(block_x),
            "y": int(block_y),
            "z": int(block_z),
            "state": resolved_state,
            "block_id": resolved_block_id,
            "source": source,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        if payload != existing:
            voxels[key] = payload
            self._dirty = True

    def observe_obstacle(self, obstacle: ObstacleInfo, *, source: str) -> None:
        if obstacle.block_x is None or obstacle.block_y is None or obstacle.block_z is None:
            return
        block_id = str(obstacle.block_id or "").strip().lower()
        if not block_id:
            return
        block_class = classify_observed_look_block(block_id, self.region)
        state = self._classify_state(
            block_id=block_id,
            block_class=block_class,
            block_x=int(obstacle.block_x),
            block_y=int(obstacle.block_y),
            block_z=int(obstacle.block_z),
        )
        if state is None:
            return
        self._upsert_voxel(
            block_x=int(obstacle.block_x),
            block_y=int(obstacle.block_y),
            block_z=int(obstacle.block_z),
            state=state,
            block_id=block_id,
            source=source,
        )
        self.maybe_flush()

    def note_reset(self, *, pose: Optional[Pose] = None) -> None:
        entry = self._entry()
        voxels = entry.get("voxels")
        if not isinstance(voxels, dict):
            return
        changed = False
        keys_to_delete: list[str] = []
        for key, voxel in voxels.items():
            if not isinstance(voxel, dict):
                continue
            state = str(voxel.get("state") or "")
            if state not in {"mineable_mined_air", "mineable_unmined"}:
                continue
            keys_to_delete.append(str(key))
            changed = True
        for key in keys_to_delete:
            voxels.pop(key, None)
        entry["reset_count"] = int(entry.get("reset_count", 0) or 0) + 1
        entry["last_reset_at_utc"] = datetime.now(timezone.utc).isoformat()
        if pose is not None:
            entry["last_reset_pose"] = {
                "x": round(pose.x, 3),
                "y": round(pose.y, 3),
                "z": round(pose.z, 3),
                "yaw": round(normalize_yaw(pose.yaw), 3),
                "pitch": round(pose.pitch, 3),
                "ts_utc": pose.ts_utc,
            }
        self._dirty = self._dirty or changed or True
        self.maybe_flush(force=True)

    def ingest_scout_report(self, report: Optional[dict[str, object]]) -> None:
        if not isinstance(report, dict):
            return
        observation_samples = report.get("observation_samples")
        if isinstance(observation_samples, list):
            for sample in observation_samples:
                if not isinstance(sample, dict):
                    continue
                block_x = _coerce_int(sample.get("block_x"))
                block_y = _coerce_int(sample.get("block_y"))
                block_z = _coerce_int(sample.get("block_z"))
                if block_x is None or block_y is None or block_z is None:
                    continue
                block_id = str(sample.get("look_block") or "minecraft:air").strip().lower()
                block_class = str(sample.get("look_block_class") or classify_observed_look_block(block_id, self.region)).strip().lower()
                state = self._classify_state(
                    block_id=block_id,
                    block_class=block_class,
                    block_x=block_x,
                    block_y=block_y,
                    block_z=block_z,
                )
                if state is None:
                    continue
                self._upsert_voxel(
                    block_x=block_x,
                    block_y=block_y,
                    block_z=block_z,
                    state=state,
                    block_id=block_id,
                    source="scout",
                )

        if self.region is not None:
            trace_points = report.get("trace_points")
            usable_trace_points = [
                point
                for point in trace_points
                if isinstance(point, dict)
                and isinstance(point.get("x"), (int, float))
                and isinstance(point.get("z"), (int, float))
            ] if isinstance(trace_points, list) else []
            coverage_y = int(self.region.max_y)
            for idx, point in enumerate(usable_trace_points):
                next_point = usable_trace_points[idx + 1] if idx + 1 < len(usable_trace_points) else None
                if next_point is None:
                    cells = _iter_xz_line_cells(float(point["x"]), float(point["z"]), float(point["x"]), float(point["z"]))
                else:
                    cells = _iter_xz_line_cells(
                        float(point["x"]),
                        float(point["z"]),
                        float(next_point["x"]),
                        float(next_point["z"]),
                    )
                for block_x, block_z in cells:
                    if not (self.region.min_x <= block_x <= self.region.max_x and self.region.min_z <= block_z <= self.region.max_z):
                        continue
                    self._upsert_voxel(
                        block_x=block_x,
                        block_y=coverage_y,
                        block_z=block_z,
                        state="mineable_unmined",
                        block_id=_default_region_mineable_block_id(self.region),
                        source="scout_coverage",
                    )

            border_y = max(int(self.region.min_y), int(self.region.max_y) - 1)
            outer_x_min = int(self.region.min_x) - 1
            outer_x_max = int(self.region.max_x) + 1
            outer_z_min = int(self.region.min_z) - 1
            outer_z_max = int(self.region.max_z) + 1
            for block_x in range(outer_x_min, outer_x_max + 1):
                self._upsert_voxel(
                    block_x=block_x,
                    block_y=border_y,
                    block_z=outer_z_min,
                    state="border",
                    block_id="minecraft:red_wool",
                    source="scout_border",
                )
                self._upsert_voxel(
                    block_x=block_x,
                    block_y=border_y,
                    block_z=outer_z_max,
                    state="border",
                    block_id="minecraft:red_wool",
                    source="scout_border",
                )
            for block_z in range(outer_z_min, outer_z_max + 1):
                self._upsert_voxel(
                    block_x=outer_x_min,
                    block_y=border_y,
                    block_z=block_z,
                    state="border",
                    block_id="minecraft:red_wool",
                    source="scout_border",
                )
                self._upsert_voxel(
                    block_x=outer_x_max,
                    block_y=border_y,
                    block_z=block_z,
                    state="border",
                    block_id="minecraft:red_wool",
                    source="scout_border",
                )
        self.maybe_flush(force=True)

    def look_type_for_obstacle(self, obstacle: ObstacleInfo) -> Optional[str]:
        if self.region is None:
            return None
        if obstacle.block_x is None or obstacle.block_y is None or obstacle.block_z is None:
            return None
        block_x = int(obstacle.block_x)
        block_y = int(obstacle.block_y)
        block_z = int(obstacle.block_z)
        if not is_point_in_region(block_x, block_y, block_z, self.region):
            return "BLOCKING"
        entry = self._entry()
        voxels = entry.get("voxels")
        if not isinstance(voxels, dict):
            return None
        voxel = voxels.get(_voxel_point_key(block_x, block_y, block_z))
        if not isinstance(voxel, dict):
            return None
        state = str(voxel.get("state") or "")
        block_id = str(obstacle.block_id or "").strip().lower()
        if state in {"border", "non_mineable"}:
            return "BLOCKING"
        if state == "mineable_mined_air" and block_id == "minecraft:air":
            return "AIR"
        return None


def load_learned_blocking_snapshot() -> dict[str, object]:
    path = learned_blocking_path()
    if not path.exists():
        return {"version": "1.0", "regions": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": "1.0", "regions": {}}


def get_telemetry_conn():
    db_name = os.getenv("MAM_TELEMETRY_DB") or os.getenv("POSTGRES_DB", "mam_telemetry")
    user = os.getenv("MAM_TELEMETRY_USER") or os.getenv("POSTGRES_USER", "mam_user")
    password = os.getenv("MAM_TELEMETRY_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "postgres")
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


def load_region_config() -> Optional[RegionConfig]:
    logger = logging.getLogger("minecraft_auto_miner")
    try:
        root = Path(__file__).resolve().parents[2]
        path = root / "data" / "mine_bounds.json"
        if not path.exists():
            logger.warning(
                "mine_bounds.json not found at %s; autonomous region control is disabled.",
                path,
            )
            return None

        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)

        regions = raw.get("regions") or []
        if not regions:
            logger.warning("mine_bounds.json has no regions; autonomous region control is disabled.")
            return None

        r0 = regions[0]
        min_obj = r0.get("min", {})
        max_obj = r0.get("max", {})
        configured_blocking = set(str(b).lower() for b in r0.get("blocking_block_ids", []))
        learned_snapshot = load_learned_blocking_snapshot()
        learned_regions = learned_snapshot.get("regions") or {}
        learned_region = learned_regions.get(str(r0.get("name", "region0"))) or {}
        learned_blocking = set(
            str(b).lower() for b in learned_region.get("learned_blocking_block_ids", [])
        )

        cfg = RegionConfig(
            name=str(r0.get("name", "region0")),
            dimension=str(r0.get("dimension")) if r0.get("dimension") else None,
            min_x=int(min_obj.get("x", 0)),
            min_y=int(min_obj.get("y", 0)),
            min_z=int(min_obj.get("z", 0)),
            max_x=int(max_obj.get("x", 0)),
            max_y=int(max_obj.get("y", 0)),
            max_z=int(max_obj.get("z", 0)),
            allowed_block_ids=set(str(b).lower() for b in r0.get("allowed_block_ids", [])),
            blocking_block_ids=configured_blocking | learned_blocking,
            configured_blocking_block_ids=configured_blocking,
            learned_blocking_block_ids=learned_blocking,
        )

        logger.info(
            "Loaded region '%s' bounds: x=[%d,%d], y=[%d,%d], z=[%d,%d], dimension=%s",
            cfg.name,
            cfg.min_x,
            cfg.max_x,
            cfg.min_y,
            cfg.max_y,
            cfg.min_z,
            cfg.max_z,
            cfg.dimension or "<any>",
        )
        if cfg.learned_blocking_block_ids:
            logger.info(
                "Loaded %d learned blocking materials for region '%s': %s",
                len(cfg.learned_blocking_block_ids),
                cfg.name,
                ", ".join(sorted(cfg.learned_blocking_block_ids)),
            )
        return cfg
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to load mine_bounds.json: %s", exc, exc_info=True)
        return None


def is_point_in_region(x: int, y: int, z: int, region: Optional[RegionConfig]) -> bool:
    if region is None:
        return True
    return (
        region.min_x <= x <= region.max_x
        and region.min_y <= y <= region.max_y
        and region.min_z <= z <= region.max_z
    )


def is_pose_in_region(pose: Optional[Pose], region: Optional[RegionConfig]) -> bool:
    if pose is None:
        return False
    if region is not None and region.dimension and pose.dimension and pose.dimension != region.dimension:
        return False
    return is_point_in_region(
        int(math.floor(pose.x)),
        int(math.floor(pose.y)),
        int(math.floor(pose.z)),
        region,
    )


def distance_from_pose_to_region(pose: Optional[Pose], region: Optional[RegionConfig]) -> float:
    if pose is None or region is None:
        return 0.0

    dx = 0.0
    if pose.x < region.min_x:
        dx = region.min_x - pose.x
    elif pose.x > region.max_x:
        dx = pose.x - region.max_x

    dy = 0.0
    if pose.y < region.min_y:
        dy = region.min_y - pose.y
    elif pose.y > region.max_y:
        dy = pose.y - region.max_y

    dz = 0.0
    if pose.z < region.min_z:
        dz = region.min_z - pose.z
    elif pose.z > region.max_z:
        dz = pose.z - region.max_z

    return math.sqrt(dx * dx + dy * dy + dz * dz)


def is_block_in_bounds(obstacle: ObstacleInfo, region: Optional[RegionConfig]) -> bool:
    if region is None:
        return True
    if obstacle.block_x is None or obstacle.block_y is None or obstacle.block_z is None:
        return False
    return is_point_in_region(obstacle.block_x, obstacle.block_y, obstacle.block_z, region)


def classify_block_type(obstacle: ObstacleInfo, region: Optional[RegionConfig]) -> str:
    bid = (obstacle.block_id or "").lower()
    if not bid:
        return "UNKNOWN"

    if bid == "minecraft:air":
        return "AIR"

    if region:
        if bid in region.allowed_block_ids:
            return "ALLOWED"
        if bid in region.blocking_block_ids:
            return "BLOCKING"
        if region.allowed_block_ids:
            # When the region explicitly lists mineable blocks, treat every
            # other solid block as non-mineable for this mine stage.
            return "BLOCKING"

    if bid.endswith(":stone") or bid.endswith("_stone"):
        return "ALLOWED"
    if bid.endswith("_wool") or "glass" in bid or "barrier" in bid:
        return "BLOCKING"
    return "OTHER"


def classify_observed_look_block(block_id: object, region: Optional[RegionConfig]) -> str:
    bid = str(block_id or "").strip().lower()
    if not bid:
        return "unknown"
    if bid == "minecraft:air":
        return "air"
    if region is not None and region.allowed_block_ids:
        return "mineable" if bid in region.allowed_block_ids else "non_mineable"
    if bid.endswith(":stone") or bid.endswith("_stone"):
        return "mineable"
    return "non_mineable"


def _coerce_int(value: object) -> Optional[int]:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def derive_vertical_look_calibration(profile: Optional[dict[str, object]]) -> Optional[dict[str, float | int]]:
    if not isinstance(profile, dict):
        return None

    strict_profile = profile.get("strict_mouse_calibration")
    if isinstance(strict_profile, dict):
        try:
            down_deg = abs(float(strict_profile.get("pitch_step_down_deg") or 0.0))
            up_deg = abs(float(strict_profile.get("pitch_step_up_deg") or 0.0))
            step_pixels = max(1, int(strict_profile.get("pitch_step_pixels") or 0))
            deg_per_pixel = float(strict_profile.get("pitch_deg_per_pixel") or 0.0)
        except (TypeError, ValueError):
            down_deg = 0.0
            up_deg = 0.0
            step_pixels = 0
            deg_per_pixel = 0.0
        if step_pixels > 0 and deg_per_pixel > 0.0 and down_deg > 0.0 and up_deg > 0.0:
            return {
                "pitch_step_down_deg": down_deg,
                "pitch_step_up_deg": up_deg,
                "pitch_step_pixels": step_pixels,
                "pitch_deg_per_pixel": deg_per_pixel,
            }

    look_metrics = profile.get("look_metrics")
    if not isinstance(look_metrics, dict):
        return None

    look_down = look_metrics.get("look_down")
    look_up = look_metrics.get("look_up")
    if not isinstance(look_down, dict) or not isinstance(look_up, dict):
        return None

    try:
        down_deg = abs(float(look_down.get("delta_pitch") or 0.0))
        up_deg = abs(float(look_up.get("delta_pitch") or 0.0))
    except (TypeError, ValueError):
        return None
    if down_deg <= 0.0 or up_deg <= 0.0:
        return None

    step_pixels = 8
    deg_per_pixel = ((down_deg / step_pixels) + (up_deg / step_pixels)) / 2.0
    if deg_per_pixel <= 0.0:
        return None
    return {
        "pitch_step_down_deg": down_deg,
        "pitch_step_up_deg": up_deg,
        "pitch_step_pixels": step_pixels,
        "pitch_deg_per_pixel": deg_per_pixel,
    }


class MinerState(str, Enum):
    IDLE = "IDLE"
    AUTONOMOUS = "AUTONOMOUS"
    SCOUT = "SCOUT"
    CALIBRATION = "CALIBRATION"
    MANUAL_RECORDING = "MANUAL_RECORDING"
    PAUSED = "PAUSED"


class MinerAction(str, Enum):
    NONE = "NONE"
    WAIT_FOR_TELEMETRY = "WAIT_FOR_TELEMETRY"
    CALIBRATION_TEST = "CALIBRATION_TEST"
    CALIBRATION_COMPLETE = "CALIBRATION_COMPLETE"
    NAVIGATE_TO_REGION = "NAVIGATE_TO_REGION"
    REORIENT_AFTER_RESET = "REORIENT_AFTER_RESET"
    FORWARD_MINE = "FORWARD_MINE"
    REACQUIRE_STONE = "REACQUIRE_STONE"
    SCOUT_APPROACH_FACE = "SCOUT_APPROACH_FACE"
    SCOUT_TRACE_SOUTH = "SCOUT_TRACE_SOUTH"
    SCOUT_TRACE_EAST = "SCOUT_TRACE_EAST"
    SCOUT_TRACE_NORTH = "SCOUT_TRACE_NORTH"
    SCOUT_TRACE_WEST = "SCOUT_TRACE_WEST"
    SCOUT_COMPLETE = "SCOUT_COMPLETE"
    MANUAL_RECORDING = "MANUAL_RECORDING"
    SHIFT_POSITIVE = "SHIFT_POSITIVE"
    SHIFT_NEGATIVE = "SHIFT_NEGATIVE"
    RESET_GMINE = "RESET_GMINE"
    STOP_ALL = "STOP_ALL"
    USER_PAUSED = "USER_PAUSED"


class Cardinal(str, Enum):
    NORTH = "NORTH"
    SOUTH = "SOUTH"
    EAST = "EAST"
    WEST = "WEST"


CARDINAL_YAWS: Dict[Cardinal, float] = {
    Cardinal.SOUTH: 0.0,
    Cardinal.WEST: 90.0,
    Cardinal.NORTH: 180.0,
    Cardinal.EAST: -90.0,
}


def normalize_yaw(yaw: float) -> float:
    value = yaw
    while value <= -180.0:
        value += 360.0
    while value > 180.0:
        value -= 360.0
    return value


def yaw_delta(current_yaw: float, target_yaw: float) -> float:
    return normalize_yaw(target_yaw - current_yaw)


def heading_sign(heading: Cardinal) -> float:
    return 1.0 if heading in {Cardinal.EAST, Cardinal.SOUTH} else -1.0


def yaw_to_cardinal(yaw: float) -> Cardinal:
    best = Cardinal.SOUTH
    best_abs = float("inf")
    for heading, target_yaw in CARDINAL_YAWS.items():
        delta = abs(yaw_delta(yaw, target_yaw))
        if delta < best_abs:
            best = heading
            best_abs = delta
    return best


def opposite_heading(heading: Cardinal) -> Cardinal:
    if heading == Cardinal.NORTH:
        return Cardinal.SOUTH
    if heading == Cardinal.SOUTH:
        return Cardinal.NORTH
    if heading == Cardinal.EAST:
        return Cardinal.WEST
    return Cardinal.EAST


def left_heading(heading: Cardinal) -> Cardinal:
    idx = CARDINAL_CLOCKWISE.index(heading)
    return CARDINAL_CLOCKWISE[(idx - 1) % len(CARDINAL_CLOCKWISE)]


def right_heading(heading: Cardinal) -> Cardinal:
    idx = CARDINAL_CLOCKWISE.index(heading)
    return CARDINAL_CLOCKWISE[(idx + 1) % len(CARDINAL_CLOCKWISE)]


CARDINAL_CLOCKWISE: Tuple[Cardinal, ...] = (
    Cardinal.NORTH,
    Cardinal.EAST,
    Cardinal.SOUTH,
    Cardinal.WEST,
)


def movement_key_for_reference(reference: Cardinal, desired: Cardinal) -> str:
    reference_index = CARDINAL_CLOCKWISE.index(reference)
    desired_index = CARDINAL_CLOCKWISE.index(desired)
    delta = (desired_index - reference_index) % 4
    return {
        0: "w",
        1: "d",
        2: "s",
        3: "a",
    }[delta]


@dataclass
class MinerRuntimeState:
    mining_enabled: bool = False
    mapping_enabled: bool = False
    calibration_enabled: bool = False
    manual_recording_enabled: bool = False
    user_paused: bool = False
    should_exit: bool = False
    state_name: MinerState = MinerState.IDLE
    last_action: MinerAction = MinerAction.NONE
    pause_hotkey_suppress_until: float = 0.0
    pause_key_state: Dict[str, bool] = field(default_factory=dict)
    control_key_state: Dict[str, bool] = field(default_factory=dict)


class SimpleInputController:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self._held_keys: Set[str] = set()
        self._mining_down = False
        self._stop_keys: Tuple[str, ...] = ("w", "a", "s", "d", "ctrl", "shift", "space")
        self._win32_mouse_available = bool(getattr(ctypes, "windll", None) and getattr(ctypes.windll, "user32", None))

        if self._win32_mouse_available:
            self._mouse_backend = "win32"
        elif pydirectinput is not None:
            self._mouse_backend = "pydirectinput"
        elif pyautogui is not None:
            self._mouse_backend = "pyautogui"
        else:
            self._mouse_backend = "mouse"
        logger.info("SimpleInputController using mouse backend: %s", self._mouse_backend)

    def get_mouse_backend(self) -> str:
        return self._mouse_backend

    def get_available_mouse_backends(self) -> Tuple[str, ...]:
        backends: list[str] = []
        if self._win32_mouse_available:
            backends.append("win32")
        if pydirectinput is not None:
            backends.append("pydirectinput")
        if pyautogui is not None:
            backends.append("pyautogui")
        backends.append("mouse")
        return tuple(dict.fromkeys(backends))

    def set_mouse_backend(self, backend: str) -> bool:
        available = self.get_available_mouse_backends()
        if backend not in available:
            return False
        if backend == self._mouse_backend:
            return True
        self._mouse_backend = backend
        self.logger.info("SimpleInputController switched mouse backend to: %s", backend)
        return True

    def hold_key(self, key: str) -> None:
        if key in self._held_keys:
            return
        keyboard.press(key)
        self._held_keys.add(key)

    def release_key(self, key: str) -> None:
        if key not in self._held_keys:
            return
        keyboard.release(key)
        self._held_keys.discard(key)

    def pulse_key(self, key: str, seconds: float) -> None:
        self.hold_key(key)
        time.sleep(seconds)
        self.release_key(key)

    def tap_key(self, key: str, seconds: float = 0.03) -> None:
        self.pulse_key(key, seconds)

    def double_tap_key(self, key: str, *, tap_seconds: float = 0.03, between_taps_sec: float = 0.06) -> None:
        self.tap_key(key, tap_seconds)
        time.sleep(max(0.0, between_taps_sec))
        self.tap_key(key, tap_seconds)

    def press_and_release_key(self, key: str) -> None:
        try:
            keyboard.press_and_release(key)
        except Exception:
            self.tap_key(key, 0.02)

    def double_press_and_release_key(self, key: str, *, between_taps_sec: float = 0.08) -> None:
        self.press_and_release_key(key)
        time.sleep(max(0.0, between_taps_sec))
        self.press_and_release_key(key)

    def hold_forward(self, sprint: bool = False) -> None:
        self.hold_key("w")
        if sprint:
            self.hold_key("ctrl")
        else:
            self.release_key("ctrl")

    def start_mining(self) -> None:
        if self._mining_down:
            return
        mouse.press(button="left")
        self._mining_down = True

    def stop_mining(self) -> None:
        if not self._mining_down:
            return
        mouse.release(button="left")
        self._mining_down = False

    def tap_jump(self, seconds: float = 0.08) -> None:
        self.tap_key("space", seconds)

    def toggle_fly_mode(self, jump_seconds: float = 0.06, between_taps_sec: float = 0.08) -> None:
        self.all_stop()
        self.double_tap_key("space", tap_seconds=max(0.02, jump_seconds * 0.5), between_taps_sec=max(0.03, between_taps_sec))

    def enable_fly_mode(self, attempt_idx: int = 1) -> None:
        # Enabling flight is most reliable as a quick double-jump, but the
        # exact tap spacing can vary slightly across environments.
        profiles = (
            (0.035, 0.045),
            (0.035, 0.065),
            (0.040, 0.080),
        )
        profile_idx = min(max(1, int(attempt_idx)), len(profiles)) - 1
        tap_seconds, between_taps_sec = profiles[profile_idx]
        self.all_stop()
        self.double_tap_key("space", tap_seconds=tap_seconds, between_taps_sec=between_taps_sec)

    def disable_fly_mode(self, attempt_idx: int = 1) -> None:
        # Disabling flight also benefits from discrete taps, but with a slightly
        # longer spacing than fly-enable.
        profiles = (
            (0.035, 0.065),
            (0.040, 0.085),
        )
        profile_idx = min(max(1, int(attempt_idx)), len(profiles)) - 1
        tap_seconds, between_taps_sec = profiles[profile_idx]
        self.all_stop()
        self.double_tap_key("space", tap_seconds=tap_seconds, between_taps_sec=between_taps_sec)

    def hold_backward(self) -> None:
        self.hold_key("s")
        self.release_key("ctrl")

    def hold_strafe_left(self) -> None:
        self.hold_key("a")

    def hold_strafe_right(self) -> None:
        self.hold_key("d")

    def hold_fly_up(self) -> None:
        self.hold_key("space")

    def hold_fly_down(self) -> None:
        self.hold_key("shift")

    def hold_crouch(self) -> None:
        self.hold_key("shift")

    def stop_vertical_motion(self) -> None:
        self.release_key("space")
        self.release_key("shift")

    def _move_mouse(self, dx: int, dy: int) -> None:
        try:
            if self._mouse_backend == "win32" and self._win32_mouse_available:
                ctypes.windll.user32.mouse_event(0x0001, int(dx), int(dy), 0, 0)
            elif self._mouse_backend == "pydirectinput" and pydirectinput is not None:
                pydirectinput.moveRel(dx, dy, duration=0)
            elif self._mouse_backend == "pyautogui" and pyautogui is not None:
                pyautogui.moveRel(dx, dy, duration=0)
            else:
                x, y = mouse.get_position()
                mouse.move(x + dx, y + dy, absolute=True, duration=0)
        except Exception:
            pass

    def look(self, dx: int, dy: int, *, steps: int = 1, delay: float = 0.005) -> None:
        for _ in range(max(1, steps)):
            self._move_mouse(dx, dy)
            if delay > 0:
                time.sleep(delay)

    def look_down_small(self, steps: int = 6, step_pixels: int = 5, delay: float = 0.005) -> None:
        self.look(0, step_pixels, steps=steps, delay=delay)

    def look_up_small(self, steps: int = 6, step_pixels: int = 5, delay: float = 0.005) -> None:
        self.look(0, -step_pixels, steps=steps, delay=delay)

    def look_left_small(self, steps: int = 6, step_pixels: int = 5, delay: float = 0.005) -> None:
        self.look(-step_pixels, 0, steps=steps, delay=delay)

    def look_right_small(self, steps: int = 6, step_pixels: int = 5, delay: float = 0.005) -> None:
        self.look(step_pixels, 0, steps=steps, delay=delay)

    def _slow_press(self, key: str, delay: float = 0.5) -> None:
        try:
            if len(key) == 1 and key.isprintable():
                keyboard.write(key)
            else:
                keyboard.send(key)
        except Exception:
            pass
        time.sleep(delay)

    def go_to_gmine(self) -> None:
        self.all_stop()
        for key in ("t", "/", "g", "m", "i", "n", "e", "enter"):
            self._slow_press(key, delay=0.12)

    def all_stop(self) -> None:
        for key in tuple(self._held_keys):
            keyboard.release(key)
        for key in self._stop_keys:
            try:
                keyboard.release(key)
            except Exception:
                pass
        self._held_keys.clear()
        for _ in range(2):
            self.stop_mining()
            try:
                mouse.release(button="left")
            except Exception:
                pass
            time.sleep(0.01)
        self._mining_down = False


class BlockingBlockLearner:
    def __init__(self, logger: logging.Logger, threshold: int = 3) -> None:
        self.logger = logger.getChild("blocking_learner")
        self.threshold = max(1, threshold)
        self._path = learned_blocking_path()
        self._data = load_learned_blocking_snapshot()

    def observe(
        self,
        region: Optional[RegionConfig],
        pose: Optional[Pose],
        obstacle: ObstacleInfo,
        *,
        in_region: bool,
    ) -> bool:
        if region is None or pose is None:
            return False

        block_id = str(obstacle.block_id or "").strip().lower()
        if not block_id or block_id == "minecraft:air":
            return False
        if block_id in region.allowed_block_ids or block_id in region.blocking_block_ids:
            return False
        if not self._should_consider(region, pose, obstacle, in_region):
            return False

        regions = self._data.setdefault("regions", {})
        region_bucket = regions.setdefault(region.name, {})
        counts = region_bucket.setdefault("observation_counts", {})
        last_seen = region_bucket.setdefault("last_seen_utc", {})
        learned = set(str(v).lower() for v in region_bucket.get("learned_blocking_block_ids", []))

        count = int(counts.get(block_id, 0)) + 1
        counts[block_id] = count
        last_seen[block_id] = datetime.now(timezone.utc).isoformat()

        if count < self.threshold:
            self._save()
            return False

        if block_id in learned:
            self._save()
            return False

        learned.add(block_id)
        region_bucket["learned_blocking_block_ids"] = sorted(learned)
        region.learned_blocking_block_ids.add(block_id)
        region.blocking_block_ids.add(block_id)
        self._save()
        self.logger.info(
            "Learned blocking material for region '%s': %s (count=%d)",
            region.name,
            block_id,
            count,
        )
        return True

    def _should_consider(
        self,
        region: RegionConfig,
        pose: Pose,
        obstacle: ObstacleInfo,
        in_region: bool,
    ) -> bool:
        if obstacle.block_x is not None and obstacle.block_y is not None and obstacle.block_z is not None:
            if not is_block_in_bounds(obstacle, region):
                return True
            if obstacle.block_y <= region.min_y:
                return True
        if in_region:
            return True
        return distance_from_pose_to_region(pose, region) <= 2.5

    def _save(self) -> None:
        try:
            self._path.write_text(json.dumps(self._data, indent=2), encoding="utf-8")
        except Exception as exc:
            self.logger.warning("Failed to save learned blocking materials: %s", exc)


class MiningStrategyPlanner:
    def __init__(self, logger: logging.Logger, default_failures_before_shift: int) -> None:
        self.logger = logger.getChild("strategy_planner")
        self.default_failures_before_shift = default_failures_before_shift

    def select_plan(self, region: Optional[RegionConfig], pose: Optional[Pose]) -> MiningPlan:
        default_plan = self._default_plan(region, pose)
        try:
            conn = get_telemetry_conn()
        except Exception as exc:
            self.logger.info("Planning fallback to default: telemetry DB unavailable (%s).", exc)
            return default_plan

        try:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        decision_window_id,
                        state_name,
                        action_name,
                        reward_value,
                        feature_json
                    FROM silver.decision_window
                    WHERE reward_value IS NOT NULL
                    ORDER BY end_ts_utc DESC
                    LIMIT 240
                    """
                )
                rows = [dict(r) for r in cur.fetchall()]
            conn.commit()
        except Exception as exc:
            conn.rollback()
            self.logger.info("Planning fallback to default: planner query failed (%s).", exc)
            return default_plan
        finally:
            conn.close()

        if not rows:
            self.logger.info("Planning fallback to default: no historical decision windows yet.")
            return default_plan

        return self._choose_plan_from_rows(rows, region, pose, default_plan)

    def _default_plan(self, region: Optional[RegionConfig], pose: Optional[Pose]) -> MiningPlan:
        shift_direction = 1
        rationale = "default"
        if region is not None and pose is not None:
            midpoint_z = (region.min_z + region.max_z) / 2.0
            shift_direction = 1 if pose.z <= midpoint_z else -1
            rationale = f"default_from_z:{'north_half' if shift_direction > 0 else 'south_half'}"
        return MiningPlan(
            name="default_shallow_snake",
            pitch_sequence=(8.0, 12.0, 18.0, 24.0, 18.0, 12.0),
            preferred_shift_direction=shift_direction,
            reacquire_failures_before_shift=self.default_failures_before_shift,
            rationale=rationale,
        )

    def _choose_plan_from_rows(
        self,
        rows: list[dict[str, object]],
        region: Optional[RegionConfig],
        pose: Optional[Pose],
        default_plan: MiningPlan,
    ) -> MiningPlan:
        current_z_band = self._z_band(region, pose)
        shift_scores = {1: [], -1: []}
        high_reward_feats: list[dict[str, object]] = []

        rewards = [float(r["reward_value"]) for r in rows if r.get("reward_value") is not None]
        reward_cutoff = sorted(rewards, reverse=True)[max(0, min(len(rewards) - 1, len(rewards) // 3))] if rewards else 0.0

        for row in rows:
            feature_json = row.get("feature_json")
            feat = feature_json if isinstance(feature_json, dict) else {}
            reward_value = float(row.get("reward_value") or 0.0)
            row_z_band = self._feature_z_band(feat, region)
            if row.get("action_name") == "SHIFT_POSITIVE" and row_z_band == current_z_band:
                shift_scores[1].append(reward_value)
            elif row.get("action_name") == "SHIFT_NEGATIVE" and row_z_band == current_z_band:
                shift_scores[-1].append(reward_value)

            if reward_value >= reward_cutoff:
                high_reward_feats.append(feat)

        preferred_shift = default_plan.preferred_shift_direction
        pos_avg = self._mean(shift_scores[1])
        neg_avg = self._mean(shift_scores[-1])
        if pos_avg is not None or neg_avg is not None:
            if neg_avg is None or (pos_avg is not None and pos_avg >= neg_avg):
                preferred_shift = 1
            else:
                preferred_shift = -1

        avg_speed = self._mean(
            [self._feature_float(feat, "avg_horizontal_speed_bps") for feat in high_reward_feats]
        )
        avg_stone_frac = self._mean(
            [self._feature_float(feat, "frac_stone") for feat in high_reward_feats]
        )
        avg_y = self._mean(
            [self._feature_float(feat, "avg_y") for feat in high_reward_feats]
        )

        pitch_sequence = list(default_plan.pitch_sequence)
        plan_name = "default_shallow_snake"
        failures_before_shift = default_plan.reacquire_failures_before_shift
        rationale_parts = [f"z_band={current_z_band}"]

        if region is not None and avg_y is not None:
            floorish = avg_y <= (region.min_y + 12.0)
        elif region is not None and pose is not None:
            floorish = pose.y <= (region.min_y + 12.0)
        else:
            floorish = False

        if floorish:
            pitch_sequence = [8.0, 12.0, 18.0, 12.0]
            failures_before_shift = max(failures_before_shift, 6)
            plan_name = "floor_avoidant_shallow"
            rationale_parts.append("floor_avoidant")
        elif avg_speed is not None and avg_speed >= 1.25 and (avg_stone_frac or 0.0) >= 0.4:
            pitch_sequence = [12.0, 18.0, 24.0, 18.0, 12.0]
            failures_before_shift = max(4, failures_before_shift - 1)
            plan_name = "fast_strip"
            rationale_parts.append(f"speed={avg_speed:.2f}")
        else:
            rationale_parts.append("shallow_default")

        return MiningPlan(
            name=plan_name,
            pitch_sequence=tuple(pitch_sequence),
            preferred_shift_direction=preferred_shift,
            reacquire_failures_before_shift=failures_before_shift,
            rationale="|".join(rationale_parts),
        )

    def _feature_float(self, feat: dict[str, object], key: str) -> Optional[float]:
        value = feat.get(key)
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _mean(self, values: list[Optional[float]]) -> Optional[float]:
        filtered = [v for v in values if v is not None]
        if not filtered:
            return None
        return sum(filtered) / len(filtered)

    def _z_band(self, region: Optional[RegionConfig], pose: Optional[Pose]) -> str:
        if region is None or pose is None:
            return "mid"
        span = max(1.0, float(region.max_z - region.min_z))
        frac = (pose.z - region.min_z) / span
        if frac < 0.33:
            return "low"
        if frac < 0.66:
            return "mid"
        return "high"

    def _feature_z_band(self, feat: dict[str, object], region: Optional[RegionConfig]) -> str:
        if region is None:
            return "mid"
        avg_z = self._feature_float(feat, "start_z")
        end_z = self._feature_float(feat, "end_z")
        if avg_z is None and end_z is None:
            return "mid"
        z_value = avg_z if avg_z is not None else end_z
        if z_value is None:
            return "mid"
        span = max(1.0, float(region.max_z - region.min_z))
        frac = (z_value - region.min_z) / span
        if frac < 0.33:
            return "low"
        if frac < 0.66:
            return "mid"
        return "high"


class ManualTrainingRecorder:
    def __init__(
        self,
        logger: logging.Logger,
        world_model: WorldModelForge,
        sample_interval_sec: float,
    ) -> None:
        self.logger = logger.getChild("manual_training")
        self.world_model = world_model
        self.sample_interval_sec = max(0.02, sample_interval_sec)
        self._tracked_keys: Tuple[str, ...] = ("w", "a", "s", "d", "space", "ctrl", "shift")
        self._output_dir = Path(__file__).resolve().parents[2] / "data" / "manual_training"
        self._session_path: Optional[Path] = None
        self._summary_path: Optional[Path] = None
        self._handle = None
        self._active = False
        self._started_mono = 0.0
        self._started_at_utc: Optional[str] = None
        self._last_sample_mono = 0.0
        self._sample_count = 0

    def start(self) -> Optional[Path]:
        if self._active:
            return self._session_path

        self._output_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        self._session_path = self._output_dir / f"manual_session_{stamp}.jsonl"
        self._summary_path = self._output_dir / f"manual_session_{stamp}.summary.json"
        self._handle = self._session_path.open("a", encoding="utf-8")
        self._active = True
        self._started_mono = time.monotonic()
        self._started_at_utc = datetime.now(timezone.utc).isoformat()
        self._last_sample_mono = 0.0
        self._sample_count = 0
        self.logger.info("Manual training session recording to %s", self._session_path)
        return self._session_path

    def is_active(self) -> bool:
        return self._active

    def current_session_path(self) -> Optional[Path]:
        return self._session_path

    def sample(self) -> None:
        if not self._active or self._handle is None:
            return

        now = time.monotonic()
        if now - self._last_sample_mono < self.sample_interval_sec:
            return
        self._last_sample_mono = now

        self.world_model.update()
        pose = self.world_model.get_player_pose()
        obstacle = self.world_model.is_obstacle_ahead()
        motion = self.world_model.get_motion_estimate()

        key_state: Dict[str, bool] = {}
        for key_name in self._tracked_keys:
            try:
                key_state[key_name] = bool(keyboard.is_pressed(key_name))
            except Exception:
                key_state[key_name] = False

        mouse_state = {
            "left": self._safe_mouse_pressed("left"),
            "right": self._safe_mouse_pressed("right"),
        }

        payload: dict[str, object] = {
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            "pose_ts_utc": None if pose is None else pose.ts_utc,
            "x": None if pose is None else round(pose.x, 3),
            "y": None if pose is None else round(pose.y, 3),
            "z": None if pose is None else round(pose.z, 3),
            "yaw": None if pose is None else round(normalize_yaw(pose.yaw), 3),
            "pitch": None if pose is None else round(pose.pitch, 3),
            "dimension": None if pose is None else pose.dimension,
            "look_block": obstacle.block_id,
            "block_x": obstacle.block_x,
            "block_y": obstacle.block_y,
            "block_z": obstacle.block_z,
            "keys": key_state,
            "mouse": mouse_state,
            "motion": None
            if motion is None
            else {
                "dt_sec": round(motion.dt_sec, 4),
                "vel_x": round(motion.vel_x, 3),
                "vel_y": round(motion.vel_y, 3),
                "vel_z": round(motion.vel_z, 3),
                "speed_bps": round(motion.speed_bps, 3),
                "horizontal_speed_bps": round(motion.horizontal_speed_bps, 3),
            },
        }
        self._handle.write(json.dumps(payload) + "\n")
        self._handle.flush()
        self._sample_count += 1

    def stop(self, reason: str = "manual_toggle_off") -> None:
        if not self._active:
            return

        stopped_at_utc = datetime.now(timezone.utc).isoformat()
        duration_sec = max(0.0, time.monotonic() - self._started_mono)
        if self._handle is not None:
            self._handle.close()
            self._handle = None

        summary = {
            "version": "1.0",
            "reason": reason,
            "started_at_utc": self._started_at_utc,
            "stopped_at_utc": stopped_at_utc,
            "duration_sec": round(duration_sec, 3),
            "sample_count": self._sample_count,
            "sample_interval_sec": self.sample_interval_sec,
            "session_path": None if self._session_path is None else str(self._session_path),
        }
        if self._summary_path is not None:
            self._summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        self.logger.info(
            "Manual training session stopped: samples=%d duration=%.2fs path=%s",
            self._sample_count,
            duration_sec,
            self._session_path,
        )
        self._active = False
        self._started_at_utc = None
        self._last_sample_mono = 0.0
        self._sample_count = 0
        self._session_path = None
        self._summary_path = None

    def _safe_mouse_pressed(self, button: str) -> bool:
        try:
            return bool(mouse.is_pressed(button=button))
        except Exception:
            return False


@dataclass(frozen=True)
class ControlCalibrationSegment:
    name: str
    duration_sec: float


@dataclass(frozen=True)
class ControlStateProbe:
    label: str
    inferred_state: str
    start_on_ground: bool
    end_on_ground: bool
    peak_delta_y: float
    trough_delta_y: float
    end_delta_y: float
    duration_sec: float


class ControlCalibrationRunner:
    def __init__(
        self,
        cfg: AppConfig,
        logger: logging.Logger,
        world_model: WorldModelForge,
        inputs: SimpleInputController,
        region: Optional[RegionConfig] = None,
        strict_controller: Optional["StrictLaneController"] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        self.cfg = cfg
        self.logger = logger.getChild("control_calibration")
        self.world_model = world_model
        self.inputs = inputs
        self.region = region
        self.strict_controller = strict_controller
        self.stop_event = stop_event or threading.Event()
        self.current_action = MinerAction.WAIT_FOR_TELEMETRY
        self._segments: Tuple[ControlCalibrationSegment, ...] = (
            ControlCalibrationSegment("idle_baseline", 0.8),
            ControlCalibrationSegment("look_right", 0.8),
            ControlCalibrationSegment("look_left", 0.8),
            ControlCalibrationSegment("look_down", 0.8),
            ControlCalibrationSegment("look_up", 0.8),
            ControlCalibrationSegment("forward", 1.8),
            ControlCalibrationSegment("sprint_forward", 1.8),
            ControlCalibrationSegment("backward", 1.2),
            ControlCalibrationSegment("strafe_left", 1.2),
            ControlCalibrationSegment("strafe_right", 1.2),
            ControlCalibrationSegment("diag_forward_right", 1.2),
            ControlCalibrationSegment("diag_forward_left", 1.2),
            ControlCalibrationSegment("jump", 0.6),
            ControlCalibrationSegment("crouch", 0.6),
            ControlCalibrationSegment("toggle_fly_on", 0.8),
            ControlCalibrationSegment("fly_up", 1.6),
            ControlCalibrationSegment("fly_forward", 1.5),
            ControlCalibrationSegment("fly_sprint_forward", 1.5),
            ControlCalibrationSegment("fly_strafe_left", 1.2),
            ControlCalibrationSegment("fly_strafe_right", 1.2),
            ControlCalibrationSegment("fly_diag_forward_right", 1.4),
            ControlCalibrationSegment("fly_diag_forward_left", 1.4),
            ControlCalibrationSegment("fly_up_forward", 1.2),
            ControlCalibrationSegment("fly_down", 1.6),
            ControlCalibrationSegment("fly_down_forward", 1.0),
            ControlCalibrationSegment("toggle_fly_off", 0.8),
        )
        self._output_dir = control_calibration_dir()
        self._last_run_path = control_calibration_last_run_path()
        self._profile_path = control_calibration_profile_path()
        self._memory_path = control_calibration_memory_path()
        self._scout_memory_path = perimeter_scout_memory_path()
        self._planned_duration_sec = sum(segment.duration_sec for segment in self._segments)
        self.reset_for_new_run()

    def _stop_requested(self) -> bool:
        return self.stop_event.is_set()

    def _sleep_interruptibly(self, seconds: float) -> bool:
        deadline = time.monotonic() + max(0.0, seconds)
        while time.monotonic() < deadline:
            if self._stop_requested():
                return False
            time.sleep(min(0.02, deadline - time.monotonic()))
        return not self._stop_requested()

    def _has_fresh_telemetry(self) -> bool:
        age = self.world_model.pose_provider.get_last_update_age_sec()
        return age is not None and age <= self.cfg.stale_pose_timeout_sec

    def reset_for_new_run(self) -> None:
        self.inputs.all_stop()
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._status = "idle"
        self._complete = False
        self._memory_reused = False
        self._started_mono = 0.0
        self._script_started_mono = 0.0
        self._started_at_utc: Optional[str] = None
        self._session_path: Optional[Path] = None
        self._summary_path: Optional[Path] = None
        self._handle = None
        self._segment_index = -1
        self._segment_started_mono = 0.0
        self._segment_started_at_utc: Optional[str] = None
        self._segment_start_pose: Optional[Pose] = None
        self._segment_sample_count = 0
        self._segment_horizontal_speed_sum = 0.0
        self._segment_horizontal_speed_max = 0.0
        self._segment_one_shot_done = False
        self._last_sample_mono = 0.0
        self._last_look_mono = 0.0
        self._last_stale_log_mono = 0.0
        self._samples_written = 0
        self._segment_results: list[dict[str, object]] = []
        self._initial_pose: Optional[Pose] = None
        self._final_pose: Optional[Pose] = None
        self._strict_profile: Optional[dict[str, object]] = None
        self._segment_min_y: float = 0.0
        self._segment_max_y: float = 0.0
        self._segment_min_pitch: float = 0.0
        self._segment_max_pitch: float = 0.0
        self._segment_start_state: Optional[dict[str, object]] = None
        self._calibration_expected_start_state = "grounded_non_flying"
        self._preflight_validation: dict[str, object] = {}
        self._validation_failures: list[str] = []
        self._validation_passed: bool = True

    def begin_run(self) -> None:
        self.reset_for_new_run()
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        self._session_path = self._output_dir / f"control_calibration_{stamp}.jsonl"
        self._summary_path = self._output_dir / f"control_calibration_{stamp}.summary.json"
        self._handle = self._session_path.open("a", encoding="utf-8")
        self._status = "running"
        self.logger.info(
            "Control calibration enabled; running a scripted %.1fs control test from the current position.",
            self._planned_duration_sec,
        )
        self.inputs.all_stop()

    def is_complete(self) -> bool:
        return self._complete

    def memory_was_reused(self) -> bool:
        return self._memory_reused

    def has_reportable_progress(self) -> bool:
        return self._samples_written > 0 or bool(self._segment_results)

    def snapshot(self, status: str = "interrupted") -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None
        report = self._build_report(status=status)
        if self._summary_path is not None:
            self._summary_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        self._last_run_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        profile = self._build_profile(report)
        validation = report.get("validation") if isinstance(report.get("validation"), dict) else {}
        profile_valid = bool(validation.get("passed", False))
        if status == "complete" and profile_valid:
            self._profile_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
            self._update_success_memory(report, profile)
            self.logger.info("Control calibration saved report to %s", self._summary_path)
            self.logger.info("Control calibration saved profile to %s", self._profile_path)
        elif status == "complete":
            self.logger.warning(
                "Control calibration failed validation; report was saved but the reusable profile was not updated."
            )

    def tick(self) -> Tuple[MinerAction, ObstacleInfo]:
        if self._stop_requested():
            self.inputs.all_stop()
            self.current_action = MinerAction.STOP_ALL
            return self.current_action, ObstacleInfo(False, "minecraft:air", None, None, None)

        self._refresh_scout_memory_if_needed()
        self.world_model.update()
        obstacle = self.world_model.is_obstacle_ahead()
        pose = self.world_model.get_player_pose()
        now = time.monotonic()

        if not self._has_fresh_telemetry() or pose is None:
            self.inputs.all_stop()
            if now - self._last_stale_log_mono >= 2.0:
                self.logger.warning(
                    "Forge telemetry is missing or stale; control calibration will not move blindly."
                )
                self._last_stale_log_mono = now
            self.current_action = MinerAction.WAIT_FOR_TELEMETRY
            return self.current_action, obstacle

        if self._started_mono == 0.0:
            self._started_mono = now
            self._started_at_utc = datetime.now(timezone.utc).isoformat()
            self._initial_pose = pose
            self._capture_strict_mouse_calibration()
            pose = self._prepare_expected_start_state(pose) or pose
            self.world_model.update()
            pose = self.world_model.get_player_pose() or pose
            now = time.monotonic()
            self._script_started_mono = now
            self._advance_segment(pose, now)

        self._sample(pose, obstacle, now)

        if self._segment_index < 0 or self._segment_index >= len(self._segments):
            self._complete_calibration(pose)
            return self.current_action, obstacle

        script_elapsed = (
            0.0
            if self._script_started_mono == 0.0
            else max(0.0, now - self._script_started_mono)
        )
        hard_timeout_sec = max(self.cfg.control_calibration_duration_sec + 12.0, self._planned_duration_sec + 12.0)
        if script_elapsed >= hard_timeout_sec:
            self._finish_segment(pose, now)
            self._complete_calibration(pose)
            return self.current_action, obstacle

        segment = self._segments[self._segment_index]
        if now - self._segment_started_mono >= segment.duration_sec:
            self._finish_segment(pose, now)
            self._advance_segment(pose, now)
            if self._complete:
                return self.current_action, obstacle
            segment = self._segments[self._segment_index]

        self._apply_segment_controls(segment, now)
        self.current_action = MinerAction.CALIBRATION_TEST
        return self.current_action, obstacle

    def _advance_segment(self, pose: Pose, now: float) -> None:
        self.inputs.all_stop()
        self._segment_index += 1
        if self._segment_index >= len(self._segments):
            self._complete_calibration(pose)
            return
        segment = self._segments[self._segment_index]
        self._segment_started_mono = now
        self._segment_started_at_utc = datetime.now(timezone.utc).isoformat()
        self._segment_start_pose = pose
        self._segment_sample_count = 0
        self._segment_horizontal_speed_sum = 0.0
        self._segment_horizontal_speed_max = 0.0
        self._segment_one_shot_done = False
        self._last_look_mono = 0.0
        self._segment_min_y = pose.y
        self._segment_max_y = pose.y
        self._segment_min_pitch = pose.pitch
        self._segment_max_pitch = pose.pitch
        self._segment_start_state = self._pose_state_snapshot(pose)
        self.logger.info(
            "Control calibration segment %d/%d: %s (%.1fs)",
            self._segment_index + 1,
            len(self._segments),
            segment.name,
            segment.duration_sec,
        )

    def _finish_segment(self, pose: Pose, now: float) -> None:
        if self._segment_index < 0 or self._segment_index >= len(self._segments):
            return
        segment = self._segments[self._segment_index]
        start_pose = self._segment_start_pose or pose
        duration = max(0.0, now - self._segment_started_mono)
        avg_horizontal_speed = (
            self._segment_horizontal_speed_sum / self._segment_sample_count
            if self._segment_sample_count > 0
            else 0.0
        )
        result = {
            "name": segment.name,
            "started_at_utc": self._segment_started_at_utc,
            "duration_sec": round(duration, 3),
            "start_pose": {
                "x": round(start_pose.x, 3),
                "y": round(start_pose.y, 3),
                "z": round(start_pose.z, 3),
                "yaw": round(normalize_yaw(start_pose.yaw), 3),
                "pitch": round(start_pose.pitch, 3),
            },
            "end_pose": {
                "x": round(pose.x, 3),
                "y": round(pose.y, 3),
                "z": round(pose.z, 3),
                "yaw": round(normalize_yaw(pose.yaw), 3),
                "pitch": round(pose.pitch, 3),
            },
            "start_state": self._segment_start_state,
            "end_state": self._pose_state_snapshot(pose),
            "delta": {
                "x": round(pose.x - start_pose.x, 3),
                "y": round(pose.y - start_pose.y, 3),
                "z": round(pose.z - start_pose.z, 3),
                "yaw": round(normalize_yaw(pose.yaw - start_pose.yaw), 3),
                "pitch": round(pose.pitch - start_pose.pitch, 3),
                "horizontal_distance": round(math.sqrt((pose.x - start_pose.x) ** 2 + (pose.z - start_pose.z) ** 2), 3),
                "peak_delta_y": round(self._segment_max_y - start_pose.y, 3),
                "trough_delta_y": round(self._segment_min_y - start_pose.y, 3),
                "pitch_range": round(self._segment_max_pitch - self._segment_min_pitch, 3),
            },
            "avg_horizontal_speed_bps": round(avg_horizontal_speed, 3),
            "max_horizontal_speed_bps": round(self._segment_horizontal_speed_max, 3),
            "samples": self._segment_sample_count,
            "mouse_backend": self.inputs.get_mouse_backend(),
        }
        if segment.name in {"toggle_fly_on", "toggle_fly_off"}:
            surface_y_hint = self._preflight_validation.get("surface_y_hint")
            probe = self._verify_fly_toggle_state(
                segment.name,
                surface_y=float(surface_y_hint) if isinstance(surface_y_hint, (int, float)) else None,
            )
            result["post_state_probe"] = self._probe_to_report_dict(probe)
        validation = self._validate_segment_result(result)
        result["validation"] = validation
        if not bool(validation.get("passed", True)):
            self._validation_passed = False
            reason = str(validation.get("reason") or f"segment:{segment.name}")
            self._validation_failures.append(reason)
        self._segment_results.append(result)
        self.inputs.all_stop()

    def _complete_calibration(self, pose: Pose, *, status: str = "complete") -> None:
        self.inputs.all_stop()
        self._final_pose = pose
        self._complete = True
        self._status = status
        self.current_action = MinerAction.CALIBRATION_COMPLETE
        self.snapshot(status=status)

    def _fail_calibration(self, pose: Pose, message: str) -> None:
        self.inputs.all_stop()
        self.logger.warning(message)
        self._validation_passed = False
        self._validation_failures.append(message)
        self._complete_calibration(pose, status="failed")

    def _capture_strict_mouse_calibration(self) -> None:
        if self.strict_controller is None:
            return
        self.strict_controller._calibrate_horizontal_mouse()
        self._strict_profile = {
            "preferred_mouse_backend": self.inputs.get_mouse_backend(),
            "yaw_step_right_deg": round(self.strict_controller._yaw_step_right_deg, 4),
            "yaw_step_left_deg": round(self.strict_controller._yaw_step_left_deg, 4),
            "yaw_step_pixels": self.strict_controller._yaw_step_pixels,
            "yaw_deg_per_pixel": round(self.strict_controller._yaw_deg_per_pixel, 6),
            "horizontal_calibration_ready": self.strict_controller._horizontal_calibration_ready,
        }

    def _sample(self, pose: Pose, obstacle: ObstacleInfo, now: float) -> None:
        if self._handle is None:
            return
        if now - self._last_sample_mono < self.cfg.manual_record_interval_sec:
            return
        self._last_sample_mono = now

        motion = self.world_model.get_motion_estimate()
        key_state: Dict[str, bool] = {}
        for key_name in ("w", "a", "s", "d", "space", "ctrl", "shift"):
            try:
                key_state[key_name] = bool(keyboard.is_pressed(key_name))
            except Exception:
                key_state[key_name] = False

        payload = {
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            "segment": None if self._segment_index < 0 or self._segment_index >= len(self._segments) else self._segments[self._segment_index].name,
            "mouse_backend": self.inputs.get_mouse_backend(),
            "pose_ts_utc": pose.ts_utc,
            "x": round(pose.x, 3),
            "y": round(pose.y, 3),
            "z": round(pose.z, 3),
            "yaw": round(normalize_yaw(pose.yaw), 3),
            "pitch": round(pose.pitch, 3),
            "dimension": pose.dimension,
            "look_block": obstacle.block_id,
            "block_x": obstacle.block_x,
            "block_y": obstacle.block_y,
            "block_z": obstacle.block_z,
            "keys": key_state,
            "mouse": {
                "left": self._safe_mouse_pressed("left"),
                "right": self._safe_mouse_pressed("right"),
            },
            "motion": None
            if motion is None
            else {
                "dt_sec": round(motion.dt_sec, 4),
                "vel_x": round(motion.vel_x, 3),
                "vel_y": round(motion.vel_y, 3),
                "vel_z": round(motion.vel_z, 3),
                "speed_bps": round(motion.speed_bps, 3),
                "horizontal_speed_bps": round(motion.horizontal_speed_bps, 3),
            },
        }
        self._handle.write(json.dumps(payload) + "\n")
        self._handle.flush()
        self._samples_written += 1
        self._segment_sample_count += 1
        self._segment_min_y = min(self._segment_min_y, pose.y)
        self._segment_max_y = max(self._segment_max_y, pose.y)
        self._segment_min_pitch = min(self._segment_min_pitch, pose.pitch)
        self._segment_max_pitch = max(self._segment_max_pitch, pose.pitch)
        if motion is not None:
            self._segment_horizontal_speed_sum += motion.horizontal_speed_bps
            self._segment_horizontal_speed_max = max(self._segment_horizontal_speed_max, motion.horizontal_speed_bps)

    def _apply_segment_controls(self, segment: ControlCalibrationSegment, now: float) -> None:
        self.inputs.stop_mining()
        self.inputs.release_key("w")
        self.inputs.release_key("a")
        self.inputs.release_key("s")
        self.inputs.release_key("d")
        self.inputs.release_key("ctrl")
        self.inputs.release_key("shift")
        self.inputs.release_key("space")

        if segment.name == "forward":
            self.inputs.hold_forward(sprint=False)
            return
        if segment.name == "sprint_forward":
            self.inputs.hold_forward(sprint=True)
            return
        if segment.name == "backward":
            self.inputs.hold_backward()
            return
        if segment.name == "strafe_left":
            self.inputs.hold_strafe_left()
            return
        if segment.name == "strafe_right":
            self.inputs.hold_strafe_right()
            return
        if segment.name == "diag_forward_right":
            self.inputs.hold_forward(sprint=False)
            self.inputs.hold_strafe_right()
            return
        if segment.name == "diag_forward_left":
            self.inputs.hold_forward(sprint=False)
            self.inputs.hold_strafe_left()
            return
        if segment.name == "fly_up":
            self.inputs.hold_fly_up()
            return
        if segment.name == "fly_forward":
            self.inputs.hold_forward(sprint=False)
            return
        if segment.name == "fly_sprint_forward":
            self.inputs.hold_forward(sprint=True)
            return
        if segment.name == "fly_strafe_left":
            self.inputs.hold_strafe_left()
            return
        if segment.name == "fly_strafe_right":
            self.inputs.hold_strafe_right()
            return
        if segment.name == "fly_diag_forward_right":
            self.inputs.hold_forward(sprint=False)
            self.inputs.hold_strafe_right()
            return
        if segment.name == "fly_diag_forward_left":
            self.inputs.hold_forward(sprint=False)
            self.inputs.hold_strafe_left()
            return
        if segment.name == "fly_up_forward":
            self.inputs.hold_forward(sprint=False)
            self.inputs.hold_fly_up()
            return
        if segment.name == "fly_down":
            self.inputs.hold_fly_down()
            return
        if segment.name == "fly_down_forward":
            self.inputs.hold_forward(sprint=False)
            self.inputs.hold_fly_down()
            return
        if segment.name == "crouch":
            self.inputs.hold_crouch()
            return
        if segment.name == "jump":
            if not self._segment_one_shot_done:
                self.inputs.tap_jump(0.08)
                self._segment_one_shot_done = True
            return
        if segment.name in {"toggle_fly_on", "toggle_fly_off"}:
            if not self._segment_one_shot_done:
                if segment.name == "toggle_fly_on":
                    self.inputs.enable_fly_mode()
                else:
                    self.inputs.disable_fly_mode()
                self._segment_one_shot_done = True
            return

        look_interval = 0.18
        if now - self._last_look_mono < look_interval:
            return
        self._last_look_mono = now
        if segment.name == "look_right":
            self.inputs.look_right_small(steps=1, step_pixels=16, delay=0.002)
        elif segment.name == "look_left":
            self.inputs.look_left_small(steps=1, step_pixels=16, delay=0.002)
        elif segment.name == "look_down":
            self.inputs.look_down_small(steps=1, step_pixels=8, delay=0.002)
        elif segment.name == "look_up":
            self.inputs.look_up_small(steps=1, step_pixels=8, delay=0.002)

    def _safe_mouse_pressed(self, button: str) -> bool:
        try:
            return bool(mouse.is_pressed(button=button))
        except Exception:
            return False

    def _pose_state_snapshot(self, pose: Pose) -> dict[str, object]:
        motion = self.world_model.get_motion_estimate()
        vertical_speed = None if motion is None else round(motion.vel_y, 3)
        horizontal_speed = None if motion is None else round(motion.horizontal_speed_bps, 3)
        inferred = "grounded"
        if not pose.on_ground:
            if motion is not None and motion.vel_y > 0.12:
                inferred = "ascending"
            elif motion is not None and motion.vel_y < -0.12:
                inferred = "descending"
            else:
                inferred = "hovering_or_flying"
        return {
            "on_ground": bool(pose.on_ground),
            "is_sprinting": bool(pose.is_sprinting),
            "inferred_motion_state": inferred,
            "vertical_speed_bps": vertical_speed,
            "horizontal_speed_bps": horizontal_speed,
        }

    def _wait_for_pose_settle(self, seconds: float) -> Optional[Pose]:
        deadline = time.monotonic() + max(0.05, seconds)
        latest_pose: Optional[Pose] = None
        while time.monotonic() < deadline and not self._stop_requested():
            self.world_model.update()
            pose = self.world_model.get_player_pose()
            if pose is not None:
                latest_pose = pose
            time.sleep(0.02)
        self.world_model.update()
        return self.world_model.get_player_pose() or latest_pose

    def _load_scout_memory_region_entry(self) -> Optional[dict[str, object]]:
        return load_region_memory_entry(self._scout_memory_path, self.region)

    def _estimate_surface_y(self, pose: Optional[Pose]) -> Optional[float]:
        candidates: list[float] = []
        if pose is not None and self.region is not None:
            horizontal_margin = 8.0
            if (
                self.region.min_x - horizontal_margin <= pose.x <= self.region.max_x + horizontal_margin
                and self.region.min_z - horizontal_margin <= pose.z <= self.region.max_z + horizontal_margin
                and abs(pose.y - float(self.region.max_y)) <= 12.0
            ):
                candidates.append(float(self.region.max_y))
        scout_memory = self._load_scout_memory_region_entry()
        if isinstance(scout_memory, dict):
            surface_y = scout_memory.get("ground_surface_y")
            if isinstance(surface_y, (int, float)):
                surface_y_value = float(surface_y)
                if pose is None or abs(surface_y_value - pose.y) <= 12.0:
                    candidates.append(surface_y_value)
        if pose is not None and abs(pose.y - round(pose.y)) <= 0.08:
            candidates.append(float(round(pose.y)))
        if not candidates:
            return None
        if pose is None:
            return candidates[0]
        return min(candidates, key=lambda value: abs(value - pose.y))

    def _is_surface_contact(self, pose: Optional[Pose], surface_y: Optional[float]) -> bool:
        if pose is None:
            return False
        if pose.on_ground:
            return True
        if surface_y is None:
            return False
        motion = self.world_model.get_motion_estimate()
        vertical_speed = 0.0 if motion is None else abs(motion.vel_y)
        return abs(pose.y - surface_y) <= 0.08 and vertical_speed <= 0.08

    def _ensure_ground_contact(
        self,
        pose: Pose,
        timeout_sec: float = 2.5,
        *,
        surface_y: Optional[float] = None,
    ) -> Optional[Pose]:
        current_pose = pose
        if self._is_surface_contact(current_pose, surface_y):
            return current_pose
        if surface_y is None:
            self.logger.info(
                "Control calibration preflight: pose is airborne at y=%.2f; descending to a grounded state before validation.",
                current_pose.y,
            )
        else:
            self.logger.info(
                "Control calibration preflight: pose is airborne at y=%.2f; descending toward surface_y=%.2f before validation.",
                current_pose.y,
                surface_y,
            )
        deadline = time.monotonic() + max(0.5, timeout_sec)
        while time.monotonic() < deadline and not self._stop_requested():
            self.inputs.hold_fly_down()
            self.world_model.update()
            latest_pose = self.world_model.get_player_pose()
            if latest_pose is not None:
                current_pose = latest_pose
            if self._is_surface_contact(current_pose, surface_y):
                self.inputs.release_key("shift")
                return self._wait_for_pose_settle(0.15) or current_pose
            time.sleep(0.04)
        self.inputs.release_key("shift")
        settled_pose = self._wait_for_pose_settle(0.10) or current_pose
        return settled_pose if self._is_surface_contact(settled_pose, surface_y) else None

    def _attempt_disable_flight_and_land(
        self,
        pose: Pose,
        *,
        surface_y: Optional[float],
        attempt_idx: int,
    ) -> Pose:
        current_pose = self._wait_for_pose_settle(0.08) or pose
        if surface_y is not None and current_pose.y <= surface_y + 0.18:
            self.logger.info(
                "Control calibration preflight: lifting slightly off surface_y=%.2f before flight-off attempt %d.",
                surface_y,
                attempt_idx,
            )
            self.inputs.hold_fly_up()
            self._sleep_interruptibly(0.16)
            self.inputs.all_stop()
            current_pose = self._wait_for_pose_settle(0.18) or current_pose

        self.inputs.disable_fly_mode()
        current_pose = self._wait_for_pose_settle(0.35) or current_pose
        grounded_pose = self._ensure_ground_contact(current_pose, timeout_sec=3.0, surface_y=surface_y)
        return grounded_pose or (self._wait_for_pose_settle(0.12) or current_pose)

    def _run_jump_probe(self, label: str) -> Optional[ControlStateProbe]:
        self.inputs.all_stop()
        start_pose = self._wait_for_pose_settle(0.08)
        if start_pose is None:
            return None
        started = time.monotonic()
        peak_y = start_pose.y
        trough_y = start_pose.y
        end_pose = start_pose
        self.inputs.tap_jump(0.08)
        deadline = time.monotonic() + 0.9
        while time.monotonic() < deadline and not self._stop_requested():
            self.world_model.update()
            latest_pose = self.world_model.get_player_pose()
            if latest_pose is not None:
                end_pose = latest_pose
                peak_y = max(peak_y, latest_pose.y)
                trough_y = min(trough_y, latest_pose.y)
            if (
                end_pose.on_ground
                and (time.monotonic() - started) > 0.18
                and abs(end_pose.y - start_pose.y) <= 0.10
            ):
                break
            time.sleep(0.02)

        peak_delta_y = peak_y - start_pose.y
        trough_delta_y = trough_y - start_pose.y
        end_delta_y = end_pose.y - start_pose.y
        if peak_delta_y < 0.08 and abs(end_delta_y) < 0.08:
            inferred = "no_jump_response"
        elif not end_pose.on_ground and peak_delta_y >= 0.75:
            inferred = "flying_enabled_or_hover"
        elif end_pose.on_ground and abs(end_delta_y) <= 0.10:
            inferred = "grounded_non_flying"
        elif end_delta_y > 0.18 and not end_pose.on_ground:
            inferred = "flying_enabled_or_hover"
        else:
            inferred = "uncertain"
        return ControlStateProbe(
            label=label,
            inferred_state=inferred,
            start_on_ground=bool(start_pose.on_ground),
            end_on_ground=bool(end_pose.on_ground),
            peak_delta_y=round(peak_delta_y, 3),
            trough_delta_y=round(trough_delta_y, 3),
            end_delta_y=round(end_delta_y, 3),
            duration_sec=round(time.monotonic() - started, 3),
        )

    def _run_space_hold_probe(
        self,
        label: str,
        *,
        hold_sec: float = 0.32,
        settle_sec: float = 0.72,
        surface_y: Optional[float] = None,
    ) -> Optional[ControlStateProbe]:
        self.inputs.all_stop()
        start_pose = self._wait_for_pose_settle(0.10)
        if start_pose is None:
            return None

        started = time.monotonic()
        peak_y = start_pose.y
        trough_y = start_pose.y
        end_pose = start_pose

        hold_deadline = time.monotonic() + max(0.10, hold_sec)
        self.inputs.hold_fly_up()
        while time.monotonic() < hold_deadline and not self._stop_requested():
            self.world_model.update()
            latest_pose = self.world_model.get_player_pose()
            if latest_pose is not None:
                end_pose = latest_pose
                peak_y = max(peak_y, latest_pose.y)
                trough_y = min(trough_y, latest_pose.y)
            time.sleep(0.02)
        self.inputs.release_key("space")

        settle_deadline = time.monotonic() + max(0.20, settle_sec)
        while time.monotonic() < settle_deadline and not self._stop_requested():
            self.world_model.update()
            latest_pose = self.world_model.get_player_pose()
            if latest_pose is not None:
                end_pose = latest_pose
                peak_y = max(peak_y, latest_pose.y)
                trough_y = min(trough_y, latest_pose.y)
            time.sleep(0.02)

        peak_delta_y = peak_y - start_pose.y
        trough_delta_y = trough_y - start_pose.y
        end_delta_y = end_pose.y - start_pose.y
        near_surface_end = surface_y is not None and abs(end_pose.y - surface_y) <= 0.12
        if peak_delta_y < 0.08 and abs(end_delta_y) < 0.08:
            inferred = "no_jump_response"
        elif peak_delta_y >= 0.18 and (abs(end_delta_y) <= 0.12 or near_surface_end):
            inferred = "grounded_non_flying"
        elif end_delta_y >= 0.35:
            inferred = "flying_enabled_or_hover"
        elif peak_delta_y >= 0.55 and abs(end_delta_y) >= 0.18:
            inferred = "flying_enabled_or_hover"
        else:
            inferred = "uncertain"

        return ControlStateProbe(
            label=label,
            inferred_state=inferred,
            start_on_ground=bool(start_pose.on_ground),
            end_on_ground=bool(end_pose.on_ground),
            peak_delta_y=round(peak_delta_y, 3),
            trough_delta_y=round(trough_delta_y, 3),
            end_delta_y=round(end_delta_y, 3),
            duration_sec=round(time.monotonic() - started, 3),
        )

    def _pose_to_report_dict(self, pose: Optional[Pose]) -> Optional[dict[str, object]]:
        if pose is None:
            return None
        return {
            "x": round(pose.x, 3),
            "y": round(pose.y, 3),
            "z": round(pose.z, 3),
            "yaw": round(normalize_yaw(pose.yaw), 3),
            "pitch": round(pose.pitch, 3),
            "on_ground": bool(pose.on_ground),
            "is_sprinting": bool(pose.is_sprinting),
            "dimension": pose.dimension,
            "ts_utc": pose.ts_utc,
            "heading": yaw_to_cardinal(pose.yaw).value,
        }

    def _probe_to_report_dict(self, probe: Optional[ControlStateProbe]) -> Optional[dict[str, object]]:
        if probe is None:
            return None
        return {
            "label": probe.label,
            "inferred_state": probe.inferred_state,
            "start_on_ground": probe.start_on_ground,
            "end_on_ground": probe.end_on_ground,
            "peak_delta_y": probe.peak_delta_y,
            "trough_delta_y": probe.trough_delta_y,
            "end_delta_y": probe.end_delta_y,
            "duration_sec": probe.duration_sec,
        }

    def _segment_shows_confirmed_flight(self, result: Optional[dict[str, object]]) -> bool:
        if not isinstance(result, dict):
            return False
        name = str(result.get("name") or "")
        if not name.startswith("fly_"):
            return False

        delta = result.get("delta") if isinstance(result.get("delta"), dict) else {}
        start_pose = result.get("start_pose") if isinstance(result.get("start_pose"), dict) else {}
        end_pose = result.get("end_pose") if isinstance(result.get("end_pose"), dict) else {}
        horizontal_distance = float(delta.get("horizontal_distance") or 0.0)
        peak_delta_y = float(delta.get("peak_delta_y") or 0.0)
        trough_delta_y = float(delta.get("trough_delta_y") or 0.0)
        surface_y_hint = self._preflight_validation.get("surface_y_hint")
        surface_y = float(surface_y_hint) if isinstance(surface_y_hint, (int, float)) else None
        start_height = (
            float(start_pose.get("y")) - surface_y
            if surface_y is not None and isinstance(start_pose.get("y"), (int, float))
            else 0.0
        )
        end_height = (
            float(end_pose.get("y")) - surface_y
            if surface_y is not None and isinstance(end_pose.get("y"), (int, float))
            else 0.0
        )
        high_above_surface = max(start_height, end_height) >= 0.45

        if name == "fly_up":
            return peak_delta_y >= 0.45 or end_height >= 0.45 or start_height >= 0.45
        if name == "fly_down":
            return high_above_surface or trough_delta_y <= -0.15
        if name == "fly_down_forward":
            return horizontal_distance >= 0.10 and (high_above_surface or trough_delta_y <= -0.10)
        if name == "fly_up_forward":
            return horizontal_distance >= 0.15 and (peak_delta_y >= 0.35 or high_above_surface)
        if name in {
            "fly_forward",
            "fly_sprint_forward",
            "fly_strafe_left",
            "fly_strafe_right",
            "fly_diag_forward_right",
            "fly_diag_forward_left",
        }:
            return horizontal_distance >= 0.15 and (peak_delta_y >= 0.35 or high_above_surface)
        return False

    def _verify_fly_toggle_state(
        self,
        segment_name: str,
        *,
        surface_y: Optional[float],
    ) -> Optional[ControlStateProbe]:
        target_state = "flying_enabled_or_hover" if segment_name == "toggle_fly_on" else "grounded_non_flying"
        max_attempts = 3 if segment_name == "toggle_fly_on" else 2
        last_probe: Optional[ControlStateProbe] = None
        for attempt_idx in range(1, max_attempts + 1):
            hold_sec = 0.18 if segment_name == "toggle_fly_on" else 0.32
            settle_sec = 0.18 if segment_name == "toggle_fly_on" else 0.72
            last_probe = self._run_space_hold_probe(
                f"{segment_name}_space_hold_probe_attempt_{attempt_idx}",
                surface_y=surface_y,
                hold_sec=hold_sec,
                settle_sec=settle_sec,
            )
            if last_probe is not None and last_probe.inferred_state == target_state:
                return last_probe
            if attempt_idx >= max_attempts or self._stop_requested():
                break
            if segment_name == "toggle_fly_on":
                self.logger.warning(
                    "Control calibration %s probe observed %s; retrying quick double-jump fly enable (attempt %d/%d).",
                    segment_name,
                    "n/a" if last_probe is None else last_probe.inferred_state,
                    attempt_idx + 1,
                    max_attempts,
                )
                self.inputs.enable_fly_mode(attempt_idx=attempt_idx + 1)
            else:
                self.logger.warning(
                    "Control calibration %s probe observed %s; retrying fly disable (attempt %d/%d).",
                    segment_name,
                    "n/a" if last_probe is None else last_probe.inferred_state,
                    attempt_idx + 1,
                    max_attempts,
                )
                self.inputs.disable_fly_mode(attempt_idx=attempt_idx + 1)
            self._wait_for_pose_settle(0.10 if segment_name == "toggle_fly_on" else 0.22)
        return last_probe

    def _prepare_expected_start_state(self, pose: Pose) -> Optional[Pose]:
        current_pose = self._wait_for_pose_settle(0.10) or pose
        started_airborne = not current_pose.on_ground
        surface_y_hint = self._estimate_surface_y(current_pose)
        preflight: dict[str, object] = {
            "expected_start_state": self._calibration_expected_start_state,
            "initial_pose": self._pose_to_report_dict(pose),
            "initial_pose_state": self._pose_state_snapshot(pose),
            "initial_heading": yaw_to_cardinal(pose.yaw).value,
            "surface_y_hint": None if surface_y_hint is None else round(surface_y_hint, 3),
            "normalization_actions": [],
            "probes": [],
            "proceed": True,
            "ready_for_profile": False,
        }
        self.inputs.all_stop()
        if not self._is_surface_contact(current_pose, surface_y_hint):
            preflight["normalization_actions"].append("descend_to_ground")
            grounded_pose = self._ensure_ground_contact(current_pose, timeout_sec=3.5, surface_y=surface_y_hint)
            preflight["ground_contact_normalized"] = grounded_pose is not None
            if grounded_pose is not None:
                current_pose = grounded_pose
        else:
            preflight["ground_contact_normalized"] = True

        attempted_airborne_fly_recovery = False
        baseline_ready = False
        baseline_probe: Optional[ControlStateProbe] = None

        for attempt_idx in range(1, 4):
            baseline_probe = self._run_space_hold_probe(
                f"preflight_space_hold_probe_{attempt_idx}",
                surface_y=surface_y_hint,
            )
            preflight["probes"].append(self._probe_to_report_dict(baseline_probe))
            if baseline_probe is None:
                preflight["normalization_actions"].append(f"probe_unavailable_attempt_{attempt_idx}")
                current_pose = self._wait_for_pose_settle(0.20) or current_pose
                continue

            self.logger.info(
                "Control calibration preflight observed state: inferred=%s peak_dy=%.2f trough_dy=%.2f end_dy=%.2f.",
                baseline_probe.inferred_state,
                baseline_probe.peak_delta_y,
                baseline_probe.trough_delta_y,
                baseline_probe.end_delta_y,
            )
            current_pose = self._wait_for_pose_settle(0.12) or current_pose

            if baseline_probe.inferred_state == "grounded_non_flying":
                baseline_ready = True
                break

            if baseline_probe.inferred_state == "flying_enabled_or_hover":
                self.logger.warning(
                    "Control calibration preflight detected flight already enabled; toggling flight state off and retrying baseline normalization."
                )
                preflight["normalization_actions"].append(f"toggle_fly_off_attempt_{attempt_idx}")
                current_pose = self._attempt_disable_flight_and_land(
                    current_pose,
                    surface_y=surface_y_hint,
                    attempt_idx=attempt_idx,
                )
                current_pose = (
                    self._ensure_ground_contact(current_pose, timeout_sec=4.0, surface_y=surface_y_hint)
                    or current_pose
                )
                continue

            if baseline_probe.inferred_state == "no_jump_response" and started_airborne and not attempted_airborne_fly_recovery:
                self.logger.warning(
                    "Control calibration preflight saw an ambiguous jump response after an airborne start; retrying with a flight-state recovery toggle."
                )
                preflight["normalization_actions"].append("airborne_start_fly_recovery_toggle")
                attempted_airborne_fly_recovery = True
                self.inputs.toggle_fly_mode()
                current_pose = self._wait_for_pose_settle(0.35) or current_pose
                current_pose = (
                    self._ensure_ground_contact(current_pose, timeout_sec=3.0, surface_y=surface_y_hint)
                    or current_pose
                )
                continue

            if baseline_probe.inferred_state == "no_jump_response":
                self.logger.warning(
                    "Control calibration preflight saw no jump response; nudging to a clearer baseline position before retrying."
                )
                preflight["normalization_actions"].append(f"reposition_nudge_attempt_{attempt_idx}")
                self.inputs.hold_strafe_right()
                self._sleep_interruptibly(0.18)
                self.inputs.all_stop()
                current_pose = self._wait_for_pose_settle(0.20) or current_pose
                current_pose = (
                    self._ensure_ground_contact(current_pose, timeout_sec=2.0, surface_y=surface_y_hint)
                    or current_pose
                )
                continue

            preflight["normalization_actions"].append(f"settle_retry_attempt_{attempt_idx}")
            current_pose = self._wait_for_pose_settle(0.25) or current_pose
            current_pose = (
                self._ensure_ground_contact(current_pose, timeout_sec=2.5, surface_y=surface_y_hint)
                or current_pose
            )

        self.inputs.all_stop()
        current_pose = self._wait_for_pose_settle(0.12) or current_pose
        preflight["final_probe"] = self._probe_to_report_dict(baseline_probe)
        preflight["normalized_pose"] = self._pose_to_report_dict(current_pose)
        preflight["normalized_pose_state"] = self._pose_state_snapshot(current_pose)
        preflight["normalized_heading"] = yaw_to_cardinal(current_pose.yaw).value
        preflight["baseline_yaw"] = round(normalize_yaw(current_pose.yaw), 3)
        preflight["baseline_pitch"] = round(current_pose.pitch, 3)
        preflight["effective_surface_contact"] = self._is_surface_contact(current_pose, surface_y_hint)
        preflight["passed"] = baseline_ready
        preflight["ready_for_profile"] = baseline_ready
        if baseline_ready:
            preflight["reason"] = "Calibration preflight normalized to a grounded non-flying baseline before starting the scripted test."
            self.logger.info(
                "Control calibration preflight baseline ready: heading=%s yaw=%.1f pitch=%.1f state=%s.",
                preflight["normalized_heading"],
                preflight["baseline_yaw"],
                preflight["baseline_pitch"],
                self._calibration_expected_start_state,
            )
        else:
            preflight["reason"] = (
                "Calibration preflight could not fully normalize to the expected grounded non-flying baseline. "
                "Proceeding with a best-effort baseline so the scripted test can still collect pass/fail evidence."
            )
            self.logger.warning(str(preflight["reason"]))
        self._preflight_validation = preflight
        return current_pose

    def _recent_fly_enable_verified(self) -> bool:
        for segment in reversed(self._segment_results):
            segment_name = str(segment.get("name") or "")
            if segment_name == "toggle_fly_on":
                probe = segment.get("post_state_probe")
                if isinstance(probe, dict) and str(probe.get("inferred_state") or "") == "flying_enabled_or_hover":
                    return True
                continue
            if self._segment_shows_confirmed_flight(segment):
                return True
        return False

    def _validate_segment_result(self, result: dict[str, object]) -> dict[str, object]:
        name = str(result.get("name") or "")
        delta = result.get("delta") if isinstance(result.get("delta"), dict) else {}
        start_state = result.get("start_state") if isinstance(result.get("start_state"), dict) else {}
        end_state = result.get("end_state") if isinstance(result.get("end_state"), dict) else {}
        post_state_probe = result.get("post_state_probe") if isinstance(result.get("post_state_probe"), dict) else {}
        start_pose = result.get("start_pose") if isinstance(result.get("start_pose"), dict) else {}
        end_pose = result.get("end_pose") if isinstance(result.get("end_pose"), dict) else {}
        horizontal_distance = float(delta.get("horizontal_distance") or 0.0)
        delta_yaw = float(delta.get("yaw") or 0.0)
        delta_pitch = float(delta.get("pitch") or 0.0)
        peak_delta_y = float(delta.get("peak_delta_y") or 0.0)
        trough_delta_y = float(delta.get("trough_delta_y") or 0.0)
        avg_horizontal_speed = float(result.get("avg_horizontal_speed_bps") or 0.0)
        max_horizontal_speed = float(result.get("max_horizontal_speed_bps") or 0.0)
        end_on_ground = bool(end_state.get("on_ground")) if end_state else False
        probe_state = str(post_state_probe.get("inferred_state") or "")
        probe_peak_delta_y = float(post_state_probe.get("peak_delta_y") or 0.0)
        probe_trough_delta_y = float(post_state_probe.get("trough_delta_y") or 0.0)
        probe_end_delta_y = float(post_state_probe.get("end_delta_y") or 0.0)
        surface_y_hint = self._preflight_validation.get("surface_y_hint")
        surface_y = float(surface_y_hint) if isinstance(surface_y_hint, (int, float)) else None
        near_surface_start = (
            surface_y is not None
            and isinstance(start_pose.get("y"), (int, float))
            and abs(float(start_pose["y"]) - surface_y) <= 0.15
        )
        near_surface_end = (
            surface_y is not None
            and isinstance(end_pose.get("y"), (int, float))
            and abs(float(end_pose["y"]) - surface_y) <= 0.15
        )
        start_height = (
            float(start_pose.get("y")) - surface_y
            if surface_y is not None and isinstance(start_pose.get("y"), (int, float))
            else 0.0
        )
        end_height = (
            float(end_pose.get("y")) - surface_y
            if surface_y is not None and isinstance(end_pose.get("y"), (int, float))
            else 0.0
        )
        current_segment_confirms_flight = self._segment_shows_confirmed_flight(result)
        fly_enable_verified = self._recent_fly_enable_verified() or current_segment_confirms_flight

        passed = True
        reason = "ok"

        if name == "look_right":
            passed = delta_yaw >= 0.5
            reason = f"expected positive yaw delta, observed {delta_yaw:.2f}"
        elif name == "look_left":
            passed = delta_yaw <= -0.5
            reason = f"expected negative yaw delta, observed {delta_yaw:.2f}"
        elif name == "look_down":
            passed = delta_pitch >= 0.5
            reason = f"expected positive pitch delta, observed {delta_pitch:.2f}"
        elif name == "look_up":
            passed = delta_pitch <= -0.5
            reason = f"expected negative pitch delta, observed {delta_pitch:.2f}"
        elif name == "forward":
            passed = horizontal_distance >= 0.25
            reason = f"expected forward movement, observed {horizontal_distance:.2f} blocks"
        elif name == "sprint_forward":
            passed = horizontal_distance >= 0.35
            reason = f"expected sprint movement, observed {horizontal_distance:.2f} blocks"
        elif name in {"backward", "strafe_left", "strafe_right", "diag_forward_right", "diag_forward_left"}:
            passed = horizontal_distance >= 0.15
            reason = f"expected lateral movement, observed {horizontal_distance:.2f} blocks"
        elif name == "jump":
            passed = peak_delta_y >= 0.20
            reason = f"expected upward jump impulse, observed peak delta_y={peak_delta_y:.2f}"
        elif name == "toggle_fly_on":
            passed = (
                probe_state == "flying_enabled_or_hover"
                or peak_delta_y >= 0.20
                or probe_peak_delta_y >= 0.20
            )
            reason = (
                "expected quick double-jump flight enable or at least a usable vertical toggle response, "
                f"observed probe_state={probe_state or 'n/a'} peak_delta_y={float(post_state_probe.get('peak_delta_y') or 0.0):.2f} "
                f"end_delta_y={float(post_state_probe.get('end_delta_y') or 0.0):.2f}"
            )
        elif name == "fly_up":
            passed = (
                fly_enable_verified
                and (peak_delta_y >= 0.45 or start_height >= 0.45 or end_height >= 0.45)
                and (not near_surface_end or start_height >= 0.45)
            )
            reason = (
                "expected confirmed-flight ascent, "
                f"observed fly_enable_verified={fly_enable_verified} peak_delta_y={peak_delta_y:.2f} "
                f"start_height={start_height:.2f} end_height={end_height:.2f} "
                f"end_on_ground={end_on_ground} near_surface_end={near_surface_end}"
            )
        elif name in {"fly_forward", "fly_sprint_forward", "fly_strafe_left", "fly_strafe_right", "fly_diag_forward_right", "fly_diag_forward_left", "fly_up_forward"}:
            passed = fly_enable_verified and horizontal_distance >= 0.15 and not end_on_ground
            reason = (
                "expected confirmed-flight movement, "
                f"observed fly_enable_verified={fly_enable_verified} horizontal_distance={horizontal_distance:.2f} "
                f"avg_horizontal_speed={avg_horizontal_speed:.2f} max_horizontal_speed={max_horizontal_speed:.2f} "
                f"end_on_ground={end_on_ground}"
            )
        elif name == "fly_down":
            passed = fly_enable_verified and (trough_delta_y <= -0.15 or end_on_ground or near_surface_end)
            reason = (
                "expected confirmed-flight downward movement or landing, "
                f"observed fly_enable_verified={fly_enable_verified} trough_delta_y={trough_delta_y:.2f} "
                f"end_on_ground={end_on_ground}"
            )
        elif name == "fly_down_forward":
            passed = (
                fly_enable_verified
                and (trough_delta_y <= -0.10 or end_on_ground or near_surface_end or near_surface_start)
                and horizontal_distance >= 0.10
            )
            reason = (
                "expected confirmed-flight descending forward motion, "
                f"observed fly_enable_verified={fly_enable_verified} trough_delta_y={trough_delta_y:.2f} "
                f"horizontal_distance={horizontal_distance:.2f}"
            )
        elif name == "toggle_fly_off":
            passed = (
                probe_state == "grounded_non_flying"
                or bool(start_state.get("on_ground"))
                or end_on_ground
                or trough_delta_y <= -0.10
                or near_surface_end
                or probe_trough_delta_y <= -0.50
                or (surface_y is not None and probe_end_delta_y <= 0.12 and near_surface_end)
            )
            reason = (
                "expected fly-off normalization, "
                f"observed probe_state={probe_state or 'n/a'} end_on_ground={end_on_ground} "
                f"trough_delta_y={trough_delta_y:.2f} probe_trough_delta_y={probe_trough_delta_y:.2f}"
            )

        return {
            "passed": passed,
            "reason": "ok" if passed else reason,
        }

    def _build_report(self, *, status: str) -> dict[str, object]:
        scout_report = Path(__file__).resolve().parents[2] / "data" / "perimeter_scout_last_run.json"
        total_duration = 0.0 if self._started_mono == 0.0 else max(0.0, time.monotonic() - self._started_mono)
        preflight_duration = (
            0.0
            if self._started_mono == 0.0 or self._script_started_mono == 0.0
            else max(0.0, self._script_started_mono - self._started_mono)
        )
        scripted_duration = (
            0.0
            if self._script_started_mono == 0.0
            else max(0.0, time.monotonic() - self._script_started_mono)
        )
        preflight_ready_for_profile = bool(self._preflight_validation.get("ready_for_profile", False))
        validation_failures = list(self._validation_failures)
        if status == "complete" and not preflight_ready_for_profile:
            preflight_reason = str(self._preflight_validation.get("reason") or "").strip()
            if preflight_reason and preflight_reason not in validation_failures:
                validation_failures.insert(0, preflight_reason)
        return {
            "version": "1.0",
            "mode": "control_calibration",
            "status": status,
            "started_at_utc": self._started_at_utc,
            "duration_sec": round(total_duration, 3),
            "preflight_duration_sec": round(preflight_duration, 3),
            "scripted_duration_sec": round(scripted_duration, 3),
            "sample_count": self._samples_written,
            "session_path": None if self._session_path is None else str(self._session_path),
            "strict_mouse_calibration": self._strict_profile,
            "initial_pose": None
            if self._initial_pose is None
            else {
                "x": round(self._initial_pose.x, 3),
                "y": round(self._initial_pose.y, 3),
                "z": round(self._initial_pose.z, 3),
                "yaw": round(normalize_yaw(self._initial_pose.yaw), 3),
                "pitch": round(self._initial_pose.pitch, 3),
            },
            "final_pose": None
            if self._final_pose is None
            else {
                "x": round(self._final_pose.x, 3),
                "y": round(self._final_pose.y, 3),
                "z": round(self._final_pose.z, 3),
                "yaw": round(normalize_yaw(self._final_pose.yaw), 3),
                "pitch": round(self._final_pose.pitch, 3),
            },
            "expected_start_state": self._calibration_expected_start_state,
            "preflight_validation": self._preflight_validation,
            "validation": {
                "passed": bool(status == "complete" and self._validation_passed and preflight_ready_for_profile),
                "segment_validation_passed": bool(status == "complete" and self._validation_passed),
                "preflight_ready_for_profile": preflight_ready_for_profile,
                "failure_count": len(validation_failures),
                "failures": validation_failures,
            },
            "segments": self._segment_results,
            "scout_report_path": str(scout_report) if scout_report.exists() else None,
        }

    def _build_profile(self, report: dict[str, object]) -> dict[str, object]:
        movement_metrics: Dict[str, object] = {}
        look_metrics: Dict[str, object] = {}
        for segment in self._segment_results:
            name = str(segment.get("name"))
            delta = segment.get("delta") if isinstance(segment.get("delta"), dict) else {}
            summary = {
                "horizontal_distance": delta.get("horizontal_distance"),
                "delta_yaw": delta.get("yaw"),
                "delta_pitch": delta.get("pitch"),
                "avg_horizontal_speed_bps": segment.get("avg_horizontal_speed_bps"),
                "max_horizontal_speed_bps": segment.get("max_horizontal_speed_bps"),
            }
            if name.startswith("look"):
                look_metrics[name] = summary
            else:
                movement_metrics[name] = summary
        strict_profile = self._strict_profile.copy() if isinstance(self._strict_profile, dict) else {}
        vertical_profile = derive_vertical_look_calibration(
            {
                "strict_mouse_calibration": strict_profile,
                "look_metrics": look_metrics,
            }
        )
        if isinstance(vertical_profile, dict):
            strict_profile.update(vertical_profile)

        return {
            "version": "1.0",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "preferred_mouse_backend": self.inputs.get_mouse_backend(),
            "strict_mouse_calibration": strict_profile,
            "expected_start_state": report.get("expected_start_state"),
            "preflight_validation": report.get("preflight_validation"),
            "validation": report.get("validation"),
            "movement_metrics": movement_metrics,
            "look_metrics": look_metrics,
            "last_run_report_path": None if self._summary_path is None else str(self._summary_path),
            "last_run_status": report.get("status"),
            "notes": [
                "This is a timed control calibration profile intended to bootstrap mine mode with known-good control behavior.",
                "Profiles are reused only when the scripted segment validation passes and preflight successfully normalizes to the expected grounded non-flying baseline.",
                "The profile captures empirical look and movement responses but does not yet replace higher-level mining strategy learning.",
            ],
        }

    def _update_success_memory(self, report: dict[str, object], profile: dict[str, object]) -> None:
        snapshot = load_control_calibration_memory()
        regions = snapshot.setdefault("regions", {})
        if not isinstance(regions, dict):
            snapshot["regions"] = {}
            regions = snapshot["regions"]
        region_key = _region_memory_key(self.region)
        previous = regions.get(region_key)
        entry = previous.copy() if isinstance(previous, dict) else {}
        previous_runs = int(entry.get("successful_runs", 0) or 0)
        run_count = previous_runs + 1

        mouse_backend_counts = (
            entry.get("mouse_backend_counts").copy()
            if isinstance(entry.get("mouse_backend_counts"), dict)
            else {}
        )
        backend_name = self.inputs.get_mouse_backend()
        mouse_backend_counts[backend_name] = int(mouse_backend_counts.get(backend_name, 0) or 0) + 1

        segment_averages = (
            entry.get("segment_averages").copy()
            if isinstance(entry.get("segment_averages"), dict)
            else {}
        )
        for segment in self._segment_results:
            segment_name = str(segment.get("name") or "unknown")
            delta = segment.get("delta") if isinstance(segment.get("delta"), dict) else {}
            existing = segment_averages.get(segment_name)
            aggregate = existing.copy() if isinstance(existing, dict) else {}
            samples = int(aggregate.get("samples", 0) or 0)
            next_samples = samples + 1
            aggregate["samples"] = next_samples
            metric_map = {
                "avg_horizontal_distance": float(delta.get("horizontal_distance") or 0.0),
                "avg_delta_yaw": float(delta.get("yaw") or 0.0),
                "avg_delta_pitch": float(delta.get("pitch") or 0.0),
                "avg_peak_delta_y": float(delta.get("peak_delta_y") or 0.0),
                "avg_trough_delta_y": float(delta.get("trough_delta_y") or 0.0),
                "avg_horizontal_speed_bps": float(segment.get("avg_horizontal_speed_bps") or 0.0),
            }
            for metric_name, metric_value in metric_map.items():
                previous_avg = float(aggregate.get(metric_name, 0.0) or 0.0)
                aggregate[metric_name] = round(
                    ((previous_avg * samples) + metric_value) / next_samples,
                    6,
                )
            segment_averages[segment_name] = aggregate

        strict_profile = profile.get("strict_mouse_calibration")
        surface_y_hint = None
        preflight = report.get("preflight_validation")
        if isinstance(preflight, dict):
            raw_surface_y = preflight.get("surface_y_hint")
            if isinstance(raw_surface_y, (int, float)):
                surface_y_hint = round(float(raw_surface_y), 3)

        entry.update(
            {
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "successful_runs": run_count,
                "region_name": region_key,
                "configured_region": None
                if self.region is None
                else {
                    "name": self.region.name,
                    "dimension": self.region.dimension,
                    "min": {"x": self.region.min_x, "y": self.region.min_y, "z": self.region.min_z},
                    "max": {"x": self.region.max_x, "y": self.region.max_y, "z": self.region.max_z},
                },
                "ground_surface_y": surface_y_hint,
                "mouse_backend_counts": mouse_backend_counts,
                "segment_averages": segment_averages,
                "last_success_report_path": None if self._summary_path is None else str(self._summary_path),
                "last_success_profile": profile,
                "last_success_report_summary": {
                    "started_at_utc": report.get("started_at_utc"),
                    "duration_sec": report.get("duration_sec"),
                    "preflight_duration_sec": report.get("preflight_duration_sec"),
                    "scripted_duration_sec": report.get("scripted_duration_sec"),
                    "sample_count": report.get("sample_count"),
                    "strict_mouse_calibration": strict_profile,
                },
            }
        )
        regions[region_key] = entry
        save_region_memory_snapshot(self._memory_path, snapshot)


@dataclass
class PendingRecoveryEvaluation:
    context: str
    action: MinerAction
    heading: Cardinal
    start_x: float
    start_z: float
    started_mono: float


@dataclass
class NavigationProbe:
    started_mono: float
    last_distance: float


@dataclass
class ScoutLegProbe:
    leg_start_axis: float
    last_check_mono: float
    last_check_axis: float


@dataclass
class ActionSoakGuard:
    action: MinerAction
    target_lane_z: Optional[float]
    heading: Optional[str]
    anchor_x: float
    anchor_y: float
    anchor_z: float
    started_mono: float


@dataclass
class ActivePatternExecution:
    pattern_name: str
    started_mono: float
    started_at_utc: str
    start_x: float
    start_y: float
    start_z: float
    target_lane_z: Optional[float]
    heading: Optional[str]
    initial_look_block: str
    metadata: dict[str, object] = field(default_factory=dict)
    retries: int = 0
    sample_count: int = 0
    max_horizontal_speed_bps: float = 0.0
    max_abs_vertical_speed_bps: float = 0.0
    horizontal_distance: float = 0.0
    max_delta_y: float = 0.0
    min_delta_y: float = 0.0
    last_x: float = 0.0
    last_y: float = 0.0
    last_z: float = 0.0


class MiningPatternExecutionMonitor:
    def __init__(self, logger: logging.Logger, region: Optional[RegionConfig]) -> None:
        self.logger = logger.getChild("pattern_monitor")
        self.region = region
        self._path = strategy_stats_path()
        self._snapshot = load_strategy_stats_snapshot()
        self._dirty = False
        self._last_save_mono = 0.0
        self._save_interval_sec = 1.0
        self._active: Optional[ActivePatternExecution] = None
        self._current_episode: dict[str, object] = {}
        self._episode_started_mono = 0.0
        self._run_started_at_utc: Optional[str] = None

    def begin_run(self) -> None:
        self._active = None
        self._run_started_at_utc = datetime.now(timezone.utc).isoformat()
        self._episode_started_mono = time.monotonic()
        self._current_episode = {
            "started_at_utc": self._run_started_at_utc,
            "patterns": {},
            "reset_count": 0,
            "score_total": 0.0,
            "notes": [],
        }

    def start_pattern(
        self,
        pattern_name: str,
        *,
        pose: Pose,
        obstacle: Optional[ObstacleInfo],
        metadata: Optional[dict[str, object]] = None,
        target_lane_z: Optional[float],
        heading: Optional[str],
    ) -> None:
        if self._active is not None and self._active.pattern_name == pattern_name:
            return
        if self._active is not None:
            self.finish_active_pattern(reason="superseded", pose=pose, obstacle=obstacle)
        self._active = ActivePatternExecution(
            pattern_name=pattern_name,
            started_mono=time.monotonic(),
            started_at_utc=datetime.now(timezone.utc).isoformat(),
            start_x=pose.x,
            start_y=pose.y,
            start_z=pose.z,
            target_lane_z=target_lane_z,
            heading=heading,
            initial_look_block=str(obstacle.block_id or "minecraft:air") if obstacle is not None else "minecraft:air",
            metadata=dict(metadata or {}),
            last_x=pose.x,
            last_y=pose.y,
            last_z=pose.z,
        )

    def note_retry(self) -> None:
        if self._active is None:
            return
        self._active.retries += 1

    def peek_active_result(
        self,
        *,
        reason: str,
        pose: Optional[Pose],
        obstacle: Optional[ObstacleInfo],
    ) -> Optional[dict[str, object]]:
        active = self._active
        if active is None or pose is None:
            return None
        duration_sec = max(0.0, time.monotonic() - active.started_mono)
        return self._evaluate_pattern(active, duration_sec=duration_sec, pose=pose, obstacle=obstacle, reason=reason)

    def sample(
        self,
        *,
        pose: Optional[Pose],
        obstacle: Optional[ObstacleInfo],
        motion: Optional[object],
        look_type: Optional[str],
    ) -> None:
        active = self._active
        if active is None or pose is None:
            return
        active.sample_count += 1
        active.last_x = pose.x
        active.last_y = pose.y
        active.last_z = pose.z
        active.horizontal_distance = math.sqrt((pose.x - active.start_x) ** 2 + (pose.z - active.start_z) ** 2)
        delta_y = pose.y - active.start_y
        active.max_delta_y = max(active.max_delta_y, delta_y)
        active.min_delta_y = min(active.min_delta_y, delta_y)
        if motion is not None:
            try:
                active.max_horizontal_speed_bps = max(
                    active.max_horizontal_speed_bps,
                    float(getattr(motion, "horizontal_speed_bps", 0.0) or 0.0),
                )
                active.max_abs_vertical_speed_bps = max(
                    active.max_abs_vertical_speed_bps,
                    abs(float(getattr(motion, "vel_y", 0.0) or 0.0)),
                )
            except Exception:
                pass
        if look_type and "last_look_type" not in active.metadata:
            active.metadata["last_look_type"] = look_type
        if obstacle is not None:
            active.metadata["last_look_block"] = str(obstacle.block_id or "minecraft:air")

    def finish_active_pattern(
        self,
        *,
        reason: str,
        pose: Optional[Pose],
        obstacle: Optional[ObstacleInfo],
    ) -> Optional[dict[str, object]]:
        active = self._active
        if active is None:
            return None
        self._active = None
        duration_sec = max(0.0, time.monotonic() - active.started_mono)
        result = self._evaluate_pattern(active, duration_sec=duration_sec, pose=pose, obstacle=obstacle, reason=reason)
        self._record_result(result)
        self.logger.info(
            "Pattern execution result: pattern=%s outcome=%s reason=%s distance=%.2f max_h_speed=%.2f delta_y=[%.2f, %.2f] retries=%d",
            result["pattern_name"],
            result["outcome"],
            reason,
            result["horizontal_distance"],
            result["max_horizontal_speed_bps"],
            result["min_delta_y"],
            result["max_delta_y"],
            result["retries"],
        )
        return result

    def note_reset(self, pose: Optional[Pose]) -> None:
        if self._current_episode:
            self._current_episode["reset_count"] = int(self._current_episode.get("reset_count", 0) or 0) + 1
            if pose is not None:
                self._current_episode["last_reset_pose"] = {
                    "x": round(pose.x, 3),
                    "y": round(pose.y, 3),
                    "z": round(pose.z, 3),
                    "yaw": round(normalize_yaw(pose.yaw), 3),
                    "pitch": round(pose.pitch, 3),
                    "ts_utc": pose.ts_utc,
                }
            self._current_episode["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
            self._dirty = True
            self.maybe_flush()

    def maybe_flush(self, *, force: bool = False) -> None:
        if not self._dirty:
            return
        now = time.monotonic()
        if not force and (now - self._last_save_mono) < self._save_interval_sec:
            return
        save_region_memory_snapshot(self._path, self._snapshot)
        self._dirty = False
        self._last_save_mono = now

    def _evaluate_pattern(
        self,
        active: ActivePatternExecution,
        *,
        duration_sec: float,
        pose: Optional[Pose],
        obstacle: Optional[ObstacleInfo],
        reason: str,
    ) -> dict[str, object]:
        target_speed = float(active.metadata.get("target_speed_bps") or 0.0)
        walk_speed = float(active.metadata.get("walk_speed_bps") or 0.0)
        vertical_gain = active.max_delta_y
        horizontal_distance = active.horizontal_distance
        max_horizontal_speed = active.max_horizontal_speed_bps
        likely_airborne = vertical_gain >= 0.18 or active.max_abs_vertical_speed_bps >= 0.75
        strong_ground_progress = horizontal_distance >= max(4.0, walk_speed * max(0.75, duration_sec * 0.5))
        target_met = max_horizontal_speed >= max(0.1, target_speed)
        near_target = max_horizontal_speed >= max(0.1, target_speed * 0.88)

        if active.pattern_name == "flight_tunnel_burst":
            if target_met or (likely_airborne and horizontal_distance >= max(3.5, target_speed * max(0.4, duration_sec * 0.3))):
                outcome = "flight_success"
            elif near_target or strong_ground_progress:
                outcome = "ceiling_constrained_ground_burst"
            else:
                outcome = "failed_enable_or_blocked"
        else:
            outcome = "completed"

        return {
            "pattern_name": active.pattern_name,
            "started_at_utc": active.started_at_utc,
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "duration_sec": round(duration_sec, 3),
            "reason": reason,
            "outcome": outcome,
            "start_pose": {
                "x": round(active.start_x, 3),
                "y": round(active.start_y, 3),
                "z": round(active.start_z, 3),
            },
            "end_pose": None
            if pose is None
            else {
                "x": round(pose.x, 3),
                "y": round(pose.y, 3),
                "z": round(pose.z, 3),
                "yaw": round(normalize_yaw(pose.yaw), 3),
                "pitch": round(pose.pitch, 3),
                "ts_utc": pose.ts_utc,
            },
            "final_look_block": None if obstacle is None else str(obstacle.block_id or "minecraft:air"),
            "target_lane_z": active.target_lane_z,
            "heading": active.heading,
            "initial_look_block": active.initial_look_block,
            "horizontal_distance": round(horizontal_distance, 3),
            "max_horizontal_speed_bps": round(max_horizontal_speed, 3),
            "max_abs_vertical_speed_bps": round(active.max_abs_vertical_speed_bps, 3),
            "max_delta_y": round(active.max_delta_y, 3),
            "min_delta_y": round(active.min_delta_y, 3),
            "retries": active.retries,
            "sample_count": active.sample_count,
            "metadata": active.metadata,
        }

    def _record_result(self, result: dict[str, object]) -> None:
        patterns = self._snapshot.setdefault("patterns", {})
        if not isinstance(patterns, dict):
            self._snapshot["patterns"] = {}
            patterns = self._snapshot["patterns"]
        pattern_name = str(result.get("pattern_name") or "unknown")
        score = self._score_result(result)
        result["score"] = score
        bucket = patterns.get(pattern_name)
        if not isinstance(bucket, dict):
            bucket = {}
            patterns[pattern_name] = bucket
        bucket["attempts"] = int(bucket.get("attempts", 0) or 0) + 1
        outcome = str(result.get("outcome") or "unknown")
        bucket[outcome] = int(bucket.get(outcome, 0) or 0) + 1
        bucket["best_horizontal_speed_bps"] = round(
            max(float(bucket.get("best_horizontal_speed_bps", 0.0) or 0.0), float(result.get("max_horizontal_speed_bps") or 0.0)),
            3,
        )
        bucket["best_horizontal_distance"] = round(
            max(float(bucket.get("best_horizontal_distance", 0.0) or 0.0), float(result.get("horizontal_distance") or 0.0)),
            3,
        )
        bucket["avg_duration_sec"] = round(
            (
                (float(bucket.get("avg_duration_sec", 0.0) or 0.0) * (bucket["attempts"] - 1))
                + float(result.get("duration_sec") or 0.0)
            ) / max(1, bucket["attempts"]),
            3,
        )
        bucket["avg_score"] = round(
            (
                (float(bucket.get("avg_score", 0.0) or 0.0) * (bucket["attempts"] - 1))
                + score
            ) / max(1, bucket["attempts"]),
            3,
        )
        bucket["best_score"] = round(
            max(float(bucket.get("best_score", score) or score), score),
            3,
        )
        bucket["updated_at_utc"] = datetime.now(timezone.utc).isoformat()

        episode_patterns = self._current_episode.setdefault("patterns", {})
        if not isinstance(episode_patterns, dict):
            self._current_episode["patterns"] = {}
            episode_patterns = self._current_episode["patterns"]
        pattern_stats = episode_patterns.get(pattern_name)
        if not isinstance(pattern_stats, dict):
            pattern_stats = {}
            episode_patterns[pattern_name] = pattern_stats
        pattern_stats["attempts"] = int(pattern_stats.get("attempts", 0) or 0) + 1
        pattern_stats[outcome] = int(pattern_stats.get(outcome, 0) or 0) + 1
        pattern_stats["latest_reason"] = str(result.get("reason") or "")
        pattern_stats["avg_score"] = round(
            (
                (float(pattern_stats.get("avg_score", 0.0) or 0.0) * (pattern_stats["attempts"] - 1))
                + score
            ) / max(1, pattern_stats["attempts"]),
            3,
        )
        pattern_stats["best_score"] = round(
            max(float(pattern_stats.get("best_score", score) or score), score),
            3,
        )
        self._current_episode["score_total"] = round(
            float(self._current_episode.get("score_total", 0.0) or 0.0) + score,
            3,
        )
        self._current_episode["updated_at_utc"] = datetime.now(timezone.utc).isoformat()

        episodes = self._snapshot.setdefault("episodes", [])
        if isinstance(episodes, list):
            episode_summary = {
                "started_at_utc": self._run_started_at_utc,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "reset_count": int(self._current_episode.get("reset_count", 0) or 0),
                "score_total": float(self._current_episode.get("score_total", 0.0) or 0.0),
                "patterns": self._current_episode.get("patterns", {}),
                "latest_pattern_result": result,
            }
            episodes.append(episode_summary)
            if len(episodes) > 80:
                del episodes[:-80]

        self._dirty = True
        self.maybe_flush()

    def recommend_pattern_usage(self, pattern_name: str, *, default_enabled: bool = True) -> tuple[bool, str]:
        patterns = self._snapshot.get("patterns")
        if not isinstance(patterns, dict):
            return default_enabled, "warmup:no_snapshot"
        bucket = patterns.get(pattern_name)
        if not isinstance(bucket, dict):
            return default_enabled, "warmup:no_history"

        attempts = int(bucket.get("attempts", 0) or 0)
        success_count = int(bucket.get("flight_success", 0) or 0)
        constrained_count = int(bucket.get("ceiling_constrained_ground_burst", 0) or 0)
        failed_count = int(bucket.get("failed_enable_or_blocked", 0) or 0)
        avg_score = float(bucket.get("avg_score", 0.0) or 0.0)
        effective_success_rate = 0.0
        if attempts > 0:
            effective_success_rate = (success_count + (0.5 * constrained_count)) / attempts

        if attempts < 4:
            return default_enabled, f"warmup:attempts={attempts}"

        if effective_success_rate >= 0.58 and avg_score >= 6.0:
            explore_rate = 0.12
            use_pattern = random.random() >= explore_rate
            mode = "exploit_pattern" if use_pattern else "ground_explore"
            return use_pattern, (
                f"{mode}:attempts={attempts} success_rate={effective_success_rate:.2f} "
                f"avg_score={avg_score:.2f} explore_rate={explore_rate:.2f}"
            )

        if failed_count >= max(3, success_count + constrained_count) or avg_score <= 1.5:
            explore_rate = 0.18
            use_pattern = random.random() < explore_rate
            mode = "flight_explore" if use_pattern else "exploit_ground"
            return use_pattern, (
                f"{mode}:attempts={attempts} failed={failed_count} "
                f"success_rate={effective_success_rate:.2f} avg_score={avg_score:.2f} "
                f"explore_rate={explore_rate:.2f}"
            )

        explore_rate = 0.35
        use_pattern = random.random() >= 0.5
        mode = "mixed_flight" if use_pattern else "mixed_ground"
        return use_pattern, (
            f"{mode}:attempts={attempts} success_rate={effective_success_rate:.2f} "
            f"avg_score={avg_score:.2f} explore_rate={explore_rate:.2f}"
        )

    def _score_result(self, result: dict[str, object]) -> float:
        outcome = str(result.get("outcome") or "unknown")
        distance = float(result.get("horizontal_distance") or 0.0)
        max_speed = float(result.get("max_horizontal_speed_bps") or 0.0)
        retries = int(result.get("retries", 0) or 0)
        duration_sec = float(result.get("duration_sec") or 0.0)

        score = distance + (max_speed * 0.6) - (retries * 1.5) - max(0.0, duration_sec - 2.5) * 0.35
        if outcome == "flight_success":
            score += 6.0
        elif outcome == "ceiling_constrained_ground_burst":
            score += 2.0
        elif outcome == "failed_enable_or_blocked":
            score -= 5.0
        return round(score, 3)


class RecoveryBandit:
    def __init__(self, epsilon: float, logger: logging.Logger) -> None:
        self.epsilon = epsilon
        self.logger = logger.getChild("recovery_bandit")
        self._path = Path(__file__).resolve().parents[2] / "data" / "recovery_policy.json"
        self._stats: Dict[str, Dict[str, Dict[str, float]]] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            self._stats = raw.get("stats", {}) or {}
            self.logger.info("Loaded recovery policy from %s", self._path)
        except Exception as exc:  # pragma: no cover
            self.logger.warning("Failed to load recovery policy: %s", exc)

    def save(self) -> None:
        try:
            payload = {
                "version": "1.0",
                "epsilon": self.epsilon,
                "stats": self._stats,
            }
            self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception as exc:  # pragma: no cover
            self.logger.warning("Failed to save recovery policy: %s", exc)

    def _value(self, context: str, action: MinerAction) -> float:
        return float(self._stats.get(context, {}).get(action.value, {}).get("avg_reward", 0.0))

    def select_action(self, context: str, available: Sequence[MinerAction]) -> MinerAction:
        if not available:
            return MinerAction.RESET_GMINE
        if len(available) == 1:
            return available[0]
        if random.random() < self.epsilon:
            choice = random.choice(list(available))
            self.logger.info("Exploring recovery action: context=%s action=%s", context, choice.value)
            return choice

        best = available[0]
        best_value = self._value(context, best)
        for action in available[1:]:
            value = self._value(context, action)
            if value > best_value:
                best = action
                best_value = value
        return best

    def update(self, context: str, action: MinerAction, reward: float) -> None:
        ctx = self._stats.setdefault(context, {})
        bucket = ctx.setdefault(action.value, {"count": 0.0, "avg_reward": 0.0})
        count = int(bucket.get("count", 0)) + 1
        avg = float(bucket.get("avg_reward", 0.0))
        avg += (reward - avg) / count
        bucket["count"] = count
        bucket["avg_reward"] = avg
        self.logger.info(
            "Updated recovery policy: context=%s action=%s count=%d avg_reward=%.3f",
            context,
            action.value,
            count,
            avg,
        )
        self.save()


def resolve_default_forge_log_path() -> Path:
    override = os.getenv("MAM_FORGE_LOG_PATH")
    if override:
        return Path(override)
    appdata = os.environ.get("APPDATA")
    if not appdata:
        appdata = str(Path.home() / "AppData" / "Roaming")
    return Path(appdata) / ".minecraft" / "mam_telemetry" / "mam_f3_stream.log"


def rotate_forge_telemetry_log(
    log_path: Path,
    logger: logging.Logger,
    *,
    keep_tail_mb: float = 0.0,
) -> Optional[Path]:
    if not log_path.exists():
        logger.warning("Forge telemetry log not found at %s; nothing to rotate.", log_path)
        return None

    keep_tail_bytes = max(0, int(max(0.0, keep_tail_mb) * 1024 * 1024))
    tail_bytes: bytes = b""
    if keep_tail_bytes > 0:
        with log_path.open("rb") as raw:
            raw.seek(0, os.SEEK_END)
            size = raw.tell()
            raw.seek(max(0, size - keep_tail_bytes))
            tail_bytes = raw.read()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rotated_path = log_path.with_name(f"{log_path.stem}.{timestamp}.rotated{log_path.suffix}")
    try:
        log_path.replace(rotated_path)
    except PermissionError:
        logger.error(
            "Failed to rotate Forge telemetry log at %s because Windows still has it open. "
            "Close Minecraft first, then rerun `python -m minecraft_auto_miner.app --rotate-forge-log`.",
            log_path,
        )
        return None

    if keep_tail_bytes > 0:
        log_path.write_bytes(tail_bytes)
        logger.info(
            "Rotated Forge telemetry log to %s and preserved the last %.1f MB in %s.",
            rotated_path,
            keep_tail_bytes / (1024 * 1024),
            log_path,
        )
    else:
        logger.info("Rotated Forge telemetry log to %s.", rotated_path)
    return rotated_path


def report_forge_telemetry_preflight(log_path: Path, logger: logging.Logger) -> None:
    try:
        stat = log_path.stat()
        drive_root = Path(log_path.anchor) if log_path.anchor else log_path.parent
        usage = shutil.disk_usage(drive_root)
        last_write_utc = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
        logger.info(
            "Forge telemetry preflight: log_size_mb=%.1f free_disk_gb=%.2f last_write_utc=%s",
            stat.st_size / (1024 * 1024),
            usage.free / (1024 * 1024 * 1024),
            last_write_utc,
        )
        if usage.free < 5 * 1024 * 1024 * 1024:
            logger.warning(
                "Low disk space on %s: only %.2f GB free. The Forge telemetry mod previously failed with "
                "\"There is not enough space on the disk\" and may stop writing again until space is freed.",
                drive_root,
                usage.free / (1024 * 1024 * 1024),
            )
        if stat.st_size >= 256 * 1024 * 1024:
            logger.warning(
                "Forge telemetry log is large (%.1f MB). Consider rotating or deleting %s after closing Minecraft "
                "to prevent future telemetry write failures. Helper: `python -m minecraft_auto_miner.app --rotate-forge-log`.",
                stat.st_size / (1024 * 1024),
                log_path,
            )
    except FileNotFoundError:
        logger.warning("Forge telemetry log does not exist yet at %s", log_path)
        return
    except Exception as exc:  # pragma: no cover
        logger.warning("Unable to inspect Forge telemetry log preflight: %s", exc)

    latest_log_path = log_path.parent.parent / "logs" / "latest.log"
    try:
        if not latest_log_path.exists():
            return
        with latest_log_path.open("rb") as raw:
            raw.seek(max(0, latest_log_path.stat().st_size - 262144))
            tail = raw.read().decode("utf-8", errors="replace")
        if (
            "MamForgeTelemetry" in tail
            and "There is not enough space on the disk" in tail
        ):
            logger.warning(
                "Minecraft latest.log shows MamForgeTelemetry disabled itself after a disk-space write failure. "
                "Free disk space, restart Minecraft, and if needed rotate %s before the next mining run.",
                log_path,
            )
    except Exception as exc:  # pragma: no cover
        logger.warning("Unable to inspect Minecraft latest.log for telemetry issues: %s", exc)


def run_telemetry_pipeline_loop(stop_event: threading.Event, interval_sec: float) -> None:
    logger = logging.getLogger("minecraft_auto_miner")
    bronze_max_rows = _env_int("MAM_BRONZE_MAX_ROWS_PER_LOOP", 5000)
    logger.info("Telemetry pipeline thread starting (interval=%.2fs)", interval_sec)

    while not stop_event.is_set():
        start = time.time()

        try:
            inserted = bronze_f3_ingest.main(max_rows=bronze_max_rows)
            logger.info("bronze_f3_ingest.main() inserted %s new rows.", inserted)
        except Exception as exc:  # pragma: no cover
            logger.error("bronze_f3_ingest.main() failed: %s", exc, exc_info=True)

        try:
            silver_f3_compress.main()
        except Exception as exc:  # pragma: no cover
            logger.error("silver_f3_compress.main() failed: %s", exc, exc_info=True)

        try:
            episodes_from_silver.main()
        except Exception as exc:  # pragma: no cover
            logger.error("episodes_from_silver.main() failed: %s", exc, exc_info=True)

        try:
            gold_views.main()
        except Exception as exc:  # pragma: no cover
            logger.error("gold_views.main() failed: %s", exc, exc_info=True)

        elapsed = time.time() - start
        sleep_for = max(0.1, interval_sec - elapsed)
        if stop_event.wait(timeout=sleep_for):
            break

    logger.info("Telemetry pipeline thread exiting.")


def launch_dashboard_if_enabled(logger: logging.Logger) -> None:
    flag = os.getenv("MAM_DASHBOARD_AUTO_LAUNCH", "1").lower()
    if flag not in {"1", "true", "yes", "on"}:
        logger.info("Dashboard auto-launch disabled via MAM_DASHBOARD_AUTO_LAUNCH=%s", flag)
        return

    script_path = Path(__file__).resolve().parent / "dashboard" / "main_streamlit.py"
    if not script_path.exists():
        logger.warning("Streamlit dashboard not found at %s; skipping auto-launch.", script_path)
        return

    try:
        cmd = [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(script_path),
            "--server.headless=true",
        ]
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.info("Streamlit dashboard auto-launched on the default Streamlit port.")
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to auto-launch Streamlit dashboard: %s", exc, exc_info=True)


class AutonomousController:
    def __init__(
        self,
        cfg: AppConfig,
        logger: logging.Logger,
        world_model: WorldModelForge,
        inputs: SimpleInputController,
        region: Optional[RegionConfig],
        recovery_policy: RecoveryBandit,
        blocking_learner: BlockingBlockLearner,
        strategy_planner: MiningStrategyPlanner,
        voxel_memory: Optional[VoxelWorldMemory] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        self.cfg = cfg
        self.logger = logger.getChild("controller")
        self.world_model = world_model
        self.inputs = inputs
        self.region = region
        self.recovery_policy = recovery_policy
        self.blocking_learner = blocking_learner
        self.strategy_planner = strategy_planner
        self.voxel_memory = voxel_memory
        self.stop_event = stop_event or threading.Event()

        self.current_action: MinerAction = MinerAction.NONE
        self.desired_heading: Optional[Cardinal] = None
        self.lane_shift_direction: int = 1
        self.last_control_mono: float = 0.0
        self.last_stale_log_mono: float = 0.0
        self.progress_probe: Optional[Tuple[float, float, float]] = None
        self.pending_eval: Optional[PendingRecoveryEvaluation] = None
        self.consecutive_recoveries: int = 0
        self.region_disabled_for_run: bool = False
        self._was_in_region: bool = False
        self.navigation_probe: Optional[NavigationProbe] = None
        self.entry_break_in_active: bool = False
        self.last_navigation_log_mono: float = 0.0
        self.last_pose: Optional[Pose] = None
        self.top_reset_reorient_until_mono: float = 0.0
        self.reacquire_scan_bias: int = 1
        self.pitch_sweep_sequence: Tuple[float, ...] = (8.0, 12.0, 18.0, 24.0, 18.0, 12.0)
        self.pitch_sweep_index: int = 1
        self.reacquire_failures: int = 0
        self.reacquire_failures_before_shift: int = cfg.reacquire_failures_before_shift
        self.active_plan: Optional[MiningPlan] = None
        self.plan_needs_refresh: bool = True
        self.target_lane_z: Optional[float] = None
        self.entry_lane_z: Optional[float] = None
        self._profiled_pitch_step_down_deg: float = 0.0
        self._profiled_pitch_step_up_deg: float = 0.0
        self._profiled_pitch_step_pixels: int = 8
        self._profiled_pitch_deg_per_pixel: float = 0.0
        self._expected_walk_sprint_speed_bps: float = 0.0
        self._expected_fly_forward_speed_bps: float = 0.0
        self._expected_fly_sprint_speed_bps: float = 0.0
        self._prefer_flight_tunnel: bool = False
        self._flight_tunnel_active: bool = False
        self._last_flight_toggle_mono: float = 0.0
        self._last_flight_retry_mono: float = 0.0
        self._flight_tunnel_retry_count: int = 0
        self._flight_tunnel_suppressed_for_lane: bool = False
        self._flight_policy_enabled_for_lane: Optional[bool] = None
        self._flight_policy_reason_for_lane: Optional[str] = None
        self._flight_tunnel_runtime_outcome: Optional[str] = None
        self._flight_tunnel_confirmed_runtime: bool = False
        self._lane_drift_stall_count: int = 0
        self._soak_guard: Optional[ActionSoakGuard] = None
        self._scout_memory_path = perimeter_scout_memory_path()
        self._scout_memory_mtime_ns: Optional[int] = None
        self._scout_memory_known_non_mineable_points: Set[Tuple[int, int, int]] = set()
        self._scout_memory_known_mineable_points: Set[Tuple[int, int, int]] = set()
        self._scout_memory_known_air_points: Set[Tuple[int, int, int]] = set()
        self._scout_memory_non_mineable_block_ids: Set[str] = set()
        self._scout_memory_report: Optional[dict[str, object]] = None
        self.pattern_monitor = MiningPatternExecutionMonitor(self.logger, self.region)
        self._load_calibrated_motion_capabilities()
        self._refresh_scout_memory_if_needed(force=True)

    def _stop_requested(self) -> bool:
        return self.stop_event.is_set()

    def _sleep_interruptibly(self, seconds: float) -> bool:
        deadline = time.monotonic() + max(0.0, seconds)
        while time.monotonic() < deadline:
            if self._stop_requested():
                return False
            time.sleep(min(0.02, deadline - time.monotonic()))
        return not self._stop_requested()

    def begin_run(self) -> None:
        self.reset_for_new_run()
        self.pattern_monitor.begin_run()
        self._refresh_scout_memory_if_needed(force=True)
        self.inputs.all_stop()

    def reset_for_new_run(self) -> None:
        self.current_action = MinerAction.WAIT_FOR_TELEMETRY
        self.desired_heading = None
        self.lane_shift_direction = 1
        self.last_control_mono = 0.0
        self.progress_probe = None
        self.pending_eval = None
        self.consecutive_recoveries = 0
        self.region_disabled_for_run = False
        self._was_in_region = False
        self.navigation_probe = None
        self.entry_break_in_active = False
        self.last_navigation_log_mono = 0.0
        self.last_pose = None
        self.top_reset_reorient_until_mono = 0.0
        self.reacquire_scan_bias = 1
        self.pitch_sweep_index = 1
        self.reacquire_failures = 0
        self.reacquire_failures_before_shift = self.cfg.reacquire_failures_before_shift
        self.active_plan = None
        self.plan_needs_refresh = True
        self.target_lane_z = None
        self.entry_lane_z = None
        self._flight_tunnel_active = False
        self._last_flight_toggle_mono = 0.0
        self._last_flight_retry_mono = 0.0
        self._flight_tunnel_retry_count = 0
        self._flight_tunnel_suppressed_for_lane = False
        self._flight_policy_enabled_for_lane = None
        self._flight_policy_reason_for_lane = None
        self._flight_tunnel_runtime_outcome = None
        self._flight_tunnel_confirmed_runtime = False
        self._lane_drift_stall_count = 0
        self._soak_guard = None
        self.inputs.all_stop()

    def flush_runtime_learning(self, *, force: bool = False) -> None:
        self.pattern_monitor.maybe_flush(force=force)

    def apply_control_calibration_profile(self, profile: Optional[dict[str, object]]) -> None:
        self._profiled_pitch_step_down_deg = 0.0
        self._profiled_pitch_step_up_deg = 0.0
        self._profiled_pitch_step_pixels = 8
        self._profiled_pitch_deg_per_pixel = 0.0
        vertical_calibration = derive_vertical_look_calibration(profile)
        if not isinstance(vertical_calibration, dict):
            return
        try:
            down_deg = abs(float(vertical_calibration.get("pitch_step_down_deg") or 0.0))
            up_deg = abs(float(vertical_calibration.get("pitch_step_up_deg") or 0.0))
            step_pixels = max(1, int(vertical_calibration.get("pitch_step_pixels") or 8))
            deg_per_pixel = float(vertical_calibration.get("pitch_deg_per_pixel") or 0.0)
        except (TypeError, ValueError):
            return
        if down_deg <= 0.0 or up_deg <= 0.0 or deg_per_pixel <= 0.0:
            return
        self._profiled_pitch_step_down_deg = down_deg
        self._profiled_pitch_step_up_deg = up_deg
        self._profiled_pitch_step_pixels = step_pixels
        self._profiled_pitch_deg_per_pixel = deg_per_pixel
        self.logger.info(
            "Controller loaded vertical look calibration profile: step_pixels=%d pitch_deg_per_pixel=%.4f",
            self._profiled_pitch_step_pixels,
            self._profiled_pitch_deg_per_pixel,
        )

    def _profiled_pitch_ready(self) -> bool:
        return self._profiled_pitch_deg_per_pixel > 0.0 and self._profiled_pitch_step_pixels > 0

    def _load_calibrated_motion_capabilities(self) -> None:
        segment_averages = load_control_segment_averages(self.region)

        def _avg_speed(name: str) -> float:
            meta = segment_averages.get(name)
            if not isinstance(meta, dict):
                return 0.0
            try:
                return max(0.0, float(meta.get("avg_horizontal_speed_bps") or 0.0))
            except (TypeError, ValueError):
                return 0.0

        self._expected_walk_sprint_speed_bps = max(_avg_speed("sprint_forward"), _avg_speed("forward"))
        self._expected_fly_forward_speed_bps = _avg_speed("fly_forward")
        self._expected_fly_sprint_speed_bps = _avg_speed("fly_sprint_forward")
        self._prefer_flight_tunnel = self._expected_fly_sprint_speed_bps >= max(
            7.0,
            self._expected_walk_sprint_speed_bps + 2.0,
        )
        if (
            self._expected_walk_sprint_speed_bps > 0.0
            or self._expected_fly_forward_speed_bps > 0.0
            or self._expected_fly_sprint_speed_bps > 0.0
        ):
            self.logger.info(
                "Loaded calibrated motion capabilities: walk_sprint=%.2f fly_forward=%.2f fly_sprint=%.2f prefer_flight_tunnel=%s",
                self._expected_walk_sprint_speed_bps,
                self._expected_fly_forward_speed_bps,
                self._expected_fly_sprint_speed_bps,
                self._prefer_flight_tunnel,
            )

    def _flight_tunnel_speed_target_bps(self) -> float:
        fly_reference = self._expected_fly_sprint_speed_bps or self._expected_fly_forward_speed_bps
        if fly_reference > 0.0:
            return max(self._expected_walk_sprint_speed_bps * 0.95, fly_reference * 0.58)
        return max(6.0, self._expected_walk_sprint_speed_bps + 1.0)

    def _should_use_flight_tunnel(self, pose: Pose, obstacle: Optional[ObstacleInfo] = None) -> bool:
        if not self._prefer_flight_tunnel or self.region is None:
            return False
        if self._flight_tunnel_suppressed_for_lane:
            return False
        if pose.y > (self.region.max_y - 3.0):
            return False
        if pose.y <= (self.region.min_y + 3.0):
            return False
        if self.target_lane_z is not None and abs(pose.z - self.target_lane_z) > max(
            0.85,
            self.cfg.lane_drift_tolerance * 1.35,
        ):
            return False
        if obstacle is not None and self._effective_look_type(obstacle) != "ALLOWED":
            return False
        if self._flight_policy_enabled_for_lane is None:
            use_pattern, reason = self.pattern_monitor.recommend_pattern_usage(
                "flight_tunnel_burst",
                default_enabled=self._prefer_flight_tunnel,
            )
            self._flight_policy_enabled_for_lane = use_pattern
            self._flight_policy_reason_for_lane = reason
            self.logger.info(
                "Flight tunnel lane policy: use=%s reason=%s target_lane_z=%.1f y=%.1f",
                use_pattern,
                reason,
                self.target_lane_z if self.target_lane_z is not None else float("nan"),
                pose.y,
            )
        return bool(self._flight_policy_enabled_for_lane)

    def _sample_pattern_monitor(
        self,
        *,
        pose: Optional[Pose],
        obstacle: Optional[ObstacleInfo],
        look_type: Optional[str],
    ) -> None:
        self.pattern_monitor.sample(
            pose=pose,
            obstacle=obstacle,
            motion=self.world_model.get_motion_estimate(),
            look_type=look_type,
        )

    def _active_flight_tunnel_result(
        self,
        *,
        pose: Optional[Pose],
        obstacle: Optional[ObstacleInfo],
        reason: str = "in_progress",
    ) -> Optional[dict[str, object]]:
        return self.pattern_monitor.peek_active_result(reason=reason, pose=pose, obstacle=obstacle)

    def _reset_lane_runtime_decisions(self) -> None:
        self._flight_tunnel_retry_count = 0
        self._flight_tunnel_suppressed_for_lane = False
        self._flight_policy_enabled_for_lane = None
        self._flight_policy_reason_for_lane = None
        self._flight_tunnel_runtime_outcome = None
        self._flight_tunnel_confirmed_runtime = False
        self._lane_drift_stall_count = 0
        self._soak_guard = None

    def _check_action_soak_guard(self, pose: Pose) -> Optional[str]:
        watch_actions = {
            MinerAction.FORWARD_MINE,
            MinerAction.REACQUIRE_STONE,
            MinerAction.SHIFT_POSITIVE,
            MinerAction.SHIFT_NEGATIVE,
            MinerAction.RESET_GMINE,
            MinerAction.NAVIGATE_TO_REGION,
        }
        if self.current_action not in watch_actions:
            self._soak_guard = None
            return None

        lane_key = None if self.target_lane_z is None else round(self.target_lane_z, 1)
        heading_key = None if self.desired_heading is None else self.desired_heading.value
        moved_since_anchor = None
        now = time.monotonic()
        guard = self._soak_guard
        if guard is not None:
            moved_since_anchor = math.sqrt(
                (pose.x - guard.anchor_x) ** 2
                + (pose.y - guard.anchor_y) ** 2
                + (pose.z - guard.anchor_z) ** 2
            )
        signature_changed = (
            guard is None
            or guard.action != self.current_action
            or guard.target_lane_z != lane_key
            or guard.heading != heading_key
        )
        if signature_changed or (moved_since_anchor is not None and moved_since_anchor > self.cfg.strict_soak_pose_distance):
            self._soak_guard = ActionSoakGuard(
                action=self.current_action,
                target_lane_z=lane_key,
                heading=heading_key,
                anchor_x=pose.x,
                anchor_y=pose.y,
                anchor_z=pose.z,
                started_mono=now,
            )
            return None

        elapsed = now - guard.started_mono
        timeout_sec = (
            self.cfg.strict_soak_correction_timeout_sec
            if self.current_action in {MinerAction.REACQUIRE_STONE, MinerAction.SHIFT_POSITIVE, MinerAction.SHIFT_NEGATIVE}
            else self.cfg.strict_soak_timeout_sec
        )
        if elapsed < timeout_sec:
            return None
        return (
            f"action={self.current_action.value} lane={lane_key} heading={heading_key} "
            f"elapsed={elapsed:.1f}s moved={moved_since_anchor or 0.0:.2f}"
        )

    def _engage_flight_tunnel_mode(
        self,
        *,
        pose: Optional[Pose] = None,
        obstacle: Optional[ObstacleInfo] = None,
    ) -> bool:
        if not self._prefer_flight_tunnel:
            return False
        if self._flight_tunnel_active:
            return True
        now = time.monotonic()
        if now - self._last_flight_toggle_mono < 0.35:
            return False
        self.inputs.stop_mining()
        self.inputs.release_key("w")
        self.inputs.release_key("a")
        self.inputs.release_key("d")
        self.inputs.release_key("ctrl")
        self.inputs.stop_vertical_motion()
        self.inputs.enable_fly_mode()
        time.sleep(0.12)
        self._flight_tunnel_active = True
        self._last_flight_toggle_mono = time.monotonic()
        self._last_flight_retry_mono = 0.0
        self._flight_tunnel_retry_count = 0
        self._flight_tunnel_runtime_outcome = None
        self._flight_tunnel_confirmed_runtime = False
        current_pose = pose or self.world_model.get_player_pose()
        current_obstacle = obstacle or self.world_model.is_obstacle_ahead()
        if current_pose is not None:
            self.pattern_monitor.start_pattern(
                "flight_tunnel_burst",
                pose=current_pose,
                obstacle=current_obstacle,
                metadata={
                    "target_speed_bps": round(self._flight_tunnel_speed_target_bps(), 3),
                    "walk_speed_bps": round(self._expected_walk_sprint_speed_bps, 3),
                    "expected_fly_forward_bps": round(self._expected_fly_forward_speed_bps, 3),
                    "expected_fly_sprint_bps": round(self._expected_fly_sprint_speed_bps, 3),
                    "region_name": None if self.region is None else self.region.name,
                    "scout_known_non_mineable_materials": len(self._scout_memory_non_mineable_block_ids),
                    "initial_look_type": None if current_obstacle is None else self._effective_look_type(current_obstacle),
                    "lane_policy_reason": self._flight_policy_reason_for_lane,
                },
                target_lane_z=self.target_lane_z,
                heading=None if self.desired_heading is None else self.desired_heading.value,
            )
        self.logger.info(
            "Engaging flight tunnel mode: expected_fly_sprint=%.2f walk_sprint=%.2f",
            self._expected_fly_sprint_speed_bps,
            self._expected_walk_sprint_speed_bps,
        )
        return True

    def _disengage_flight_tunnel_mode(
        self,
        *,
        reason: str,
        pose: Optional[Pose] = None,
        obstacle: Optional[ObstacleInfo] = None,
    ) -> None:
        if not self._flight_tunnel_active:
            return
        current_pose = pose or self.world_model.get_player_pose()
        current_obstacle = obstacle or self.world_model.is_obstacle_ahead()
        result = self.pattern_monitor.finish_active_pattern(reason=reason, pose=current_pose, obstacle=current_obstacle)
        outcome = None if result is None else str(result.get("outcome") or "")
        if outcome:
            self._flight_tunnel_runtime_outcome = outcome
            if outcome == "flight_success":
                self._flight_tunnel_confirmed_runtime = True
        self.inputs.stop_mining()
        self.inputs.release_key("w")
        self.inputs.release_key("a")
        self.inputs.release_key("d")
        self.inputs.release_key("ctrl")
        self.inputs.stop_vertical_motion()
        should_toggle_off = self._flight_tunnel_confirmed_runtime or outcome == "flight_success"
        if should_toggle_off:
            self.inputs.disable_fly_mode()
            time.sleep(0.14)
        elif outcome in {"ceiling_constrained_ground_burst", "failed_enable_or_blocked"}:
            self.logger.info(
                "Skipping fly-disable toggle because telemetry classified the burst as %s rather than confirmed flight.",
                outcome,
            )
        self._flight_tunnel_active = False
        self._last_flight_toggle_mono = time.monotonic()
        self._flight_tunnel_runtime_outcome = None
        self._flight_tunnel_confirmed_runtime = False
        self.logger.info("Disengaging flight tunnel mode: reason=%s", reason)

    def _maybe_retry_flight_tunnel(
        self,
        *,
        pose: Optional[Pose] = None,
        obstacle: Optional[ObstacleInfo] = None,
    ) -> None:
        if not self._flight_tunnel_active:
            return
        now = time.monotonic()
        if now - self._last_flight_toggle_mono < 0.45 or now - self._last_flight_retry_mono < 0.75:
            return
        current_pose = pose or self.world_model.get_player_pose()
        current_obstacle = obstacle or self.world_model.is_obstacle_ahead()
        active_result = self._active_flight_tunnel_result(pose=current_pose, obstacle=current_obstacle)
        if active_result is not None:
            live_outcome = str(active_result.get("outcome") or "")
            if live_outcome == "flight_success":
                if self._flight_tunnel_runtime_outcome != live_outcome:
                    self.logger.info(
                        "Flight tunnel telemetry validator confirmed a successful fly burst: distance=%.2f max_h_speed=%.2f max_delta_y=%.2f.",
                        float(active_result.get("horizontal_distance") or 0.0),
                        float(active_result.get("max_horizontal_speed_bps") or 0.0),
                        float(active_result.get("max_delta_y") or 0.0),
                    )
                self._flight_tunnel_runtime_outcome = live_outcome
                self._flight_tunnel_confirmed_runtime = True
                self._flight_tunnel_retry_count = 0
                return
            if live_outcome == "ceiling_constrained_ground_burst":
                if self._flight_tunnel_runtime_outcome != live_outcome:
                    self.logger.info(
                        "Flight tunnel telemetry validator classified this burst as a ceiling-constrained ground burst: distance=%.2f max_h_speed=%.2f. Keeping the productive burst but skipping further fly retries for this lane.",
                        float(active_result.get("horizontal_distance") or 0.0),
                        float(active_result.get("max_horizontal_speed_bps") or 0.0),
                    )
                self._flight_tunnel_runtime_outcome = live_outcome
                self._flight_tunnel_suppressed_for_lane = True
                self._flight_tunnel_retry_count = 0
                return
        motion = self.world_model.get_motion_estimate()
        if motion is None:
            return
        target_speed = self._flight_tunnel_speed_target_bps()
        if motion.horizontal_speed_bps >= target_speed:
            self._flight_tunnel_retry_count = 0
            return
        if self._flight_tunnel_retry_count >= 2:
            self.logger.info(
                "Flight tunnel underperformed for this lane: observed=%.2f target>=%.2f; falling back to ground mode until the next lane transition.",
                motion.horizontal_speed_bps,
                target_speed,
            )
            self._flight_tunnel_suppressed_for_lane = True
            self._disengage_flight_tunnel_mode(reason="flight_tunnel_underperforming")
            return
        self.logger.info(
            "Flight tunnel speed check: observed=%.2f target>=%.2f; retrying fly enable.",
            motion.horizontal_speed_bps,
            target_speed,
        )
        self.inputs.enable_fly_mode(attempt_idx=2)
        self.pattern_monitor.note_retry()
        self._last_flight_retry_mono = now
        self._last_flight_toggle_mono = now
        self._flight_tunnel_retry_count += 1

    def _pitch_step_delay_sec(self) -> float:
        return min(self.cfg.camera_step_delay_sec, 0.0025 if self._profiled_pitch_ready() else 0.006)

    def _pitch_settle_delay_sec(self) -> float:
        return min(self.cfg.camera_settle_delay_sec, 0.008 if self._profiled_pitch_ready() else 0.02)

    def _pitch_step_pixels_for_delta(self, delta_deg: float) -> int:
        if delta_deg <= 0.0:
            return 1
        if not self._profiled_pitch_ready():
            return min(10, max(3, int(math.ceil(delta_deg / 2.5))))
        target_step_deg = min(max(delta_deg * 0.55, 1.8), 7.5)
        pixels = int(math.ceil(target_step_deg / max(self._profiled_pitch_deg_per_pixel, 1e-6)))
        return max(self._profiled_pitch_step_pixels, min(32, pixels))

    def _clear_scout_memory_cache(self) -> None:
        self._scout_memory_known_non_mineable_points.clear()
        self._scout_memory_known_mineable_points.clear()
        self._scout_memory_known_air_points.clear()
        self._scout_memory_non_mineable_block_ids.clear()
        self._scout_memory_report = None

    def _scout_memory_report_from_entry(self, entry: Optional[dict[str, object]]) -> Optional[dict[str, object]]:
        if not isinstance(entry, dict):
            return None
        report_template = entry.get("report_template")
        if isinstance(report_template, dict):
            return report_template
        return entry

    def _refresh_scout_memory_if_needed(self, *, force: bool = False) -> None:
        if self.region is None:
            return
        try:
            stat = self._scout_memory_path.stat()
        except FileNotFoundError:
            if force or self._scout_memory_mtime_ns is not None:
                self._scout_memory_mtime_ns = None
                self._clear_scout_memory_cache()
            return
        except Exception:
            return

        current_mtime_ns = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))
        if not force and self._scout_memory_mtime_ns == current_mtime_ns:
            return

        entry = load_region_memory_entry(self._scout_memory_path, self.region)
        report = self._scout_memory_report_from_entry(entry)
        self._clear_scout_memory_cache()
        self._scout_memory_mtime_ns = current_mtime_ns
        if not isinstance(report, dict):
            return

        observation_samples = report.get("observation_samples")
        if isinstance(observation_samples, list):
            for sample in observation_samples:
                if not isinstance(sample, dict):
                    continue
                block_x = _coerce_int(sample.get("block_x"))
                block_y = _coerce_int(sample.get("block_y"))
                block_z = _coerce_int(sample.get("block_z"))
                if block_x is None or block_y is None or block_z is None:
                    continue
                point = (block_x, block_y, block_z)
                look_block_class = str(sample.get("look_block_class") or "").strip().lower()
                if look_block_class == "non_mineable":
                    self._scout_memory_known_non_mineable_points.add(point)
                elif look_block_class == "mineable":
                    self._scout_memory_known_mineable_points.add(point)
                elif look_block_class == "air":
                    self._scout_memory_known_air_points.add(point)

        block_catalog = entry.get("block_catalog") if isinstance(entry, dict) else None
        if isinstance(block_catalog, dict):
            for block_id, meta in block_catalog.items():
                if not isinstance(block_id, str) or not isinstance(meta, dict):
                    continue
                if str(meta.get("last_classification") or "").strip().lower() == "non_mineable":
                    self._scout_memory_non_mineable_block_ids.add(block_id.strip().lower())

        self._scout_memory_report = report
        if (
            self._scout_memory_known_non_mineable_points
            or self._scout_memory_known_mineable_points
            or self._scout_memory_known_air_points
            or self._scout_memory_non_mineable_block_ids
        ):
            self.logger.info(
                "Loaded scout mining memory for region '%s': mineable_points=%d air_points=%d non_mineable_points=%d non_mineable_materials=%d.",
                self.region.name,
                len(self._scout_memory_known_mineable_points),
                len(self._scout_memory_known_air_points),
                len(self._scout_memory_known_non_mineable_points),
                len(self._scout_memory_non_mineable_block_ids),
            )

    def _memory_aware_look_type(self, obstacle: ObstacleInfo) -> Optional[str]:
        if self.region is None:
            return None

        block_id = str(obstacle.block_id or "").strip().lower()
        if not block_id:
            return None

        if self.voxel_memory is not None:
            voxel_type = self.voxel_memory.look_type_for_obstacle(obstacle)
            if voxel_type is not None:
                return voxel_type

        block_x = obstacle.block_x
        block_y = obstacle.block_y
        block_z = obstacle.block_z
        point = None
        if block_x is not None and block_y is not None and block_z is not None:
            point = (int(block_x), int(block_y), int(block_z))
            if not is_point_in_region(point[0], point[1], point[2], self.region):
                return "BLOCKING"
            if point in self._scout_memory_known_non_mineable_points:
                return "BLOCKING"
            if point in self._scout_memory_known_air_points and block_id == "minecraft:air":
                return "AIR"
            if point in self._scout_memory_known_mineable_points and block_id in self.region.allowed_block_ids:
                return "ALLOWED"

        if block_id == "minecraft:air":
            return "AIR"
        if block_id in self.region.allowed_block_ids:
            return "ALLOWED"
        if block_id in self.region.blocking_block_ids or block_id in self._scout_memory_non_mineable_block_ids:
            return "BLOCKING"
        if self.region.allowed_block_ids:
            return "BLOCKING"
        return None

    def tick(self) -> Tuple[MinerAction, ObstacleInfo]:
        if self._stop_requested():
            self.inputs.all_stop()
            self.current_action = MinerAction.STOP_ALL
            return self.current_action, ObstacleInfo(False, "minecraft:air", None, None, None)

        self.world_model.update()
        obstacle = self.world_model.is_obstacle_ahead()
        pose = self.world_model.get_player_pose()
        now = time.monotonic()

        if not self._has_fresh_telemetry() or pose is None:
            self.inputs.all_stop()
            if now - self.last_stale_log_mono >= 2.0:
                self.logger.warning(
                    "Forge telemetry is missing or stale; refusing to move blindly. "
                    "This usually means Minecraft is not open in-world, the Forge telemetry mod "
                    "is not running, or mam_f3_stream.log is not receiving fresh lines."
                )
                self.last_stale_log_mono = now
            self.current_action = MinerAction.WAIT_FOR_TELEMETRY
            return self.current_action, obstacle

        if self._maybe_handle_reset_or_reorient(pose, obstacle, now):
            return self.current_action, obstacle

        self._sample_pattern_monitor(
            pose=pose,
            obstacle=obstacle,
            look_type=self._memory_aware_look_type(obstacle),
        )

        if self.voxel_memory is not None:
            self.voxel_memory.observe_obstacle(obstacle, source="mine_live")

        self._maybe_complete_pending_eval(pose, obstacle)

        if self.region and self.region.dimension and pose.dimension and pose.dimension != self.region.dimension:
            self.inputs.all_stop()
            if now - self.last_stale_log_mono >= 2.0:
                self.logger.warning(
                    "Pose dimension %s does not match configured region dimension %s.",
                    pose.dimension,
                    self.region.dimension,
                )
                self.last_stale_log_mono = now
            self.current_action = MinerAction.WAIT_FOR_TELEMETRY
            return self.current_action, obstacle

        if self.region is not None:
            region_distance = distance_from_pose_to_region(pose, self.region)
            if region_distance > self.cfg.max_region_mismatch_distance:
                self.logger.warning(
                    "Configured mine bounds look stale for this run "
                    "(player at %.1f, %.1f, %.1f is %.1f blocks from region '%s'). "
                    "Disabling region bounds and falling back to local barrier-based mining.",
                    pose.x,
                    pose.y,
                    pose.z,
                    region_distance,
                    self.region.name,
                )
                self.region = None

        if now - self.last_control_mono < self.cfg.control_interval_sec:
            return self.current_action, obstacle
        self.last_control_mono = now

        in_region = is_pose_in_region(pose, self.region)
        self.blocking_learner.observe(self.region, pose, obstacle, in_region=in_region)
        if not in_region:
            self._was_in_region = False
            self._navigate_to_region(pose)
            self.current_action = MinerAction.NAVIGATE_TO_REGION
            return self.current_action, obstacle
        elif not self._was_in_region:
            entry_target_z = self.target_lane_z if self.target_lane_z is not None else self.entry_lane_z
            if (
                self.cfg.strict_east_lane_mode
                and entry_target_z is not None
                and abs(pose.z - entry_target_z) > self.cfg.lane_drift_tolerance
            ):
                self.target_lane_z = entry_target_z
                self.desired_heading = Cardinal.EAST
                self._maybe_correct_lane_drift(pose)
                self.current_action = MinerAction.NAVIGATE_TO_REGION
                return self.current_action, self.world_model.is_obstacle_ahead()
            self.navigation_probe = None
            self.entry_break_in_active = False
            self.entry_lane_z = None
            if self.plan_needs_refresh:
                self._refresh_mining_plan(pose)
            self.logger.info(
                "Entered configured mine region at x=%.1f y=%.1f z=%.1f; switching to mining posture.",
                pose.x,
                pose.y,
                pose.z,
            )
            self._was_in_region = True

        if not self._ensure_pitch(self._desired_lane_pitch(pose)):
            self.inputs.stop_mining()
            self.inputs.release_key("w")
            self.inputs.release_key("ctrl")
            self.current_action = MinerAction.REACQUIRE_STONE
            return self.current_action, self.world_model.is_obstacle_ahead()

        if self.desired_heading is None:
            self.desired_heading = self._initial_heading(pose)
            self.lane_shift_direction = self._initial_lane_shift_direction(pose)

        if not self._align_heading(self.desired_heading):
            self.inputs.stop_mining()
            self.inputs.release_key("w")
            self.inputs.release_key("ctrl")
            self.current_action = MinerAction.REACQUIRE_STONE
            return self.current_action, self.world_model.is_obstacle_ahead()
        if self.cfg.strict_east_lane_mode and self.target_lane_z is not None:
            drift = abs(pose.z - self.target_lane_z)
            if drift > max(2.0, self.cfg.lane_drift_tolerance * 2.0):
                self.logger.info(
                    "Strict lane drift reset: current_z=%.1f target_lane_z=%.1f drift=%.2f",
                    pose.z,
                    self.target_lane_z,
                    drift,
                )
                self._gmine_reset()
                self.current_action = MinerAction.RESET_GMINE
                return self.current_action, self.world_model.is_obstacle_ahead()
        elif self._maybe_correct_lane_drift(pose):
            return self.current_action, self.world_model.is_obstacle_ahead()

        look_type = self._effective_look_type(obstacle)
        low_progress = self._is_low_progress(pose)
        at_lane_end = self._is_at_lane_end(pose)

        if look_type != "ALLOWED" and not at_lane_end and (
            low_progress or look_type in {"AIR", "BLOCKING", "OTHER", "UNKNOWN"}
        ):
            if self._try_reacquire_stone_face(pose, obstacle, look_type):
                return self.current_action, obstacle
            self.reacquire_failures += 1
            if (
                look_type in {"AIR", "OTHER", "UNKNOWN"}
                and self.reacquire_failures < self.reacquire_failures_before_shift
            ):
                self._hold_lane_scan_retry()
                return self.current_action, obstacle

        if look_type == "BLOCKING" or at_lane_end or (
            low_progress
            and look_type != "ALLOWED"
            and self.reacquire_failures >= self.reacquire_failures_before_shift
        ):
            self._perform_recovery(pose, obstacle, low_progress=low_progress, look_type=look_type)
            return self.current_action, obstacle

        if self._should_use_flight_tunnel(pose, obstacle):
            self._engage_flight_tunnel_mode(pose=pose, obstacle=obstacle)
            self._maybe_retry_flight_tunnel(pose=pose, obstacle=obstacle)
        else:
            self._disengage_flight_tunnel_mode(reason="ground_forward", pose=pose, obstacle=obstacle)
        self._drive_forward()
        self.current_action = MinerAction.FORWARD_MINE
        return self.current_action, obstacle

    def _has_fresh_telemetry(self) -> bool:
        age = self.world_model.pose_provider.get_last_update_age_sec()
        return age is not None and age <= self.cfg.stale_pose_timeout_sec

    def _drive_forward(self) -> None:
        self.inputs.stop_vertical_motion()
        self.inputs.hold_forward(sprint=self.cfg.mining_use_sprint or self._flight_tunnel_active)
        self.inputs.start_mining()
        self.consecutive_recoveries = 0
        self.reacquire_failures = 0

    def _move_forward_without_mining(self) -> None:
        self.inputs.stop_vertical_motion()
        self.inputs.hold_forward(sprint=self.cfg.use_sprint)
        self.inputs.stop_mining()

    def _reset_lane_state(self) -> None:
        self.desired_heading = None
        self.progress_probe = None
        self.pending_eval = None
        self.consecutive_recoveries = 0
        self.reacquire_failures = 0
        self._was_in_region = False
        self.navigation_probe = None
        self.entry_break_in_active = False
        self.entry_lane_z = None
        self._flight_tunnel_active = False
        self._reset_lane_runtime_decisions()

    def _maybe_handle_reset_or_reorient(
        self,
        pose: Pose,
        obstacle: ObstacleInfo,
        now: float,
    ) -> bool:
        last_pose = self.last_pose
        self.last_pose = pose

        if self.region is None:
            return False

        if last_pose is not None:
            dy = pose.y - last_pose.y
            if dy >= self.cfg.reset_vertical_teleport_distance:
                if self.voxel_memory is not None:
                    self.voxel_memory.note_reset(pose=pose)
                self.pattern_monitor.finish_active_pattern(reason="mine_reset", pose=pose, obstacle=obstacle)
                self.pattern_monitor.note_reset(pose)
                in_region = is_pose_in_region(pose, self.region)
                self._reset_lane_state()
                if in_region and pose.y >= (self.region.max_y - self.cfg.top_reset_activation_margin):
                    self.top_reset_reorient_until_mono = now + self.cfg.top_reset_reorient_sec
                    self.logger.info(
                        "Detected mine reset teleport to top of region at x=%.1f y=%.1f z=%.1f; reorienting before resuming mining.",
                        pose.x,
                        pose.y,
                        pose.z,
                    )
                else:
                    self.top_reset_reorient_until_mono = 0.0
                    self.logger.info(
                        "Detected upward teleport/reset at x=%.1f y=%.1f z=%.1f; clearing lane state and reacquiring region.",
                        pose.x,
                        pose.y,
                        pose.z,
                    )

        if self.top_reset_reorient_until_mono <= 0.0:
            return False

        if now >= self.top_reset_reorient_until_mono or pose.y <= (self.region.max_y - self.cfg.top_reset_clear_y_drop):
            self.top_reset_reorient_until_mono = 0.0
            self.logger.info(
                "Top-of-mine reset reorientation complete at x=%.1f y=%.1f z=%.1f; resuming autonomous mining.",
                pose.x,
                pose.y,
                pose.z,
            )
            return False

        target_heading = self.desired_heading or self._initial_heading(pose)
        self.desired_heading = target_heading
        aligned_heading = self._align_heading(target_heading)
        aligned_pitch = self._ensure_pitch(self.cfg.top_reset_pitch)
        if not (aligned_heading and aligned_pitch):
            self.inputs.all_stop()
            self.current_action = MinerAction.REORIENT_AFTER_RESET
            return True
        self._move_forward_without_mining()
        self.current_action = MinerAction.REORIENT_AFTER_RESET
        return True

    def _navigate_to_region(self, pose: Pose) -> None:
        self._disengage_flight_tunnel_mode(reason="navigate_to_region")
        if self.region is None:
            self.navigation_probe = None
            self.entry_break_in_active = False
            self._ensure_pitch(self.cfg.navigation_pitch)
            self._move_forward_without_mining()
            return

        if self._navigate_spawn_anchor_entry(pose):
            return

        target_heading: Optional[Cardinal] = None
        if pose.x < self.region.min_x:
            target_heading = Cardinal.EAST
        elif pose.x > self.region.max_x:
            target_heading = Cardinal.WEST
        elif pose.z < self.region.min_z:
            target_heading = Cardinal.SOUTH
        elif pose.z > self.region.max_z:
            target_heading = Cardinal.NORTH

        if target_heading is not None:
            now = time.monotonic()
            if now - self.last_navigation_log_mono >= 1.5:
                target_yaw = CARDINAL_YAWS[target_heading]
                self.logger.info(
                    "NAV toward region: target=%s pose=(%.1f, %.1f, %.1f) yaw=%.1f delta=%.1f",
                    target_heading.value,
                    pose.x,
                    pose.y,
                    pose.z,
                    normalize_yaw(pose.yaw),
                    yaw_delta(pose.yaw, target_yaw),
                )
                self.last_navigation_log_mono = now
            self.inputs.stop_mining()
            self.inputs.release_key("w")
            self.inputs.release_key("ctrl")
            if not self._align_heading(target_heading):
                self.current_action = MinerAction.NAVIGATE_TO_REGION
                return

        distance_to_entry = self._distance_to_region_along_heading(pose, target_heading)
        should_break_in = self._should_break_into_region(distance_to_entry)

        if should_break_in:
            if not self._ensure_pitch(self.cfg.desired_pitch):
                self.current_action = MinerAction.NAVIGATE_TO_REGION
                return
            self._drive_forward()
        else:
            if not self._ensure_pitch(self.cfg.navigation_pitch):
                self.current_action = MinerAction.NAVIGATE_TO_REGION
                return
            self._move_forward_without_mining()

    def _navigate_spawn_anchor_entry(self, pose: Pose) -> bool:
        if self.region is None:
            return False
        if pose.x >= self.region.min_x:
            return False
        if pose.y < (self.region.max_y - (self.cfg.top_reset_activation_margin + 2.0)):
            return False

        min_lane = float(self.region.min_z) + 0.5
        max_lane = float(self.region.max_z) - 0.5
        preferred_lane_z = self.target_lane_z if self.target_lane_z is not None else self.entry_lane_z
        if preferred_lane_z is None:
            preferred_lane_z = self._snap_lane_center(pose.z)
        self.entry_lane_z = min(max(preferred_lane_z, min_lane), max_lane)

        drift = pose.z - self.entry_lane_z
        self.inputs.stop_mining()
        self.inputs.release_key("a")
        self.inputs.release_key("d")
        self.inputs.release_key("w")
        self.inputs.release_key("ctrl")

        if abs(drift) > self.cfg.lane_drift_tolerance:
            strafe_key = "a" if drift > 0 else "d"
            self.logger.info(
                "Spawn-anchored entry correction: current_z=%.1f target_z=%.1f drift=%.2f key=%s",
                pose.z,
                self.entry_lane_z,
                drift,
                strafe_key,
            )
            self.inputs.hold_key(strafe_key)
            time.sleep(self.cfg.lane_strafe_pulse_sec)
            self.inputs.release_key(strafe_key)
            time.sleep(self.cfg.lane_strafe_settle_sec)
            self.current_action = MinerAction.NAVIGATE_TO_REGION
            return True

        if not self._ensure_pitch(self.cfg.navigation_pitch):
            self.current_action = MinerAction.NAVIGATE_TO_REGION
            return True

        self._move_forward_without_mining()
        self.current_action = MinerAction.NAVIGATE_TO_REGION
        return True

    def _distance_to_region_along_heading(
        self,
        pose: Pose,
        target_heading: Optional[Cardinal],
    ) -> float:
        if self.region is None:
            return 0.0
        if target_heading == Cardinal.EAST:
            return max(0.0, self.region.min_x - pose.x)
        if target_heading == Cardinal.WEST:
            return max(0.0, pose.x - self.region.max_x)
        if target_heading == Cardinal.SOUTH:
            return max(0.0, self.region.min_z - pose.z)
        if target_heading == Cardinal.NORTH:
            return max(0.0, pose.z - self.region.max_z)
        return distance_from_pose_to_region(pose, self.region)

    def _should_break_into_region(self, distance_to_entry: float) -> bool:
        if self.region is None:
            self.navigation_probe = None
            self.entry_break_in_active = False
            return False

        now = time.monotonic()
        if distance_to_entry > self.cfg.entry_break_in_distance:
            self.navigation_probe = NavigationProbe(now, distance_to_entry)
            if self.entry_break_in_active:
                self.logger.info(
                    "Leaving entry-mining mode; region face is %.2f blocks away again.",
                    distance_to_entry,
                )
            self.entry_break_in_active = False
            return False

        probe = self.navigation_probe
        if probe is None:
            self.navigation_probe = NavigationProbe(now, distance_to_entry)
            return self.entry_break_in_active

        if now - probe.started_mono < self.cfg.entry_stall_window_sec:
            return self.entry_break_in_active

        progress = probe.last_distance - distance_to_entry
        self.navigation_probe = NavigationProbe(now, distance_to_entry)
        if progress < self.cfg.entry_stall_progress_distance:
            if not self.entry_break_in_active:
                self.logger.info(
                    "Navigation progress stalled %.2f blocks from the configured region; "
                    "switching to entry-mining posture.",
                    distance_to_entry,
                )
            self.entry_break_in_active = True
        elif self.entry_break_in_active:
            self.logger.info(
                "Navigation progress resumed toward the region; returning to walk-in posture."
            )
            self.entry_break_in_active = False

        return self.entry_break_in_active

    def _initial_heading(self, pose: Pose) -> Cardinal:
        if self.cfg.strict_east_lane_mode and self.region is not None:
            return Cardinal.EAST
        if self.region is None:
            return yaw_to_cardinal(pose.yaw)
        midpoint_x = (self.region.min_x + self.region.max_x) / 2.0
        return Cardinal.EAST if pose.x <= midpoint_x else Cardinal.WEST

    def _initial_lane_shift_direction(self, pose: Pose) -> int:
        if self.region is None:
            return 1
        midpoint_z = (self.region.min_z + self.region.max_z) / 2.0
        return 1 if pose.z <= midpoint_z else -1

    def _ensure_pitch(self, target_pitch: float, *, blocking: bool = False) -> bool:
        deadline = time.monotonic() + (0.8 if blocking else 0.18)
        while True:
            if self._stop_requested():
                self.inputs.all_stop()
                return False
            self.world_model.update()
            pose = self.world_model.get_player_pose()
            if pose is None:
                return False
            delta = target_pitch - pose.pitch
            if abs(delta) <= self.cfg.pitch_tolerance:
                return True
            step_pixels = self._pitch_step_pixels_for_delta(abs(delta))
            if delta > 0:
                self.inputs.look_down_small(
                    steps=1,
                    step_pixels=step_pixels,
                    delay=self._pitch_step_delay_sec(),
                )
            else:
                self.inputs.look_up_small(
                    steps=1,
                    step_pixels=step_pixels,
                    delay=self._pitch_step_delay_sec(),
                )
            time.sleep(self._pitch_settle_delay_sec())
            if time.monotonic() >= deadline:
                return False

    def _align_heading(self, heading: Cardinal, *, blocking: bool = False) -> bool:
        self.desired_heading = heading
        if self.cfg.strict_east_lane_mode and self.region is not None:
            return True
        return self._aim_to_heading(heading, blocking=blocking)

    def _aim_to_heading(self, heading: Cardinal, *, blocking: bool = False) -> bool:
        target_yaw = CARDINAL_YAWS[heading]
        deadline = time.monotonic() + (self.cfg.rotate_timeout_sec if blocking else 0.0)
        while True:
            if self._stop_requested():
                self.inputs.all_stop()
                return False
            self.world_model.update()
            pose = self.world_model.get_player_pose()
            if pose is None:
                return False
            delta = yaw_delta(pose.yaw, target_yaw)
            if abs(delta) <= self.cfg.heading_tolerance_deg:
                return True
            if delta > 0:
                self.inputs.look_right_small(
                    steps=1,
                    step_pixels=1,
                    delay=self.cfg.camera_step_delay_sec,
                )
            else:
                self.inputs.look_left_small(
                    steps=1,
                    step_pixels=1,
                    delay=self.cfg.camera_step_delay_sec,
                )
            time.sleep(self.cfg.camera_settle_delay_sec)
            if not blocking or time.monotonic() >= deadline:
                return False

    def _is_at_lane_end(self, pose: Pose) -> bool:
        if self.region is None or self.desired_heading is None:
            return False
        margin = 0.75
        if self.desired_heading == Cardinal.EAST:
            return pose.x >= (self.region.max_x - margin)
        if self.desired_heading == Cardinal.WEST:
            return pose.x <= (self.region.min_x + margin)
        if self.desired_heading == Cardinal.SOUTH:
            return pose.z >= (self.region.max_z - margin)
        return pose.z <= (self.region.min_z + margin)

    def _is_low_progress(self, pose: Pose) -> bool:
        now = time.monotonic()
        if self.progress_probe is None:
            self.progress_probe = (now, pose.x, pose.z)
            return False

        started_mono, start_x, start_z = self.progress_probe
        if now - started_mono < self.cfg.low_progress_window_sec:
            return False

        if self.desired_heading in {Cardinal.EAST, Cardinal.WEST}:
            progress = abs(pose.x - start_x)
        else:
            progress = abs(pose.z - start_z)

        self.progress_probe = (now, pose.x, pose.z)
        return progress < self.cfg.low_progress_distance

    def _effective_look_type(self, obstacle: ObstacleInfo) -> str:
        look_type = self._memory_aware_look_type(obstacle) or classify_block_type(obstacle, self.region)
        if (
            self.region is not None
            and obstacle.block_y is not None
            and obstacle.block_y <= (self.region.min_y + self.cfg.floor_guard_blocks)
            and look_type != "AIR"
        ):
            return "BLOCKING"
        if (
            self.region is not None
            and obstacle.block_x is not None
            and obstacle.block_y is not None
            and obstacle.block_z is not None
            and not is_block_in_bounds(obstacle, self.region)
            and look_type != "AIR"
        ):
            return "BLOCKING"
        return look_type

    def _is_boundary_marker_obstacle(self, obstacle: ObstacleInfo) -> bool:
        block_id = str(obstacle.block_id or "").strip().lower()
        if not block_id:
            return False
        return block_id.endswith("_wool") or "barrier" in block_id or "glass" in block_id

    def _reacquire_pitch_candidates(self, pose: Pose, obstacle: ObstacleInfo) -> list[float]:
        if self.region is not None and pose.y <= (self.region.min_y + 3.0):
            return [8.0, 12.0, 18.0]
        if self.region is not None and pose.y >= (self.region.max_y - 3.0):
            return [12.0, 18.0, 24.0, 8.0]
        ordered_indices = [
            (self.pitch_sweep_index + offset) % len(self.pitch_sweep_sequence)
            for offset in range(1, len(self.pitch_sweep_sequence) + 1)
        ]
        candidates: list[float] = []
        for idx in ordered_indices:
            pitch = self.pitch_sweep_sequence[idx]
            if pitch not in candidates:
                candidates.append(pitch)
        return candidates

    def _current_mining_pitch(self) -> float:
        if not self.pitch_sweep_sequence:
            return self.cfg.desired_pitch
        return float(self.pitch_sweep_sequence[self.pitch_sweep_index % len(self.pitch_sweep_sequence)])

    def _refresh_mining_plan(self, pose: Pose) -> None:
        plan = self.strategy_planner.select_plan(self.region, pose)
        self.active_plan = plan
        self.pitch_sweep_sequence = plan.pitch_sequence
        self.pitch_sweep_index = 0
        self.lane_shift_direction = plan.preferred_shift_direction
        self.reacquire_failures_before_shift = plan.reacquire_failures_before_shift
        self.target_lane_z = self._snap_lane_center(pose.z)
        self.plan_needs_refresh = False
        self.logger.info(
            "Planning period selected mining plan: name=%s shift=%s max_retries=%d pitches=%s rationale=%s",
            plan.name,
            "positive" if plan.preferred_shift_direction > 0 else "negative",
            plan.reacquire_failures_before_shift,
            ",".join(f"{p:.0f}" for p in plan.pitch_sequence),
            plan.rationale,
        )

    def _snap_lane_center(self, z_value: float) -> float:
        return math.floor(z_value) + 0.5

    def _clamp_lane_center(self, z_value: float) -> float:
        snapped = self._snap_lane_center(z_value)
        if self.region is None:
            return snapped
        min_lane = float(self.region.min_z) + 0.5
        max_lane = float(self.region.max_z) - 0.5
        return max(min_lane, min(max_lane, snapped))

    def _next_snake_shift_sign(self, pose: Pose) -> Optional[int]:
        if self.region is None:
            return self.lane_shift_direction

        if self.target_lane_z is None:
            self.target_lane_z = self._snap_lane_center(pose.z)

        min_lane = float(self.region.min_z) + 0.5
        max_lane = float(self.region.max_z) - 0.5
        proposed = self.target_lane_z + (self.cfg.lane_step_blocks * self.lane_shift_direction)

        if proposed > max_lane:
            self.lane_shift_direction = -1
            proposed = self.target_lane_z + (self.cfg.lane_step_blocks * self.lane_shift_direction)
        elif proposed < min_lane:
            self.lane_shift_direction = 1
            proposed = self.target_lane_z + (self.cfg.lane_step_blocks * self.lane_shift_direction)

        if proposed < min_lane or proposed > max_lane:
            return None

        self.target_lane_z = proposed
        return self.lane_shift_direction

    def _lane_drift(self, pose: Pose) -> float:
        if (
            self.region is None
            or self.target_lane_z is None
            or self.desired_heading not in {Cardinal.EAST, Cardinal.WEST}
        ):
            return 0.0
        return pose.z - self.target_lane_z

    def _adopt_current_lane_target(self, pose: Pose, *, reason: str) -> bool:
        if self.region is None:
            return False
        old_target = self.target_lane_z
        new_target = self._clamp_lane_center(pose.z)
        if old_target is not None and abs(new_target - old_target) < 0.25:
            return False
        if old_target is not None:
            self.lane_shift_direction = 1 if new_target >= old_target else -1
        self.target_lane_z = new_target
        self.entry_lane_z = new_target
        self._reset_lane_runtime_decisions()
        if getattr(self, "_strict_target_yaw", None) is not None and self.desired_heading is not None:
            setattr(self, "_strict_target_yaw", CARDINAL_YAWS[self.desired_heading])
        self.progress_probe = None
        self.reacquire_failures = 0
        self.logger.info(
            "Adopting local lane target: reason=%s old_target_lane_z=%.1f new_target_lane_z=%.1f pose_z=%.1f shift=%s",
            reason,
            float("nan") if old_target is None else old_target,
            new_target,
            pose.z,
            "positive" if self.lane_shift_direction > 0 else "negative",
        )
        return True

    def _strict_yaw_delta(self, pose: Pose) -> float:
        return yaw_delta(pose.yaw, CARDINAL_YAWS[Cardinal.EAST])

    def _maybe_correct_lane_drift(self, pose: Pose) -> bool:
        if (
            self.region is None
            or self.target_lane_z is None
            or self.desired_heading not in {Cardinal.EAST, Cardinal.WEST}
        ):
            return False

        self._disengage_flight_tunnel_mode(reason="lane_drift_correction")
        drift = self._lane_drift(pose)
        if abs(drift) <= self.cfg.lane_drift_tolerance:
            self._lane_drift_stall_count = 0
            return False

        shift_sign = -1 if drift > 0 else 1
        action = MinerAction.SHIFT_NEGATIVE if shift_sign < 0 else MinerAction.SHIFT_POSITIVE
        key = self._strafe_key_for_heading(self.desired_heading, shift_sign)
        start_abs_drift = abs(drift)
        self.logger.info(
            "Lane drift correction: heading=%s current_z=%.1f target_lane_z=%.1f drift=%.2f",
            self.desired_heading.value,
            pose.z,
            self.target_lane_z,
            drift,
        )
        self.inputs.stop_mining()
        self.inputs.release_key("w")
        self.inputs.release_key("ctrl")
        self._align_heading(self.desired_heading, blocking=True)

        deadline = time.monotonic() + min(self.cfg.strafe_timeout_sec, 0.8)
        active_key: Optional[str] = None
        while time.monotonic() < deadline:
            if self._stop_requested():
                self.inputs.all_stop()
                return True
            self.world_model.update()
            current_pose = self.world_model.get_player_pose()
            if current_pose is None:
                break
            current_drift = self._lane_drift(current_pose)
            if abs(current_drift) <= max(0.25, self.cfg.lane_drift_tolerance / 2.0):
                break
            pulse_sec = self.cfg.lane_strafe_pulse_sec
            next_pose, next_key, next_drift = self._adaptive_lane_strafe_pulse(
                current_pose,
                target_lane_z=self.target_lane_z,
                heading=self.desired_heading,
                active_key=active_key or key,
                pulse_sec=pulse_sec,
            )
            active_key = next_key
            if next_pose is None:
                break
            if abs(next_drift) <= max(0.25, self.cfg.lane_drift_tolerance / 2.0):
                break

        current_pose = self.world_model.get_player_pose() or pose
        final_abs_drift = abs(self._lane_drift(current_pose))
        if final_abs_drift >= max(self.cfg.lane_drift_tolerance, start_abs_drift - 0.12):
            self._lane_drift_stall_count += 1
            self.logger.info(
                "Lane drift correction stalled: heading=%s start_drift=%.2f final_drift=%.2f stall_count=%d",
                self.desired_heading.value,
                start_abs_drift,
                final_abs_drift,
                self._lane_drift_stall_count,
            )
        else:
            self._lane_drift_stall_count = 0
        if self._lane_drift_stall_count >= 2:
            if final_abs_drift >= max(1.5, self.cfg.lane_drift_tolerance * 2.0) and self._adopt_current_lane_target(
                current_pose,
                reason="drift_stall",
            ):
                self._ensure_pitch(self._desired_lane_pitch(current_pose), blocking=True)
                self._drive_forward()
                self.progress_probe = None
                self.current_action = MinerAction.REACQUIRE_STONE
                return True
            self.logger.warning(
                "Lane drift correction stalled repeatedly at x=%.1f y=%.1f z=%.1f; escalating out of local correction.",
                current_pose.x,
                current_pose.y,
                current_pose.z,
            )
            self._lane_drift_stall_count = 0
            return False
        self._ensure_pitch(self._desired_lane_pitch(current_pose), blocking=True)
        self._drive_forward()
        self.progress_probe = None
        self.current_action = action
        return True

    def _adaptive_lane_strafe_pulse(
        self,
        pose: Pose,
        *,
        target_lane_z: float,
        heading: Cardinal,
        active_key: str,
        pulse_sec: float,
    ) -> tuple[Optional[Pose], str, float]:
        drift_before = pose.z - target_lane_z
        self.inputs.hold_key(active_key)
        time.sleep(max(0.03, pulse_sec))
        self.inputs.release_key(active_key)
        time.sleep(self.cfg.lane_strafe_settle_sec)
        self.world_model.update()
        next_pose = self.world_model.get_player_pose()
        if next_pose is None:
            return None, active_key, drift_before

        drift_after = next_pose.z - target_lane_z
        moved_z = next_pose.z - pose.z
        need_increase = drift_before < 0.0
        moved_toward_target = abs(drift_after) < abs(drift_before) - 0.02
        moved_in_expected_direction = (need_increase and moved_z > 0.01) or ((not need_increase) and moved_z < -0.01)
        next_key = active_key
        if (
            abs(moved_z) > 0.02
            and not moved_in_expected_direction
            and abs(drift_after) > abs(drift_before) + 0.05
        ):
            next_key = "a" if active_key == "d" else "d"
            self.logger.info(
                "Adaptive lane-strafe correction: heading=%s key=%s worsened drift from %.2f to %.2f; switching to %s.",
                heading.value,
                active_key,
                drift_before,
                drift_after,
                next_key,
            )
        elif moved_toward_target:
            next_key = active_key
        return next_pose, next_key, drift_after

    def _desired_lane_pitch(self, pose: Pose) -> float:
        pitch = self._current_mining_pitch()
        if self.region is None:
            return min(pitch, 24.0)
        if pose.y >= (self.region.max_y - 2.0):
            return max(pitch, 58.0)
        if pose.y >= (self.region.max_y - 6.0):
            return max(pitch, 48.0)
        if pose.y <= (self.region.min_y + 2.5):
            return 8.0
        if pose.y <= (self.region.min_y + 6.0):
            return min(pitch, 12.0)
        if pose.y <= (self.region.min_y + 16.0):
            return min(pitch, 18.0)
        return min(pitch, 24.0)

    def _try_reacquire_stone_face(self, pose: Pose, obstacle: ObstacleInfo, look_type: str) -> bool:
        if self.region is None:
            return False
        if self.desired_heading is None:
            self.desired_heading = self._initial_heading(pose)
        self._disengage_flight_tunnel_mode(reason="reacquire_scan")

        pitches = self._reacquire_pitch_candidates(pose, obstacle)
        original_heading = self.desired_heading

        if self._stop_requested():
            self.inputs.all_stop()
            return False

        self._align_heading(original_heading, blocking=True)
        for pitch in pitches:
            if self._stop_requested():
                self.inputs.all_stop()
                return False
            self._ensure_pitch(pitch, blocking=True)
            self.world_model.update()
            candidate_pose = self.world_model.get_player_pose()
            candidate_obstacle = self.world_model.is_obstacle_ahead()
            if candidate_pose is None:
                continue
            if (
                self.cfg.strict_east_lane_mode
                and self.target_lane_z is not None
                and abs(candidate_pose.z - self.target_lane_z) > max(1.0, self.cfg.lane_drift_tolerance)
            ):
                continue
            candidate_type = self._effective_look_type(candidate_obstacle)
            if candidate_type == "ALLOWED":
                self.desired_heading = original_heading
                try:
                    self.pitch_sweep_index = self.pitch_sweep_sequence.index(pitch)
                except ValueError:
                    pass
                self.progress_probe = None
                self.inputs.hold_forward(sprint=self.cfg.mining_use_sprint)
                self.inputs.start_mining()
                self.current_action = MinerAction.REACQUIRE_STONE
                self.logger.info(
                    "Reacquired mineable face in-lane: heading=%s pitch=%.1f block=%s at x=%.1f y=%.1f z=%.1f.",
                    original_heading.value,
                    pitch,
                    candidate_obstacle.block_id,
                    candidate_pose.x,
                    candidate_pose.y,
                    candidate_pose.z,
                )
                return True

        self.desired_heading = original_heading
        if look_type != "ALLOWED":
            self.pitch_sweep_index = (self.pitch_sweep_index + 1) % len(self.pitch_sweep_sequence)
            self._ensure_pitch(self._current_mining_pitch())
        return False

    def _hold_lane_scan_retry(self) -> None:
        self._disengage_flight_tunnel_mode(reason="lane_scan_retry")
        if self.desired_heading is not None:
            self._align_heading(self.desired_heading, blocking=True)
        pose = self.world_model.get_player_pose()
        if pose is not None:
            self._ensure_pitch(self._desired_lane_pitch(pose), blocking=True)
        self.inputs.stop_mining()
        self.inputs.hold_forward(sprint=self.cfg.mining_use_sprint)
        self.current_action = MinerAction.REACQUIRE_STONE
        self.logger.info(
            "Lane scan retry in current heading: failures=%d pitch=%.1f",
            self.reacquire_failures,
            self._current_mining_pitch(),
        )

    def _recovery_context(self, pose: Pose, look_type: str, low_progress: bool) -> str:
        if self.region is None:
            z_band = "na"
        else:
            span = max(1.0, float(self.region.max_z - self.region.min_z))
            frac = (pose.z - self.region.min_z) / span
            if frac < 0.33:
                z_band = "low"
            elif frac < 0.66:
                z_band = "mid"
            else:
                z_band = "high"

        heading = (self.desired_heading or yaw_to_cardinal(pose.yaw)).value
        return f"heading:{heading}|z:{z_band}|look:{look_type}|lp:{int(low_progress)}"

    def _perform_recovery(self, pose: Pose, obstacle: ObstacleInfo, *, low_progress: bool, look_type: str) -> None:
        self._disengage_flight_tunnel_mode(reason="recovery")
        context = self._recovery_context(pose, look_type, low_progress)
        choice: MinerAction
        if self.region is not None and self.cfg.strict_east_lane_mode:
            shift_sign = self._next_snake_shift_sign(pose)
            choice = MinerAction.RESET_GMINE
            if shift_sign is None or self.consecutive_recoveries >= 3:
                self.logger.info(
                    "Strict lane reset: context=%s consecutive=%d",
                    context,
                    self.consecutive_recoveries,
                )
            else:
                self.logger.info(
                    "Strict lane advance via /gmine reset: context=%s next_target_lane_z=%.1f",
                    context,
                    self.target_lane_z if self.target_lane_z is not None else float("nan"),
                )
        elif self.region is not None:
            shift_sign = self._next_snake_shift_sign(pose)
            if shift_sign is None or self.consecutive_recoveries >= 3:
                choice = MinerAction.RESET_GMINE
                self.logger.info(
                    "Snake recovery reset: context=%s consecutive=%d",
                    context,
                    self.consecutive_recoveries,
                )
            else:
                choice = MinerAction.SHIFT_POSITIVE if shift_sign > 0 else MinerAction.SHIFT_NEGATIVE
                self.logger.info(
                    "Snake lane shift: context=%s choice=%s target_lane_z=%.1f",
                    context,
                    choice.value,
                    self.target_lane_z if self.target_lane_z is not None else float("nan"),
                )
        else:
            ordered_available = [MinerAction.RESET_GMINE]
            choice = self.recovery_policy.select_action(context, ordered_available)

        if choice == MinerAction.SHIFT_POSITIVE:
            self.lane_shift_direction = 1
            self._shift_lane(pose, shift_sign=1)
        elif choice == MinerAction.SHIFT_NEGATIVE:
            self.lane_shift_direction = -1
            self._shift_lane(pose, shift_sign=-1)
        else:
            self._gmine_reset()

        self.consecutive_recoveries += 1
        self.reacquire_failures = 0
        self.current_action = choice

        self.world_model.update()
        new_pose = self.world_model.get_player_pose() or pose
        if self.desired_heading is not None:
            self.pending_eval = PendingRecoveryEvaluation(
                context=context,
                action=choice,
                heading=self.desired_heading,
                start_x=new_pose.x,
                start_z=new_pose.z,
                started_mono=time.monotonic(),
            )

    def _shift_lane(self, pose: Pose, *, shift_sign: int) -> None:
        self._disengage_flight_tunnel_mode(reason="lane_shift")
        if self._stop_requested():
            self.inputs.all_stop()
            return
        if self.desired_heading is None:
            self.desired_heading = self._initial_heading(pose)

        self.inputs.all_stop()
        self._align_heading(self.desired_heading, blocking=True)
        key = self._strafe_key_for_heading(self.desired_heading, shift_sign)
        start_pose = self.world_model.get_player_pose() or pose
        target_lane_z = self.target_lane_z if self.target_lane_z is not None else (start_pose.z + shift_sign * self.cfg.lane_step_blocks)

        if self.cfg.tap_jump_on_shift:
            self.inputs.tap_jump(0.06)

        deadline = time.monotonic() + self.cfg.strafe_timeout_sec
        active_key = key
        while time.monotonic() < deadline:
            if self._stop_requested():
                break
            self.world_model.update()
            current_pose = self.world_model.get_player_pose()
            if current_pose is None:
                time.sleep(self.cfg.lane_strafe_settle_sec)
                continue
            if shift_sign > 0 and current_pose.z >= (target_lane_z - 0.1):
                break
            if shift_sign < 0 and current_pose.z <= (target_lane_z + 0.1):
                break
            pulse_sec = self.cfg.lane_strafe_pulse_sec
            next_pose, next_key, _ = self._adaptive_lane_strafe_pulse(
                current_pose,
                target_lane_z=target_lane_z,
                heading=self.desired_heading,
                active_key=active_key,
                pulse_sec=pulse_sec,
            )
            active_key = next_key
            if next_pose is None:
                continue
        self.inputs.release_key(active_key)
        if self._stop_requested():
            self.inputs.all_stop()
            return

        self.desired_heading = opposite_heading(self.desired_heading)
        self._reset_lane_runtime_decisions()
        self._align_heading(self.desired_heading, blocking=True)
        current_pose = self.world_model.get_player_pose() or pose
        if abs(current_pose.z - target_lane_z) > max(0.35, self.cfg.lane_drift_tolerance):
            self._maybe_correct_lane_drift(current_pose)
            current_pose = self.world_model.get_player_pose() or current_pose
        self._ensure_pitch(self._desired_lane_pitch(current_pose), blocking=True)
        self._drive_forward()
        self.progress_probe = None
        self.reacquire_failures = 0

    def _gmine_reset(self) -> None:
        self._disengage_flight_tunnel_mode(reason="gmine_reset")
        if self._stop_requested():
            self.inputs.all_stop()
            return
        self.inputs.all_stop()
        self.inputs.go_to_gmine()
        if not self._sleep_interruptibly(0.8):
            self.inputs.all_stop()
            return
        self.desired_heading = None
        self.progress_probe = None
        self.consecutive_recoveries = 0

    def _strafe_key_for_heading(self, heading: Cardinal, shift_sign: int) -> str:
        if heading == Cardinal.EAST:
            return "d" if shift_sign > 0 else "a"
        if heading == Cardinal.WEST:
            return "a" if shift_sign > 0 else "d"
        if heading == Cardinal.NORTH:
            return "d" if shift_sign > 0 else "a"
        return "a" if shift_sign > 0 else "d"

    def _maybe_complete_pending_eval(self, pose: Pose, obstacle: ObstacleInfo) -> None:
        pending = self.pending_eval
        if pending is None:
            return
        if time.monotonic() - pending.started_mono < self.cfg.recovery_grace_sec:
            return

        if pending.heading in {Cardinal.EAST, Cardinal.WEST}:
            progress = abs(pose.x - pending.start_x)
        else:
            progress = abs(pose.z - pending.start_z)

        reward = progress
        look_type = classify_block_type(obstacle, self.region)
        if look_type == "ALLOWED":
            reward += 2.0
        elif look_type == "BLOCKING":
            reward -= 3.0
        elif look_type in {"AIR", "UNKNOWN"}:
            reward -= 1.0

        if is_pose_in_region(pose, self.region):
            reward += 1.0
        else:
            reward -= 2.0

        if progress < self.cfg.low_progress_distance:
            reward -= 2.0

        self.recovery_policy.update(pending.context, pending.action, reward)
        self.pending_eval = None


class StrictLaneController(AutonomousController):
    def __init__(
        self,
        cfg: AppConfig,
        logger: logging.Logger,
        world_model: WorldModelForge,
        inputs: SimpleInputController,
        region: Optional[RegionConfig],
        recovery_policy: RecoveryBandit,
        blocking_learner: BlockingBlockLearner,
        strategy_planner: MiningStrategyPlanner,
        voxel_memory: Optional[VoxelWorldMemory] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        super().__init__(
            cfg=cfg,
            logger=logger,
            world_model=world_model,
            inputs=inputs,
            region=region,
            recovery_policy=recovery_policy,
            blocking_learner=blocking_learner,
            strategy_planner=strategy_planner,
            voxel_memory=voxel_memory,
            stop_event=stop_event,
        )
        self.logger = logger.getChild("strict_lane")
        self._strict_pitch_sequence: Tuple[float, ...] = (38.0, 48.0, 58.0, 48.0)
        self._yaw_step_right_deg: float = 0.0
        self._yaw_step_left_deg: float = 0.0
        self._yaw_step_pixels: int = 1
        self._yaw_deg_per_pixel: float = 0.0
        self._profiled_mouse_backend: Optional[str] = None
        self._profiled_yaw_step_right_deg: float = 0.0
        self._profiled_yaw_step_left_deg: float = 0.0
        self._profiled_yaw_step_pixels: int = 1
        self._profiled_yaw_deg_per_pixel: float = 0.0
        self._horizontal_calibration_ready: bool = False
        self._horizontal_calibration_next_retry_mono: float = 0.0
        self._strict_reset_wait_until_mono: float = 0.0
        self._last_heading_log_mono: float = 0.0
        self._last_entry_log_mono: float = 0.0
        self._strict_target_yaw: Optional[float] = None

    def reset_for_new_run(self) -> None:
        super().reset_for_new_run()
        self.desired_heading = Cardinal.EAST
        self.pitch_sweep_sequence = self._strict_pitch_sequence
        self.pitch_sweep_index = 0
        self.reacquire_failures_before_shift = 2
        self.active_plan = MiningPlan(
            name="strict_deterministic_east_lane",
            pitch_sequence=self._strict_pitch_sequence,
            preferred_shift_direction=1,
            reacquire_failures_before_shift=2,
            rationale="deterministic_baseline",
        )
        self.plan_needs_refresh = False
        self._yaw_step_right_deg = 0.0
        self._yaw_step_left_deg = 0.0
        self._yaw_step_pixels = 1
        self._yaw_deg_per_pixel = 0.0
        self._horizontal_calibration_ready = False
        self._horizontal_calibration_next_retry_mono = 0.0
        self._strict_reset_wait_until_mono = 0.0
        self._last_heading_log_mono = 0.0
        self._last_entry_log_mono = 0.0
        self._strict_target_yaw = CARDINAL_YAWS[Cardinal.EAST]
        self._restore_profiled_control_calibration()

    def apply_control_calibration_profile(self, profile: Optional[dict[str, object]]) -> None:
        super().apply_control_calibration_profile(profile)
        self._profiled_mouse_backend = None
        self._profiled_yaw_step_right_deg = 0.0
        self._profiled_yaw_step_left_deg = 0.0
        self._profiled_yaw_step_pixels = 1
        self._profiled_yaw_deg_per_pixel = 0.0
        if not profile:
            return

        strict_profile = profile.get("strict_mouse_calibration")
        if not isinstance(strict_profile, dict):
            return

        backend = strict_profile.get("preferred_mouse_backend")
        if isinstance(backend, str) and backend in self.inputs.get_available_mouse_backends():
            self._profiled_mouse_backend = backend
        right_deg = strict_profile.get("yaw_step_right_deg")
        left_deg = strict_profile.get("yaw_step_left_deg")
        step_pixels = strict_profile.get("yaw_step_pixels")
        yaw_deg_per_pixel = strict_profile.get("yaw_deg_per_pixel")
        try:
            right_value = float(right_deg)
            left_value = float(left_deg)
            step_value = max(1, int(step_pixels))
            deg_per_pixel_value = float(yaw_deg_per_pixel)
        except (TypeError, ValueError):
            return
        if right_value <= 0.0 or left_value >= 0.0 or deg_per_pixel_value <= 0.0:
            return

        self._profiled_yaw_step_right_deg = right_value
        self._profiled_yaw_step_left_deg = left_value
        self._profiled_yaw_step_pixels = step_value
        self._profiled_yaw_deg_per_pixel = deg_per_pixel_value
        self._restore_profiled_control_calibration()
        self.logger.info(
            "Strict controller loaded control calibration profile: backend=%s step_pixels=%d yaw_deg_per_pixel=%.4f",
            self._profiled_mouse_backend or self.inputs.get_mouse_backend(),
            self._profiled_yaw_step_pixels,
            self._profiled_yaw_deg_per_pixel,
        )

    def _restore_profiled_control_calibration(self) -> None:
        if self._profiled_yaw_step_right_deg <= 0.0 or self._profiled_yaw_step_left_deg >= 0.0:
            return
        if self._profiled_mouse_backend:
            self.inputs.set_mouse_backend(self._profiled_mouse_backend)
        self._yaw_step_right_deg = self._profiled_yaw_step_right_deg
        self._yaw_step_left_deg = self._profiled_yaw_step_left_deg
        self._yaw_step_pixels = self._profiled_yaw_step_pixels
        self._yaw_deg_per_pixel = self._profiled_yaw_deg_per_pixel
        self._horizontal_calibration_ready = True
        self._horizontal_calibration_next_retry_mono = 0.0

    def tick(self) -> Tuple[MinerAction, ObstacleInfo]:
        if self._stop_requested():
            self.inputs.all_stop()
            self.current_action = MinerAction.STOP_ALL
            return self.current_action, ObstacleInfo(False, "minecraft:air", None, None, None)

        self._refresh_scout_memory_if_needed()
        self.world_model.update()
        obstacle = self.world_model.is_obstacle_ahead()
        pose = self.world_model.get_player_pose()
        now = time.monotonic()

        if not self._has_fresh_telemetry() or pose is None:
            self.inputs.all_stop()
            if now - self.last_stale_log_mono >= 2.0:
                self.logger.warning(
                    "Forge telemetry is missing or stale; strict controller will not move blindly. "
                    "This usually means Minecraft is not open in-world, the Forge telemetry mod "
                    "is not running, or mam_f3_stream.log is not receiving fresh lines."
                )
                self.last_stale_log_mono = now
            self.current_action = MinerAction.WAIT_FOR_TELEMETRY
            return self.current_action, obstacle

        self._handle_strict_reset_detection(pose, obstacle)

        self._sample_pattern_monitor(
            pose=pose,
            obstacle=obstacle,
            look_type=self._effective_look_type(obstacle),
        )

        if self.voxel_memory is not None:
            self.voxel_memory.observe_obstacle(obstacle, source="mine_live")

        if self.region and self.region.dimension and pose.dimension and pose.dimension != self.region.dimension:
            self.inputs.all_stop()
            self.current_action = MinerAction.WAIT_FOR_TELEMETRY
            return self.current_action, obstacle

        if self.region is not None:
            region_distance = distance_from_pose_to_region(pose, self.region)
            if region_distance > self.cfg.max_region_mismatch_distance:
                self.logger.warning(
                    "Configured mine bounds look stale for this run "
                    "(player at %.1f, %.1f, %.1f is %.1f blocks from region '%s'). "
                    "Strict deterministic controller is refusing to operate out of calibration.",
                    pose.x,
                    pose.y,
                    pose.z,
                    region_distance,
                    self.region.name,
                )
                self.inputs.all_stop()
                self.current_action = MinerAction.WAIT_FOR_TELEMETRY
                return self.current_action, obstacle

        if now - self.last_control_mono < self.cfg.control_interval_sec:
            return self.current_action, obstacle
        self.last_control_mono = now

        if self.region is None:
            self.inputs.all_stop()
            self.current_action = MinerAction.WAIT_FOR_TELEMETRY
            return self.current_action, obstacle

        if self.current_action == MinerAction.RESET_GMINE and not self._is_top_entry_pose(pose):
            if time.monotonic() < self._strict_reset_wait_until_mono:
                self.inputs.all_stop()
                return self.current_action, obstacle
            self.logger.warning(
                "Strict reset did not land in a top-entry pose; reissuing /gmine from x=%.1f y=%.1f z=%.1f.",
                pose.x,
                pose.y,
                pose.z,
            )
            self._reset_from_top(reason="reset_not_landed", advance_lane=False)
            return self.current_action, self.world_model.is_obstacle_ahead()

        if self.target_lane_z is None:
            self.target_lane_z = self._clamp_lane_z(self._snap_lane_center(pose.z))

        in_region = is_pose_in_region(pose, self.region)
        self.blocking_learner.observe(self.region, pose, obstacle, in_region=in_region)
        soak_reason = self._check_action_soak_guard(pose)
        if soak_reason is not None:
            self.logger.warning("Strict soak protection triggered: %s", soak_reason)
            if in_region and abs(self._lane_drift(pose)) >= max(1.5, self.cfg.lane_drift_tolerance * 2.0):
                if self._adopt_current_lane_target(pose, reason="soak_guard"):
                    self.current_action = MinerAction.REACQUIRE_STONE
                    return self.current_action, self.world_model.is_obstacle_ahead()
            self._reset_from_top(reason="soak_guard", advance_lane=False)
            return self.current_action, self.world_model.is_obstacle_ahead()

        if not in_region:
            if self._was_in_region and self._is_top_edge_lane_exit_pose(pose):
                self._advance_target_lane()
                next_heading = self._entry_heading_for_pose(pose)
                self.inputs.all_stop()
                self._was_in_region = False
                self.progress_probe = None
                self.reacquire_failures = 0
                self.pitch_sweep_index = 0
                self.entry_lane_z = self.target_lane_z
                if next_heading is not None:
                    self.desired_heading = next_heading
                self.logger.info(
                    "Strict top-edge snake advance: pose=(%.1f, %.1f, %.1f) next_heading=%s next_target_lane_z=%.1f",
                    pose.x,
                    pose.y,
                    pose.z,
                    (next_heading.value if next_heading is not None else "UNKNOWN"),
                    self.target_lane_z if self.target_lane_z is not None else float("nan"),
                )
                self.current_action = MinerAction.NAVIGATE_TO_REGION
                return self.current_action, self.world_model.is_obstacle_ahead()

            if time.monotonic() < self._strict_reset_wait_until_mono:
                self.inputs.all_stop()
                self.current_action = MinerAction.RESET_GMINE
                return self.current_action, self.world_model.is_obstacle_ahead()

            self._was_in_region = False
            self._tick_strict_entry(pose)
            return self.current_action, self.world_model.is_obstacle_ahead()

        if not self._was_in_region:
            if self._tick_strict_top_surface_recovery(pose):
                return self.current_action, self.world_model.is_obstacle_ahead()
            self.progress_probe = None
            self.reacquire_failures = 0
            self.pitch_sweep_index = 0
            self.entry_lane_z = None
            self.logger.info(
                "Entered configured mine region at x=%.1f y=%.1f z=%.1f; switching to strict deterministic lane mining.",
                pose.x,
                pose.y,
                pose.z,
            )
            self.logger.info(
                "Strict deterministic lane plan active: target_lane_z=%.1f pitches=%s",
                self.target_lane_z,
                ",".join(f"{p:.0f}" for p in self.pitch_sweep_sequence),
            )
            self._was_in_region = True

        self._tick_strict_lane(pose, obstacle)
        return self.current_action, self.world_model.is_obstacle_ahead()

    def _handle_strict_reset_detection(self, pose: Pose, obstacle: Optional[ObstacleInfo] = None) -> None:
        last_pose = self.last_pose
        self.last_pose = pose
        if self.region is None or last_pose is None:
            return
        if pose.y - last_pose.y < self.cfg.reset_vertical_teleport_distance:
            return
        if self.voxel_memory is not None:
            self.voxel_memory.note_reset(pose=pose)
        self.pattern_monitor.finish_active_pattern(reason="mine_reset", pose=pose, obstacle=obstacle)
        self.pattern_monitor.note_reset(pose)
        self.inputs.all_stop()
        self.progress_probe = None
        self.reacquire_failures = 0
        self._was_in_region = False
        self.entry_lane_z = self.target_lane_z
        self._flight_tunnel_active = False
        self._reset_lane_runtime_decisions()
        self.logger.info(
            "Detected mine reset teleport at x=%.1f y=%.1f z=%.1f; returning to top-entry alignment for lane z=%.1f.",
            pose.x,
            pose.y,
            pose.z,
            self.target_lane_z if self.target_lane_z is not None else float("nan"),
        )

    def _tick_strict_top_surface_recovery(self, pose: Pose) -> bool:
        if self.region is None or self.target_lane_z is None:
            return False
        if pose.y < (self.region.max_y - (self.cfg.top_reset_activation_margin + 2.0)):
            return False

        if self.desired_heading not in {Cardinal.EAST, Cardinal.WEST}:
            inferred_heading = yaw_to_cardinal(pose.yaw)
            if inferred_heading in {Cardinal.EAST, Cardinal.WEST}:
                self.desired_heading = inferred_heading

        if self.desired_heading not in {Cardinal.EAST, Cardinal.WEST}:
            return False

        if not self._align_spawn_heading(pose, self.desired_heading):
            self.current_action = MinerAction.NAVIGATE_TO_REGION
            return True

        if not self._ensure_pitch(self.cfg.navigation_pitch):
            self.current_action = MinerAction.NAVIGATE_TO_REGION
            return True

        drift = pose.z - self.target_lane_z
        self.inputs.stop_mining()
        self.inputs.release_key("ctrl")
        self.inputs.release_key("w")
        self.inputs.release_key("a")
        self.inputs.release_key("d")

        if abs(drift) > self.cfg.lane_drift_tolerance:
            shift_sign = -1 if drift > 0 else 1
            strafe_key = self._strafe_key_for_heading(self.desired_heading, shift_sign)
            pulse_sec = self._strict_strafe_pulse_duration(drift)
            self.logger.info(
                "Strict top-surface recovery correction: heading=%s current_z=%.1f target_z=%.1f drift=%.2f key=%s pulse=%.2fs",
                self.desired_heading.value,
                pose.z,
                self.target_lane_z,
                drift,
                strafe_key,
                pulse_sec,
            )
            self.inputs.hold_key(strafe_key)
            time.sleep(pulse_sec)
            self.inputs.release_key(strafe_key)
            time.sleep(self.cfg.lane_strafe_settle_sec)
            self.current_action = MinerAction.NAVIGATE_TO_REGION
            return True

        return False

    def _tick_strict_entry(self, pose: Pose) -> None:
        entry_heading = self._entry_heading_for_pose(pose)
        if entry_heading is None:
            self.logger.info(
                "Strict entry reset: pose=(%.1f, %.1f, %.1f) is not in the expected top-entry zone; issuing /gmine.",
                pose.x,
                pose.y,
                pose.z,
            )
            self._reset_from_top(reason="entry_zone_mismatch", advance_lane=False)
            return

        self.desired_heading = entry_heading

        if not self._align_spawn_heading(pose, entry_heading):
            self.current_action = MinerAction.NAVIGATE_TO_REGION
            return

        if not self._ensure_pitch(self.cfg.navigation_pitch):
            self.current_action = MinerAction.NAVIGATE_TO_REGION
            return

        drift = pose.z - self.target_lane_z
        self.inputs.stop_mining()
        self.inputs.release_key("ctrl")
        self.inputs.release_key("w")
        self.inputs.release_key("a")
        self.inputs.release_key("d")

        if abs(drift) > self.cfg.lane_drift_tolerance:
            shift_sign = -1 if drift > 0 else 1
            strafe_key = self._strafe_key_for_heading(entry_heading, shift_sign)
            pulse_sec = self._strict_strafe_pulse_duration(drift)
            self.logger.info(
                "Strict top-entry correction: heading=%s current_z=%.1f target_z=%.1f drift=%.2f key=%s pulse=%.2fs",
                entry_heading.value,
                pose.z,
                self.target_lane_z,
                drift,
                strafe_key,
                pulse_sec,
            )
            self.inputs.hold_key(strafe_key)
            time.sleep(pulse_sec)
            self.inputs.release_key(strafe_key)
            time.sleep(self.cfg.lane_strafe_settle_sec)
            self.current_action = MinerAction.NAVIGATE_TO_REGION
            return

        self._move_forward_without_mining()
        self.current_action = MinerAction.NAVIGATE_TO_REGION

    def _tick_strict_lane(self, pose: Pose, obstacle: ObstacleInfo) -> None:
        now = time.monotonic()
        target_heading = self.desired_heading or Cardinal.EAST
        target_yaw = self._strict_target_yaw if self._strict_target_yaw is not None else CARDINAL_YAWS[target_heading]
        yaw_error = abs(yaw_delta(pose.yaw, target_yaw))
        yaw_limit = 28.0 if self._strict_target_yaw is not None and abs(yaw_delta(target_yaw, CARDINAL_YAWS[target_heading])) > 1.0 else 20.0
        if yaw_error > yaw_limit:
            self.logger.info(
                "Strict heading drift reset: heading=%s yaw=%.1f heading_delta=%.1f target_yaw=%.1f",
                target_heading.value,
                normalize_yaw(pose.yaw),
                yaw_error,
                normalize_yaw(target_yaw),
            )
            self._reset_from_top(reason="heading_drift", advance_lane=False)
            return

        lane_drift = abs(pose.z - self.target_lane_z)
        if lane_drift > max(2.0, self.cfg.lane_drift_tolerance * 2.0):
            if self._maybe_correct_lane_drift(pose):
                self.current_action = MinerAction.REACQUIRE_STONE
                return
            self.logger.info(
                "Strict lane drift reset: current_z=%.1f target_lane_z=%.1f drift=%.2f",
                pose.z,
                self.target_lane_z,
                lane_drift,
            )
            self._reset_from_top(reason="lane_drift", advance_lane=False)
            return

        desired_pitch = self._desired_lane_pitch(pose)
        if not self._ensure_pitch(desired_pitch):
            self.current_action = MinerAction.REACQUIRE_STONE
            return

        look_type = self._effective_look_type(obstacle)
        at_lane_end = self._is_at_lane_end(pose)
        low_progress = self._is_low_progress(pose)

        if look_type == "ALLOWED" and not at_lane_end and not low_progress:
            if not self._align_tunnel_heading(target_heading):
                self.current_action = MinerAction.REACQUIRE_STONE
                return
            if self._should_use_flight_tunnel(pose, obstacle):
                self._engage_flight_tunnel_mode(pose=pose, obstacle=obstacle)
                self._maybe_retry_flight_tunnel(pose=pose, obstacle=obstacle)
            else:
                self._disengage_flight_tunnel_mode(reason="strict_ground_forward", pose=pose, obstacle=obstacle)
            self._drive_forward()
            self.current_action = MinerAction.FORWARD_MINE
            return

        if look_type == "AIR" and not at_lane_end and not low_progress:
            self._disengage_flight_tunnel_mode(reason="strict_air_reacquire", pose=pose, obstacle=obstacle)
            if self._try_strict_reacquire():
                return
            self._drive_forward()
            self.current_action = MinerAction.REACQUIRE_STONE
            if now - self._last_entry_log_mono >= 1.0:
                self.logger.info(
                    "Strict forward scan through AIR: heading=%s pitch=%.1f x=%.1f y=%.1f z=%.1f.",
                    target_heading.value,
                    self._desired_lane_pitch(pose),
                    pose.x,
                    pose.y,
                    pose.z,
                )
                self._last_entry_log_mono = now
            return

        if look_type == "BLOCKING" and self._is_boundary_marker_obstacle(obstacle):
            self._disengage_flight_tunnel_mode(reason="strict_transition_boundary_marker", pose=pose, obstacle=obstacle)
            if self._try_strict_lane_transition(pose, reason="boundary_marker"):
                return
            self._reset_from_top(reason="boundary_marker", advance_lane=at_lane_end)
            return

        if not at_lane_end and self._try_strict_reacquire():
            return

        if at_lane_end:
            reason = "lane_end"
        elif low_progress:
            reason = "low_progress"
        else:
            reason = f"look:{look_type.lower()}"
        self._disengage_flight_tunnel_mode(reason=f"strict_transition_{reason}", pose=pose, obstacle=obstacle)
        if self._try_strict_lane_transition(pose, reason=reason):
            return
        self._reset_from_top(reason=reason, advance_lane=at_lane_end)

    def _try_strict_reacquire(self) -> bool:
        self.inputs.stop_mining()
        self.inputs.release_key("w")
        self.inputs.release_key("ctrl")
        target_heading = self.desired_heading or Cardinal.EAST
        self.world_model.update()
        base_pose = self.world_model.get_player_pose()
        if base_pose is None:
            return False
        candidate_yaws = self._strict_reacquire_yaw_candidates(base_pose)
        for target_yaw in candidate_yaws:
            if self._stop_requested():
                self.inputs.all_stop()
                return False
            if not self._align_to_target_yaw(
                target_yaw,
                tolerance_deg=max(6.0, self.cfg.strict_yaw_tolerance_deg * 0.75),
                log_label="Strict local stone seek",
            ):
                continue
            for pitch in self._strict_pitch_sequence:
                if self._stop_requested():
                    self.inputs.all_stop()
                    return False
                if not self._ensure_pitch(pitch, blocking=True):
                    continue
                self.world_model.update()
                candidate_pose = self.world_model.get_player_pose()
                candidate_obstacle = self.world_model.is_obstacle_ahead()
                if candidate_pose is None:
                    continue
                if abs(candidate_pose.z - self.target_lane_z) > max(2.0, self.cfg.lane_drift_tolerance * 2.0):
                    continue
                if self._effective_look_type(candidate_obstacle) != "ALLOWED":
                    continue
                self._strict_target_yaw = normalize_yaw(candidate_pose.yaw)
                self.pitch_sweep_index = self._strict_pitch_sequence.index(pitch)
                self.progress_probe = None
                heading_target = CARDINAL_YAWS[target_heading]
                yaw_offset = yaw_delta(self._strict_target_yaw, heading_target)
                if abs(yaw_offset) >= 6.0:
                    self.reacquire_scan_bias = -1 if yaw_offset < 0.0 else 1
                self.inputs.hold_forward(sprint=False)
                self.inputs.start_mining()
                self.current_action = MinerAction.REACQUIRE_STONE
                self.logger.info(
                    "Strict lane reacquire: heading=%s target_yaw=%.1f pitch=%.1f block=%s at x=%.1f y=%.1f z=%.1f.",
                    target_heading.value,
                    normalize_yaw(self._strict_target_yaw),
                    pitch,
                    candidate_obstacle.block_id,
                    candidate_pose.x,
                    candidate_pose.y,
                    candidate_pose.z,
                )
                return True
        self._strict_target_yaw = CARDINAL_YAWS[target_heading]
        self.reacquire_scan_bias *= -1
        self.current_action = MinerAction.REACQUIRE_STONE
        return False

    def _strict_reacquire_yaw_candidates(self, pose: Pose) -> list[float]:
        base_yaw = pose.yaw
        if self.desired_heading in {Cardinal.EAST, Cardinal.WEST}:
            base_yaw = CARDINAL_YAWS[self.desired_heading]
        primary_sign = 1 if self.reacquire_scan_bias >= 0 else -1
        offsets = (
            0.0,
            primary_sign * 12.0,
            primary_sign * 24.0,
            primary_sign * 36.0,
            primary_sign * 52.0,
            primary_sign * 72.0,
            -primary_sign * 12.0,
            -primary_sign * 24.0,
            -primary_sign * 36.0,
            -primary_sign * 52.0,
            -primary_sign * 72.0,
        )
        candidates: list[float] = []
        seen: set[float] = set()
        for offset in offsets:
            yaw_value = round(normalize_yaw(base_yaw + offset), 3)
            if yaw_value in seen:
                continue
            seen.add(yaw_value)
            candidates.append(yaw_value)
        return candidates

    def _try_strict_lane_transition(self, pose: Pose, *, reason: str) -> bool:
        if self.region is None:
            return False
        shift_sign = self._next_snake_shift_sign(pose)
        if shift_sign is None:
            return False
        if self._stop_requested():
            self.inputs.all_stop()
            return False
        self.inputs.stop_mining()
        self.inputs.release_key("w")
        self.inputs.release_key("ctrl")
        self.inputs.hold_key("s")
        time.sleep(0.16)
        self.inputs.release_key("s")
        self._strict_target_yaw = None
        self._shift_lane(pose, shift_sign=shift_sign)
        self.current_action = MinerAction.SHIFT_POSITIVE if shift_sign > 0 else MinerAction.SHIFT_NEGATIVE
        self.logger.info(
            "Strict local lane transition: reason=%s shift=%s next_target_lane_z=%.1f next_heading=%s",
            reason,
            "positive" if shift_sign > 0 else "negative",
            self.target_lane_z if self.target_lane_z is not None else float("nan"),
            (self.desired_heading.value if self.desired_heading is not None else "UNKNOWN"),
        )
        return True

    def _strict_strafe_pulse_duration(self, drift: float) -> float:
        base = self.cfg.lane_strafe_pulse_sec
        extra = min(0.08, max(0.0, abs(drift) - self.cfg.lane_drift_tolerance) * 0.05)
        return max(0.05, base + extra)

    def _gmine_reset(self) -> None:
        preserved_heading = self.desired_heading
        super()._gmine_reset()
        if preserved_heading in {Cardinal.EAST, Cardinal.WEST}:
            self.desired_heading = preserved_heading

    def _reset_from_top(self, *, reason: str, advance_lane: bool) -> None:
        if advance_lane:
            self._advance_target_lane()
            self.logger.info(
                "Strict lane advance via /gmine reset: reason=%s next_target_lane_z=%.1f",
                reason,
                self.target_lane_z if self.target_lane_z is not None else float("nan"),
            )
        else:
            self.logger.info(
                "Strict lane reset: reason=%s target_lane_z=%.1f",
                reason,
                self.target_lane_z if self.target_lane_z is not None else float("nan"),
            )
        self._gmine_reset()
        self._strict_target_yaw = CARDINAL_YAWS[self.desired_heading] if self.desired_heading is not None else None
        self._was_in_region = False
        self.progress_probe = None
        self.reacquire_failures = 0
        self.entry_lane_z = self.target_lane_z
        self._reset_lane_runtime_decisions()
        self._strict_reset_wait_until_mono = time.monotonic() + self.cfg.strict_reset_command_cooldown_sec
        self.current_action = MinerAction.RESET_GMINE

    def _advance_target_lane(self) -> None:
        if self.region is None:
            return
        if self.target_lane_z is None:
            self.target_lane_z = self._clamp_lane_z(float(self.region.min_z) + 0.5)
            self._reset_lane_runtime_decisions()
            return
        proposed = self.target_lane_z + (self.cfg.lane_step_blocks * self.lane_shift_direction)
        min_lane = float(self.region.min_z) + 0.5
        max_lane = float(self.region.max_z) - 0.5
        if proposed > max_lane:
            self.lane_shift_direction = -1
            proposed = self.target_lane_z + (self.cfg.lane_step_blocks * self.lane_shift_direction)
        elif proposed < min_lane:
            self.lane_shift_direction = 1
            proposed = self.target_lane_z + (self.cfg.lane_step_blocks * self.lane_shift_direction)
        self.target_lane_z = self._clamp_lane_z(proposed)
        self._reset_lane_runtime_decisions()

    def _clamp_lane_z(self, z_value: float) -> float:
        if self.region is None:
            return z_value
        min_lane = float(self.region.min_z) + 0.5
        max_lane = float(self.region.max_z) - 0.5
        return max(min_lane, min(max_lane, z_value))

    def _is_top_entry_pose(self, pose: Pose) -> bool:
        return self._entry_heading_for_pose(pose) is not None

    def _entry_heading_for_pose(self, pose: Pose) -> Optional[Cardinal]:
        if self.region is None:
            return None
        if pose.y < (self.region.max_y - (self.cfg.top_reset_activation_margin + 2.0)):
            return None
        if pose.x <= (self.region.min_x + 0.5):
            return Cardinal.EAST
        if pose.x >= (self.region.max_x - 0.5):
            return Cardinal.WEST
        return None

    def _is_top_edge_lane_exit_pose(self, pose: Pose) -> bool:
        if self.region is None:
            return False
        if pose.y < (self.region.max_y - (self.cfg.top_reset_activation_margin + 2.0)):
            return False
        if self.target_lane_z is not None and abs(pose.z - self.target_lane_z) > max(1.5, self.cfg.lane_drift_tolerance * 2.0):
            return False
        east_exit = pose.x >= (self.region.max_x - 0.5) and pose.x <= (self.region.max_x + 4.0)
        west_exit = pose.x <= (self.region.min_x + 0.5) and pose.x >= (self.region.min_x - 4.0)
        return east_exit or west_exit

    def _align_spawn_heading(self, pose: Pose, heading: Cardinal) -> bool:
        target_yaw = CARDINAL_YAWS[heading]
        return self._align_to_target_yaw(
            target_yaw,
            tolerance_deg=self.cfg.strict_yaw_tolerance_deg,
            log_label=f"Strict top-entry heading alignment ({heading.value})",
        )

    def _align_to_target_yaw(
        self,
        target_yaw: float,
        *,
        tolerance_deg: Optional[float] = None,
        log_label: str = "Strict yaw alignment",
    ) -> bool:
        self.world_model.update()
        pose = self.world_model.get_player_pose()
        if pose is None:
            return False
        initial_delta = yaw_delta(pose.yaw, target_yaw)
        effective_tolerance = self.cfg.strict_yaw_tolerance_deg if tolerance_deg is None else max(1.0, float(tolerance_deg))
        if abs(initial_delta) <= effective_tolerance:
            self._strict_target_yaw = normalize_yaw(target_yaw)
            return True

        if not self._horizontal_calibration_ready:
            now = time.monotonic()
            if now >= self._horizontal_calibration_next_retry_mono:
                self._calibrate_horizontal_mouse()
                now = time.monotonic()
            if not self._horizontal_calibration_ready:
                self.inputs.stop_mining()
                self.inputs.release_key("w")
                self.inputs.release_key("a")
                self.inputs.release_key("d")
                self.inputs.release_key("ctrl")
                self.world_model.update()
                fallback_pose = self.world_model.get_player_pose()
                if fallback_pose is not None:
                    fallback_delta = yaw_delta(fallback_pose.yaw, target_yaw)
                    if abs(fallback_delta) <= effective_tolerance:
                        self.logger.info(
                            "%s fallback accepted current yaw without mouse calibration: yaw=%.1f delta=%.1f",
                            log_label,
                            normalize_yaw(fallback_pose.yaw),
                            fallback_delta,
                        )
                        self._strict_target_yaw = normalize_yaw(target_yaw)
                        return True
                if now - self._last_heading_log_mono >= 2.0:
                    retry_in = max(0.0, self._horizontal_calibration_next_retry_mono - now)
                    self.logger.warning(
                        "%s paused while strict heading calibration is unavailable; retrying in %.1fs.",
                        log_label,
                        retry_in,
                    )
                    self._last_heading_log_mono = now
                return False

        if self._yaw_step_right_deg == 0.0 or self._yaw_step_left_deg == 0.0:
            return False

        self.inputs.stop_mining()
        self.inputs.release_key("w")
        self.inputs.release_key("a")
        self.inputs.release_key("d")
        self.inputs.release_key("ctrl")

        attempts = 0
        while attempts < 24 and not self._stop_requested():
            self.world_model.update()
            current_pose = self.world_model.get_player_pose()
            if current_pose is None:
                return False
            delta = yaw_delta(current_pose.yaw, target_yaw)
            if abs(delta) <= effective_tolerance:
                self._strict_target_yaw = normalize_yaw(target_yaw)
                return True

            step_pixels = self._alignment_step_pixels_for_delta(delta)

            now = time.monotonic()
            if now - self._last_heading_log_mono >= 0.75:
                self.logger.info(
                    "%s: yaw=%.1f target=%.1f delta=%.1f tol=%.1f step_pixels=%d",
                    log_label,
                    normalize_yaw(current_pose.yaw),
                    normalize_yaw(target_yaw),
                    delta,
                    effective_tolerance,
                    step_pixels,
                )
                self._last_heading_log_mono = now

            predicted_right_yaw = normalize_yaw(
                current_pose.yaw + self._estimated_yaw_delta_for_pixels(self._yaw_step_right_deg, step_pixels)
            )
            predicted_left_yaw = normalize_yaw(
                current_pose.yaw + self._estimated_yaw_delta_for_pixels(self._yaw_step_left_deg, step_pixels)
            )
            right_abs = abs(yaw_delta(predicted_right_yaw, target_yaw))
            left_abs = abs(yaw_delta(predicted_left_yaw, target_yaw))

            if right_abs <= left_abs:
                self.inputs.look_right_small(
                    steps=1,
                    step_pixels=step_pixels,
                    delay=self.cfg.camera_step_delay_sec,
                )
            else:
                self.inputs.look_left_small(
                    steps=1,
                    step_pixels=step_pixels,
                    delay=self.cfg.camera_step_delay_sec,
                )
            self._wait_for_fresh_pose(current_pose.ts_utc, timeout_sec=0.35)
            attempts += 1
        self.world_model.update()
        settled_pose = self.world_model.get_player_pose()
        if settled_pose is None:
            return False
        settled_delta = yaw_delta(settled_pose.yaw, target_yaw)
        if abs(settled_delta) <= effective_tolerance:
            self._strict_target_yaw = normalize_yaw(target_yaw)
            return True
        return False

    def _align_heading(self, heading: Cardinal, *, blocking: bool = False) -> bool:
        self.desired_heading = heading
        target_yaw = CARDINAL_YAWS[heading]
        success = self._align_to_target_yaw(
            target_yaw,
            tolerance_deg=self.cfg.strict_yaw_tolerance_deg,
            log_label=f"Strict heading alignment ({heading.value})",
        )
        if success:
            self._strict_target_yaw = normalize_yaw(target_yaw)
        return success

    def _align_tunnel_heading(self, heading: Cardinal) -> bool:
        target_yaw = CARDINAL_YAWS[heading]
        success = self._align_to_target_yaw(
            target_yaw,
            tolerance_deg=min(5.0, max(3.0, self.cfg.strict_yaw_tolerance_deg * 0.4)),
            log_label=f"Strict tunnel heading settle ({heading.value})",
        )
        if success:
            self._strict_target_yaw = normalize_yaw(target_yaw)
        return success

    def _calibrate_horizontal_mouse(self) -> None:
        original_backend = self.inputs.get_mouse_backend()
        self.world_model.update()
        baseline_pose = self.world_model.get_player_pose()
        baseline_yaw = baseline_pose.yaw if baseline_pose is not None else None
        best_choice: Optional[Tuple[float, str, int, float, float, float]] = None

        for backend in self.inputs.get_available_mouse_backends():
            self.inputs.set_mouse_backend(backend)
            backend_choice: Optional[Tuple[float, int, float, float, float]] = None
            for step_pixels in (1, 2, 3):
                sample = self._sample_horizontal_mouse_backend(step_pixels)
                if sample is None:
                    continue
                right_delta, left_delta = sample
                self.logger.info(
                    "Strict heading calibration sample: backend=%s step_pixels=%d right_step_deg=%.2f left_step_deg=%.2f",
                    backend,
                    step_pixels,
                    right_delta,
                    left_delta,
                )
                if right_delta * left_delta >= 0.0:
                    continue
                if abs(right_delta) < 0.05 or abs(left_delta) < 0.05:
                    continue

                deg_per_pixel = ((abs(right_delta) + abs(left_delta)) / 2.0) / max(1, step_pixels)
                signal = min(abs(right_delta), abs(left_delta))
                if backend_choice is None or signal > backend_choice[0]:
                    backend_choice = (signal, step_pixels, right_delta, left_delta, deg_per_pixel)

            if backend_choice is None:
                continue

            _, step_pixels, right_delta, left_delta, deg_per_pixel = backend_choice
            if backend == "win32":
                self.inputs.set_mouse_backend(backend)
                self._yaw_step_right_deg = right_delta
                self._yaw_step_left_deg = left_delta
                self._yaw_step_pixels = step_pixels
                self._yaw_deg_per_pixel = deg_per_pixel
                self._horizontal_calibration_ready = True
                self._horizontal_calibration_next_retry_mono = 0.0
                if baseline_yaw is not None and not self._restore_heading_to_target(baseline_yaw, timeout_sec=1.5):
                    self.logger.warning(
                        "Strict heading calibration accepted backend=%s without a perfect baseline restore; proceeding with yaw_deg_per_pixel=%.4f.",
                        backend,
                        deg_per_pixel,
                    )
                self.logger.info(
                    "Strict heading calibration: backend=%s step_pixels=%d right_step_deg=%.2f left_step_deg=%.2f yaw_deg_per_pixel=%.4f",
                    backend,
                    step_pixels,
                    right_delta,
                    left_delta,
                    deg_per_pixel,
                )
                return

            average_step = (abs(right_delta) + abs(left_delta)) / 2.0
            if best_choice is None or average_step < best_choice[0]:
                best_choice = (average_step, backend, step_pixels, right_delta, left_delta, deg_per_pixel)

        self.inputs.set_mouse_backend(original_backend)
        if best_choice is not None:
            _, backend, step_pixels, right_delta, left_delta, deg_per_pixel = best_choice
            self.inputs.set_mouse_backend(backend)
            self._yaw_step_right_deg = right_delta
            self._yaw_step_left_deg = left_delta
            self._yaw_step_pixels = step_pixels
            self._yaw_deg_per_pixel = deg_per_pixel
            self._horizontal_calibration_ready = True
            self._horizontal_calibration_next_retry_mono = 0.0
            if baseline_yaw is not None and not self._restore_heading_to_target(baseline_yaw, timeout_sec=1.5):
                self.logger.warning(
                    "Strict heading calibration selected a backend but could not settle baseline yaw cleanly; startup alignment will wait for the next retry window."
                )
                self._yaw_step_right_deg = 0.0
                self._yaw_step_left_deg = 0.0
                self._yaw_step_pixels = 1
                self._yaw_deg_per_pixel = 0.0
                self._horizontal_calibration_ready = False
                self._horizontal_calibration_next_retry_mono = time.monotonic() + self.cfg.strict_calibration_retry_sec
                return
            self.logger.info(
                "Strict heading calibration: backend=%s step_pixels=%d right_step_deg=%.2f left_step_deg=%.2f yaw_deg_per_pixel=%.4f",
                backend,
                step_pixels,
                right_delta,
                left_delta,
                deg_per_pixel,
            )
            return

        self._yaw_step_right_deg = 0.0
        self._yaw_step_left_deg = 0.0
        self._yaw_step_pixels = 1
        self._yaw_deg_per_pixel = 0.0
        self._horizontal_calibration_ready = False
        self._horizontal_calibration_next_retry_mono = time.monotonic() + self.cfg.strict_calibration_retry_sec
        self.logger.warning(
            "Strict heading calibration failed to find a reliable horizontal mouse backend; "
            "strict startup alignment will stay paused instead of spinning."
        )

    def _sample_horizontal_mouse_backend(self, step_pixels: int) -> Optional[Tuple[float, float]]:
        self.world_model.update()
        start_pose = self.world_model.get_player_pose()
        if start_pose is None:
            return None

        self.inputs.look_right_small(
            steps=1,
            step_pixels=step_pixels,
            delay=self.cfg.camera_step_delay_sec,
        )
        right_pose = self._wait_for_fresh_pose(start_pose.ts_utc, timeout_sec=0.35)
        if right_pose is None:
            return None
        right_delta = normalize_yaw(right_pose.yaw - start_pose.yaw)

        self.inputs.look_left_small(
            steps=1,
            step_pixels=step_pixels,
            delay=self.cfg.camera_step_delay_sec,
        )
        restored_pose = self._wait_for_fresh_pose(right_pose.ts_utc, timeout_sec=0.35)
        if restored_pose is None:
            return None

        self.inputs.look_left_small(
            steps=1,
            step_pixels=step_pixels,
            delay=self.cfg.camera_step_delay_sec,
        )
        left_pose = self._wait_for_fresh_pose(restored_pose.ts_utc, timeout_sec=0.35)
        if left_pose is None:
            return None
        left_delta = normalize_yaw(left_pose.yaw - restored_pose.yaw)

        self.inputs.look_right_small(
            steps=1,
            step_pixels=step_pixels,
            delay=self.cfg.camera_step_delay_sec,
        )
        self._wait_for_fresh_pose(left_pose.ts_utc, timeout_sec=0.35)
        return right_delta, left_delta

    def _wait_for_fresh_pose(self, previous_ts_utc: str, timeout_sec: float = 0.35) -> Optional[Pose]:
        deadline = time.monotonic() + max(0.05, timeout_sec)
        while time.monotonic() < deadline and not self._stop_requested():
            self.world_model.update()
            pose = self.world_model.get_player_pose()
            if pose is not None and pose.ts_utc != previous_ts_utc:
                return pose
            time.sleep(0.01)
        self.world_model.update()
        return self.world_model.get_player_pose()

    def _alignment_step_pixels_for_delta(self, delta_deg: float) -> int:
        base_pixels = max(1, self._yaw_step_pixels)
        if self._yaw_deg_per_pixel <= 0.0:
            return base_pixels
        target_pixels = int(round((abs(delta_deg) / max(self._yaw_deg_per_pixel, 0.01)) * 0.18))
        soft_cap = 180 if abs(delta_deg) >= 90.0 else 132
        return max(base_pixels, min(soft_cap, target_pixels))

    def _estimated_yaw_delta_for_pixels(self, base_delta_deg: float, step_pixels: int) -> float:
        if step_pixels <= 0:
            return base_delta_deg
        if self._yaw_deg_per_pixel > 0.0:
            scaled_delta = self._yaw_deg_per_pixel * step_pixels
            return math.copysign(max(abs(base_delta_deg), scaled_delta), base_delta_deg)
        scale = step_pixels / max(1, self._yaw_step_pixels)
        return base_delta_deg * scale

    def _restore_heading_to_target(self, target_yaw: float, timeout_sec: float = 1.0) -> bool:
        if self._yaw_step_right_deg == 0.0 or self._yaw_step_left_deg == 0.0:
            return False

        deadline = time.monotonic() + max(0.2, timeout_sec)
        tolerance = max(
            4.0,
            min(abs(self._yaw_step_right_deg), abs(self._yaw_step_left_deg)) * 0.55,
        )
        while time.monotonic() < deadline and not self._stop_requested():
            self.world_model.update()
            pose = self.world_model.get_player_pose()
            if pose is None:
                return False
            delta = yaw_delta(pose.yaw, target_yaw)
            if abs(delta) <= tolerance:
                return True

            step_pixels = self._alignment_step_pixels_for_delta(delta)
            predicted_right_yaw = normalize_yaw(
                pose.yaw + self._estimated_yaw_delta_for_pixels(self._yaw_step_right_deg, step_pixels)
            )
            predicted_left_yaw = normalize_yaw(
                pose.yaw + self._estimated_yaw_delta_for_pixels(self._yaw_step_left_deg, step_pixels)
            )
            right_abs = abs(yaw_delta(predicted_right_yaw, target_yaw))
            left_abs = abs(yaw_delta(predicted_left_yaw, target_yaw))

            previous_ts = pose.ts_utc
            if right_abs <= left_abs:
                self.inputs.look_right_small(
                    steps=1,
                    step_pixels=step_pixels,
                    delay=self.cfg.camera_step_delay_sec,
                )
            else:
                self.inputs.look_left_small(
                    steps=1,
                    step_pixels=step_pixels,
                    delay=self.cfg.camera_step_delay_sec,
                )
            self._wait_for_fresh_pose(previous_ts, timeout_sec=0.35)

        self.world_model.update()
        pose = self.world_model.get_player_pose()
        if pose is None:
            return False
        return abs(yaw_delta(pose.yaw, target_yaw)) <= tolerance


class ScoutPhase(str, Enum):
    APPROACH_FACE = "APPROACH_FACE"
    TRACE_SOUTH = "TRACE_SOUTH"
    TRACE_EAST = "TRACE_EAST"
    TRACE_NORTH = "TRACE_NORTH"
    TRACE_WEST = "TRACE_WEST"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True)
class ScoutLeg:
    phase: ScoutPhase
    heading: Cardinal
    action: MinerAction
    min_progress: float


class PerimeterScout:
    def __init__(
        self,
        cfg: AppConfig,
        logger: logging.Logger,
        world_model: WorldModelForge,
        inputs: SimpleInputController,
        region: Optional[RegionConfig],
        blocking_learner: BlockingBlockLearner,
        voxel_memory: Optional[VoxelWorldMemory] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        self.cfg = cfg
        self.logger = logger.getChild("perimeter_scout")
        self.world_model = world_model
        self.inputs = inputs
        self.region = region
        self.blocking_learner = blocking_learner
        self.voxel_memory = voxel_memory
        self.stop_event = stop_event or threading.Event()
        self._report_path = perimeter_scout_last_run_path()
        self._memory_path = perimeter_scout_memory_path()
        self._legs = [
            ScoutLeg(ScoutPhase.APPROACH_FACE, Cardinal.EAST, MinerAction.SCOUT_APPROACH_FACE, 1.5),
            ScoutLeg(ScoutPhase.TRACE_SOUTH, Cardinal.SOUTH, MinerAction.SCOUT_TRACE_SOUTH, 4.0),
            ScoutLeg(ScoutPhase.TRACE_EAST, Cardinal.EAST, MinerAction.SCOUT_TRACE_EAST, 4.0),
            ScoutLeg(ScoutPhase.TRACE_NORTH, Cardinal.NORTH, MinerAction.SCOUT_TRACE_NORTH, 4.0),
            ScoutLeg(ScoutPhase.TRACE_WEST, Cardinal.WEST, MinerAction.SCOUT_TRACE_WEST, 4.0),
        ]
        self.reset_for_new_run()

    def _stop_requested(self) -> bool:
        return self.stop_event.is_set()

    def _sleep_interruptibly(self, seconds: float) -> bool:
        deadline = time.monotonic() + max(0.0, seconds)
        while time.monotonic() < deadline:
            if self._stop_requested():
                return False
            time.sleep(min(0.02, deadline - time.monotonic()))
        return not self._stop_requested()

    def reset_for_new_run(self) -> None:
        self.current_action = MinerAction.WAIT_FOR_TELEMETRY
        self.current_phase: ScoutPhase = self._legs[0].phase
        self._leg_index = 0
        self._leg_probe: Optional[ScoutLegProbe] = None
        self._trace_points: list[dict[str, object]] = []
        self._observation_samples: list[dict[str, object]] = []
        self._corner_points: list[dict[str, object]] = []
        self._last_sample_pose: Optional[Tuple[float, float, float]] = None
        self._last_sample_mono: float = 0.0
        self._last_observation_axis: Optional[float] = None
        self._last_observation_phase: Optional[ScoutPhase] = None
        self._last_observation_mono: float = 0.0
        self._spawn_pose: Optional[Pose] = None
        self._reference_heading: Optional[Cardinal] = None
        self._status: str = "idle"
        self._complete = False
        self._memory_reused = False
        self._boundary_confirmed = False
        self.inputs.all_stop()

    def begin_run(self) -> None:
        self.reset_for_new_run()
        self.inputs.all_stop()
        reused_report = self._build_reused_report_from_memory()
        if reused_report is not None:
            self._memory_reused = True
            self._status = "complete"
            self._complete = True
            self.current_phase = ScoutPhase.COMPLETE
            self.current_action = MinerAction.SCOUT_COMPLETE
            self._report_path.write_text(json.dumps(reused_report, indent=2), encoding="utf-8")
            self.logger.info(
                "Perimeter scout reused verified memory for region '%s'; skipping live scout walk.",
                _region_memory_key(self.region),
            )

    def is_complete(self) -> bool:
        return self._complete

    def memory_was_reused(self) -> bool:
        return self._memory_reused

    def has_reportable_progress(self) -> bool:
        return self._spawn_pose is not None or bool(self._trace_points) or bool(self._corner_points)

    def snapshot(self, status: str = "interrupted") -> None:
        if not self.has_reportable_progress():
            return
        self._write_report(status=status)

    def tick(self) -> Tuple[MinerAction, ObstacleInfo]:
        if self._stop_requested():
            self.inputs.all_stop()
            self.current_action = MinerAction.STOP_ALL
            return self.current_action, ObstacleInfo(False, "minecraft:air", None, None, None)

        self.world_model.update()
        obstacle = self.world_model.is_obstacle_ahead()
        pose = self.world_model.get_player_pose()
        now = time.monotonic()

        if pose is None or not self._has_fresh_telemetry():
            self.inputs.all_stop()
            self.current_action = MinerAction.WAIT_FOR_TELEMETRY
            return self.current_action, obstacle

        if self._complete:
            self.inputs.all_stop()
            self.current_action = MinerAction.SCOUT_COMPLETE
            return self.current_action, obstacle

        if self._spawn_pose is None:
            self._spawn_pose = pose
            self._reference_heading = yaw_to_cardinal(pose.yaw)
            self._status = "running"
            self.logger.info(
                "Perimeter scout starting from x=%.2f y=%.2f z=%.2f with reference heading=%s.",
                pose.x,
                pose.y,
                pose.z,
                self._reference_heading.value if self._reference_heading else "UNKNOWN",
            )
            self._write_report(status="running")

        self._record_trace_point(pose, obstacle, force=not self._trace_points)
        self.blocking_learner.observe(
            self.region,
            pose,
            obstacle,
            in_region=is_pose_in_region(pose, self.region),
        )

        leg = self._legs[self._leg_index]
        self.current_phase = leg.phase
        self.current_action = leg.action

        self._maybe_capture_observation_sweep(
            pose,
            leg,
            now,
            force=self._last_observation_phase != leg.phase,
        )
        move_heading = self._desired_move_heading(pose, leg)
        self._aim_to_heading(move_heading)
        self._ensure_pitch(max(8.0, self.cfg.navigation_pitch))
        self._move_for_leg(move_heading)
        self.world_model.update()
        pose = self.world_model.get_player_pose() or pose
        obstacle = self.world_model.is_obstacle_ahead()
        self._record_trace_point(pose, obstacle)

        if self._leg_has_completed(pose, leg, now):
            self._finish_leg(pose, obstacle)

        return self.current_action, obstacle

    def _has_fresh_telemetry(self) -> bool:
        age = self.world_model.pose_provider.get_last_update_age_sec()
        return age is not None and age <= self.cfg.stale_pose_timeout_sec

    def _move_forward_without_mining(self) -> None:
        self.inputs.hold_forward(sprint=self.cfg.use_sprint)
        self.inputs.stop_mining()

    def _orthogonal_correction_heading(self, pose: Pose, leg: ScoutLeg) -> Optional[Cardinal]:
        if self.region is None:
            return None
        if leg.phase == ScoutPhase.TRACE_SOUTH:
            target_x = float(self.region.min_x) + 0.5
            drift = pose.x - target_x
            if abs(drift) <= 0.45:
                return None
            return Cardinal.WEST if drift > 0 else Cardinal.EAST
        if leg.phase == ScoutPhase.TRACE_EAST:
            target_z = float(self.region.max_z) - 0.5
            drift = pose.z - target_z
            if abs(drift) <= 0.45:
                return None
            return Cardinal.SOUTH if drift > 0 else Cardinal.NORTH
        if leg.phase == ScoutPhase.TRACE_NORTH:
            target_x = float(self.region.max_x) - 0.5
            drift = pose.x - target_x
            if abs(drift) <= 0.45:
                return None
            return Cardinal.WEST if drift > 0 else Cardinal.EAST
        if leg.phase == ScoutPhase.TRACE_WEST:
            target_z = float(self.region.min_z) + 0.5
            drift = pose.z - target_z
            if abs(drift) <= 0.45:
                return None
            return Cardinal.SOUTH if drift > 0 else Cardinal.NORTH
        return None

    def _target_point_for_leg(self, leg: ScoutLeg) -> Optional[Tuple[float, float]]:
        if self.region is None:
            return None
        west_x = float(self.region.min_x) + 0.5
        east_x = float(self.region.max_x) - 0.5
        north_z = float(self.region.min_z) + 0.5
        south_z = float(self.region.max_z) - 0.5
        if leg.phase == ScoutPhase.APPROACH_FACE:
            anchor_pose = self._spawn_pose or self.world_model.get_player_pose()
            anchor_z = south_z if anchor_pose is None else min(max(anchor_pose.z, north_z), south_z)
            return west_x, anchor_z
        if leg.phase == ScoutPhase.TRACE_SOUTH:
            return west_x, south_z
        if leg.phase == ScoutPhase.TRACE_EAST:
            return east_x, south_z
        if leg.phase == ScoutPhase.TRACE_NORTH:
            return east_x, north_z
        if leg.phase == ScoutPhase.TRACE_WEST:
            return west_x, north_z
        return None

    def _desired_move_heading(self, pose: Pose, leg: ScoutLeg) -> Cardinal:
        target_point = self._target_point_for_leg(leg)
        if target_point is None:
            return leg.heading
        target_x, target_z = target_point
        dx = target_x - pose.x
        dz = target_z - pose.z
        if abs(dx) <= 0.35 and abs(dz) <= 0.35:
            return leg.heading
        if abs(dx) >= abs(dz):
            return Cardinal.EAST if dx > 0.0 else Cardinal.WEST
        return Cardinal.SOUTH if dz > 0.0 else Cardinal.NORTH

    def _is_near_leg_boundary(self, pose: Pose, leg: ScoutLeg, tolerance: float = 1.0) -> bool:
        target_point = self._target_point_for_leg(leg)
        if target_point is None:
            return False
        target_x, target_z = target_point
        if leg.phase == ScoutPhase.TRACE_SOUTH:
            return abs(pose.x - target_x) <= tolerance
        if leg.phase == ScoutPhase.TRACE_EAST:
            return abs(pose.z - target_z) <= tolerance
        if leg.phase == ScoutPhase.TRACE_NORTH:
            return abs(pose.x - target_x) <= tolerance
        if leg.phase == ScoutPhase.TRACE_WEST:
            return abs(pose.z - target_z) <= tolerance
        return abs(pose.x - target_x) <= tolerance and abs(pose.z - target_z) <= tolerance

    def _move_for_leg(self, desired_heading: Cardinal) -> None:
        self.inputs.release_key("a")
        self.inputs.release_key("d")
        self.inputs.release_key("s")
        self.inputs.hold_forward(sprint=False)
        self.inputs.release_key("ctrl")
        self.inputs.stop_mining()

    def _observation_heading_for_leg(self, leg: ScoutLeg) -> Cardinal:
        if leg.phase == ScoutPhase.APPROACH_FACE:
            return leg.heading
        return left_heading(leg.heading)

    def _observation_pitch_for_leg(self, leg: ScoutLeg) -> float:
        if leg.phase == ScoutPhase.APPROACH_FACE:
            return max(18.0, self.cfg.navigation_pitch)
        return 42.0

    def _infer_observation_state(
        self,
        *,
        block_class: str,
        block_x: Optional[int],
        block_y: Optional[int],
        block_z: Optional[int],
        role: str,
    ) -> str:
        if self.region is None:
            return block_class
        if block_class == "air":
            return "interior_air" if role.startswith("inward") else "exterior_air"
        if block_x is None or block_y is None or block_z is None:
            if role.startswith("inward"):
                return f"interior_{block_class}"
            return f"exterior_{block_class}"
        in_bounds = (
            self.region.min_x <= block_x <= self.region.max_x
            and self.region.min_y <= block_y <= self.region.max_y
            and self.region.min_z <= block_z <= self.region.max_z
        )
        if in_bounds:
            return f"interior_{block_class}"
        return f"exterior_{block_class}"

    def _append_observation_sample(
        self,
        pose: Pose,
        obstacle: ObstacleInfo,
        *,
        phase: ScoutPhase,
        role: str,
        heading: Cardinal,
        pitch_target: float,
    ) -> None:
        look_block_class = classify_observed_look_block(obstacle.block_id, self.region)
        observation_state = self._infer_observation_state(
            block_class=look_block_class,
            block_x=obstacle.block_x,
            block_y=obstacle.block_y,
            block_z=obstacle.block_z,
            role=role,
        )
        self._observation_samples.append(
            {
                "phase": phase.value,
                "ts_utc": pose.ts_utc,
                "x": round(pose.x, 3),
                "y": round(pose.y, 3),
                "z": round(pose.z, 3),
                "yaw": round(normalize_yaw(pose.yaw), 3),
                "pitch": round(pose.pitch, 3),
                "look_heading": heading.value,
                "pitch_target": round(pitch_target, 3),
                "observation_role": role,
                "look_block": obstacle.block_id,
                "look_block_class": look_block_class,
                "observation_state": observation_state,
                "block_x": obstacle.block_x,
                "block_y": obstacle.block_y,
                "block_z": obstacle.block_z,
            }
        )
        if (
            self.region is not None
            and str(obstacle.block_id or "").strip().lower() in self.region.blocking_block_ids
        ):
            self._boundary_confirmed = True

    def _maybe_capture_observation_sweep(
        self,
        pose: Pose,
        leg: ScoutLeg,
        now: float,
        *,
        force: bool = False,
    ) -> None:
        if self.region is None:
            return
        axis = self._axis_value(pose, leg.heading)
        if not force:
            if self._last_observation_phase == leg.phase and self._last_observation_axis is not None:
                if abs(axis - self._last_observation_axis) < 6.0:
                    return
            if now - self._last_observation_mono < 1.2:
                return

        inward_heading = self._observation_heading_for_leg(leg)
        outward_heading = leg.heading if leg.phase == ScoutPhase.APPROACH_FACE else right_heading(leg.heading)
        sample_plan: list[tuple[str, Cardinal, float]] = [
            ("inward_surface", inward_heading, 24.0),
            ("inward_mid", inward_heading, 42.0),
            ("inward_deep", inward_heading, 60.0),
        ]
        if (
            leg.phase != ScoutPhase.APPROACH_FACE
            and not self._boundary_confirmed
            and self._is_near_leg_boundary(pose, leg)
        ):
            sample_plan.append(("outward_boundary", outward_heading, 24.0))

        self.inputs.all_stop()
        for role, heading, pitch_target in sample_plan:
            if self._stop_requested():
                break
            self._aim_to_heading(heading)
            self._ensure_pitch(pitch_target)
            self.world_model.update()
            sample_pose = self.world_model.get_player_pose()
            sample_obstacle = self.world_model.is_obstacle_ahead()
            if sample_pose is None:
                continue
            self._append_observation_sample(
                sample_pose,
                sample_obstacle,
                phase=leg.phase,
                role=role,
                heading=heading,
                pitch_target=pitch_target,
            )
            self.blocking_learner.observe(
                self.region,
                sample_pose,
                sample_obstacle,
                in_region=is_pose_in_region(sample_pose, self.region),
            )
            time.sleep(0.04)

        self._aim_to_heading(inward_heading)
        self._ensure_pitch(self._observation_pitch_for_leg(leg))
        self.inputs.all_stop()
        self._last_observation_axis = axis
        self._last_observation_phase = leg.phase
        self._last_observation_mono = now

    def _ensure_pitch(self, target_pitch: float) -> None:
        deadline = time.monotonic() + 0.8
        while time.monotonic() < deadline:
            if self._stop_requested():
                self.inputs.all_stop()
                return
            self.world_model.update()
            pose = self.world_model.get_player_pose()
            if pose is None:
                return
            delta = target_pitch - pose.pitch
            if abs(delta) <= self.cfg.pitch_tolerance:
                return
            if delta > 0:
                self.inputs.look_down_small(steps=3, step_pixels=5, delay=0.003)
            else:
                self.inputs.look_up_small(steps=3, step_pixels=5, delay=0.003)
            time.sleep(0.02)

    def _aim_to_heading(self, heading: Cardinal) -> None:
        target_yaw = CARDINAL_YAWS[heading]
        deadline = time.monotonic() + self.cfg.rotate_timeout_sec
        while time.monotonic() < deadline:
            if self._stop_requested():
                self.inputs.all_stop()
                return
            self.world_model.update()
            pose = self.world_model.get_player_pose()
            if pose is None:
                return
            delta = yaw_delta(pose.yaw, target_yaw)
            if abs(delta) <= self.cfg.heading_tolerance_deg:
                return
            if delta > 0:
                self.inputs.look_right_small(
                    steps=3 if abs(delta) < 45.0 else 6,
                    step_pixels=5,
                    delay=0.003,
                )
            else:
                self.inputs.look_left_small(
                    steps=3 if abs(delta) < 45.0 else 6,
                    step_pixels=5,
                    delay=0.003,
                )
            time.sleep(0.02)

    def _axis_value(self, pose: Pose, heading: Cardinal) -> float:
        return pose.x if heading in {Cardinal.EAST, Cardinal.WEST} else pose.z

    def _target_axis_for_leg(self, leg: ScoutLeg) -> Optional[float]:
        if self.region is None:
            return None
        if leg.phase == ScoutPhase.APPROACH_FACE:
            if leg.heading == Cardinal.EAST:
                return float(self.region.min_x) + 0.5
            if leg.heading == Cardinal.WEST:
                return float(self.region.max_x) - 0.5
            if leg.heading == Cardinal.SOUTH:
                return float(self.region.min_z) + 0.5
            return float(self.region.max_z) - 0.5
        if leg.phase == ScoutPhase.TRACE_SOUTH:
            return float(self.region.max_z) - 0.5
        if leg.phase == ScoutPhase.TRACE_EAST:
            return float(self.region.max_x) - 0.5
        if leg.phase == ScoutPhase.TRACE_NORTH:
            return float(self.region.min_z) + 0.5
        if leg.phase == ScoutPhase.TRACE_WEST:
            return float(self.region.min_x) + 0.5
        return None

    def _has_reached_target_axis(self, axis: float, target_axis: float, heading: Cardinal) -> bool:
        if heading in {Cardinal.EAST, Cardinal.SOUTH}:
            return axis >= target_axis
        return axis <= target_axis

    def _leg_has_completed(self, pose: Pose, leg: ScoutLeg, now: float) -> bool:
        target_point = self._target_point_for_leg(leg)
        if target_point is not None:
            target_x, target_z = target_point
            if abs(pose.x - target_x) <= 0.65 and abs(pose.z - target_z) <= 0.65:
                return True

        axis = self._axis_value(pose, leg.heading)
        target_axis = self._target_axis_for_leg(leg)
        if leg.phase != ScoutPhase.APPROACH_FACE and target_axis is not None and self._has_reached_target_axis(axis, target_axis, leg.heading):
            return True

        probe = self._leg_probe
        if probe is None:
            self._leg_probe = ScoutLegProbe(
                leg_start_axis=axis,
                last_check_mono=now,
                last_check_axis=axis,
            )
            return False

        if now - probe.last_check_mono < self.cfg.scout_stall_window_sec:
            return False

        direction = heading_sign(leg.heading)
        total_progress = direction * (axis - probe.leg_start_axis)
        incremental_progress = direction * (axis - probe.last_check_axis)
        motion = self.world_model.get_motion_estimate()
        horizontal_speed = motion.horizontal_speed_bps if motion is not None else None
        self._leg_probe = ScoutLegProbe(
            leg_start_axis=probe.leg_start_axis,
            last_check_mono=now,
            last_check_axis=axis,
        )
        stalled = incremental_progress < self.cfg.scout_stall_progress_distance
        if horizontal_speed is not None:
            stalled = stalled or horizontal_speed <= self.cfg.scout_stall_speed_bps
        if target_axis is not None:
            remaining_distance = abs(target_axis - axis)
            if remaining_distance > max(1.5, self.cfg.scout_sample_distance * 2.0):
                return False
        return (
            total_progress >= leg.min_progress
            and total_progress >= self.cfg.scout_min_leg_progress
            and stalled
        )

    def _finish_leg(self, pose: Pose, obstacle: ObstacleInfo) -> None:
        leg = self._legs[self._leg_index]
        self._maybe_capture_observation_sweep(pose, leg, time.monotonic(), force=True)
        self._record_trace_point(pose, obstacle, force=True)
        look_block_class = classify_observed_look_block(obstacle.block_id, self.region)
        self._corner_points.append(
            {
                "phase": leg.phase.value,
                "heading": leg.heading.value,
                "ts_utc": pose.ts_utc,
                "x": round(pose.x, 3),
                "y": round(pose.y, 3),
                "z": round(pose.z, 3),
                "yaw": round(normalize_yaw(pose.yaw), 3),
                "pitch": round(pose.pitch, 3),
                "look_block": obstacle.block_id,
                "look_block_class": look_block_class,
                "block_x": obstacle.block_x,
                "block_y": obstacle.block_y,
                "block_z": obstacle.block_z,
            }
        )
        self.logger.info(
            "Perimeter scout completed %s at x=%.2f y=%.2f z=%.2f.",
            leg.phase.value,
            pose.x,
            pose.y,
            pose.z,
        )
        self._write_report(status="running")

        self._leg_index += 1
        self._leg_probe = None
        if self._leg_index >= len(self._legs):
            self._complete_scout()
        else:
            next_leg = self._legs[self._leg_index]
            self.current_phase = next_leg.phase
            self.current_action = next_leg.action

    def _complete_scout(self) -> None:
        self.inputs.all_stop()
        self._status = "complete"
        self._write_report(status="complete")
        report = _load_json_dict(self._report_path)
        if isinstance(report, dict):
            self._update_memory_from_report(report)
            if self.voxel_memory is not None:
                self.voxel_memory.ingest_scout_report(report)
        self.logger.info("Perimeter scout saved report to %s", self._report_path)
        self.current_phase = ScoutPhase.COMPLETE
        self.current_action = MinerAction.SCOUT_COMPLETE
        self._complete = True

    def _record_trace_point(self, pose: Pose, obstacle: ObstacleInfo, *, force: bool = False) -> None:
        now = time.monotonic()
        current = (pose.x, pose.y, pose.z)
        if not force and self._last_sample_pose is not None:
            dx = current[0] - self._last_sample_pose[0]
            dy = current[1] - self._last_sample_pose[1]
            dz = current[2] - self._last_sample_pose[2]
            if math.sqrt(dx * dx + dy * dy + dz * dz) < self.cfg.scout_sample_distance:
                if now - self._last_sample_mono < self.cfg.scout_stall_window_sec:
                    return

        motion = self.world_model.get_motion_estimate()
        look_block_class = classify_observed_look_block(obstacle.block_id, self.region)
        observation_state = self._infer_observation_state(
            block_class=look_block_class,
            block_x=obstacle.block_x,
            block_y=obstacle.block_y,
            block_z=obstacle.block_z,
            role="trace_current",
        )
        self._trace_points.append(
            {
                "phase": self.current_phase.value,
                "ts_utc": pose.ts_utc,
                "x": round(pose.x, 3),
                "y": round(pose.y, 3),
                "z": round(pose.z, 3),
                "yaw": round(normalize_yaw(pose.yaw), 3),
                "pitch": round(pose.pitch, 3),
                "speed_bps": None if motion is None else round(motion.speed_bps, 3),
                "horizontal_speed_bps": None if motion is None else round(motion.horizontal_speed_bps, 3),
                "look_block": obstacle.block_id,
                "look_block_class": look_block_class,
                "observation_state": observation_state,
                "block_x": obstacle.block_x,
                "block_y": obstacle.block_y,
                "block_z": obstacle.block_z,
            }
        )
        self._last_sample_pose = current
        self._last_sample_mono = now
        self._write_report(status="running")

    def _write_report(self, *, status: Optional[str] = None) -> None:
        if status is not None:
            self._status = status
        report = self._build_report()
        self._report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    def _build_block_catalog(self, points: Sequence[dict[str, object]]) -> dict[str, dict[str, object]]:
        catalog: dict[str, dict[str, object]] = {}
        for point in points:
            block_id = str(point.get("look_block") or "minecraft:air").strip().lower()
            if not block_id:
                continue
            block_class = str(point.get("look_block_class") or "unknown")
            observation_state = str(point.get("observation_state") or "unclassified")
            entry = catalog.setdefault(
                block_id,
                {
                    "sample_count": 0,
                    "mineable_hits": 0,
                    "non_mineable_hits": 0,
                    "air_hits": 0,
                    "interior_hits": 0,
                    "exterior_hits": 0,
                    "last_classification": "unknown",
                },
            )
            entry["sample_count"] = int(entry.get("sample_count", 0) or 0) + 1
            if block_class == "mineable":
                entry["mineable_hits"] = int(entry.get("mineable_hits", 0) or 0) + 1
            elif block_class == "air":
                entry["air_hits"] = int(entry.get("air_hits", 0) or 0) + 1
            else:
                entry["non_mineable_hits"] = int(entry.get("non_mineable_hits", 0) or 0) + 1
            if observation_state.startswith("interior_"):
                entry["interior_hits"] = int(entry.get("interior_hits", 0) or 0) + 1
            elif observation_state.startswith("exterior_"):
                entry["exterior_hits"] = int(entry.get("exterior_hits", 0) or 0) + 1
            if int(entry.get("mineable_hits", 0) or 0) > 0:
                entry["last_classification"] = "mineable"
            elif int(entry.get("non_mineable_hits", 0) or 0) > 0:
                entry["last_classification"] = "non_mineable"
            elif int(entry.get("air_hits", 0) or 0) > 0:
                entry["last_classification"] = "air"
        return catalog

    def _bounds_match_configured_region(self, bounds: Optional[dict[str, object]], *, tolerance: float = 2.5) -> bool:
        if self.region is None or not isinstance(bounds, dict):
            return False
        try:
            return (
                abs(float(bounds.get("x_min")) - float(self.region.min_x)) <= tolerance
                and abs(float(bounds.get("x_max")) - float(self.region.max_x)) <= tolerance
                and abs(float(bounds.get("z_min")) - float(self.region.min_z)) <= tolerance
                and abs(float(bounds.get("z_max")) - float(self.region.max_z)) <= tolerance
            )
        except (TypeError, ValueError):
            return False

    def _is_report_reusable(self, report: dict[str, object]) -> bool:
        verification = report.get("region_verification")
        if not isinstance(verification, dict):
            return False
        configured_match = verification.get("configured_region_match")
        if not isinstance(configured_match, dict):
            return False
        perimeter_bounds = report.get("observed_perimeter_bounds")
        recommended_bounds = report.get("recommended_region_bounds")
        return bool(
            report.get("status") == "complete"
            and configured_match.get("matches_expectation")
            and int(configured_match.get("interior_mineable_hits", 0) or 0) >= 5
            and int(configured_match.get("exterior_non_mineable_hits", 0) or 0) >= 3
            and (
                self._bounds_match_configured_region(perimeter_bounds)
                or self._bounds_match_configured_region(recommended_bounds)
            )
        )

    def _build_memory_report_template(self, report: dict[str, object]) -> dict[str, object]:
        return {
            "version": report.get("version"),
            "mode": "perimeter_scout",
            "configured_region": report.get("configured_region"),
            "sample_count": report.get("sample_count"),
            "observation_sample_count": report.get("observation_sample_count"),
            "observed_walkable_bounds": report.get("observed_walkable_bounds"),
            "candidate_bbox_from_trace": report.get("candidate_bbox_from_trace"),
            "observed_perimeter_bounds": report.get("observed_perimeter_bounds"),
            "candidate_bbox_from_perimeter": report.get("candidate_bbox_from_perimeter"),
            "recommended_region_bounds": report.get("recommended_region_bounds"),
            "look_block_intelligence": report.get("look_block_intelligence"),
            "region_verification": report.get("region_verification"),
            "block_catalog": report.get("block_catalog"),
            "corner_points": report.get("corner_points"),
            "observation_samples": report.get("observation_samples"),
            "trace_points": report.get("trace_points"),
            "notes": report.get("notes"),
        }

    def _update_memory_from_report(self, report: dict[str, object]) -> None:
        if not self._is_report_reusable(report):
            return

        snapshot = load_perimeter_scout_memory()
        regions = snapshot.setdefault("regions", {})
        if not isinstance(regions, dict):
            snapshot["regions"] = {}
            regions = snapshot["regions"]
        region_key = _region_memory_key(self.region)
        previous = regions.get(region_key)
        entry = previous.copy() if isinstance(previous, dict) else {}
        previous_runs = int(entry.get("successful_runs", 0) or 0)

        previous_catalog = (
            entry.get("block_catalog").copy()
            if isinstance(entry.get("block_catalog"), dict)
            else {}
        )
        fresh_catalog = self._build_block_catalog(self._observation_samples if self._observation_samples else self._trace_points)
        merged_catalog: dict[str, dict[str, object]] = {}
        for block_id in sorted(set(previous_catalog) | set(fresh_catalog)):
            existing_entry = previous_catalog.get(block_id)
            merged_entry = existing_entry.copy() if isinstance(existing_entry, dict) else {}
            fresh_entry = fresh_catalog.get(block_id)
            if isinstance(fresh_entry, dict):
                for metric_name in (
                    "sample_count",
                    "mineable_hits",
                    "non_mineable_hits",
                    "air_hits",
                    "interior_hits",
                    "exterior_hits",
                ):
                    merged_entry[metric_name] = int(merged_entry.get(metric_name, 0) or 0) + int(
                        fresh_entry.get(metric_name, 0) or 0
                    )
            if int(merged_entry.get("mineable_hits", 0) or 0) > 0:
                merged_entry["last_classification"] = "mineable"
            elif int(merged_entry.get("non_mineable_hits", 0) or 0) > 0:
                merged_entry["last_classification"] = "non_mineable"
            elif int(merged_entry.get("air_hits", 0) or 0) > 0:
                merged_entry["last_classification"] = "air"
            else:
                merged_entry["last_classification"] = "unknown"
            merged_catalog[block_id] = merged_entry

        perimeter_bounds = report.get("observed_perimeter_bounds")
        ground_surface_y = None
        if isinstance(perimeter_bounds, dict):
            y_min = perimeter_bounds.get("y_min")
            y_max = perimeter_bounds.get("y_max")
            if isinstance(y_min, (int, float)) and isinstance(y_max, (int, float)) and abs(float(y_min) - float(y_max)) <= 0.01:
                ground_surface_y = round(float(y_min), 3)
        if ground_surface_y is None and self.region is not None:
            ground_surface_y = float(self.region.max_y)

        entry.update(
            {
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "successful_runs": previous_runs + 1,
                "reusable": True,
                "configured_region": report.get("configured_region"),
                "ground_surface_y": ground_surface_y,
                "look_block_intelligence": report.get("look_block_intelligence"),
                "region_verification": report.get("region_verification"),
                "recommended_region_bounds": report.get("recommended_region_bounds"),
                "observed_perimeter_bounds": report.get("observed_perimeter_bounds"),
                "block_catalog": merged_catalog,
                "report_template": self._build_memory_report_template(report),
            }
        )
        regions[region_key] = entry
        save_region_memory_snapshot(self._memory_path, snapshot)

    def _build_reused_report_from_memory(self) -> Optional[dict[str, object]]:
        entry = load_region_memory_entry(self._memory_path, self.region)
        if not isinstance(entry, dict) or not bool(entry.get("reusable")):
            return None
        template = entry.get("report_template")
        if not isinstance(template, dict):
            return None
        reusable_candidate = dict(template)
        reusable_candidate["status"] = "complete"
        if not self._is_report_reusable(reusable_candidate):
            return None
        report = dict(template)
        notes = report.get("notes")
        note_list = list(notes) if isinstance(notes, list) else []
        note_list.append(
            "This report was populated from previously verified scout memory, so the perimeter walk was skipped."
        )
        report.update(
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "status": "complete",
                "current_phase": ScoutPhase.COMPLETE.value,
                "current_action": MinerAction.SCOUT_COMPLETE.value,
                "current_target_axis": None,
                "spawn_pose": None,
                "memory_reused": True,
                "memory_updated_at_utc": entry.get("updated_at_utc"),
                "notes": note_list,
            }
        )
        return report

    def _top_counter_rows(self, counts: Counter[str], *, limit: int = 8) -> list[dict[str, object]]:
        return [
            {"look_block": block_id, "count": int(count)}
            for block_id, count in counts.most_common(limit)
        ]

    def _bounds_from_points(self, points: Sequence[dict[str, object]]) -> Optional[dict[str, int]]:
        valid_points = [
            point
            for point in points
            if point.get("block_x") is not None
            and point.get("block_y") is not None
            and point.get("block_z") is not None
        ]
        if not valid_points:
            return None
        x_values = [int(point["block_x"]) for point in valid_points]
        y_values = [int(point["block_y"]) for point in valid_points]
        z_values = [int(point["block_z"]) for point in valid_points]
        return {
            "x_min": min(x_values),
            "x_max": max(x_values),
            "y_min": min(y_values),
            "y_max": max(y_values),
            "z_min": min(z_values),
            "z_max": max(z_values),
        }

    def _bounds_delta_from_region(self, bounds: Optional[dict[str, int]]) -> Optional[dict[str, int]]:
        if self.region is None or not isinstance(bounds, dict):
            return None
        return {
            "x_min_delta": int(bounds["x_min"]) - self.region.min_x,
            "x_max_delta": int(bounds["x_max"]) - self.region.max_x,
            "y_min_delta": int(bounds["y_min"]) - self.region.min_y,
            "y_max_delta": int(bounds["y_max"]) - self.region.max_y,
            "z_min_delta": int(bounds["z_min"]) - self.region.min_z,
            "z_max_delta": int(bounds["z_max"]) - self.region.max_z,
        }

    def _build_region_verification(self, points: Sequence[dict[str, object]]) -> dict[str, object]:
        verification: dict[str, object] = {
            "sample_count": len(points),
        }
        mineable_points = [point for point in points if str(point.get("look_block_class")) == "mineable"]
        non_mineable_points = [point for point in points if str(point.get("look_block_class")) == "non_mineable"]
        air_points = [point for point in points if str(point.get("look_block_class")) == "air"]

        verification["observed_mineable_bounds"] = self._bounds_from_points(mineable_points)
        verification["observed_non_mineable_bounds"] = self._bounds_from_points(non_mineable_points)
        verification["mineable_bounds_delta_vs_configured"] = self._bounds_delta_from_region(
            verification["observed_mineable_bounds"]
        )
        verification["non_mineable_bounds_delta_vs_configured"] = self._bounds_delta_from_region(
            verification["observed_non_mineable_bounds"]
        )
        verification["mineable_count"] = len(mineable_points)
        verification["non_mineable_count"] = len(non_mineable_points)
        verification["air_count"] = len(air_points)

        state_counts: Counter[str] = Counter(
            str(point.get("observation_state") or "unclassified")
            for point in points
        )
        verification["state_counts"] = {
            name: int(count)
            for name, count in sorted(state_counts.items())
        }

        if self.region is None:
            verification["configured_region_match"] = None
            return verification

        interior_mineable = 0
        exterior_mineable = 0
        exterior_non_mineable = 0
        interior_non_mineable = 0
        for point in mineable_points:
            bx = point.get("block_x")
            by = point.get("block_y")
            bz = point.get("block_z")
            if bx is None or by is None or bz is None:
                continue
            if is_point_in_region(int(bx), int(by), int(bz), self.region):
                interior_mineable += 1
            else:
                exterior_mineable += 1
        for point in non_mineable_points:
            bx = point.get("block_x")
            by = point.get("block_y")
            bz = point.get("block_z")
            if bx is None or by is None or bz is None:
                continue
            if is_point_in_region(int(bx), int(by), int(bz), self.region):
                interior_non_mineable += 1
            else:
                exterior_non_mineable += 1

        verification["configured_region_match"] = {
            "interior_mineable_hits": interior_mineable,
            "exterior_mineable_hits": exterior_mineable,
            "interior_non_mineable_hits": interior_non_mineable,
            "exterior_non_mineable_hits": exterior_non_mineable,
            "interior_air_hits": int(state_counts.get("interior_air", 0)),
            "matches_expectation": bool(interior_mineable > 0 and exterior_non_mineable > 0 and exterior_mineable == 0),
        }
        return verification

    def _summarize_look_block_intelligence(self, points: Sequence[dict[str, object]]) -> dict[str, object]:
        class_order = ("mineable", "non_mineable", "air", "unknown")
        state_counts: Counter[str] = Counter()
        class_counts: Counter[str] = Counter()
        overall_counts: Counter[str] = Counter()
        counts_by_class: dict[str, Counter[str]] = {name: Counter() for name in class_order}
        phase_buckets: dict[str, list[dict[str, object]]] = {}

        for point in points:
            block_id = str(point.get("look_block") or "minecraft:air").strip().lower()
            block_class = str(point.get("look_block_class") or classify_observed_look_block(block_id, self.region))
            observation_state = str(point.get("observation_state") or "unclassified")
            phase = str(point.get("phase") or ScoutPhase.APPROACH_FACE.value)
            class_counts[block_class] += 1
            state_counts[observation_state] += 1
            overall_counts[block_id] += 1
            counts_by_class.setdefault(block_class, Counter())[block_id] += 1
            phase_buckets.setdefault(phase, []).append(
                {
                    "look_block": block_id,
                    "look_block_class": block_class,
                    "observation_state": observation_state,
                }
            )

        phase_summaries: dict[str, dict[str, object]] = {}
        for phase_name, phase_points in phase_buckets.items():
            phase_class_counts: Counter[str] = Counter()
            phase_state_counts: Counter[str] = Counter()
            phase_block_counts: Counter[str] = Counter()
            for point in phase_points:
                phase_class_counts[str(point["look_block_class"])] += 1
                phase_state_counts[str(point["observation_state"])] += 1
                phase_block_counts[str(point["look_block"])] += 1
            phase_summaries[phase_name] = {
                "sample_count": len(phase_points),
                "class_counts": {
                    name: int(phase_class_counts.get(name, 0))
                    for name in class_order
                },
                "state_counts": {
                    name: int(count)
                    for name, count in sorted(phase_state_counts.items())
                },
                "top_blocks": self._top_counter_rows(phase_block_counts, limit=5),
            }

        return {
            "sample_count": len(points),
            "class_counts": {
                name: int(class_counts.get(name, 0))
                for name in class_order
            },
            "state_counts": {
                name: int(count)
                for name, count in sorted(state_counts.items())
            },
            "top_blocks": self._top_counter_rows(overall_counts),
            "top_mineable_blocks": self._top_counter_rows(counts_by_class["mineable"]),
            "top_non_mineable_blocks": self._top_counter_rows(counts_by_class["non_mineable"]),
            "top_air_blocks": self._top_counter_rows(counts_by_class["air"]),
            "phase_summaries": phase_summaries,
            "learned_blocking_block_ids": []
            if self.region is None
            else sorted(self.region.learned_blocking_block_ids),
        }

    def _build_report(self) -> dict[str, object]:
        trace = self._trace_points
        observation_points = self._observation_samples if self._observation_samples else trace
        x_values = [float(point["x"]) for point in trace] if trace else []
        y_values = [float(point["y"]) for point in trace] if trace else []
        z_values = [float(point["z"]) for point in trace] if trace else []
        perimeter_trace = [
            point for point in trace if str(point.get("phase")) != ScoutPhase.APPROACH_FACE.value
        ]
        perimeter_x_values = [float(point["x"]) for point in perimeter_trace] if perimeter_trace else []
        perimeter_y_values = [float(point["y"]) for point in perimeter_trace] if perimeter_trace else []
        perimeter_z_values = [float(point["z"]) for point in perimeter_trace] if perimeter_trace else []
        corner_points = self._corner_points
        corner_x_values = [float(point["x"]) for point in corner_points] if corner_points else []
        corner_y_values = [float(point["y"]) for point in corner_points] if corner_points else []
        corner_z_values = [float(point["z"]) for point in corner_points] if corner_points else []
        motion = self.world_model.get_motion_estimate()
        observed_bounds = None
        candidate_bbox = None
        observed_perimeter_bounds = None
        candidate_bbox_from_perimeter = None
        recommended_region_bounds = None
        if trace:
            observed_bounds = {
                "x_min": round(min(x_values), 3),
                "x_max": round(max(x_values), 3),
                "y_min": round(min(y_values), 3),
                "y_max": round(max(y_values), 3),
                "z_min": round(min(z_values), 3),
                "z_max": round(max(z_values), 3),
            }
            candidate_bbox = {
                "x_min": math.floor(min(x_values)),
                "x_max": math.ceil(max(x_values)),
                "y_min": math.floor(min(y_values)),
                "y_max": math.ceil(max(y_values)),
                "z_min": math.floor(min(z_values)),
                "z_max": math.ceil(max(z_values)),
            }
        if perimeter_trace:
            observed_perimeter_bounds = {
                "x_min": round(min(perimeter_x_values), 3),
                "x_max": round(max(perimeter_x_values), 3),
                "y_min": round(min(perimeter_y_values), 3),
                "y_max": round(max(perimeter_y_values), 3),
                "z_min": round(min(perimeter_z_values), 3),
                "z_max": round(max(perimeter_z_values), 3),
            }
            candidate_bbox_from_perimeter = {
                "x_min": math.floor(min(perimeter_x_values)),
                "x_max": math.ceil(max(perimeter_x_values)),
                "y_min": math.floor(min(perimeter_y_values)),
                "y_max": math.ceil(max(perimeter_y_values)),
                "z_min": math.floor(min(perimeter_z_values)),
                "z_max": math.ceil(max(perimeter_z_values)),
            }
        if corner_points:
            recommended_region_bounds = {
                "x_min": math.floor(min(corner_x_values)),
                "x_max": math.ceil(max(corner_x_values)),
                "y_min": math.floor(min(corner_y_values)),
                "y_max": math.ceil(max(corner_y_values)),
                "z_min": math.floor(min(corner_z_values)),
                "z_max": math.ceil(max(corner_z_values)),
            }
        look_block_intelligence = self._summarize_look_block_intelligence(observation_points)
        region_verification = self._build_region_verification(observation_points)
        block_catalog = self._build_block_catalog(observation_points)
        return {
            "version": "0.1.0",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "mode": "perimeter_scout",
            "status": self._status,
            "current_phase": self.current_phase.value,
            "current_action": self.current_action.value,
            "current_target_axis": None
            if self._leg_index >= len(self._legs)
            else self._target_axis_for_leg(self._legs[self._leg_index]),
            "spawn_pose": None
            if self._spawn_pose is None
            else {
                "x": round(self._spawn_pose.x, 3),
                "y": round(self._spawn_pose.y, 3),
                "z": round(self._spawn_pose.z, 3),
                "yaw": round(normalize_yaw(self._spawn_pose.yaw), 3),
                "pitch": round(self._spawn_pose.pitch, 3),
                "dimension": self._spawn_pose.dimension,
                "ts_utc": self._spawn_pose.ts_utc,
            },
            "configured_region": None
            if self.region is None
            else {
                "name": self.region.name,
                "dimension": self.region.dimension,
                "min": {"x": self.region.min_x, "y": self.region.min_y, "z": self.region.min_z},
                "max": {"x": self.region.max_x, "y": self.region.max_y, "z": self.region.max_z},
            },
            "sample_count": len(trace),
            "observation_sample_count": len(self._observation_samples),
            "observed_walkable_bounds": observed_bounds,
            "candidate_bbox_from_trace": candidate_bbox,
            "observed_perimeter_bounds": observed_perimeter_bounds,
            "candidate_bbox_from_perimeter": candidate_bbox_from_perimeter,
            "recommended_region_bounds": recommended_region_bounds,
            "memory_reused": self._memory_reused,
            "look_block_intelligence": look_block_intelligence,
            "region_verification": region_verification,
            "block_catalog": block_catalog,
            "latest_motion": None
            if motion is None
            else {
                "dt_sec": round(motion.dt_sec, 4),
                "vel_x": round(motion.vel_x, 3),
                "vel_y": round(motion.vel_y, 3),
                "vel_z": round(motion.vel_z, 3),
                "speed_bps": round(motion.speed_bps, 3),
                "horizontal_speed_bps": round(motion.horizontal_speed_bps, 3),
                "ts_utc": motion.ts_utc,
            },
            "corner_points": self._corner_points,
            "observation_samples": self._observation_samples,
            "trace_points": trace,
            "notes": [
                "This report records the walkable perimeter trace, not a guaranteed mineable interior volume.",
                "look_block_intelligence is now summarized from deliberate scout observation samples that look inward and outward from the perimeter, not only from straight-ahead walk tracing.",
                "region_verification compares observed mineable, air, and non-mineable block coordinates against the configured mine cube so scout can verify or challenge the saved bounds.",
                "candidate_bbox_from_trace includes the approach path and can be wider than the real perimeter.",
                "candidate_bbox_from_perimeter removes the pre-perimeter approach path.",
                "recommended_region_bounds is fitted from the saved scout corner points and is the best default update target.",
            ],
        }

def run_miner(cfg: AppConfig, *, start_mode: str = "idle") -> None:
    logger = logging.getLogger("minecraft_auto_miner")
    region_config = load_region_config()
    exclusive_start_mode = start_mode if start_mode in {"mine", "scout", "calibrate"} else None

    telemetry_stop = threading.Event()
    telemetry_thread = threading.Thread(
        target=run_telemetry_pipeline_loop,
        args=(telemetry_stop, cfg.telemetry_interval_sec),
        daemon=True,
    )
    telemetry_thread.start()

    launch_dashboard_if_enabled(logger)

    forge_log_path = resolve_default_forge_log_path()
    logger.info("Using Forge F3 log path: %s", forge_log_path)
    report_forge_telemetry_preflight(forge_log_path, logger)

    pose_provider = ForgePoseProvider(log_path=forge_log_path)
    pose_provider.start()

    world_model = WorldModelForge(pose_provider)
    inputs = SimpleInputController(logger)
    control_stop = threading.Event()
    recovery_policy = RecoveryBandit(cfg.recovery_epsilon, logger)
    blocking_learner = BlockingBlockLearner(logger, cfg.blocking_learn_threshold)
    strategy_planner = MiningStrategyPlanner(logger, cfg.reacquire_failures_before_shift)
    voxel_memory = VoxelWorldMemory(logger, region_config)
    manual_recorder = ManualTrainingRecorder(logger, world_model, cfg.manual_record_interval_sec)
    controller_cls = StrictLaneController if cfg.strict_east_lane_mode else AutonomousController
    controller = controller_cls(
        cfg=cfg,
        logger=logger,
        world_model=world_model,
        inputs=inputs,
        region=region_config,
        recovery_policy=recovery_policy,
        blocking_learner=blocking_learner,
        strategy_planner=strategy_planner,
        voxel_memory=voxel_memory,
        stop_event=control_stop,
    )
    control_calibration_profile = load_control_calibration_profile(logger, region=region_config)
    controller.apply_control_calibration_profile(control_calibration_profile)
    perimeter_scout = PerimeterScout(
        cfg=cfg,
        logger=logger,
        world_model=world_model,
        inputs=inputs,
        region=region_config,
        blocking_learner=blocking_learner,
        voxel_memory=voxel_memory,
        stop_event=control_stop,
    )
    control_calibration = ControlCalibrationRunner(
        cfg=cfg,
        logger=logger,
        world_model=world_model,
        inputs=inputs,
        region=region_config,
        strict_controller=controller if isinstance(controller, StrictLaneController) else None,
        stop_event=control_stop,
    )
    state = MinerRuntimeState(
        pause_key_state={key: False for key in cfg.inventory_pause_keys},
        control_key_state={
            cfg.hotkeys.start_stop: False,
            cfg.hotkeys.perimeter_map: False,
            cfg.hotkeys.panic_stop: False,
            cfg.hotkeys.manual_record: False,
        },
    )
    control_hotkey_last_trigger: Dict[str, float] = {
        cfg.hotkeys.start_stop: 0.0,
        cfg.hotkeys.perimeter_map: 0.0,
        cfg.hotkeys.panic_stop: 0.0,
        cfg.hotkeys.manual_record: 0.0,
    }
    dashboard_command_path = dashboard_control_command_path()
    dashboard_status_path = dashboard_control_status_path()
    dashboard_runtime_path = dashboard_runtime_status_path()
    last_dashboard_runtime_write_mono = 0.0
    last_dashboard_command_id: Optional[str] = None

    def log_action_transition(action: MinerAction, obstacle: Optional[ObstacleInfo], source: str) -> None:
        if action == state.last_action:
            return
        state.last_action = action
        try:
            fsm_event_log.log_fsm_event(
                state_name=state.state_name.value,
                action_name=action.value,
                source=source,
                extra={
                    "obstacle_block_id": obstacle.block_id if obstacle else None,
                    "obstacle_block_x": obstacle.block_x if obstacle else None,
                    "obstacle_block_y": obstacle.block_y if obstacle else None,
                    "obstacle_block_z": obstacle.block_z if obstacle else None,
                },
            )
        except Exception as exc:  # pragma: no cover
            logger.error("Failed to log FSM event: %s", exc, exc_info=True)

    def write_json_atomic(path: Path, payload: dict[str, object]) -> bool:
        path.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(payload, indent=2)
        last_exc: Optional[Exception] = None
        for attempt in range(8):
            temp_path = Path(f"{path}.{os.getpid()}.{attempt}.tmp")
            try:
                temp_path.write_text(text, encoding="utf-8")
                temp_path.replace(path)
                return True
            except PermissionError as exc:
                last_exc = exc
                try:
                    path.write_text(text, encoding="utf-8")
                    try:
                        temp_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    return True
                except PermissionError as inner_exc:
                    last_exc = inner_exc
            except Exception as exc:
                last_exc = exc
            finally:
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception:
                    pass
            time.sleep(0.03 * (attempt + 1))
        logger.warning("Failed to write dashboard JSON file %s: %s", path, last_exc)
        return False

    def current_pose_payload() -> Optional[dict[str, object]]:
        pose = world_model.get_player_pose()
        if pose is None:
            return None
        return {
            "ts_utc": pose.ts_utc,
            "x": round(pose.x, 3),
            "y": round(pose.y, 3),
            "z": round(pose.z, 3),
            "yaw": round(normalize_yaw(pose.yaw), 3),
            "pitch": round(pose.pitch, 3),
            "dimension": pose.dimension,
        }

    def write_dashboard_status(
        *,
        result: str,
        message: str,
        command: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> None:
        payload = {
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "result": result,
            "message": message,
            "command": command,
            "command_id": command_id,
            "state_name": state.state_name.value,
            "mining_enabled": state.mining_enabled,
            "mapping_enabled": state.mapping_enabled,
            "calibration_enabled": state.calibration_enabled,
            "manual_recording_enabled": state.manual_recording_enabled,
            "user_paused": state.user_paused,
            "last_action": state.last_action.value,
            "pose": current_pose_payload(),
        }
        write_json_atomic(dashboard_status_path, payload)

    def maybe_write_dashboard_runtime_status(*, force: bool = False, note: Optional[str] = None) -> None:
        nonlocal last_dashboard_runtime_write_mono
        now = time.monotonic()
        if not force and (now - last_dashboard_runtime_write_mono) < 0.5:
            return
        last_dashboard_runtime_write_mono = now
        payload: dict[str, object] = {
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "start_mode": start_mode,
            "state_name": state.state_name.value,
            "mining_enabled": state.mining_enabled,
            "mapping_enabled": state.mapping_enabled,
            "calibration_enabled": state.calibration_enabled,
            "manual_recording_enabled": state.manual_recording_enabled,
            "user_paused": state.user_paused,
            "should_exit": state.should_exit,
            "last_action": state.last_action.value,
            "pose": current_pose_payload(),
        }
        if note:
            payload["note"] = note
        write_json_atomic(dashboard_runtime_path, payload)

    def stop_current_activity(reason: str, *, exit_app: bool = False) -> None:
        control_stop.set()
        if state.mapping_enabled and not perimeter_scout.is_complete():
            perimeter_scout.snapshot(status="interrupted")
        if (
            state.calibration_enabled
            and not control_calibration.is_complete()
            and control_calibration.has_reportable_progress()
        ):
            control_calibration.snapshot(status="interrupted")
        if state.manual_recording_enabled:
            manual_recorder.stop(reason)
            state.manual_recording_enabled = False
        state.mining_enabled = False
        state.mapping_enabled = False
        state.calibration_enabled = False
        state.user_paused = False
        state.state_name = MinerState.IDLE
        if exit_app:
            state.should_exit = True
        controller.flush_runtime_learning(force=True)
        voxel_memory.maybe_flush(force=True)
        inputs.all_stop()
        log_action_transition(MinerAction.STOP_ALL, None, reason)
        maybe_write_dashboard_runtime_status(force=True, note=reason)

    def start_autonomous_mining(source: str) -> bool:
        if exclusive_start_mode == "calibrate" and state.calibration_enabled:
            logger.info(
                "Mining start ignored while --mode calibrate is active; use %s to panic and exit.",
                cfg.hotkeys.panic_stop,
            )
            return False
        if exclusive_start_mode == "scout" and state.mapping_enabled:
            logger.info(
                "Mining start ignored while --mode scout is active; use %s to stop scout or %s to panic.",
                cfg.hotkeys.perimeter_map,
                cfg.hotkeys.panic_stop,
            )
            return False
        if state.mining_enabled and not state.user_paused:
            return False
        if state.mapping_enabled or state.calibration_enabled or state.manual_recording_enabled:
            stop_current_activity(f"{source}_handoff")
        control_stop.clear()
        state.mining_enabled = True
        state.mapping_enabled = False
        state.calibration_enabled = False
        state.manual_recording_enabled = False
        state.user_paused = False
        state.state_name = MinerState.AUTONOMOUS
        state.pause_hotkey_suppress_until = time.monotonic() + 2.2
        controller.begin_run()
        logger.info("Autonomous mining enabled; using the current position and waiting for Forge telemetry.")
        log_action_transition(MinerAction.WAIT_FOR_TELEMETRY, None, source)
        maybe_write_dashboard_runtime_status(force=True, note=source)
        return True

    def start_perimeter_scout(source: str) -> bool:
        if exclusive_start_mode == "calibrate" and state.calibration_enabled:
            logger.info(
                "Perimeter scout start ignored while --mode calibrate is active; use %s to panic and exit.",
                cfg.hotkeys.panic_stop,
            )
            return False
        if state.mapping_enabled and not state.user_paused:
            return False
        if state.mining_enabled or state.calibration_enabled or state.manual_recording_enabled:
            stop_current_activity(f"{source}_handoff")
        control_stop.clear()
        state.mining_enabled = False
        state.mapping_enabled = True
        state.calibration_enabled = False
        state.manual_recording_enabled = False
        state.user_paused = False
        state.state_name = MinerState.SCOUT
        state.pause_hotkey_suppress_until = time.monotonic() + 2.2
        perimeter_scout.begin_run()
        if perimeter_scout.memory_was_reused():
            logger.info("Perimeter scout enabled; using previously verified scout memory for the current region.")
        else:
            logger.info("Perimeter scout enabled; tracing the walkable mine boundary from the current position.")
        log_action_transition(MinerAction.WAIT_FOR_TELEMETRY, None, source)
        maybe_write_dashboard_runtime_status(force=True, note=source)
        return True

    def start_control_calibration(source: str) -> bool:
        if state.calibration_enabled and not state.user_paused:
            return False
        if state.mining_enabled or state.mapping_enabled or state.manual_recording_enabled:
            stop_current_activity(f"{source}_handoff")
        control_stop.clear()
        state.mining_enabled = False
        state.mapping_enabled = False
        state.calibration_enabled = True
        state.manual_recording_enabled = False
        state.user_paused = False
        state.state_name = MinerState.CALIBRATION
        state.pause_hotkey_suppress_until = time.monotonic() + 2.2
        control_calibration.begin_run()
        log_action_transition(MinerAction.WAIT_FOR_TELEMETRY, None, source)
        maybe_write_dashboard_runtime_status(force=True, note=source)
        return True

    def start_manual_recording(source: str) -> Optional[Path]:
        if exclusive_start_mode == "calibrate" and state.calibration_enabled:
            logger.info(
                "Manual recorder start ignored while --mode calibrate is active; use %s to panic and exit.",
                cfg.hotkeys.panic_stop,
            )
            return None
        if state.manual_recording_enabled:
            return None
        if state.mining_enabled or state.mapping_enabled or state.calibration_enabled:
            stop_current_activity(f"{source}_handoff")
        control_stop.set()
        state.mining_enabled = False
        state.mapping_enabled = False
        state.calibration_enabled = False
        state.user_paused = False
        state.manual_recording_enabled = True
        state.state_name = MinerState.MANUAL_RECORDING
        session_path = manual_recorder.start()
        logger.info(
            "Manual training recorder enabled; use your own controls and press %s again to stop. Session: %s",
            cfg.hotkeys.manual_record,
            session_path,
        )
        log_action_transition(MinerAction.MANUAL_RECORDING, None, source)
        maybe_write_dashboard_runtime_status(force=True, note=source)
        return session_path

    def process_dashboard_control_command() -> None:
        nonlocal last_dashboard_command_id
        if not dashboard_command_path.exists():
            return
        try:
            raw = json.loads(dashboard_command_path.read_text(encoding="utf-8"))
        except Exception as exc:
            write_dashboard_status(result="error", message=f"Failed to read dashboard command: {exc}")
            try:
                dashboard_command_path.unlink()
            except Exception:
                pass
            return
        if not isinstance(raw, dict):
            write_dashboard_status(result="error", message="Ignoring malformed dashboard command payload.")
            try:
                dashboard_command_path.unlink()
            except Exception:
                pass
            return

        command_id = str(raw.get("command_id") or "").strip()
        command = str(raw.get("command") or "").strip().lower()
        if not command_id:
            write_dashboard_status(result="error", message="Dashboard command is missing command_id.")
            try:
                dashboard_command_path.unlink()
            except Exception:
                pass
            return
        if command_id == last_dashboard_command_id:
            return

        execute_after_raw = raw.get("execute_after_utc")
        execute_after: Optional[datetime] = None
        if isinstance(execute_after_raw, str) and execute_after_raw:
            try:
                execute_after = datetime.fromisoformat(execute_after_raw.replace("Z", "+00:00"))
            except ValueError:
                execute_after = None
        if execute_after is not None and execute_after.tzinfo is None:
            execute_after = execute_after.replace(tzinfo=timezone.utc)
        if execute_after is not None and datetime.now(timezone.utc) < execute_after:
            return

        last_dashboard_command_id = command_id
        try:
            dashboard_command_path.unlink()
        except Exception:
            pass

        if command == "start_mine":
            started = start_autonomous_mining("dashboard_start_mine")
            write_dashboard_status(
                result="accepted" if started else "noop",
                message="Mine command accepted." if started else "Mine command was already active or blocked.",
                command=command,
                command_id=command_id,
            )
        elif command == "start_scout":
            started = start_perimeter_scout("dashboard_start_scout")
            write_dashboard_status(
                result="accepted" if started else "noop",
                message="Scout command accepted." if started else "Scout command was already active or blocked.",
                command=command,
                command_id=command_id,
            )
        elif command == "start_calibrate":
            started = start_control_calibration("dashboard_start_calibrate")
            write_dashboard_status(
                result="accepted" if started else "noop",
                message="Calibration command accepted." if started else "Calibration command was already active or blocked.",
                command=command,
                command_id=command_id,
            )
        elif command == "start_manual_record":
            session_path = start_manual_recording("dashboard_start_manual_record")
            write_dashboard_status(
                result="accepted" if session_path is not None else "noop",
                message=(
                    f"Manual record command accepted. Session: {session_path}"
                    if session_path is not None
                    else "Manual record command was already active or blocked."
                ),
                command=command,
                command_id=command_id,
            )
        elif command == "stop":
            stop_current_activity("dashboard_stop")
            write_dashboard_status(
                result="accepted",
                message="Stop command accepted; miner returned to idle.",
                command=command,
                command_id=command_id,
            )
        else:
            write_dashboard_status(
                result="error",
                message=f"Unsupported dashboard command: {command or 'unknown'}",
                command=command,
                command_id=command_id,
            )

    def on_toggle_mining() -> None:
        if state.mining_enabled and state.user_paused:
            control_stop.clear()
            state.user_paused = False
            state.state_name = MinerState.AUTONOMOUS
            controller.last_control_mono = 0.0
            maybe_write_dashboard_runtime_status(force=True, note="toggle_resume_mine")
            logger.info("Resuming autonomous miner after user pause.")
            return

        if state.mining_enabled:
            logger.info("Autonomous mining disabled.")
            stop_current_activity("toggle_off")
            return

        start_autonomous_mining("toggle_on")

    def on_toggle_mapping() -> None:
        if exclusive_start_mode == "calibrate" and state.calibration_enabled:
            logger.info(
                "Perimeter scout toggle ignored while --mode calibrate is active; use %s to panic and exit.",
                cfg.hotkeys.panic_stop,
            )
            return

        if state.mapping_enabled and state.user_paused:
            control_stop.clear()
            state.user_paused = False
            state.state_name = MinerState.SCOUT
            maybe_write_dashboard_runtime_status(force=True, note="toggle_resume_scout")
            logger.info("Resuming perimeter scout after user pause.")
            return

        if state.mapping_enabled:
            logger.info("Perimeter scout disabled.")
            stop_current_activity("toggle_map_off")
            return

        start_perimeter_scout("toggle_map_on")

    def on_toggle_manual_recording() -> None:
        if exclusive_start_mode == "calibrate" and state.calibration_enabled:
            logger.info(
                "Manual recorder toggle ignored while --mode calibrate is active; use %s to panic and exit.",
                cfg.hotkeys.panic_stop,
            )
            return
        if state.manual_recording_enabled:
            manual_recorder.stop("manual_toggle_off")
            state.manual_recording_enabled = False
            state.user_paused = False
            state.state_name = MinerState.IDLE
            logger.info("Manual training recorder disabled.")
            log_action_transition(MinerAction.STOP_ALL, None, "manual_record_off")
            maybe_write_dashboard_runtime_status(force=True, note="manual_record_off")
            return
        start_manual_recording("manual_record_on")

    def on_panic_stop() -> None:
        logger.warning("PANIC STOP triggered; stopping miner and exiting.")
        stop_current_activity("panic_stop", exit_app=True)

    def on_user_pause(key_name: str) -> None:
        if (
            not state.mining_enabled
            and not state.mapping_enabled
            and not state.calibration_enabled
        ) or state.user_paused:
            return
        logger.info("User pause detected via key '%s'; miner inputs released.", key_name)
        control_stop.set()
        state.user_paused = True
        state.state_name = MinerState.PAUSED
        inputs.all_stop()
        log_action_transition(MinerAction.USER_PAUSED, None, f"user_pause:{key_name}")
        maybe_write_dashboard_runtime_status(force=True, note=f"user_pause:{key_name}")

    def poll_user_pause_keys() -> None:
        suppress = time.monotonic() < state.pause_hotkey_suppress_until
        for key_name in cfg.inventory_pause_keys:
            try:
                is_down = keyboard.is_pressed(key_name)
            except Exception:
                is_down = False
            was_down = state.pause_key_state.get(key_name, False)
            if is_down and not was_down and not suppress:
                on_user_pause(key_name)
            state.pause_key_state[key_name] = is_down

    def trigger_control_hotkey(key_name: str, callback) -> None:
        now = time.monotonic()
        if now - control_hotkey_last_trigger.get(key_name, 0.0) < 0.35:
            return
        control_hotkey_last_trigger[key_name] = now
        callback()

    def poll_control_hotkeys() -> None:
        hotkey_actions = {
            cfg.hotkeys.start_stop: on_toggle_mining,
            cfg.hotkeys.perimeter_map: on_toggle_mapping,
            cfg.hotkeys.panic_stop: on_panic_stop,
            cfg.hotkeys.manual_record: on_toggle_manual_recording,
        }
        for key_name, callback in hotkey_actions.items():
            try:
                is_down = keyboard.is_pressed(key_name)
            except Exception:
                is_down = False
            was_down = state.control_key_state.get(key_name, False)
            if is_down and not was_down:
                trigger_control_hotkey(key_name, callback)
            state.control_key_state[key_name] = is_down

    hotkey_handles: list[int] = []
    try:
        hotkey_handles.append(
            keyboard.add_hotkey(
                cfg.hotkeys.start_stop,
                lambda: trigger_control_hotkey(cfg.hotkeys.start_stop, on_toggle_mining),
            )
        )
        hotkey_handles.append(
            keyboard.add_hotkey(
                cfg.hotkeys.perimeter_map,
                lambda: trigger_control_hotkey(cfg.hotkeys.perimeter_map, on_toggle_mapping),
            )
        )
        hotkey_handles.append(
            keyboard.add_hotkey(
                cfg.hotkeys.panic_stop,
                lambda: trigger_control_hotkey(cfg.hotkeys.panic_stop, on_panic_stop),
            )
        )
        hotkey_handles.append(
            keyboard.add_hotkey(
                cfg.hotkeys.manual_record,
                lambda: trigger_control_hotkey(cfg.hotkeys.manual_record, on_toggle_manual_recording),
            )
        )
    except Exception as exc:
        logger.warning("Failed to register global hotkey callbacks: %s", exc)

    def _signal_handler(signum, frame):  # type: ignore[override]
        logger.warning("Signal %s received; shutting down.", signum)
        on_panic_stop()

    signal.signal(signal.SIGINT, _signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal_handler)

    logger.info(
        "Hotkeys: start/stop=%s, perimeter-map=%s, manual-record=%s, panic=%s, user-pause=%s",
        cfg.hotkeys.start_stop,
        cfg.hotkeys.perimeter_map,
        cfg.hotkeys.manual_record,
        cfg.hotkeys.panic_stop,
        ", ".join(cfg.inventory_pause_keys),
    )
    logger.info(
        "Miner main loop starting (v0.7.71 - deadlock-safe FSM logging, soak-protected mining loops, exploit-vs-explore flight scoring, and Forge log rotation tooling)."
    )
    maybe_write_dashboard_runtime_status(force=True, note="startup")

    if start_mode == "mine":
        logger.info("Auto-start mode: autonomous mining.")
        start_autonomous_mining("auto_start_mine")
    elif start_mode == "scout":
        logger.info("Auto-start mode: perimeter scout.")
        start_perimeter_scout("auto_start_scout")
    elif start_mode == "calibrate":
        logger.info("Auto-start mode: control calibration.")
        start_control_calibration("auto_start_calibrate")

    try:
        while not state.should_exit:
            process_dashboard_control_command()
            poll_control_hotkeys()
            poll_user_pause_keys()

            if state.mining_enabled and not state.user_paused:
                action, obstacle = controller.tick()
                log_action_transition(action, obstacle, "miner_loop")
            elif state.mapping_enabled and not state.user_paused:
                action, obstacle = perimeter_scout.tick()
                log_action_transition(action, obstacle, "perimeter_scout")
                if perimeter_scout.is_complete():
                    state.mapping_enabled = False
                    state.state_name = MinerState.IDLE
            elif state.calibration_enabled and not state.user_paused:
                action, obstacle = control_calibration.tick()
                log_action_transition(action, obstacle, "control_calibration")
                if control_calibration.is_complete():
                    state.calibration_enabled = False
                    state.state_name = MinerState.IDLE
                    if exclusive_start_mode == "calibrate":
                        if control_calibration._status == "failed":
                            logger.warning("Control calibration failed validation; exiting.")
                        else:
                            logger.info("Control calibration completed; exiting.")
                        state.should_exit = True
            elif state.manual_recording_enabled:
                manual_recorder.sample()
                log_action_transition(MinerAction.MANUAL_RECORDING, None, "manual_record")
            else:
                inputs.all_stop()

            maybe_write_dashboard_runtime_status()
            time.sleep(cfg.tick_interval_sec)

    finally:
        logger.info("Shutting down miner loop.")
        inputs.all_stop()
        if not perimeter_scout.is_complete():
            perimeter_scout.snapshot(status="interrupted")
        if not control_calibration.is_complete() and control_calibration.has_reportable_progress():
            control_calibration.snapshot(status="interrupted")
        manual_recorder.stop("shutdown")
        recovery_policy.save()
        pose_provider.stop()
        for handle in hotkey_handles:
            try:
                keyboard.remove_hotkey(handle)
            except Exception:
                pass
        logger.info("Stopping telemetry pipeline thread...")
        telemetry_stop.set()
        telemetry_thread.join(timeout=5.0)
        maybe_write_dashboard_runtime_status(force=True, note="shutdown")
        logger.info("Telemetry pipeline thread stopped; exiting.")


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Minecraft Auto Miner")
    parser.add_argument(
        "--mode",
        choices=("idle", "mine", "scout", "calibrate"),
        default=os.getenv("MAM_START_MODE", "idle"),
        help="Start in idle, autonomous mining, perimeter scout, or timed control calibration mode.",
    )
    parser.add_argument(
        "--rotate-forge-log",
        action="store_true",
        help="Rotate the Forge telemetry log after Minecraft is closed, then exit.",
    )
    parser.add_argument(
        "--rotate-forge-log-keep-mb",
        type=float,
        default=0.0,
        help="When rotating the Forge telemetry log, keep the newest N MB in place as a fresh seed file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_cli_args()
    cfg = load_basic_config()
    if args.rotate_forge_log:
        configure_logging(cfg.log_level)
        logger = logging.getLogger("minecraft_auto_miner")
        rotate_forge_telemetry_log(
            resolve_default_forge_log_path(),
            logger,
            keep_tail_mb=max(0.0, float(args.rotate_forge_log_keep_mb or 0.0)),
        )
        return
    runtime_log_path = install_runtime_console_capture()
    configure_logging(cfg.log_level)
    logger = logging.getLogger("minecraft_auto_miner")
    logger.info(
        "Minecraft Auto Miner starting (v0.7.71 - deadlock-safe FSM logging, soak-protected mining loops, exploit-vs-explore flight scoring, and Forge log rotation tooling)"
    )
    logger.info("Latest runtime console log: %s", runtime_log_path)
    run_miner(cfg, start_mode=args.mode)


if __name__ == "__main__":
    main()
