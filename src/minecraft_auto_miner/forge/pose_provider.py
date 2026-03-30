"""
minecraft_auto_miner.forge.pose_provider v0.1.0 – 2025-12-08
Generated with ChatGPT (GPT-5.1 Thinking).

ForgePoseProvider
------------------
Reads real-time Forge F3 telemetry emitted by the Forge mod:
    <.minecraft>/mam_telemetry/mam_f3_stream.log

Provides:
    - get_pose()         → last known Pose
    - get_target_block() → last known block under crosshair

This is the *authoritative* telemetry source for the miner.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any, Dict

LOGGER = logging.getLogger(__name__)


def _normalize_yaw(yaw: float) -> float:
    value = yaw
    while value <= -180.0:
        value += 360.0
    while value > 180.0:
        value -= 360.0
    return value


# ---------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class Pose:
    x: float
    y: float
    z: float
    yaw: float
    pitch: float
    dimension: str
    is_sprinting: bool
    on_ground: bool
    ts_utc: str


@dataclass(frozen=True)
class TargetBlock:
    block_id: str
    x: Optional[int]
    y: Optional[int]
    z: Optional[int]
    ts_utc: str


@dataclass(frozen=True)
class MotionEstimate:
    dt_sec: float
    vel_x: float
    vel_y: float
    vel_z: float
    speed_bps: float
    horizontal_speed_bps: float
    ts_utc: str


# ---------------------------------------------------------------------
# Forge Pose Provider
# ---------------------------------------------------------------------

class ForgePoseProvider:
    """
    Tails mam_f3_stream.log emitted by the Forge telemetry mod.

    Typical usage:
        prov = ForgePoseProvider(Path("C:/Users/<you>/.minecraft/mam_telemetry/mam_f3_stream.log"))
        prov.start()

        while True:
            pose = prov.get_pose()
            target = prov.get_target_block()
    """

    def __init__(self, log_path: Path, poll_interval_sec: float = 0.05) -> None:
        self._log_path = log_path
        self._poll = poll_interval_sec

        self._lock = threading.Lock()
        self._latest_pose: Optional[Pose] = None
        self._latest_target: Optional[TargetBlock] = None
        self._latest_motion: Optional[MotionEstimate] = None
        self._last_update_mono: Optional[float] = None

        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    # ------------------------------------------------------------------
    # Start/Stop API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start background tail thread."""
        if self._thread and self._thread.is_alive():
            return

        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="ForgePoseProvider",
            daemon=True
        )
        self._thread.start()
        LOGGER.info("ForgePoseProvider started, tailing %s", self._log_path)

    def stop(self) -> None:
        """Stop the provider thread."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1.0)
        LOGGER.info("ForgePoseProvider stopped.")

    # ------------------------------------------------------------------
    # Public getters
    # ------------------------------------------------------------------

    def get_pose(self) -> Optional[Pose]:
        with self._lock:
            return self._latest_pose

    def get_target_block(self) -> Optional[TargetBlock]:
        with self._lock:
            return self._latest_target

    def get_motion_estimate(self) -> Optional[MotionEstimate]:
        with self._lock:
            return self._latest_motion

    def get_last_update_age_sec(self) -> Optional[float]:
        with self._lock:
            if self._last_update_mono is None:
                return None
            return time.monotonic() - self._last_update_mono

    # ------------------------------------------------------------------
    # Internal tailing thread
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Background loop."""
        try:
            self._tail_loop()
        except Exception:
            LOGGER.exception("ForgePoseProvider crashed.")

    def _tail_loop(self) -> None:
        """Tail the JSON log and update pose/target."""
        # Wait until file appears (Forge mod creates it after MC launches)
        while not self._log_path.exists() and not self._stop.is_set():
            LOGGER.debug("Waiting for Forge F3 log: %s", self._log_path)
            time.sleep(0.5)

        if self._stop.is_set():
            return

        self._bootstrap_from_recent_tail()

        with self._log_path.open("r", encoding="utf-8", errors="replace") as f:
            # Seek to end: only process new live data
            f.seek(0, 2)

            while not self._stop.is_set():
                line = f.readline()
                if not line:
                    time.sleep(self._poll)
                    continue
                line = line.strip()
                if not line:
                    continue

                self._handle_line(line)

    def _bootstrap_from_recent_tail(self, max_bytes: int = 131072) -> None:
        try:
            stat = self._log_path.stat()
            size = stat.st_size
            start = max(0, size - max_bytes)
            with self._log_path.open("rb") as raw:
                raw.seek(start)
                blob = raw.read()

            text = blob.decode("utf-8", errors="replace")
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            if not lines:
                LOGGER.warning("ForgePoseProvider bootstrap found no recent lines in %s", self._log_path)
                return

            for line in reversed(lines):
                parsed = self._parse_line(line)
                if parsed is None:
                    continue
                pose, target = parsed
                prev_pose = self.get_pose()
                motion = self._estimate_motion(prev_pose, pose)
                age_sec = self._source_age_sec(pose.ts_utc)
                last_update_mono = time.monotonic()
                if age_sec is not None:
                    last_update_mono -= max(0.0, age_sec)

                with self._lock:
                    self._latest_pose = pose
                    self._latest_target = target
                    self._latest_motion = motion
                    self._last_update_mono = last_update_mono

                file_mtime_utc = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
                if age_sec is not None and age_sec > 2.0:
                    LOGGER.warning(
                        "ForgePoseProvider bootstrap found stale latest tick: ts=%s age=%.2fs file_mtime_utc=%s pose=(%.3f, %.3f, %.3f)",
                        pose.ts_utc,
                        age_sec,
                        file_mtime_utc,
                        pose.x,
                        pose.y,
                        pose.z,
                    )
                else:
                    LOGGER.info(
                        "ForgePoseProvider bootstrap latest tick: ts=%s age=%.2fs file_mtime_utc=%s pose=(%.3f, %.3f, %.3f)",
                        pose.ts_utc,
                        0.0 if age_sec is None else age_sec,
                        file_mtime_utc,
                        pose.x,
                        pose.y,
                        pose.z,
                    )
                return

            LOGGER.warning(
                "ForgePoseProvider bootstrap found no valid JSON telemetry in the last %d bytes of %s",
                max_bytes,
                self._log_path,
            )
        except Exception:
            LOGGER.exception("ForgePoseProvider bootstrap failed.")

    # ------------------------------------------------------------------
    # JSON parsing
    # ------------------------------------------------------------------

    def _handle_line(self, line: str) -> None:
        parsed = self._parse_line(line)
        if parsed is None:
            return

        pose, target = parsed
        with self._lock:
            prev_pose = self._latest_pose
            motion = self._estimate_motion(prev_pose, pose)
            self._latest_pose = pose
            self._latest_target = target
            self._latest_motion = motion
            self._last_update_mono = time.monotonic()

    def _parse_line(self, line: str) -> Optional[tuple[Pose, TargetBlock]]:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            LOGGER.debug("Skipping non-JSON line: %r", line)
            return None

        if not isinstance(obj, dict):
            return None

        event_type = str(obj.get("type", ""))

        if event_type == "FORGE_F3":
            ts_utc = str(obj.get("ts_utc", ""))
            pose_raw = obj.get("pose") or {}
            target_raw = obj.get("target") or {}
        elif event_type == "F3_TICK":
            ts_utc = str(obj.get("ts", ""))
            pose_raw = {
                "x": obj.get("x", 0.0),
                "y": obj.get("y", 0.0),
                "z": obj.get("z", 0.0),
                "yaw": obj.get("yaw", 0.0),
                "pitch": obj.get("pitch", 0.0),
                "dimension": obj.get("dimension", ""),
                "is_sprinting": obj.get("is_sprinting", False),
                "on_ground": obj.get("on_ground", False),
            }
            target_raw = {
                "block_id": obj.get("look_block", "minecraft:air"),
                "x": obj.get("look_x"),
                "y": obj.get("look_y"),
                "z": obj.get("look_z"),
            }
        else:
            return None

        pose = Pose(
            x=float(pose_raw.get("x", 0.0)),
            y=float(pose_raw.get("y", 0.0)),
            z=float(pose_raw.get("z", 0.0)),
            yaw=_normalize_yaw(float(pose_raw.get("yaw", 0.0))),
            pitch=float(pose_raw.get("pitch", 0.0)),
            dimension=str(pose_raw.get("dimension", "")),
            is_sprinting=bool(pose_raw.get("is_sprinting", False)),
            on_ground=bool(pose_raw.get("on_ground", False)),
            ts_utc=ts_utc
        )

        target = TargetBlock(
            block_id=str(target_raw.get("block_id", "minecraft:air")),
            x=self._maybe_int(target_raw.get("x")),
            y=self._maybe_int(target_raw.get("y")),
            z=self._maybe_int(target_raw.get("z")),
            ts_utc=ts_utc
        )
        return pose, target

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _maybe_int(v: Any) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_ts_utc(ts_utc: str) -> Optional[datetime]:
        if not ts_utc:
            return None
        try:
            return datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _source_age_sec(self, ts_utc: str) -> Optional[float]:
        dt = self._parse_ts_utc(ts_utc)
        if dt is None:
            return None
        now_utc = datetime.now(timezone.utc)
        return max(0.0, (now_utc - dt).total_seconds())

    def _estimate_motion(self, prev_pose: Optional[Pose], pose: Pose) -> Optional[MotionEstimate]:
        if prev_pose is None:
            return None
        if prev_pose.dimension and pose.dimension and prev_pose.dimension != pose.dimension:
            return None

        prev_dt = self._parse_ts_utc(prev_pose.ts_utc)
        curr_dt = self._parse_ts_utc(pose.ts_utc)
        if prev_dt is None or curr_dt is None:
            return None

        dt_sec = (curr_dt - prev_dt).total_seconds()
        if dt_sec <= 0.0:
            return None

        dx = pose.x - prev_pose.x
        dy = pose.y - prev_pose.y
        dz = pose.z - prev_pose.z
        horizontal_speed = (dx * dx + dz * dz) ** 0.5 / dt_sec
        speed = (dx * dx + dy * dy + dz * dz) ** 0.5 / dt_sec
        return MotionEstimate(
            dt_sec=dt_sec,
            vel_x=dx / dt_sec,
            vel_y=dy / dt_sec,
            vel_z=dz / dt_sec,
            speed_bps=speed,
            horizontal_speed_bps=horizontal_speed,
            ts_utc=pose.ts_utc,
        )
