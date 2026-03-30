"""
minecraft_auto_miner.metrics v0.4.5 – 2025-12-06
Generated with ChatGPT (GPT-5.1 Thinking) on 2025-12-06.

v0.4.5:
- Same multi-window behaviour as v0.4.4.
- Adds get_windows() so strategy logic can inspect all windows
  after a run/episode.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Optional, List

from .perception import FrameState

LOGGER = logging.getLogger("minecraft_auto_miner.metrics")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class WindowStats:
    """
    Metrics for a single mining window.

    All timestamps are stored as UTC datetimes.
    """
    start_time: datetime
    end_time: Optional[datetime] = None
    ticks: int = 0
    mining_ticks: int = 0
    block_breaks: int = 0

    @property
    def duration_seconds(self) -> float:
        if self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()

    @property
    def mining_ratio(self) -> float:
        if self.ticks == 0:
            return 0.0
        return self.mining_ticks / self.ticks

    @property
    def blocks_per_minute(self) -> float:
        if self.duration_seconds <= 0:
            return 0.0
        return (self.block_breaks / self.duration_seconds) * 60.0


class MetricsManager:
    """
    Tracks basic metrics for the miner.

    v0.4.5:
      - Multiple WindowStats per run.
      - Each window has a configured length (window_length_seconds).
      - on_tick() updates current window and rolls over when full.
      - At run end, logs all window summaries plus overall totals.
      - Exposes get_windows() for strategy logic.
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or LOGGER
        self.window_length_seconds: float = 180.0  # default; can be overridden
        self._windows: List[WindowStats] = []
        self._current_window: Optional[WindowStats] = None

    def start_run(self, window_length_seconds: Optional[float] = None) -> None:
        if window_length_seconds is not None and window_length_seconds > 0:
            self.window_length_seconds = float(window_length_seconds)

        self._windows.clear()
        start = _now_utc()
        self._current_window = WindowStats(start_time=start)
        self._windows.append(self._current_window)
        self.logger.info(
            "Metrics run started at (UTC): %s (window_length_seconds=%.1f)",
            start.isoformat(),
            self.window_length_seconds,
        )

    def _ensure_window(self) -> WindowStats:
        if self._current_window is None:
            start = _now_utc()
            self._current_window = WindowStats(start_time=start)
            self._windows.append(self._current_window)
            self.logger.warning(
                "MetricsManager._ensure_window() created a window implicitly at %s",
                start.isoformat(),
            )
        return self._current_window

    def _maybe_roll_window(self, now: datetime) -> None:
        w = self._ensure_window()
        elapsed = (now - w.start_time).total_seconds()
        if elapsed >= self.window_length_seconds:
            # Close current window and start a new one.
            w.end_time = now
            self.logger.info(
                "Metrics window closed (UTC %s -> %s, duration=%.1fs, ticks=%d, blocks=%d).",
                w.start_time.isoformat(),
                w.end_time.isoformat(),
                w.duration_seconds,
                w.ticks,
                w.block_breaks,
            )
            new_w = WindowStats(start_time=now)
            self._windows.append(new_w)
            self._current_window = new_w
            self.logger.info(
                "Metrics window started at (UTC): %s", new_w.start_time.isoformat()
            )

    def on_tick(self, frame: FrameState, is_mining: bool) -> None:
        """
        Update metrics for each main loop tick.
        """
        now = _now_utc()
        self._maybe_roll_window(now)

        w = self._ensure_window()
        w.ticks += 1
        if is_mining:
            w.mining_ticks += 1
        if frame.has_block_break:
            w.block_breaks += 1

    def end_run(self) -> None:
        # Close the current window with an end timestamp.
        if self._current_window is not None and self._current_window.end_time is None:
            self._current_window.end_time = _now_utc()
        self.logger.info("Metrics run ended at (UTC): %s", _now_utc().isoformat())

    def get_windows(self) -> List[WindowStats]:
        """
        Return a shallow copy of all windows collected during this run.
        """
        return list(self._windows)

    def log_summary(self) -> None:
        """
        Emit per-window and overall metrics summary to the logger.
        """
        if not self._windows:
            self.logger.info("No metrics windows to summarize.")
            return

        self.logger.info("==== Mining Run Metrics Summary (per window) ====")
        total_ticks = 0
        total_mining_ticks = 0
        total_block_breaks = 0
        overall_start = self._windows[0].start_time
        overall_end = self._windows[-1].end_time or _now_utc()

        for idx, w in enumerate(self._windows, start=1):
            total_ticks += w.ticks
            total_mining_ticks += w.mining_ticks
            total_block_breaks += w.block_breaks

            self.logger.info("Window #%d:", idx)
            self.logger.info("  Start (UTC)       : %s", w.start_time.isoformat())
            self.logger.info(
                "  End   (UTC)       : %s",
                (w.end_time or overall_end).isoformat(),
            )
            self.logger.info("  Duration (s)      : %.2f", w.duration_seconds)
            self.logger.info("  Total ticks       : %d", w.ticks)
            self.logger.info("  Mining ticks      : %d", w.mining_ticks)
            self.logger.info("  Mining ratio      : %.3f", w.mining_ratio)
            self.logger.info("  Block breaks      : %d", w.block_breaks)
            self.logger.info("  Blocks per minute : %.3f", w.blocks_per_minute)

        total_duration = (overall_end - overall_start).total_seconds()
        overall_mining_ratio = (total_mining_ticks / total_ticks) if total_ticks else 0.0
        overall_blocks_per_minute = (
            (total_block_breaks / total_duration) * 60.0 if total_duration > 0 else 0.0
        )

        self.logger.info("==== Overall Totals ====")
        self.logger.info("Overall start (UTC)       : %s", overall_start.isoformat())
        self.logger.info("Overall end   (UTC)       : %s", overall_end.isoformat())
        self.logger.info("Total duration (s)        : %.2f", total_duration)
        self.logger.info("Total ticks               : %d", total_ticks)
        self.logger.info("Total mining ticks        : %d", total_mining_ticks)
        self.logger.info("Overall mining ratio      : %.3f", overall_mining_ratio)
        self.logger.info("Total block breaks        : %d", total_block_breaks)
        self.logger.info(
            "Overall blocks per minute : %.3f", overall_blocks_per_minute
        )
        self.logger.info("====================================")
