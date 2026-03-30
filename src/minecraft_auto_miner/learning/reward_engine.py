"""
minecraft_auto_miner.learning.reward_engine v0.7.0 – 2025-12-08
Generated with ChatGPT (GPT-5.1 Thinking).

Changelog:
- 2025-12-08 v0.7.0: Initial reward model for Phase 1 – Window Rewards & Barrier Signals.
    * Implements blocks_per_minute-based reward with penalties for:
      - stuck events
      - low-progress watchdogs
      - resets
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Phase 1 – Reward Model (Short-Term, Pre-Forge)
# ---------------------------------------------------------------------------
#
# This module is intentionally *DB-agnostic*.
# You feed it window-level metrics + binary flags, and it returns a reward.
#
# Integration options:
#   - telemetry_collector: compute reward before writing decision_window rows
#   - policy_bootstrap: backfill reward for existing windows in Postgres
#
# From Implementation_plan v0.7.0 (Phase 1):
#
#   reward = blocks_per_minute
#          - penalty_low_progress
#          - penalty_stuck_or_reset
#
# Where penalties are triggered by:
#   - stuck episodes
#   - low-progress watchdogs
#   - resets
# ---------------------------------------------------------------------------


@dataclass
class WindowStats:
    """
    Minimal stats needed to compute reward for a decision window.

    Attributes
    ----------
    blocks_broken : int
        Total number of blocks broken in this window.
    duration_sec : float
        Duration of the window in seconds. If <= 0, we clamp to 1.0 internally
        to avoid division by zero.
    mining_ratio : Optional[float]
        Ratio of "mining-active" time to total window time (0.0–1.0).
        This is not currently used directly in the reward formula but is
        included for future tuning.
    """

    blocks_broken: int
    duration_sec: float
    mining_ratio: Optional[float] = None

    @property
    def blocks_per_minute(self) -> float:
        """Compute blocks per minute, with a defensive clamp on duration."""
        duration = max(self.duration_sec, 1.0)
        return (self.blocks_broken / duration) * 60.0


@dataclass
class PenaltyFlags:
    """
    Binary flags that indicate whether penalties should apply to this window.

    Attributes
    ----------
    had_stuck_event : bool
        True if a STUCK_EVENT occurred during (or immediately before) this
        window and is attributable to the current behaviour.
    had_low_progress_watchdog : bool
        True if a low-progress watchdog fired for this window profile.
    had_reset_event : bool
        True if a RESET_EVENT (e.g. /gmine reset) occurred due to poor
        progress, getting stuck, or hitting a barrier.
    """

    had_stuck_event: bool = False
    had_low_progress_watchdog: bool = False
    had_reset_event: bool = False


@dataclass
class RewardWeights:
    """
    Tunable weights for the reward function.

    Attributes
    ----------
    k_blocks_per_minute : float
        Base reward multiplier for blocks_per_minute.
    penalty_low_progress : float
        Fixed penalty applied when a low-progress watchdog fires.
    penalty_stuck : float
        Fixed penalty applied when a stuck event occurs.
    penalty_reset : float
        Fixed penalty applied when a reset event occurs.
    """

    k_blocks_per_minute: float = 1.0
    penalty_low_progress: float = 10.0
    penalty_stuck: float = 25.0
    penalty_reset: float = 50.0


def compute_reward(
    window: WindowStats,
    flags: PenaltyFlags,
    weights: RewardWeights | None = None,
) -> float:
    """
    Compute reward for a single decision window.

    Parameters
    ----------
    window : WindowStats
        Window-level metrics (blocks_broken, duration_sec, mining_ratio).
    flags : PenaltyFlags
        Binary indicators for stuck/low-progress/reset signals.
    weights : RewardWeights, optional
        Tunable hyperparameters. If None, defaults are used.

    Returns
    -------
    float
        Scalar reward value to be written into telemetry.decision_window.reward.

    Notes
    -----
    This is the Phase 1 "short-term, pre-Forge" model from Implementation_plan v0.7.0:

        reward = k_blocks_per_minute * blocks_per_minute
               - penalty_low_progress (if watchdog)
               - penalty_stuck       (if stuck)
               - penalty_reset       (if reset)
    """
    if weights is None:
        weights = RewardWeights()

    bpm = window.blocks_per_minute

    reward = weights.k_blocks_per_minute * bpm

    if flags.had_low_progress_watchdog:
        reward -= weights.penalty_low_progress

    if flags.had_stuck_event:
        reward -= weights.penalty_stuck

    if flags.had_reset_event:
        reward -= weights.penalty_reset

    return reward


# ---------------------------------------------------------------------------
# Convenience helpers for integration
# ---------------------------------------------------------------------------


def compute_reward_from_raw(
    *,
    blocks_broken: int,
    duration_sec: float,
    mining_ratio: Optional[float],
    had_stuck_event: bool,
    had_low_progress_watchdog: bool,
    had_reset_event: bool,
    weights: RewardWeights | None = None,
) -> float:
    """
    Convenience wrapper to compute reward directly from raw scalar values.

    This is useful in:
      - telemetry_collector: when you parse a WINDOW_SUMMARY log event and
        already know whether a stuck/reset/watchdog happened in this window.
      - policy_bootstrap: when backfilling decision_window.reward purely from
        DB fields.

    Example
    -------
    reward = compute_reward_from_raw(
        blocks_broken=window_row.blocks_broken,
        duration_sec=(window_row.end_ts - window_row.start_ts).total_seconds(),
        mining_ratio=window_row.mining_ratio,
        had_stuck_event=flags.had_stuck,
        had_low_progress_watchdog=flags.had_low_progress,
        had_reset_event=flags.had_reset,
    )
    """
    window = WindowStats(
        blocks_broken=blocks_broken,
        duration_sec=duration_sec,
        mining_ratio=mining_ratio,
    )
    flags = PenaltyFlags(
        had_stuck_event=had_stuck_event,
        had_low_progress_watchdog=had_low_progress_watchdog,
        had_reset_event=had_reset_event,
    )
    return compute_reward(window, flags, weights=weights)
