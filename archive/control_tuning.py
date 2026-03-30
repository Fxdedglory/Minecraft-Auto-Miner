# control_tuning.py v0.1.0 – 2025-12-06
"""
Control parameter tuning for Minecraft Auto Miner.

This module lets the miner *learn* over:
- pitch angle (mouse Y pixels),
- sprint duration (seconds),
- strafe pattern (x-axis pattern),

using a simple ε-greedy bandit per mining profile, with reward = blocks_per_minute.

Usage (high level, to be wired into app.py later):

from minecraft_auto_miner.control_tuning import (
    ControlParams,
    load_control_state,
    save_control_state,
    select_params_for_profile,
    record_episode_result,
    CONTROL_TUNING_PATH,
)

state = load_control_state()
params = select_params_for_profile(
    state=state,
    profile_name="straight_lane_default",
    epsilon=0.15,
)

# ... run episode using `params` ...

record_episode_result(
    state=state,
    profile_name="straight_lane_default",
    params=params,
    blocks_per_minute=episode_stats.blocks_per_minute,
)
save_control_state(state)
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Tuple, Any
import json
import logging
import math
import random

logger = logging.getLogger("minecraft_auto_miner.control_tuning")

# Default location for persistent control-tuning state
CONTROL_TUNING_PATH = Path("data") / "control_tuning.json"


# ---------------------------------------------------------------------------
# Core data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ControlParams:
    """
    Parameter bundle for a single mining episode.

    - pitch_pixels: how far to move the mouse down after /gmine
    - sprint_seconds: how long to hold sprint+forward in the initial nudge
    - strafe_pattern: name of a strafe pattern that movement code understands
                      (e.g. "none", "zigzag_small", "zigzag_large")
    """
    pitch_pixels: int
    sprint_seconds: float
    strafe_pattern: str

    def key(self) -> str:
        """Stable string key for use in stats dictionaries & JSON."""
        return f"pitch={self.pitch_pixels}_sprint={self.sprint_seconds:.2f}_strafe={self.strafe_pattern}"


@dataclass
class ControlParamsStats:
    runs: int = 0
    total_reward: float = 0.0

    @property
    def avg_reward(self) -> float:
        if self.runs <= 0:
            return 0.0
        return self.total_reward / float(self.runs)


@dataclass
class ProfileControlState:
    """
    Control tuning state for a single mining profile.

    - candidates: list of candidate ControlParams combos the agent can choose from
    - stats: per-candidate stats keyed by ControlParams.key()
    """
    candidates: List[ControlParams]
    stats: Dict[str, ControlParamsStats]


@dataclass
class ControlTuningState:
    """
    Global control tuning state, keyed by mining profile name.
    """
    profiles: Dict[str, ProfileControlState]


# ---------------------------------------------------------------------------
# Default candidate generation
# ---------------------------------------------------------------------------

def _default_candidates_for_profile(profile_name: str) -> List[ControlParams]:
    """
    Define a small, hand-picked grid of candidate control parameters.

    You can customize per profile later; for now we use a good general set.
    """
    # These are tuned for your 50x50x50 cube starting at feet level:
    # - pitch_pixels: how far to look down from horizon
    # - sprint_seconds: how long to nudge into the wall / lane
    # - strafe_pattern: "none" or small zigzag
    base_candidates: List[ControlParams] = [
        ControlParams(pitch_pixels=140, sprint_seconds=0.4, strafe_pattern="none"),
        ControlParams(pitch_pixels=180, sprint_seconds=0.6, strafe_pattern="none"),
        ControlParams(pitch_pixels=200, sprint_seconds=0.8, strafe_pattern="none"),
        ControlParams(pitch_pixels=180, sprint_seconds=0.6, strafe_pattern="zigzag_small"),
        ControlParams(pitch_pixels=200, sprint_seconds=0.8, strafe_pattern="zigzag_small"),
        ControlParams(pitch_pixels=220, sprint_seconds=0.9, strafe_pattern="zigzag_small"),
    ]

    # If you later want profile-specific sets, you can branch on profile_name here.
    return base_candidates


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def _stats_to_dict(stats: ControlParamsStats) -> Dict[str, Any]:
    return {"runs": stats.runs, "total_reward": stats.total_reward}


def _stats_from_dict(data: Dict[str, Any]) -> ControlParamsStats:
    runs = int(data.get("runs", 0))
    total_reward = float(data.get("total_reward", 0.0))
    return ControlParamsStats(runs=runs, total_reward=total_reward)


def _params_to_dict(params: ControlParams) -> Dict[str, Any]:
    return {
        "pitch_pixels": params.pitch_pixels,
        "sprint_seconds": params.sprint_seconds,
        "strafe_pattern": params.strafe_pattern,
    }


def _params_from_dict(data: Dict[str, Any]) -> ControlParams:
    return ControlParams(
        pitch_pixels=int(data["pitch_pixels"]),
        sprint_seconds=float(data["sprint_seconds"]),
        strafe_pattern=str(data["strafe_pattern"]),
    )


def _profile_to_dict(profile_state: ProfileControlState) -> Dict[str, Any]:
    return {
        "candidates": [_params_to_dict(p) for p in profile_state.candidates],
        "stats": {k: _stats_to_dict(v) for k, v in profile_state.stats.items()},
    }


def _profile_from_dict(data: Dict[str, Any]) -> ProfileControlState:
    # Load candidates
    raw_candidates = data.get("candidates", [])
    candidates = [_params_from_dict(p) for p in raw_candidates]

    # Load stats
    raw_stats = data.get("stats", {})
    stats: Dict[str, ControlParamsStats] = {
        k: _stats_from_dict(v) for k, v in raw_stats.items()
    }

    # Ensure every candidate has a stats entry
    for p in candidates:
        key = p.key()
        if key not in stats:
            stats[key] = ControlParamsStats()

    return ProfileControlState(candidates=candidates, stats=stats)


def _state_to_dict(state: ControlTuningState) -> Dict[str, Any]:
    return {
        "profiles": {
            profile_name: _profile_to_dict(profile_state)
            for profile_name, profile_state in state.profiles.items()
        }
    }


def _state_from_dict(data: Dict[str, Any]) -> ControlTuningState:
    raw_profiles = data.get("profiles", {})
    profiles: Dict[str, ProfileControlState] = {}
    for profile_name, raw_profile in raw_profiles.items():
        profiles[profile_name] = _profile_from_dict(raw_profile)
    return ControlTuningState(profiles=profiles)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def load_control_state(path: Path | None = None) -> ControlTuningState:
    """
    Load control tuning state from JSON. If the file doesn't exist or is invalid,
    return an empty state and log a warning.
    """
    if path is None:
        path = CONTROL_TUNING_PATH

    if not path.exists():
        logger.info("Control tuning file not found at %s; starting with empty state.", path)
        return ControlTuningState(profiles={})

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        state = _state_from_dict(raw)
        logger.info(
            "Loaded control tuning state from %s (profiles=%d).",
            path,
            len(state.profiles),
        )
        return state
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to load control tuning state from %s: %s", path, exc)
        return ControlTuningState(profiles={})


def save_control_state(state: ControlTuningState, path: Path | None = None) -> None:
    """
    Save control tuning state to JSON.
    """
    if path is None:
        path = CONTROL_TUNING_PATH

    try:
        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

        data = _state_to_dict(state)
        path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        logger.info(
            "Saved control tuning state to %s (profiles=%d).",
            path,
            len(state.profiles),
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to save control tuning state to %s: %s", path, exc)


# ---------------------------------------------------------------------------
# ε-greedy selector
# ---------------------------------------------------------------------------

def _ensure_profile_state(state: ControlTuningState, profile_name: str) -> ProfileControlState:
    """
    Ensure the given profile has candidates & stats; if not, initialize them.
    """
    if profile_name in state.profiles:
        profile_state = state.profiles[profile_name]
        # Ensure every candidate has stats
        for p in profile_state.candidates:
            key = p.key()
            if key not in profile_state.stats:
                profile_state.stats[key] = ControlParamsStats()
        return profile_state

    # Initialize with default candidates
    candidates = _default_candidates_for_profile(profile_name)
    stats: Dict[str, ControlParamsStats] = {p.key(): ControlParamsStats() for p in candidates}
    profile_state = ProfileControlState(candidates=candidates, stats=stats)
    state.profiles[profile_name] = profile_state

    logger.info(
        "Initialized control tuning profile '%s' with %d candidates.",
        profile_name,
        len(candidates),
    )
    return profile_state


def select_params_for_profile(
    state: ControlTuningState,
    profile_name: str,
    epsilon: float,
    rng: random.Random | None = None,
) -> ControlParams:
    """
    ε-greedy selection of ControlParams for a given mining profile.

    - With probability ε: explore (pick among least-run candidates).
    - Otherwise: exploit (pick candidate with highest avg_reward).
      If all candidates are unseen (runs=0), this degenerates to explore.
    """
    if rng is None:
        rng = random

    profile_state = _ensure_profile_state(state, profile_name)
    candidates = profile_state.candidates
    stats = profile_state.stats

    if not candidates:
        # Shouldn't happen, but guard anyway
        logger.warning(
            "No control candidates for profile '%s'; falling back to default candidates.",
            profile_name,
        )
        profile_state.candidates = _default_candidates_for_profile(profile_name)
        profile_state.stats = {p.key(): ControlParamsStats() for p in profile_state.candidates}
        candidates = profile_state.candidates
        stats = profile_state.stats

    # Decide explore vs exploit
    explore = rng.random() < epsilon

    if explore:
        # Exploration: choose among least-run candidates
        min_runs = min(stats[p.key()].runs for p in candidates)
        underexplored = [p for p in candidates if stats[p.key()].runs == min_runs]
        chosen = rng.choice(underexplored)
        logger.info(
            "ControlTuning: EXPLORING for profile '%s' (epsilon=%.3f, min_runs=%d, choice=%s).",
            profile_name,
            epsilon,
            min_runs,
            chosen.key(),
        )
        return chosen

    # Exploitation: choose candidate with best avg_reward.
    # If all runs are zero, this will effectively act like exploration anyway.
    best_params: ControlParams | None = None
    best_reward: float = -math.inf

    for p in candidates:
        s = stats[p.key()]
        avg_r = s.avg_reward
        if avg_r > best_reward:
            best_reward = avg_r
            best_params = p

    if best_params is None:
        # Fallback: everything is completely fresh, just pick random
        chosen = rng.choice(candidates)
        logger.info(
            "ControlTuning: EXPLOIT fallback random for profile '%s' (no stats). choice=%s",
            profile_name,
            chosen.key(),
        )
        return chosen

    logger.info(
        "ControlTuning: EXPLOITING for profile '%s' (epsilon=%.3f, best_avg=%.3f, choice=%s).",
        profile_name,
        epsilon,
        best_reward,
        best_params.key(),
    )
    return best_params


# ---------------------------------------------------------------------------
# Episode result recording
# ---------------------------------------------------------------------------

def record_episode_result(
    state: ControlTuningState,
    profile_name: str,
    params: ControlParams,
    blocks_per_minute: float,
) -> None:
    """
    Record the outcome of an episode for a given profile + control params.

    Reward is blocks_per_minute; this is accumulated into stats and will
    influence future ε-greedy choices.
    """
    profile_state = _ensure_profile_state(state, profile_name)
    key = params.key()

    # Ensure stats exist for this exact param combo
    if key not in profile_state.stats:
        logger.info(
            "ControlTuning: adding new candidate for profile '%s': %s",
            profile_name,
            key,
        )
        profile_state.candidates.append(params)
        profile_state.stats[key] = ControlParamsStats()

    s = profile_state.stats[key]
    s.runs += 1
    s.total_reward += float(blocks_per_minute)

    logger.info(
        "ControlTuning: recorded episode result for profile='%s' key=%s runs=%d avg_reward=%.3f (blocks_per_minute=%.3f)",
        profile_name,
        key,
        s.runs,
        s.avg_reward,
        blocks_per_minute,
    )
