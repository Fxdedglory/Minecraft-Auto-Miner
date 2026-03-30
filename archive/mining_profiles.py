"""
minecraft_auto_miner.mining_profiles v0.4.5 – 2025-12-06
Generated with ChatGPT (GPT-5.1 Thinking) on 2025-12-06.

Skeleton for mining profiles:
- Encapsulates tunable parameters for mining behaviour.
- v0.4.5: Adds behaviour knobs (sprint/strafe/mouse_sweep) but they are
  NOT yet used to change movement; they are only logged and tracked.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class MiningProfile:
    """
    Parameters that describe a mining behaviour.

    lane_length_blocks:
        How far to walk in one lane before considering a turn/reset.
    mining_window_seconds:
        Target window length for evaluating performance (e.g., 180s).
    turn_angle_degrees:
        Planned turn angle at lane end (future behaviour).

    Behaviour knobs (not yet active in v0.4.5):

    use_sprint:
        Whether to hold the sprint key while mining.
    strafe_pattern:
        Pattern for strafing while mining:
        - "none": no strafe (current behaviour)
        - "left-right": small left/right alternation
    mouse_sweep:
        Pattern for moving the mouse:
        - "none": keep aim fixed (current behaviour)
        - "horizontal": small left/right sweeps
        - "vertical": small up/down sweeps
    """

    name: str
    lane_length_blocks: int
    mining_window_seconds: float
    turn_angle_degrees: float = 0.0

    use_sprint: bool = False
    strafe_pattern: str = "none"
    mouse_sweep: str = "none"


# Current behaviour profile: straight lane, no sprint/strafe/sweep.
DEFAULT_STRAIGHT_PROFILE = MiningProfile(
    name="straight_lane_default",
    lane_length_blocks=9999,      # effectively "no lane limit" for now
    mining_window_seconds=30.0,  # 3-minute evaluation window
    turn_angle_degrees=0.0,
    use_sprint=False,
    strafe_pattern="none",
    mouse_sweep="none",
)

# Example alternative profiles – NOT yet wired into movement logic.
STRAIGHT_SPRINT_PROFILE = MiningProfile(
    name="straight_sprint",
    lane_length_blocks=9999,
    mining_window_seconds=30.0,
    turn_angle_degrees=0.0,
    use_sprint=True,
    strafe_pattern="none",
    mouse_sweep="none",
)

STRAFE_SLIGHT_PROFILE = MiningProfile(
    name="strafe_slight",
    lane_length_blocks=9999,
    mining_window_seconds=30.0,
    turn_angle_degrees=0.0,
    use_sprint=False,
    strafe_pattern="left-right",
    mouse_sweep="none",
)

ALL_PROFILES: Tuple[MiningProfile, ...] = (
    DEFAULT_STRAIGHT_PROFILE,
    STRAIGHT_SPRINT_PROFILE,
    STRAFE_SLIGHT_PROFILE,
)


def get_default_profile() -> MiningProfile:
    """
    Return the default mining profile for the miner.

    In future we may load this from config or choose dynamically.
    """
    return DEFAULT_STRAIGHT_PROFILE


def get_all_profiles() -> Tuple[MiningProfile, ...]:
    """
    Return all defined mining profiles.
    """
    return ALL_PROFILES
