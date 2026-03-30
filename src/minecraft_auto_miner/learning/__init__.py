"""
minecraft_auto_miner.learning package v0.7.0 – 2025-12-08
Generated with ChatGPT (GPT-5.1 Thinking).

Changelog:
- 2025-12-08 v0.7.0: Created learning package and exposed reward helpers.
"""

from .reward_engine import (
    WindowStats,
    PenaltyFlags,
    RewardWeights,
    compute_reward,
    compute_reward_from_raw,
)

__all__ = [
    "WindowStats",
    "PenaltyFlags",
    "RewardWeights",
    "compute_reward",
    "compute_reward_from_raw",
]
