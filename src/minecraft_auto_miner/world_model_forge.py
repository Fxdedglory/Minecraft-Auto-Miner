"""
minecraft_auto_miner.world_model_forge v0.1.0 – 2025-12-08
Forge-driven world model for Minecraft Auto Miner.

Consumes ForgePoseProvider:
    pose    → player x/y/z, yaw/pitch, sprinting, ground, dimension ID
    target  → block under crosshair (block_id + xyz)

Replaces ALL of the following:
    - world_model.py
    - pose.py
    - perception.py
    - stuck/reset detectors

This is the authoritative real-time state of the world.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .forge import ForgePoseProvider, MotionEstimate, Pose, TargetBlock


@dataclass
class ObstacleInfo:
    is_blocking: bool
    block_id: str
    block_x: Optional[int]
    block_y: Optional[int]
    block_z: Optional[int]


class WorldModelForge:
    """
    Real-time world model backed by Forge F3 telemetry.

    The miner uses this for:
        - exact player pose
        - block under crosshair
        - wall/obstacle detection
        - future navigation logic
    """

    def __init__(self, pose_provider: ForgePoseProvider):
        self.pose_provider = pose_provider
        self._latest_pose: Optional[Pose] = None
        self._latest_target: Optional[TargetBlock] = None

        # These block types stop movement.
        # Expand as needed.
        self.blocking_ids = {
            "minecraft:wool",
            "minecraft:white_wool",
            "minecraft:yellow_wool",
            "minecraft:blue_wool",
            "minecraft:glass",
            "minecraft:barrier",
        }

    # ------------------------------------------------------------------
    # Update snapshots
    # ------------------------------------------------------------------

    def update(self) -> None:
        """Refresh the latest pose + targetblock."""
        self._latest_pose = self.pose_provider.get_pose()
        self._latest_target = self.pose_provider.get_target_block()

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_player_pose(self) -> Optional[Pose]:
        return self._latest_pose

    def get_player_block_pos(self) -> Optional[tuple[int, int, int]]:
        if self._latest_pose is None:
            return None
        return (
            int(self._latest_pose.x),
            int(self._latest_pose.y),
            int(self._latest_pose.z),
        )

    def get_target_block(self) -> Optional[TargetBlock]:
        return self._latest_target

    def get_motion_estimate(self) -> Optional[MotionEstimate]:
        return self.pose_provider.get_motion_estimate()

    def is_blocking_block_id(self, block_id: str) -> bool:
        bid = block_id.lower()
        if not bid or bid == "minecraft:air":
            return False
        if bid in self.blocking_ids:
            return True
        return bid.endswith("_wool") or "glass" in bid or "barrier" in bid

    def is_obstacle_ahead(self) -> ObstacleInfo:
        """
        Returns true if the block under the crosshair is a blocking block.
        """
        t = self._latest_target
        if t is None:
            return ObstacleInfo(False, "minecraft:air", None, None, None)

        block_id = t.block_id.lower()

        is_blocking = self.is_blocking_block_id(block_id)

        return ObstacleInfo(
            is_blocking=is_blocking,
            block_id=t.block_id,
            block_x=t.x,
            block_y=t.y,
            block_z=t.z,
        )
