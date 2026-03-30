"""
lane_planner.py – v0.5.0-pre (2025-12-07)
Generates structured mining lanes across MineWorld bounds.

Generated with ChatGPT (GPT-5.1 Thinking) on 2025-12-07.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from .pose import Pose, Facing
from .world_model import MineWorld


@dataclass
class Lane:
    """
    Represents a single mining lane between two poses.
    The miner will travel from start -> end in the specified facing direction.
    """
    lane_id: int
    start_pose: Pose
    end_pose: Pose
    direction: Facing  # EAST or WEST


class LanePlanner:
    """
    Produces evenly spaced snake-pattern lanes over the MineWorld XZ plane.
    """

    def __init__(
        self,
        world: MineWorld,
        *,
        y_lane: int,
        lane_spacing: int = 2,
        logger=None,
    ):
        self.world = world
        self.y_lane = y_lane
        self.lane_spacing = lane_spacing
        self.logger = logger
        self._lanes: List[Lane] = []
        self._generate_lanes()

    # ----------------------------------------------------------------------

    def _log(self, msg: str):
        if self.logger:
            self.logger.info(msg)

    def _generate_lanes(self):
        """
        Generate lanes based on MineWorld bounds forming a snake traversal:
            Lane 0 = EAST at z_min
            Lane 1 = WEST at z_min + lane_spacing
            Lane 2 = EAST at z_min + 2*lane_spacing
            ...
        """
        b = self.world.bounds
        lanes: List[Lane] = []

        lane_id = 0
        current_z = b.z_min

        while current_z <= b.z_max:
            direction = Facing.EAST if (lane_id % 2 == 0) else Facing.WEST

            if direction is Facing.EAST:
                start = Pose(b.x_min, self.y_lane, current_z, Facing.EAST)
                end = Pose(b.x_max, self.y_lane, current_z, Facing.EAST)
            else:  # WEST
                start = Pose(b.x_max, self.y_lane, current_z, Facing.WEST)
                end = Pose(b.x_min, self.y_lane, current_z, Facing.WEST)

            lanes.append(
                Lane(
                    lane_id=lane_id,
                    start_pose=start,
                    end_pose=end,
                    direction=direction,
                )
            )

            lane_id += 1
            current_z += self.lane_spacing

        self._lanes = lanes
        self._log(f"LanePlanner: generated {len(lanes)} lanes.")

    # ----------------------------------------------------------------------
    # Access API
    # ----------------------------------------------------------------------

    @property
    def lanes(self) -> List[Lane]:
        return self._lanes

    def get_lane(self, lane_id: int) -> Lane:
        return self._lanes[lane_id]

    def get_next_lane(self, lane: Lane) -> Lane:
        next_id = (lane.lane_id + 1) % len(self._lanes)
        return self._lanes[next_id]

    def get_nearest_lane(self, pose: Pose) -> Lane:
        """
        Returns whichever lane centerline (Z = z_k) is closest to current pose.
        Used right after /gmine teleport to pick the starting lane.
        """
        best_lane = None
        best_dist = float("inf")

        for ln in self._lanes:
            dz = abs(ln.start_pose.z - pose.z)
            if dz < best_dist:
                best_dist = dz
                best_lane = ln

        return best_lane

    def is_past_end(self, pose: Pose, lane: Lane) -> bool:
        """
        Check if the miner has overshot the lane end by >1 block.
        """
        if lane.direction is Facing.EAST:
            return pose.x > lane.end_pose.x + 1
        else:  # WEST
            return pose.x < lane.end_pose.x - 1
