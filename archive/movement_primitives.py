"""
minecraft_auto_miner.movement_primitives v0.5.0-pre – 2025-12-07
Generated with ChatGPT (GPT-5.1 Thinking) on 2025-12-07.

Purpose:
- Provide block-level movement actions (step forward, turn 90°, etc.)
  on top of InputController + Pose/WorldModel.
- This module is *not yet wired* into app.py; it is safe to import
  without changing current behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from typing import Protocol, Optional

from .pose import Pose, Facing, DeadReckoningPoseProvider
from ..src.minecraft_auto_miner.input_controller import InputController

LOGGER = logging.getLogger("minecraft_auto_miner.movement")


@dataclass
class MovementCalibration:
    """
    Calibration values for translating block-level moves into
    key/mouse durations and angles.

    These values are intentionally conservative defaults and should
    be tuned empirically on your system.

    forward_block_seconds:
        Approx seconds of holding forward to move ~1 block while walking.

    sprint_forward_block_seconds:
        Approx seconds to move ~1 block while sprinting.

    turn_90_degrees:
        Yaw rotation (in degrees) that corresponds to a ~90° turn
        using InputController.yaw_relative.
    """
    forward_block_seconds: float = 0.24
    sprint_forward_block_seconds: float = 0.17
    turn_90_degrees: float = 90.0


class MovementIO(Protocol):
    """
    Abstract interface for "do the physical movement" side-effects.

    This allows us to:
    - Wrap InputController for real IO.
    - Provide fake/test implementations for dry-run unit tests.
    """

    def move_forward(self, seconds: float, *, use_sprint: bool, mining: bool) -> None:
        """Move roughly one block forward by holding keys for `seconds`."""
        ...

    def turn_yaw(self, degrees: float) -> None:
        """Rotate camera yaw by the given degrees."""
        ...


class InputMovementIO:
    """
    Concrete MovementIO implementation backed by InputController.

    NOTE:
    - For now we re-use the mining helpers on InputController
      (hold_forward_and_mine / hold_forward_sprint_and_mine) even
      when `mining=False`. This is safe but may mine some extra
      blocks during alignment; later we can extend InputController
      with non-mining movement helpers.
    """

    def __init__(self, controller: InputController, logger: Optional[logging.Logger] = None):
        self.controller = controller
        self.logger = logger or LOGGER

    def move_forward(self, seconds: float, *, use_sprint: bool, mining: bool) -> None:
        if seconds <= 0:
            return

        # For now we always "mine" while moving forward, regardless of mining flag.
        # This keeps behaviour simple until we add finer-grained helpers in
        # InputController for non-mining movement.
        try:
            if use_sprint:
                # Explicit sprint variant
                self.controller.hold_forward_sprint_and_mine()
            else:
                # Profile-driven sprint (or not)
                self.controller.hold_forward_and_mine()
        except AttributeError:
            # If these helpers are missing for any reason, log and bail.
            self.logger.warning(
                "InputController is missing hold_forward_* helpers; "
                "forward movement primitive skipped."
            )
            return

        time.sleep(seconds)

        # Release movement/mining so we don't leave keys stuck.
        try:
            self.controller.stop_all()
        except AttributeError:
            # Fallback: nothing we can do; caller must ensure cleanup.
            self.logger.warning(
                "InputController has no stop_all(); keys may remain pressed."
            )

    def turn_yaw(self, degrees: float) -> None:
        if degrees == 0:
            return
        try:
            self.controller.yaw_relative(degrees)
        except AttributeError:
            self.logger.warning(
                "InputController has no yaw_relative(); cannot turn yaw by %s degrees",
                degrees,
            )


class MovementPrimitiveController:
    """
    High-level controller that executes block-level movement primitives
    and keeps the DeadReckoningPoseProvider in sync.

    This is the main entry point used by lane_controller.py.
    """

    def __init__(
        self,
        pose_provider: DeadReckoningPoseProvider,
        movement_io: MovementIO,
        calibration: Optional[MovementCalibration] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.pose_provider = pose_provider
        self.movement_io = movement_io
        self.calibration = calibration or MovementCalibration()
        self.logger = logger or LOGGER

    # ---- Internal helpers -------------------------------------------------

    def _update_pose(self, pose: Pose) -> None:
        """Push updated pose back into the provider."""
        try:
            self.pose_provider.set_pose(pose)
        except AttributeError:
            # DeadReckoningPoseProvider exposes set_pose; but to keep this
            # generic for future providers, we guard with AttributeError.
            self.logger.debug("Pose provider does not support set_pose(); skipping update.")

    # ---- Public primitives -------------------------------------------------

    def step_forward_block(
        self,
        *,
        mining: bool = True,
        use_sprint: bool = False,
        blocks: int = 1,
    ) -> Pose:
        """
        Move forward by the specified number of blocks (approximate),
        updating and returning the new Pose.

        We currently assume flat terrain and no collisions; higher-level
        logic (LaneController + world_model) will handle low-progress /
        stuck detection.
        """
        if blocks <= 0:
            return self.pose_provider.get_pose()

        pose = self.pose_provider.get_pose()
        for _ in range(blocks):
            seconds = (
                self.calibration.sprint_forward_block_seconds
                if use_sprint
                else self.calibration.forward_block_seconds
            )

            self.logger.info(
                "MovementPrimitive: stepping forward 1 block (sprint=%s, seconds=%.3f)",
                use_sprint,
                seconds,
            )
            self.movement_io.move_forward(seconds, use_sprint=use_sprint, mining=mining)
            pose = pose.step_forward()

        self._update_pose(pose)
        return pose

    def turn_left_90(self) -> Pose:
        """
        Turn left by ~90 degrees and update Pose.facing accordingly.

        Uses calibration.turn_90_degrees; if you find the turn overshoots
        or undershoots, adjust that value.
        """
        pose = self.pose_provider.get_pose()
        degrees = -self.calibration.turn_90_degrees  # left = negative yaw
        self.logger.info("MovementPrimitive: turning left by %.1f degrees", abs(degrees))
        self.movement_io.turn_yaw(degrees)
        new_pose = pose.turn_left_90()
        self._update_pose(new_pose)
        return new_pose

    def turn_right_90(self) -> Pose:
        """
        Turn right by ~90 degrees and update Pose.facing accordingly.
        """
        pose = self.pose_provider.get_pose()
        degrees = self.calibration.turn_90_degrees  # right = positive yaw
        self.logger.info("MovementPrimitive: turning right by %.1f degrees", degrees)
        self.movement_io.turn_yaw(degrees)
        new_pose = pose.turn_right_90()
        self._update_pose(new_pose)
        return new_pose
