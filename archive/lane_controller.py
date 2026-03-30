"""
lane_controller.py – v0.5.2 (2025-12-07)
Lane-level finite state machine with optional execution and lane timeout.

Generated with ChatGPT (GPT-5.1 Thinking) on 2025-12-07.

Changes in v0.5.2:
- Adds time-based lane timeout using primitives.calibration.forward_block_seconds
  and a lane_timeout_factor from config (via app.py).
- Adds mining tick log throttling using logging.mine_tick_log_every (via app.py).
- Keeps execution optional: execute=False = logging-only, execute=True moves via
  MovementPrimitiveController.
"""

from __future__ import annotations

import time
from enum import Enum, auto
from typing import Optional

from .pose import Pose, Facing
from .lane_planner import Lane
from .movement_primitives import MovementPrimitiveController


class LanePhase(Enum):
    ALIGN_TO_START = auto()
    MINE_FORWARD = auto()
    COMPLETE = auto()


class LaneController:
    """
    Lane-level controller that defines the high-level behaviour sequence:
        ALIGN → MINE → COMPLETE → next lane

    - When execute=False (default), this is logging-only and does not move.
    - When execute=True, it uses MovementPrimitiveController to:
        * Turn to the lane direction.
        * Walk along Z towards the lane's Z.
        * Walk along X towards the lane start X.
        * (Later) supervise mining and detect lane completion.

    NOTE: Mining itself (holding W + left click) is still handled by the
    MinerStateMachine; lane controller is responsible for *where* to be
    and *which direction* to face.
    """

    def __init__(
        self,
        primitives: MovementPrimitiveController,
        planner,
        *,
        logger=None,
        lane_timeout_factor: float = 1.8,
        mine_tick_log_every: int = 20,
    ):
        self.primitives = primitives
        self.planner = planner
        self.logger = logger

        # Configurable behaviour
        self.lane_timeout_factor: float = float(lane_timeout_factor)
        self.mine_tick_log_every: int = max(1, int(mine_tick_log_every))

        # State
        self.current_lane: Optional[Lane] = None
        self.phase: LanePhase = LanePhase.ALIGN_TO_START

        # Alignment progress tracking
        self._align_z_done: bool = False
        self._align_x_done: bool = False

        # Mining-phase tracking
        self._mine_started_mono: Optional[float] = None
        self._mine_tick_counter: int = 0

        self._log(
            f"LaneController initialized: lane_timeout_factor={self.lane_timeout_factor:.2f}, "
            f"mine_tick_log_every={self.mine_tick_log_every}"
        )

    # ------------------------------------------------------------------ utils

    def _log(self, msg: str):
        if self.logger:
            self.logger.info(msg)

    def _reset_alignment_flags(self):
        self._align_z_done = False
        self._align_x_done = False

    def _reset_mine_phase_state(self):
        self._mine_started_mono = None
        self._mine_tick_counter = 0

    # ------------------------------------------------------------------ public

    def start_lane(self, lane: Lane):
        """
        Begin a new lane episode.
        """
        self.current_lane = lane
        self.phase = LanePhase.ALIGN_TO_START
        self._reset_alignment_flags()
        self._reset_mine_phase_state()
        self._log(
            f"LaneController: starting Lane {lane.lane_id} → {lane.direction.name} "
            f"at z={lane.start_pose.z}, x range {lane.start_pose.x}..{lane.end_pose.x}"
        )

    # ---------------------------------------------------------------- alignment

    def _ensure_facing(self, pose: Pose, desired: Facing, *, execute: bool) -> Pose:
        """
        Turn the miner until facing the desired direction.

        - execute=False → only log.
        - execute=True  → use turn_left_90/turn_right_90 primitives.
        """
        if pose.facing is desired:
            return pose

        # Decide the shortest rotation (we're only using 90° steps).
        # For simplicity, we allow at most two 90° turns.
        self._log(f"[ALIGN] Need to face {desired.name} (current={pose.facing.name}).")

        if not execute:
            return pose

        # For a 4-way facing enum, we can just turn right until we match.
        new_pose = pose
        for _ in range(4):
            if new_pose.facing is desired:
                break
            new_pose = self.primitives.turn_right_90()

        return new_pose

    def _align_along_z(
        self,
        pose: Pose,
        lane: Lane,
        *,
        execute: bool,
    ) -> Pose:
        """
        Move the miner along Z towards lane.start_pose.z.
        Uses step_forward_block with appropriate facing.
        """
        target_z = lane.start_pose.z
        dz = target_z - pose.z
        if dz == 0:
            self._align_z_done = True
            return pose

        direction = Facing.SOUTH if dz > 0 else Facing.NORTH
        self._log(
            f"[ALIGN-Z] pose.z={pose.z}, target_z={target_z}, dz={dz}, "
            f"desired_facing={direction.name}"
        )

        pose = self._ensure_facing(pose, direction, execute=execute)

        if not execute:
            # Logging-only: don't actually move
            return pose

        # Move 1 block towards the target along Z
        new_pose = self.primitives.step_forward_block()
        return new_pose

    def _align_along_x(
        self,
        pose: Pose,
        lane: Lane,
        *,
        execute: bool,
    ) -> Pose:
        """
        Move the miner along X towards lane.start_pose.x.
        Uses step_forward_block with appropriate facing.
        """
        target_x = lane.start_pose.x
        dx = target_x - pose.x
        if dx == 0:
            self._align_x_done = True
            return pose

        direction = Facing.EAST if dx > 0 else Facing.WEST
        self._log(
            f"[ALIGN-X] pose.x={pose.x}, target_x={target_x}, dx={dx}, "
            f"desired_facing={direction.name}"
        )

        pose = self._ensure_facing(pose, direction, execute=execute)

        if not execute:
            return pose

        # Move 1 block towards the target along X
        new_pose = self.primitives.step_forward_block()
        return new_pose

    # ------------------------------------------------------------------ helpers

    def _start_mining_phase(self, lane: Lane):
        """
        Mark the start of MINE_FORWARD for the current lane.
        """
        self.phase = LanePhase.MINE_FORWARD
        self._mine_started_mono = time.monotonic()
        self._mine_tick_counter = 0

        # Estimate lane properties
        lane_length_blocks = abs(lane.end_pose.x - lane.start_pose.x) + 1
        forward_sec = float(self.primitives.calibration.forward_block_seconds)
        ideal_lane_time = lane_length_blocks * forward_sec
        timeout_seconds = ideal_lane_time * self.lane_timeout_factor

        self._log(
            f"[ALIGN] Completed for Lane {lane.lane_id}; "
            f"ready to mine towards X={lane.end_pose.x} "
            f"(len={lane_length_blocks} blocks, ideal_time={ideal_lane_time:.1f}s, "
            f"timeout≈{timeout_seconds:.1f}s, factor={self.lane_timeout_factor:.2f})."
        )

    def _check_lane_timeout(self, lane: Lane) -> bool:
        """
        Returns True if the lane has exceeded its time budget and should complete.
        """
        if self._mine_started_mono is None:
            return False

        elapsed = time.monotonic() - self._mine_started_mono
        lane_length_blocks = abs(lane.end_pose.x - lane.start_pose.x) + 1
        forward_sec = float(self.primitives.calibration.forward_block_seconds)
        ideal_lane_time = lane_length_blocks * forward_sec
        timeout_seconds = ideal_lane_time * self.lane_timeout_factor

        if elapsed > timeout_seconds:
            self._log(
                f"[LANE COMPLETE] Lane {lane.lane_id} – timeout elapsed={elapsed:.1f}s "
                f"> timeout={timeout_seconds:.1f}s (len={lane_length_blocks} blocks, "
                f"ideal={ideal_lane_time:.1f}s, factor={self.lane_timeout_factor:.2f})."
            )
            return True

        return False

    def _advance_to_next_lane(self, execute: bool):
        """
        Move from COMPLETE to the next lane and immediately enter ALIGN phase.
        """
        if self.current_lane is None:
            return

        next_lane = self.planner.get_next_lane(self.current_lane)
        self._log(
            f"[COMPLETE] Lane {self.current_lane.lane_id} done → "
            f"next = Lane {next_lane.lane_id}"
        )
        self.start_lane(next_lane)

        # Note: mining keys are still owned by the MinerStateMachine; we do not
        # change key state here, only the planned lane and alignment.

    # ------------------------------------------------------------------ tick

    def tick(
        self,
        pose: Pose,
        *,
        execute: bool = False,
    ) -> LanePhase:
        """
        Advance the lane controller by one tick.

        Parameters
        ----------
        pose:
            Current best-guess Pose from the PoseProvider.
        execute:
            - False (default): logging-only, no real movement.
            - True: use MovementPrimitiveController to move/turn.

        Returns
        -------
        LanePhase
            Current phase after this tick.
        """
        # If no lane yet, choose the nearest and start it.
        if self.current_lane is None:
            lane = self.planner.get_nearest_lane(pose)
            self.start_lane(lane)

        lane = self.current_lane
        phase = self.phase

        if phase is LanePhase.ALIGN_TO_START:
            self._log(
                f"[ALIGN] Lane {lane.lane_id} – aligning pose "
                f"({pose.x}, {pose.y}, {pose.z}, {pose.facing.name}) "
                f"→ start ({lane.start_pose.x}, {lane.start_pose.y}, {lane.start_pose.z}, "
                f"{lane.direction.name}) (execute={execute})"
            )

            # Step 1: align Z, then X
            if not self._align_z_done:
                pose = self._align_along_z(pose, lane, execute=execute)
            elif not self._align_x_done:
                pose = self._align_along_x(pose, lane, execute=execute)

            # Once both axes are aligned, ensure facing matches lane direction.
            if self._align_z_done and self._align_x_done:
                pose = self._ensure_facing(pose, lane.direction, execute=execute)
                # Once we are at the start and facing correctly, switch to mining phase.
                self._start_mining_phase(lane)

        elif phase is LanePhase.MINE_FORWARD:
            # In this step we *do not* take over mining keys; we assume the
            # MinerStateMachine is handling W + left click when armed.
            self._mine_tick_counter += 1
            if self._mine_tick_counter % self.mine_tick_log_every == 0:
                self._log(
                    f"[MINE] Lane {lane.lane_id} – mining along lane towards "
                    f"end X={lane.end_pose.x} (tick={self._mine_tick_counter}, "
                    f"execute={execute})."
                )

            # Time-based lane completion: once timeout is exceeded, mark COMPLETE.
            if self._check_lane_timeout(lane):
                self.phase = LanePhase.COMPLETE

        if self.phase is LanePhase.COMPLETE:
            # Immediately transition to next lane; this keeps the lane planner
            # moving across the cube.
            self._advance_to_next_lane(execute=execute)

        return self.phase
