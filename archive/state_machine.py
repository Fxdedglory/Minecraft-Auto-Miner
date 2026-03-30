"""
minecraft_auto_miner.state_machine v0.4.3 – 2025-12-10
Generated with ChatGPT (GPT-5.1 Thinking) on 2025-12-10.

v0.4.1: Minimal FSM with two states:
    - IDLE
    - WALK_AND_MINE

Behaviour is intentionally equivalent to the v0.4.0
boolean is_mining flag, just structured as an FSM.

v0.4.2:
- Adds tick_with_profile(...) which takes a MiningProfile.
- If profile.use_sprint is True, attempts to call a sprint-capable
  controller method and falls back to hold_forward_and_mine() if
  that method is not present.
- Strafe / mouse_sweep are logged for now; movement is still
  primarily straight-line.

v0.4.3:
- Integrates FSM event logging via silver.fsm_event_log:
    * On each state transition, logs ENTER_STATE_<state>.
    * start_mining/stop_mining annotate source as 'hotkey'.
"""

from __future__ import annotations

from enum import Enum
import logging

from ..src.minecraft_auto_miner.input_controller import InputController
from .perception import FrameState
from .mining_profiles import MiningProfile
from ..src.minecraft_auto_miner.telemetry.fsm_event_log import log_fsm_event

LOGGER = logging.getLogger("minecraft_auto_miner.state_machine")


class MinerState(str, Enum):
    IDLE = "IDLE"
    WALK_AND_MINE = "WALK_AND_MINE"


class MinerStateMachine:
    """
    Minimal finite state machine for the miner.

    v0.4.1 semantics:
    - IDLE: stop_all()
    - WALK_AND_MINE: hold_forward_and_mine()

    v0.4.2:
    - tick_with_profile(...) adds MiningProfile-aware behaviour.

    v0.4.3:
    - Logs FSM transitions into silver.fsm_event_log so that
      episodes_from_silver can derive dominant state/action labels.
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or LOGGER
        self._state: MinerState = MinerState.IDLE
        self.logger.info("MinerStateMachine initialized in state %s.", self._state.value)

    @property
    def state(self) -> MinerState:
        return self._state

    # ----- public controls (triggered by hotkeys) -----

    def start_mining(self) -> None:
        """Transition to WALK_AND_MINE (hotkey-driven)."""
        self._transition(MinerState.WALK_AND_MINE, action_source="hotkey", action_name="USER_TOGGLE_MINING_ON")

    def stop_mining(self) -> None:
        """Transition to IDLE (hotkey-driven)."""
        self._transition(MinerState.IDLE, action_source="hotkey", action_name="USER_TOGGLE_MINING_OFF")

    # ----- core tick (legacy, profile-agnostic) -----

    def tick(self, frame: FrameState, controller: InputController) -> None:
        """
        Execute one FSM tick (profile-agnostic).

        Kept for backward compatibility; internally just delegates to
        tick_with_profile(...) with profile=None.
        """
        self.tick_with_profile(frame=frame, controller=controller, profile=None)

    # ----- core tick (profile-aware) -----

    def tick_with_profile(
        self,
        frame: FrameState,
        controller: InputController,
        profile: MiningProfile | None,
    ) -> None:
        """
        Execute one FSM tick, optionally using a MiningProfile to adjust behaviour.

        v0.4.2:
        - If profile is None, behaviour matches v0.4.1.
        - If profile.use_sprint is True, attempts to use a sprint-capable
          controller method (if available), else falls back.
        - Strafe / mouse_sweep are logged but not yet implemented as
          real movement patterns (future work).
        """
        _ = frame  # unused for now

        if self._state == MinerState.IDLE:
            controller.stop_all()
            return

        if self._state != MinerState.WALK_AND_MINE:
            # Defensive: unknown state -> fail safe.
            self.logger.warning(
                "Unknown state %s; failing safe with stop_all().",
                self._state,
            )
            controller.stop_all()
            return

        # WALK_AND_MINE behaviour
        if profile is None:
            # Legacy behaviour
            controller.hold_forward_and_mine()
            return

        # Log the profile we are applying (useful for debugging)
        self.logger.debug(
            "tick_with_profile: state=%s, profile=%s (sprint=%s, strafe=%s, sweep=%s)",
            self._state.value,
            profile.name,
            profile.use_sprint,
            profile.strafe_pattern,
            profile.mouse_sweep,
        )

        # 1) Sprint behaviour
        if profile.use_sprint:
            try:
                controller.hold_forward_sprint_and_mine()
                return
            except AttributeError:
                self.logger.debug(
                    "InputController has no hold_forward_sprint_and_mine(); "
                    "falling back to hold_forward_and_mine()."
                )

        # 2) TODO: strafe_pattern and mouse_sweep
        if profile.strafe_pattern != "none" or profile.mouse_sweep != "none":
            self.logger.debug(
                "Non-default profile behaviour requested: strafe_pattern=%s, "
                "mouse_sweep=%s (not yet implemented; straight-line mining used).",
                profile.strafe_pattern,
                profile.mouse_sweep,
            )

        # Fallback / default: straight-line mining
        controller.hold_forward_and_mine()

    # ----- internal helpers -----

    def _transition(
        self,
        new_state: MinerState,
        *,
        action_source: str = "fsm",
        action_name: str | None = None,
    ) -> None:
        """
        Internal transition helper.

        Logs:
          - local logger: "old -> new"
          - FSM table: state_name=new_state, action_name=ENTER_STATE_<new>
            (or supplied action_name).
        """
        if new_state == self._state:
            # No-op but still useful for debugging if needed
            self.logger.debug(
                "Transition requested to same state %s; ignoring.",
                new_state.value,
            )
            return

        old_state = self._state
        self._state = new_state
        self.logger.info("State transition: %s -> %s", old_state.value, new_state.value)

        # FSM log: state is the *new* state, action is "ENTER_STATE_<STATE>" unless overridden.
        try:
            effective_action_name = action_name or f"ENTER_STATE_{new_state.value}"
            log_fsm_event(
                state_name=new_state.value,
                action_name=effective_action_name,
                source=action_source,
                extra={
                    "old_state": old_state.value,
                    "new_state": new_state.value,
                },
            )
        except Exception as e:
            # Do not crash miner if logging fails.
            self.logger.error("Failed to log FSM event: %s", e)
