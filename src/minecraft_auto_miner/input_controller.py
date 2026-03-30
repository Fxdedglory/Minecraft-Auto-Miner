"""
minecraft_auto_miner.input_controller v0.4.10 – 2025-12-06
Generated with ChatGPT (GPT-5.1 Thinking) on 2025-12-06.

v0.4.9:
- Switches from keyboard.add_hotkey() to polling-based hotkeys.
- F8/F9 are now checked every tick via keyboard.is_pressed().
- register_hotkeys() becomes a no-op logger (for compatibility with app.py).
- Keeps profile-aware sprint support (use_sprint flag).

v0.4.10:
- Adds hold_forward_sprint_and_mine() for explicit sprint behaviour,
  used by MinerStateMachine.tick_with_profile(...) when a sprinting
  profile is active.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Callable, Optional

import pyautogui
import keyboard  # type: ignore

LOGGER = logging.getLogger("minecraft_auto_miner.input")
# Mouse sensitivity assumptions for yaw/pitch conversions.
# v0.5.0 – 2025-12-06
YAW_DEG_PER_PIXEL = 0.15
PITCH_DEG_PER_PIXEL = 0.08


@dataclass
class InputConfig:
    """
    Configuration for input/hotkeys.

    start_stop:
        Hotkey to toggle mining on/off.
    panic_stop:
        Hotkey to immediately stop and exit.
    forward_key:
        Key to hold for forward movement (default: 'w').
    sprint_key:
        Key to hold for sprinting while mining (default: 'shift').
    mine_button:
        Mouse button to hold for mining (default: 'left').
    """

    start_stop: str
    panic_stop: str
    forward_key: str = "w"
    sprint_key: str = "shift"
    mine_button: str = "left"


class InputController:
    """
    Encapsulates keyboard/mouse control and hotkey handling.

    Public methods:
    - register_hotkeys(on_toggle, on_panic)  # logs only (no global hooks)
    - poll_hotkeys(on_toggle, on_panic)      # called every tick
    - hold_forward_and_mine()
    - hold_forward_sprint_and_mine()
    - stop_all()
    - shutdown()
    - set_profile(profile)
    """

    def __init__(self, config: InputConfig, logger: Optional[logging.Logger] = None):
        self.logger = logger or LOGGER
        self.config = config

        # Active profile (used for sprint; can be None)
        self.active_profile: Any = None

        # Internal state flags for held keys/buttons
        self._is_forward_down: bool = False
        self._is_sprint_down: bool = False
        self._is_mining_down: bool = False

        # Edge detection for hotkeys (so holding F8/F9 doesn't spam)
        self._toggle_was_down: bool = False
        self._panic_was_down: bool = False

    # ----- construction helpers -----

    @classmethod
    def from_config(
        cls,
        raw_config: dict[str, Any],
        logger: Optional[logging.Logger] = None,
    ) -> "InputController":
        hotkeys = raw_config.get("hotkeys", {}) or {}
        start_stop = str(hotkeys.get("start_stop", "f8"))
        panic_stop = str(hotkeys.get("panic_stop", "f9"))

        movement_cfg = raw_config.get("movement", {}) or {}
        forward_key = str(movement_cfg.get("forward_key", "w"))
        sprint_key = str(movement_cfg.get("sprint_key", "shift"))
        mine_button = str(movement_cfg.get("mine_button", "left"))

        cfg = InputConfig(
            start_stop=start_stop,
            panic_stop=panic_stop,
            forward_key=forward_key,
            sprint_key=sprint_key,
            mine_button=mine_button,
        )

        log = logger or LOGGER
        log.info("InputController config resolved: %s", cfg)

        return cls(config=cfg, logger=log)

    # ----- profile control -----

    def set_profile(self, profile: Any) -> None:
        """
        Set the active mining profile.

        Currently used to decide whether sprint is enabled.
        """
        self.active_profile = profile
        self.logger.info(
            "InputController: active profile set to %s",
            getattr(profile, "name", str(profile)),
        )

    # ----- hotkey handling -----

    def register_hotkeys(
        self,
        on_toggle: Callable[[], None],
        on_panic: Callable[[], None],
    ) -> None:
        """
        Compatibility stub: we no longer use global OS hooks.

        Hotkeys are now handled via poll_hotkeys() inside the main loop,
        using keyboard.is_pressed().

        This method just logs which keys to use.
        """
        self.logger.info(
            "Hotkeys (polled each tick): toggle=%s, panic=%s",
            self.config.start_stop,
            self.config.panic_stop,
        )

    def poll_hotkeys(
        self,
        on_toggle: Callable[[], None],
        on_panic: Callable[[], None],
    ) -> None:
        """
        Check hotkey state using edge detection.

        Called once per main-loop tick from app.py.
        """
        # Toggle / start-stop
        toggle_down = keyboard.is_pressed(self.config.start_stop)
        if toggle_down and not self._toggle_was_down:
            self.logger.info(
                "Start/Stop hotkey pressed (%s).", self.config.start_stop
            )
            on_toggle()
        self._toggle_was_down = toggle_down

        # Panic
        panic_down = keyboard.is_pressed(self.config.panic_stop)
        if panic_down and not self._panic_was_down:
            self.logger.warning(
                "PANIC hotkey pressed (%s).", self.config.panic_stop
            )
            on_panic()
        self._panic_was_down = panic_down

    # ----- mining control -----

    def hold_forward_and_mine(self) -> None:
        """
        Ensure the forward key and mining button (and optionally sprint)
        are held down while mining.

        Sprint is enabled if the active profile has use_sprint=True.
        """
        # Forward
        if not self._is_forward_down:
            pyautogui.keyDown(self.config.forward_key)
            self._is_forward_down = True

        # Sprint (based on active profile)
        use_sprint = bool(getattr(self.active_profile, "use_sprint", False))
        if use_sprint and not self._is_sprint_down:
            pyautogui.keyDown(self.config.sprint_key)
            self._is_sprint_down = True
        elif not use_sprint and self._is_sprint_down:
            # Profile changed to a non-sprint profile mid-run
            pyautogui.keyUp(self.config.sprint_key)
            self._is_sprint_down = False

        # Mining button
        if not self._is_mining_down:
            pyautogui.mouseDown(button=self.config.mine_button)
            self._is_mining_down = True

    def hold_forward_sprint_and_mine(self) -> None:
        """
        Explicit sprinting variant used by FSM when a sprinting profile
        is active.

        This forces sprint ON while mining, regardless of the
        InputController.active_profile.use_sprint flag.
        """
        # Forward
        if not self._is_forward_down:
            pyautogui.keyDown(self.config.forward_key)
            self._is_forward_down = True

        # Sprint: always on during this call
        if not self._is_sprint_down:
            pyautogui.keyDown(self.config.sprint_key)
            self._is_sprint_down = True

        # Mining button
        if not self._is_mining_down:
            pyautogui.mouseDown(button=self.config.mine_button)
            self._is_mining_down = True

    def stop_all(self) -> None:
        """
        Release all keys/buttons controlled by the miner.

        Logging is only emitted if we actually release something, to avoid
        spamming the logs on repeated calls when already idle.
        """
        released_any = False

        if self._is_forward_down:
            pyautogui.keyUp(self.config.forward_key)
            self._is_forward_down = False
            released_any = True

        if self._is_sprint_down:
            pyautogui.keyUp(self.config.sprint_key)
            self._is_sprint_down = False
            released_any = True

        if self._is_mining_down:
            pyautogui.mouseUp(button=self.config.mine_button)
            self._is_mining_down = False
            released_any = True

        if released_any:
            self.logger.info(
                "InputController shutting down; releasing all keys/buttons."
            )

    def move_mouse_relative(self, dx: int, dy: int) -> None:
        """
        Move the mouse relative to the current position.

        This is a low-level primitive; higher-level strategies can call this
        with small dx/dy values to adjust aim or sweep the crosshair.
        """
        pyautogui.moveRel(dx, dy, duration=0)

    # ----- mouse utilities (v0.5.0 – 2025-12-06) -----

    def move_mouse(self, dx: int, dy: int) -> None:
        """
        Move the mouse by a relative offset in pixels.

        Positive dx -> right, negative dx -> left
        Positive dy -> down,  negative dy -> up
        """
        try:
            pyautogui.moveRel(dx, dy, duration=0)
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.warning("move_mouse failed: %s", exc)

    def yaw_relative(self, degrees: float) -> None:
        """
        Turn the camera left/right by an approximate number of degrees.

        Positive degrees -> look right, negative -> look left.
        Uses a fixed DEG_PER_PIXEL heuristic; tune YAW_DEG_PER_PIXEL as needed.
        """
        if degrees == 0:
            return
        pixels = int(degrees / YAW_DEG_PER_PIXEL)
        if pixels == 0:
            return
        self.move_mouse(pixels, 0)

    def pitch_relative(self, degrees: float) -> None:
        """
        Look up/down by an approximate number of degrees.

        Positive degrees -> look up, negative -> look down.
        Uses a fixed DEG_PER_PIXEL heuristic; tune PITCH_DEG_PER_PIXEL as needed.
        """
        if degrees == 0:
            return
        pixels = int(degrees / PITCH_DEG_PER_PIXEL)
        if pixels == 0:
            return
        # In most setups, negative Y moves the cursor up on screen.
        self.move_mouse(0, -pixels)

    # ----- chat helpers (/back support) (v0.5.0 – 2025-12-06) -----

    def send_chat_command(self, text: str) -> None:
        """
        Send a chat command in Minecraft by:
        - pressing Enter
        - typing the given text
        - pressing Enter again

        NOTE: Best used when the miner is *not* currently holding movement/mining
        keys, otherwise movement may continue while the chat is open.
        """
        try:
            self.logger.info("Sending chat command: %s", text)
            pyautogui.press("enter")
            # Small interval to reduce dropped keystrokes on slower systems.
            pyautogui.typewrite(text, interval=0.01)
            pyautogui.press("enter")
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.warning("send_chat_command failed: %s", exc)

    def send_back_command(self) -> None:
        """
        Convenience wrapper for sending the '/gmine' command to return to the
        mineable area. Uses the standard chat open/confirm sequence.
        """
        self.send_chat_command("/gmine")


    # ----- lifecycle -----

    def shutdown(self) -> None:
        """
        Cleanly release inputs.
        """
        self.stop_all()
        self.logger.info("All hotkeys unhooked (polling-based; no global hooks).")
