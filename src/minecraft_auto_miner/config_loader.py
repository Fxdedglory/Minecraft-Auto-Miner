"""
minecraft_auto_miner.config_loader v0.5.0 – 2025-12-07
Generated with ChatGPT (GPT-5.1 Thinking) on 2025-12-07.

Config loader remains intentionally simple:
- Shallow-merge YAML into DEFAULT_CONFIG.
- Most sections are read directly from the YAML by app.py.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Union

import logging

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore

LOGGER = logging.getLogger("minecraft_auto_miner.config")


DEFAULT_CONFIG: Dict[str, Any] = {
    "hotkeys": {
        "start_stop": "f8",
        "panic_stop": "f9",
    },
    "loop": {
        "tick_interval_seconds": 0.08,  # ~12.5 ticks per second
    },
    # Other sections (movement, perception, world_model, lanes, etc.)
    # are expected to be provided by config/config.yaml and are read
    # directly by app.py using config.get(...).
}


def _shallow_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Shallow-merge two dicts: values in 'override' win.

    v0.5.x keeps this intentionally simple.
    """
    merged = deepcopy(base)
    for key, value in override.items():
        merged[key] = value
    return merged


def load_config(path: Union[str, Path] = Path("config/config.yaml")) -> Dict[str, Any]:
    """
    Load config from YAML file if present; otherwise return defaults.

    - If YAML is missing → log info + return DEFAULT_CONFIG.
    - If YAML is present but yaml library is missing → log warning and return defaults.
    - If YAML is present and parsed → shallow-merge into DEFAULT_CONFIG.
    """
    path = Path(path)

    if not path.exists():
        LOGGER.info("Config file not found at %s, using DEFAULT_CONFIG", path)
        return deepcopy(DEFAULT_CONFIG)

    if yaml is None:
        LOGGER.warning(
            "PyYAML not available; cannot read %s. Using DEFAULT_CONFIG.", path
        )
        return deepcopy(DEFAULT_CONFIG)

    try:
        raw = path.read_text(encoding="utf-8")
        data = yaml.safe_load(raw) or {}
        if not isinstance(data, dict):
            LOGGER.warning(
                "Config file %s did not contain a top-level mapping; using defaults.",
                path,
            )
            return deepcopy(DEFAULT_CONFIG)

        merged = _shallow_merge(DEFAULT_CONFIG, data)
        LOGGER.info("Loaded config from %s with overrides: %s", path, data)
        return merged
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.exception("Failed to load config from %s: %s", path, exc)
        return deepcopy(DEFAULT_CONFIG)
