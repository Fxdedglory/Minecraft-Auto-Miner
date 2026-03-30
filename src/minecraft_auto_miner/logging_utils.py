"""
minecraft_auto_miner.logging_utils v0.4.0 – 2025-12-06
Generated with ChatGPT (GPT-5.1 Thinking) on 2025-12-06.

Logging strategy:
- Each run writes to a timestamped file: logs/mining_helper_YYYYMMDD_HHMMSS.log
- logs/mining_helper.log is truncated at startup and always reflects the latest run.
"""

from __future__ import annotations

import logging
from logging import Logger
from pathlib import Path
from typing import Union
from datetime import datetime


def setup_logging(log_dir: Union[str, Path] = "logs") -> Logger:
    """
    Configure a root logger that logs to:
      - A per-run timestamped file: mining_helper_YYYYMMDD_HHMMSS.log
      - A 'latest' file: mining_helper.log (truncated on each run)
      - Console (stdout)

    This keeps history for old runs while preventing the main log file
    from growing unbounded.
    """
    log_dir_path = Path(log_dir)
    log_dir_path.mkdir(parents=True, exist_ok=True)

    # Per-run filename with timestamp (local time)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    latest_log = log_dir_path / "mining_helper.log"
    run_log = log_dir_path / f"mining_helper_{run_ts}.log"

    logger = logging.getLogger("minecraft_auto_miner")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if setup is called twice in the same process
    if logger.handlers:
        return logger

    # File handler for the "latest" log (truncate on each run)
    latest_file_handler = logging.FileHandler(
        latest_log,
        mode="w",            # truncate on each run
        encoding="utf-8",
    )
    latest_file_handler.setLevel(logging.INFO)

    # File handler for the per-run, timestamped log
    run_file_handler = logging.FileHandler(
        run_log,
        mode="w",
        encoding="utf-8",
    )
    run_file_handler.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    latest_file_handler.setFormatter(formatter)
    run_file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(latest_file_handler)
    logger.addHandler(run_file_handler)
    logger.addHandler(console_handler)

    logger.info("Logging initialized.")
    logger.info("Latest log file: %s", latest_log)
    logger.info("Run log file   : %s", run_log)

    return logger
