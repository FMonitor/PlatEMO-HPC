"""Worker lifecycle logging shared by the control and runtime paths."""
from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def create_worker_logger(data_dir: Path, worker_id: str) -> logging.Logger:
    """Create a concise console logger backed by a rotating UTF-8 log file."""
    log_dir = data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"platemo_worker.{worker_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    log_path = (log_dir / "worker.log").resolve()
    if any(getattr(handler, "baseFilename", None) == str(log_path) for handler in logger.handlers):
        return logger

    formatter = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    file_handler = RotatingFileHandler(log_path, maxBytes=2 * 1024 * 1024, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
