"""Persistable summaries reconstructed from MATLAB progress files."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_progress(work_dir: Path) -> dict[str, Any]:
    try:
        value = json.loads((work_dir / "progress.json").read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def build_summary(payload: dict[str, Any], work_dir: Path, state: str, exit_code: int | None) -> dict[str, Any]:
    result = work_dir / "result.mat"
    summary: dict[str, Any] = {
        "experiment_point_id": str(payload.get("experiment_point_id", "")),
        "batch_attempt_id": str(payload.get("batch_attempt_id", "")),
        "state": state,
        "exit_code": exit_code,
        "result": str(result) if result.is_file() else None,
        "sha256": sha256(result) if result.is_file() else None,
    }
    progress = read_progress(work_dir)
    for key in ("runs", "completed_runs", "failed_runs", "running_runs", "total_runs", "pool"):
        if key in progress:
            summary[key] = progress[key]
    return summary


def mark_unfinished_cancelled(summary: dict[str, Any], seeds: list[int], max_fe: int) -> None:
    terminal = {"completed", "failed", "cancelled"}
    known = {item.get("seed"): dict(item) for item in summary.get("runs", [])
             if isinstance(item, dict) and isinstance(item.get("seed"), int)}
    runs: list[dict[str, Any]] = []
    for seed in seeds:
        run = known.get(seed, {"seed": seed, "fe": 0, "total_fe": max_fe, "elapsed_seconds": 0, "error": ""})
        if str(run.get("state", "queued")) not in terminal:
            run["state"] = "cancelled"
            run["error"] = str(run.get("error") or "cancelled by Master")
        runs.append(run)
    summary["runs"] = runs
    summary["completed_runs"] = sum(run.get("state") == "completed" for run in runs)
    summary["failed_runs"] = sum(run.get("state") == "failed" for run in runs)
    summary["running_runs"] = 0
    summary["total_runs"] = len(runs)
