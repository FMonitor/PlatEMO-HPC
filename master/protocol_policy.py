"""Pure policy checks shared by Master API routes and scheduler tests."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable


DETERMINISTIC_REJECTION_CODES = {
    "input_incompatible",
    "profile_unavailable",
    "version_mismatch",
    "insufficient_disk",
}


def worker_can_run(capabilities: dict[str, Any], config: dict[str, Any]) -> bool:
    """Return whether a reported Worker environment can satisfy an experiment."""
    profile = str(config.get("cluster_profile", "") or "")
    if profile and str(capabilities.get("cluster_profile", "") or "") != profile:
        return False
    commit = str(config.get("required_platemo_commit", "") or "")
    if commit and str(capabilities.get("platemo_commit", "") or "") != commit:
        return False
    required_disk = int(config.get("minimum_disk_free_bytes", 0) or 0)
    try:
        free_disk = int(capabilities.get("disk_free_bytes", 0) or 0)
    except (TypeError, ValueError):
        return False
    return free_disk >= required_disk


def rejection_block_expiry(code: str) -> str | None:
    """Permanent blocks prevent deterministic re-assignment loops."""
    return None if code in DETERMINISTIC_REJECTION_CODES else datetime.now(timezone.utc).isoformat()


def completed_artifacts_are_covered(
    completed_seeds: Iterable[int], artifacts: Iterable[tuple[int | None, str]],
) -> bool:
    """A batch aggregate covers all seeds; otherwise every completed seed needs one."""
    completed = set(completed_seeds)
    return completed.issubset(completed_seeds_with_artifacts(completed, artifacts))


def completed_seeds_with_artifacts(
    completed_seeds: Iterable[int], artifacts: Iterable[tuple[int | None, str]],
) -> set[int]:
    """Return completed Seeds backed by a batch result or a Seed artifact."""
    completed = set(completed_seeds)
    registered = list(artifacts)
    if any(seed is None and kind == "batch_result" for seed, kind in registered):
        return completed
    return completed.intersection(seed for seed, _ in registered if seed is not None)
