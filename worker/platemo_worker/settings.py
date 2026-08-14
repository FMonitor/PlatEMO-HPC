"""Resolve and snapshot the optional MATLAB Settings baseline."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any


def resolve_settings(platemo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    """Return task fields for the local Settings MAT, without trusting paths."""
    name = str(payload.get("settings_file", "") or "")
    if not name:
        return {}
    relative = Path(name)
    if relative.is_absolute() or relative.name != name or any(char in name for char in "\\/\"';&|`$"):
        raise ValueError("invalid settings_file")
    candidates = (platemo_root / relative, platemo_root / "Data" / relative)
    setting = next((path for path in candidates if path.is_file() and os.access(path, os.R_OK)), None)
    if setting is None:
        raise FileNotFoundError(name)
    digest = hashlib.sha256(setting.read_bytes()).hexdigest()
    expected_digest = str(payload.get("settings_sha256", "") or "")
    if expected_digest and digest != expected_digest:
        raise ValueError("settings_sha256 mismatch")
    return {
        "settings_file": setting.name,
        "settings_file_path": str(setting.resolve()),
        # Preserve the Master-issued value when supplied. It is equal to the
        # verified local digest, while an unpinned local setting records its own.
        "settings_sha256": expected_digest or digest,
    }
