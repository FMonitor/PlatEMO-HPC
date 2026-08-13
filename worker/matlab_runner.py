"""Small helpers shared by tests and operational tooling for worker runs."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
from typing import Any

def load_task(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("task JSON must contain an object")
    return value

def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
