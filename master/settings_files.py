"""Settings MAT storage and safe distribution helpers for the Master."""
from __future__ import annotations

import hashlib
from pathlib import Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def uploaded_settings_path(uploads_dir: Path, experiment_id: str, filename: str) -> Path:
    """Return an upload path while rejecting a filename that could escape its directory."""
    safe_name = Path(filename).name
    if not safe_name or safe_name != filename:
        raise ValueError("invalid settings filename")
    return uploads_dir / experiment_id / safe_name


def is_distributable_upload(path: Path, uploads_dir: Path) -> bool:
    try:
        path.resolve().relative_to(uploads_dir.resolve())
    except ValueError:
        return False
    return path.is_file()
