"""Worker HTTP service. Place this directory directly under a PlatEMO checkout."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException
import uvicorn


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_app(config_path: Path) -> FastAPI:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    root = Path(config["platemo_root"]).resolve()
    if not root.is_dir():
        raise RuntimeError(f"PlatEMO root does not exist: {root}")
    data_dir = (config_path.parent / config.get("data_dir", "Data")).resolve()
    queue_dir = data_dir / "queue"
    queue_dir.mkdir(parents=True, exist_ok=True)
    app = FastAPI(title="PlatEMO HPC Worker")
    app.state.config = config

    def verify(token: str | None) -> None:
        expected = config.get("worker_token", "")
        if expected and token != expected:
            raise HTTPException(401, "Invalid worker token")

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        return {"status": "ready", "platemo_root": str(root), "queued": len(list(queue_dir.glob("*.json"))), "time": now()}

    @app.get("/api/catalog")
    async def catalog(x_worker_token: str | None = Header(None)) -> dict[str, list[str]]:
        """Discover common PlatEMO entries without starting MATLAB."""
        verify(x_worker_token)
        def names(relative: str) -> list[str]:
            base = root / relative
            if not base.is_dir():
                return []
            return sorted(path.stem for path in base.rglob("*.m") if path.stem and not path.stem.startswith("@"))
        return {
            "algorithms": names("Algorithms"),
            "problems": names("Problems"),
            "metrics": names("Metrics"),
        }

    @app.post("/api/tasks")
    async def accept_task(payload: dict[str, Any], x_worker_token: str | None = Header(None)) -> dict[str, str]:
        verify(x_worker_token)
        task_id = str(payload.get("id", ""))
        if not task_id:
            raise HTTPException(400, "Task id is required")
        target = queue_dir / f"{task_id}.json"
        if target.exists():
            raise HTTPException(409, "Task already exists")
        payload["accepted_at"] = now()
        target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return {"status": "queued", "task_id": task_id}

    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("config.json"))
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6001)
    args = parser.parse_args()
    uvicorn.run(create_app(args.config.resolve()), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
