"""Master web service for dispatching PlatEMO experiment tasks."""
from __future__ import annotations

import argparse
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import uvicorn

APP_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.results_dir = data_dir / "results"
        self.uploads_dir = data_dir / "uploads"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = data_dir / "master.sqlite3"
        with self.connect() as con:
            con.executescript("""
                CREATE TABLE IF NOT EXISTS workers (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL,
                    token TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY, worker_id TEXT, state TEXT NOT NULL,
                    payload TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    error TEXT NOT NULL DEFAULT ''
                );
            """)

    def connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        return con

    def workers(self) -> list[dict[str, Any]]:
        with self.connect() as con:
            return [dict(row) for row in con.execute("SELECT * FROM workers ORDER BY created_at DESC")]

    def tasks(self) -> list[dict[str, Any]]:
        with self.connect() as con:
            return [dict(row) for row in con.execute("SELECT * FROM tasks ORDER BY created_at DESC LIMIT 100")]


def create_app(data_dir: Path) -> FastAPI:
    store = Store(data_dir)
    app = FastAPI(title="PlatEMO HPC Master")
    app.state.store = store

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request, message: str = "") -> HTMLResponse:
        workers = store.workers()
        async with httpx.AsyncClient(timeout=3) as client:
            for worker in workers:
                headers = {"X-Worker-Token": worker["token"]} if worker["token"] else {}
                try:
                    response = await client.get(f"{worker['url']}/api/health", headers=headers)
                    response.raise_for_status()
                    health = response.json()
                    worker.update({"online": True, "queue_count": health.get("queued", 0),
                                   "last_check": now(), "health_error": ""})
                except httpx.HTTPError as exc:
                    worker.update({"online": False, "queue_count": "-", "last_check": now(),
                                   "health_error": str(exc)})
        return templates.TemplateResponse(request, "index.html", {
            "workers": workers, "tasks": store.tasks(), "data_dir": str(data_dir), "message": message,
        })

    @app.get("/api/workers")
    async def list_workers() -> list[dict[str, Any]]:
        return [{key: value for key, value in worker.items() if key != "token"} for worker in store.workers()]

    @app.post("/api/workers")
    async def add_worker(request: Request, name: str = Form(...), url: str = Form(...), token: str = Form("")) -> RedirectResponse:
        worker_id = str(uuid.uuid4())
        with store.connect() as con:
            con.execute("INSERT INTO workers VALUES (?, ?, ?, ?, ?)",
                        (worker_id, name, url.rstrip("/"), token, now()))
        return RedirectResponse(url=f"/?message=Worker+added%3A+{worker_id}", status_code=303)

    @app.post("/api/workers/{worker_id}/delete")
    async def delete_worker(worker_id: str) -> RedirectResponse:
        with store.connect() as con:
            worker = con.execute("SELECT id FROM workers WHERE id = ?", (worker_id,)).fetchone()
            if worker is None:
                raise HTTPException(404, "Worker not found")
            active = con.execute(
                "SELECT COUNT(*) AS count FROM tasks WHERE worker_id = ? AND state IN ('dispatching', 'queued', 'running')",
                (worker_id,),
            ).fetchone()["count"]
            if active:
                return RedirectResponse(url="/?message=Cannot+delete+worker+with+active+tasks", status_code=303)
            con.execute("DELETE FROM workers WHERE id = ?", (worker_id,))
        return RedirectResponse(url="/?message=Worker+deleted", status_code=303)

    @app.post("/api/tasks")
    async def create_task(worker_id: str = Form(...), algorithm: str = Form(...), problem: str = Form(...),
                          seeds: str = Form("1"), pool_size: int = Form(1), settings: UploadFile | None = File(None)) -> RedirectResponse:
        try:
            seed_list = [int(value.strip()) for value in seeds.split(",") if value.strip()]
        except ValueError as exc:
            raise HTTPException(400, "Seeds must be comma-separated integers") from exc
        task_id = str(uuid.uuid4())
        payload: dict[str, Any] = {"id": task_id, "algorithm": algorithm, "problem": problem,
                                   "seeds": seed_list, "pool_size": pool_size, "created_at": now()}
        if settings and settings.filename:
            settings_dir = store.uploads_dir / task_id
            settings_dir.mkdir()
            destination = settings_dir / "settings.mat"
            destination.write_bytes(await settings.read())
            payload["settings_file"] = str(destination)
        with store.connect() as con:
            worker = con.execute("SELECT * FROM workers WHERE id = ?", (worker_id,)).fetchone()
            if worker is None:
                raise HTTPException(404, "Worker not found")
            con.execute("INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, '')",
                        (task_id, worker_id, "dispatching", json.dumps(payload), now(), now()))
        headers = {"X-Worker-Token": worker["token"]} if worker["token"] else {}
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.post(f"{worker['url']}/api/tasks", json=payload, headers=headers)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            with store.connect() as con:
                con.execute("UPDATE tasks SET state = ?, error = ?, updated_at = ? WHERE id = ?",
                            ("dispatch_failed", str(exc), now(), task_id))
            raise HTTPException(502, f"Worker did not accept task: {exc}") from exc
        with store.connect() as con:
            con.execute("UPDATE tasks SET state = ?, updated_at = ? WHERE id = ?", ("queued", now(), task_id))
        return RedirectResponse(url=f"/?message=Task+queued%3A+{task_id}", status_code=303)

    @app.post("/api/results/{task_id}")
    async def receive_result(task_id: str, result: UploadFile = File(...), state: str = Form("completed"), error: str = Form("")) -> dict[str, str]:
        task_dir = store.results_dir / task_id
        task_dir.mkdir(parents=True, exist_ok=True)
        filename = Path(result.filename or "result.mat").name
        (task_dir / filename).write_bytes(await result.read())
        with store.connect() as con:
            con.execute("UPDATE tasks SET state = ?, error = ?, updated_at = ? WHERE id = ?",
                        (state, error, now(), task_id))
        return {"status": "stored", "path": str(task_dir / filename)}

    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path.cwd() / "Data")
    parser.add_argument("--host", default="0.0.0.0")
    # Chromium blocks port 6000 as unsafe, so use a nearby browser-safe default.
    parser.add_argument("--port", type=int, default=6080)
    args = parser.parse_args()
    uvicorn.run(create_app(args.data_dir.resolve()), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
