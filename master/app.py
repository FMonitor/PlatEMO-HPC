"""Master web service for dispatching PlatEMO experiment tasks."""
from __future__ import annotations

import argparse
import asyncio
import json
from io import BytesIO
import sqlite3
import uuid
import hashlib
from datetime import timedelta
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from scipy.io import loadmat
from fastapi.templating import Jinja2Templates
import uvicorn

from platemo import (
    discover_catalogs,
    discover_existing_tests,
    list_setting_files,
    native_settings_mat,
    parse_platemo_setting_file,
    parse_setting_data,
)

APP_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))
STATIC_DIR = APP_DIR / "static"


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, data_dir: Path, platemo_path: Path | None = None) -> None:
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
                    token TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                    online INTEGER NOT NULL DEFAULT 0, queue_count INTEGER,
                    last_check TEXT NOT NULL DEFAULT '', health_error TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY, worker_id TEXT, state TEXT NOT NULL,
                    payload TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    error TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    task_id TEXT NOT NULL, attempt_id TEXT PRIMARY KEY, attempt_no INTEGER NOT NULL,
                    lease_token TEXT NOT NULL, state TEXT NOT NULL, started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL DEFAULT '', exit_code INTEGER, error TEXT NOT NULL DEFAULT '',
                    completed_runs INTEGER NOT NULL DEFAULT 0, failed_runs INTEGER NOT NULL DEFAULT 0,
                    running_runs INTEGER NOT NULL DEFAULT 0, total_runs INTEGER NOT NULL DEFAULT 0,
                    pool_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS seed_runs (
                    task_id TEXT NOT NULL, attempt_id TEXT NOT NULL, seed INTEGER NOT NULL,
                    state TEXT NOT NULL, fe INTEGER NOT NULL DEFAULT 0, total_fe INTEGER NOT NULL DEFAULT 0,
                    elapsed_seconds REAL NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL,
                    PRIMARY KEY (attempt_id, seed)
                );
                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY, task_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
                    kind TEXT NOT NULL, path TEXT NOT NULL, sha256 TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, task_id TEXT, attempt_id TEXT,
                    payload TEXT NOT NULL, created_at TEXT NOT NULL
                );
            """)
            if platemo_path is not None:
                con.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('platemo_path', ?)",
                            (str(platemo_path.resolve()),))
            # Existing databases from the first scaffold do not have health columns.
            columns = {row["name"] for row in con.execute("PRAGMA table_info(workers)")}
            for name, definition in (
                ("online", "INTEGER NOT NULL DEFAULT 0"),
                ("queue_count", "INTEGER"),
                ("last_check", "TEXT NOT NULL DEFAULT ''"),
                ("health_error", "TEXT NOT NULL DEFAULT ''"),
                ("capabilities_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("last_heartbeat", "TEXT NOT NULL DEFAULT ''"),
                ("node_token", "TEXT NOT NULL DEFAULT ''"),
                ("status", "TEXT NOT NULL DEFAULT 'unknown'"),
                ("priority", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if name not in columns:
                    con.execute(f"ALTER TABLE workers ADD COLUMN {name} {definition}")

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

    def update_health(self, worker_id: str, online: bool, queue_count: int | None, error: str) -> None:
        with self.connect() as con:
            # A reachability probe is not a scheduling heartbeat. Only a signed
            # Worker heartbeat can renew its lease liveness.
            con.execute("UPDATE workers SET online = ?, queue_count = ?, last_check = ?, health_error = ?, status = ? WHERE id = ?",
                        (int(online), queue_count, now(), error, "online" if online else "offline", worker_id))

    def event(self, event_type: str, task_id: str | None, attempt_id: str | None, payload: dict[str, Any]) -> None:
        with self.connect() as con:
            con.execute("INSERT INTO events (type, task_id, attempt_id, payload, created_at) VALUES (?, ?, ?, ?, ?)",
                        (event_type, task_id, attempt_id, json.dumps(payload, ensure_ascii=False), now()))

    def create_attempt(self, task_id: str, seeds: list[int]) -> tuple[str, str]:
        attempt_id, lease_token = str(uuid.uuid4()), str(uuid.uuid4())
        with self.connect() as con:
            attempt_no = con.execute("SELECT COUNT(*) FROM attempts WHERE task_id = ?", (task_id,)).fetchone()[0] + 1
            con.execute("INSERT INTO attempts (task_id, attempt_id, attempt_no, lease_token, state, started_at, total_runs) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (task_id, attempt_id, attempt_no, lease_token, "dispatching", now(), len(seeds)))
            for seed in seeds:
                con.execute("INSERT INTO seed_runs (task_id, attempt_id, seed, state, total_fe, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                            (task_id, attempt_id, seed, "queued", 0, now()))
        return attempt_id, lease_token

    def attempt(self, task_id: str, attempt_id: str, lease_token: str) -> sqlite3.Row | None:
        with self.connect() as con:
            return con.execute("SELECT * FROM attempts WHERE task_id = ? AND attempt_id = ? AND lease_token = ?", (task_id, attempt_id, lease_token)).fetchone()

    def apply_progress(self, task_id: str, attempt_id: str, progress: dict[str, Any]) -> None:
        runs = progress.get("runs", [])
        if not isinstance(runs, list):
            raise ValueError("runs must be an array")
        state = str(progress.get("state") or progress.get("phase") or "running")
        with self.connect() as con:
            con.execute("UPDATE attempts SET state = ?, completed_runs = ?, failed_runs = ?, running_runs = ?, total_runs = ?, pool_json = ? WHERE task_id = ? AND attempt_id = ?",
                        (state, int(progress.get("completed_runs", 0)), int(progress.get("failed_runs", 0)),
                         int(progress.get("running_runs", 0)), int(progress.get("total_runs", len(runs))),
                         json.dumps(progress.get("pool", {})), task_id, attempt_id))
            for item in runs:
                if not isinstance(item, dict) or not isinstance(item.get("seed"), int):
                    continue
                con.execute("INSERT INTO seed_runs (task_id, attempt_id, seed, state, fe, total_fe, elapsed_seconds, error, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                            "ON CONFLICT(attempt_id, seed) DO UPDATE SET state=excluded.state, fe=excluded.fe, total_fe=excluded.total_fe, elapsed_seconds=excluded.elapsed_seconds, error=excluded.error, updated_at=excluded.updated_at",
                            (task_id, attempt_id, item["seed"], str(item.get("state", "running")), int(item.get("fe", 0)),
                             int(item.get("total_fe", 0)), float(item.get("elapsed_seconds", 0)), str(item.get("error", "")), now()))
            con.execute("UPDATE tasks SET state = CASE WHEN state='cancel_requested' THEN state ELSE ? END, updated_at = ? WHERE id = ?", (state, now(), task_id))
        self.event("task.progress", task_id, attempt_id, progress)

    def finish_attempt(self, task_id: str, attempt_id: str, state: str, exit_code: int | None, error: str) -> None:
        if state not in {"completed", "failed", "cancelled"}:
            raise ValueError("invalid completion state")
        with self.connect() as con:
            counts = con.execute("SELECT COUNT(*) AS total, SUM(state='completed') AS completed, SUM(state='failed') AS failed, SUM(state='running') AS running FROM seed_runs WHERE task_id=? AND attempt_id=?", (task_id, attempt_id)).fetchone()
            con.execute("UPDATE attempts SET state = ?, finished_at = ?, exit_code = ?, error = ? WHERE task_id = ? AND attempt_id = ?",
                        (state, now(), exit_code, error, task_id, attempt_id))
            con.execute("UPDATE attempts SET completed_runs=?, failed_runs=?, running_runs=?, total_runs=? WHERE attempt_id=?",
                        (int(counts["completed"] or 0), int(counts["failed"] or 0), int(counts["running"] or 0), int(counts["total"] or 0), attempt_id))
            con.execute("UPDATE tasks SET state = ?, error = ?, updated_at = ? WHERE id = ?", (state, error, now(), task_id))
        self.event("task.completed", task_id, attempt_id, {"state": state, "exit_code": exit_code, "error": error})

    def record_artifact(self, task_id: str, attempt_id: str, kind: str, path: Path, sha256: str = "") -> None:
        digest = sha256 or _sha256(path)
        with self.connect() as con:
            con.execute("INSERT INTO artifacts (id, task_id, attempt_id, kind, path, sha256, size, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (str(uuid.uuid4()), task_id, attempt_id, kind, str(path), digest, path.stat().st_size, now()))

    def setting(self, key: str, default: str = "") -> str:
        with self.connect() as con:
            row = con.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self.connect() as con:
            con.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def create_app(data_dir: Path, platemo_path: Path | None = None, join_token: str = "") -> FastAPI:
    store = Store(data_dir, platemo_path)
    app = FastAPI(title="PlatEMO HPC Master")
    app.state.store = store
    configured_join_token = join_token or store.setting("worker_join_token")
    if not configured_join_token:
        configured_join_token = str(uuid.uuid4())
        store.set_setting("worker_join_token", configured_join_token)
    app.state.worker_join_token = configured_join_token

    def settings_catalog() -> list[dict[str, str]]:
        return list_setting_files(Path(store.setting("platemo_path")))

    def current_catalogs() -> dict[str, list[dict[str, Any]]]:
        root = Path(store.setting("platemo_path"))
        return discover_catalogs(root) if root.is_dir() else {"algorithms": [], "problems": []}

    def current_existing_tests() -> list[dict[str, Any]]:
        catalogs = current_catalogs()
        return discover_existing_tests(Path(store.setting("platemo_path")),
                                       (item["name"] for item in catalogs["problems"]))

    async def probe_worker(worker: dict[str, Any]) -> dict[str, Any]:
        node_token = worker.get("node_token") or worker.get("token", "")
        headers = {"Authorization": f"Bearer {node_token}"} if node_token else {}
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                response = await client.post(f"{worker['url']}/api/v1/health", json={"free_slots": 0}, headers=headers)
                response.raise_for_status()
                health = response.json()
            store.update_health(worker["id"], True, int(health.get("queued", 0)), "")
            store.event("worker.probed", None, None, {"worker_id": worker["id"], "health": health})
        except httpx.HTTPError as exc:
            store.update_health(worker["id"], False, None, str(exc))
        return next(item for item in store.workers() if item["id"] == worker["id"])

    async def probe_all_forever() -> None:
        while True:
            workers = store.workers()
            if workers:
                await asyncio.gather(*(probe_worker(worker) for worker in workers))
            await asyncio.sleep(30)

    async def watchdog_forever() -> None:
        while True:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=30)
            reclaimed: list[tuple[str, str, str, list[int]]] = []
            with store.connect() as con:
                rows = con.execute("SELECT id, last_heartbeat FROM workers WHERE status = 'online'").fetchall()
                for worker in rows:
                    try:
                        heartbeat = datetime.fromisoformat(worker["last_heartbeat"])
                    except (TypeError, ValueError):
                        heartbeat = datetime.min.replace(tzinfo=timezone.utc)
                    if heartbeat < cutoff:
                        con.execute("UPDATE workers SET online=0, status='suspect' WHERE id=?", (worker["id"],))
                        stale_tasks = con.execute("SELECT id, payload FROM tasks WHERE worker_id=? AND state IN ('leased','running')", (worker["id"],)).fetchall()
                        for task in stale_tasks:
                            replacement = con.execute(
                                "SELECT id FROM workers WHERE id<>? AND status='online' AND last_heartbeat>=? "
                                "ORDER BY priority DESC, created_at LIMIT 1",
                                (worker["id"], cutoff.isoformat()),
                            ).fetchone()
                            replacement_id = replacement["id"] if replacement else worker["id"]
                            con.execute("UPDATE tasks SET worker_id=?, state='queued', updated_at=?, error=? WHERE id=?",
                                        (replacement_id, now(), "worker heartbeat timeout; requeued", task["id"]))
                            con.execute("UPDATE attempts SET state='reclaimed', finished_at=?, error=? WHERE task_id=? AND state IN ('leased','running')", (now(), "three missed heartbeats", task["id"]))
                            try:
                                seeds = [int(seed) for seed in json.loads(task["payload"]).get("seeds", [])]
                            except (TypeError, ValueError, json.JSONDecodeError):
                                seeds = []
                            reclaimed.append((task["id"], worker["id"], replacement_id, seeds))
            for task_id, worker_id, replacement_id, seeds in reclaimed:
                attempt_id, _ = store.create_attempt(task_id, seeds)
                store.event("task.reclaimed", task_id, attempt_id, {"worker_id": worker_id, "replacement_worker_id": replacement_id, "reason": "three_missed_heartbeats"})
            await asyncio.sleep(10)

    @app.on_event("startup")
    async def start_health_probes() -> None:
        app.state.probe_task = asyncio.create_task(probe_all_forever())
        app.state.watchdog_task = asyncio.create_task(watchdog_forever())

    @app.on_event("shutdown")
    async def stop_health_probes() -> None:
        app.state.probe_task.cancel()
        app.state.watchdog_task.cancel()
        try:
            await app.state.probe_task
        except asyncio.CancelledError:
            pass
        try:
            await app.state.watchdog_task
        except asyncio.CancelledError:
            pass

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request, message: str = "") -> HTMLResponse:
        frontend_index = STATIC_DIR / "index.html"
        if frontend_index.is_file():
            return FileResponse(frontend_index)
        return templates.TemplateResponse(request, "index.html", {
            "workers": store.workers(), "tasks": store.tasks(), "data_dir": str(data_dir), "message": message,
            "platemo_path": store.setting("platemo_path"),
            "algorithms": current_catalogs()["algorithms"], "problems": current_catalogs()["problems"],
            "settings": settings_catalog(),
        })

    @app.get("/api/catalog")
    async def api_catalog() -> dict[str, Any]:
        catalogs = current_catalogs()
        return {**catalogs, "settings": settings_catalog(), "existing_tests": current_existing_tests(),
                "platemo_path": store.setting("platemo_path")}

    @app.get("/api/settings/catalog")
    async def api_settings_catalog() -> dict[str, Any]:
        return {"settings": settings_catalog(), "existing_tests": current_existing_tests()}

    @app.post("/api/settings/platemo-path")
    async def set_platemo_path(platemo_path: str = Form(...)) -> RedirectResponse:
        path = Path(platemo_path).expanduser()
        if not path.is_dir():
            raise HTTPException(400, "PlatEMO path does not exist")
        store.set_setting("platemo_path", str(path.resolve()))
        return RedirectResponse(url="/?message=PlatEMO+path+updated", status_code=303)

    @app.put("/api/platemo-path")
    async def api_set_platemo_path(payload: dict[str, str]) -> dict[str, str]:
        """Set the configured PlatEMO root for the Vue client."""
        path = Path(payload.get("platemo_path", "")).expanduser()
        if not (path / "Algorithms").is_dir() or not (path / "Problems").is_dir() or not (path / "Data").is_dir():
            raise HTTPException(422, "PlatEMO path must contain Algorithms, Problems, and Data")
        resolved = str(path.resolve())
        store.set_setting("platemo_path", resolved)
        return {"platemo_path": resolved}

    @app.post("/api/settings/load")
    async def load_settings(settings_upload: UploadFile = File(...)) -> dict[str, Any]:
        """Import a PlatEMO Setting*.mat file and return an editable preview."""
        if not settings_upload.filename or not settings_upload.filename.lower().endswith(".mat"):
            raise HTTPException(400, "Select a MAT file")
        try:
            data = loadmat(BytesIO(await settings_upload.read()), simplify_cells=True)
        except Exception as exc:
            raise HTTPException(400, f"Cannot read MAT file: {exc}") from exc
        try:
            parsed = parse_setting_data(data, current_catalogs(), settings_upload.filename)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        return {"filename": settings_upload.filename, "values": parsed}

    @app.get("/api/settings/preview")
    async def preview_setting(filename: str) -> dict[str, Any]:
        """Preview a bundled Data/Setting*.mat without exposing arbitrary paths."""
        root = Path(store.setting("platemo_path")) / "Data"
        candidate = root / Path(filename).name
        if candidate.parent != root or not candidate.is_file() or candidate.suffix.lower() != ".mat":
            raise HTTPException(404, "Setting file not found")
        try:
            return parse_platemo_setting_file(candidate, current_catalogs())
        except (OSError, ValueError) as exc:
            raise HTTPException(422, f"Cannot parse setting file: {exc}") from exc

    @app.get("/api/existing-tests")
    async def existing_tests() -> list[dict[str, Any]]:
        return current_existing_tests()

    @app.post("/api/settings/save")
    async def save_settings(config_json: str = Form(...), filename: str = Form("PlatEMO-settings.mat")) -> StreamingResponse:
        try:
            config = json.loads(config_json)
        except json.JSONDecodeError as exc:
            raise HTTPException(400, "Invalid settings configuration") from exc
        clean_name = Path(filename).name
        if not clean_name.lower().endswith(".mat"):
            clean_name += ".mat"
        output = BytesIO()
        output = native_settings_mat(config)
        return StreamingResponse(output, media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{clean_name}"'})

    @app.get("/api/workers")
    async def list_workers() -> list[dict[str, Any]]:
        return [{key: value for key, value in worker.items() if key not in {"token", "node_token"}} for worker in store.workers()]

    @app.get("/api/v1/ui/tasks")
    async def list_tasks() -> list[dict[str, Any]]:
        """Return recent seed-level task state for the run-status panel without secrets."""
        worker_names = {worker["id"]: worker["name"] for worker in store.workers()}
        result: list[dict[str, Any]] = []
        for task in store.tasks():
            try:
                payload = json.loads(task["payload"])
            except json.JSONDecodeError:
                payload = {}
            with store.connect() as con:
                attempt = con.execute("SELECT * FROM attempts WHERE task_id = ? ORDER BY attempt_no DESC LIMIT 1", (task["id"],)).fetchone()
                seed_rows = con.execute("SELECT * FROM seed_runs WHERE task_id = ? AND attempt_id = ? ORDER BY seed", (task["id"], attempt["attempt_id"])).fetchall() if attempt else []
            parameters = dict(payload.get("problem", {}).get("parameters", {})) if isinstance(payload.get("problem"), dict) else {}
            for name in ("N", "M", "D"):
                if name in payload:
                    parameters[name] = payload[name]
            seeds = seed_rows or [{"seed": seed, "state": task["state"], "fe": 0, "total_fe": payload.get("max_fe", 0), "elapsed_seconds": 0, "error": task["error"]} for seed in payload.get("seeds", [])]
            for seed in seeds:
                result.append({
                    "id": f"{task['id']}:{seed['seed']}", "task_id": task["id"], "attempt_id": attempt["attempt_id"] if attempt else "",
                    "state": seed["state"], "worker_id": task["worker_id"], "worker_name": worker_names.get(task["worker_id"], "未分配"),
                    "algorithm": payload.get("algorithm", {}).get("name", "") if isinstance(payload.get("algorithm"), dict) else "",
                    "problem": payload.get("problem", {}).get("name", "") if isinstance(payload.get("problem"), dict) else "",
                    "seed": seed["seed"], "parameters": parameters, "fe": seed["fe"], "total_fe": seed["total_fe"],
                    "elapsed_seconds": seed["elapsed_seconds"], "created_at": task["created_at"], "updated_at": task["updated_at"], "error": seed["error"],
                })
        return result

    @app.post("/api/workers")
    async def add_worker(request: Request, name: str = Form(...), url: str = Form(...), token: str = Form(""), priority: int = Form(0)) -> RedirectResponse:
        worker_id = str(uuid.uuid4())
        with store.connect() as con:
            con.execute("INSERT INTO workers (id, name, url, node_token, created_at, status, priority) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (worker_id, name, url.rstrip("/"), token, now(), "unknown", priority))
        return RedirectResponse(url=f"/?message=Worker+added%3A+{worker_id}", status_code=303)

    @app.post("/api/workers/{worker_id}/probe")
    async def probe_one(worker_id: str) -> dict[str, Any]:
        worker = next((item for item in store.workers() if item["id"] == worker_id), None)
        if worker is None:
            raise HTTPException(404, "Worker not found")
        result = await probe_worker(worker)
        return {"online": bool(result["online"]), "queue_count": result["queue_count"],
                "last_check": result["last_check"], "error": result["health_error"]}

    @app.post("/api/workers/probe-all")
    async def probe_all() -> RedirectResponse:
        await asyncio.gather(*(probe_worker(worker) for worker in store.workers()))
        return RedirectResponse(url="/?message=Workers+probed", status_code=303)

    @app.post("/api/workers/{worker_id}/edit")
    async def edit_worker(worker_id: str, name: str = Form(...), url: str = Form(...), token: str = Form(""), priority: int = Form(0)) -> RedirectResponse:
        with store.connect() as con:
            existing = con.execute("SELECT id FROM workers WHERE id = ?", (worker_id,)).fetchone()
            if existing is None:
                raise HTTPException(404, "Worker not found")
            if token:
                con.execute("UPDATE workers SET name = ?, url = ?, node_token = ?, priority = ? WHERE id = ?",
                            (name, url.rstrip("/"), token, priority, worker_id))
            else:
                con.execute("UPDATE workers SET name = ?, url = ?, priority = ? WHERE id = ?",
                            (name, url.rstrip("/"), priority, worker_id))
        return RedirectResponse(url="/?message=Worker+updated", status_code=303)

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

    def require_node(worker_id: str, authorization: str | None) -> dict[str, Any]:
        if not authorization or not authorization.lower().startswith("bearer "):
            raise HTTPException(401, "Bearer node token required")
        worker = next((item for item in store.workers() if item["id"] == worker_id), None)
        if worker is None or authorization[7:] != worker.get("node_token", ""):
            raise HTTPException(401, "Invalid node token")
        return worker

    @app.post("/api/v1/workers/register")
    async def register_worker(payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, Any]:
        if not authorization or not authorization.lower().startswith("bearer ") or authorization[7:] != app.state.worker_join_token:
            raise HTTPException(401, "Invalid worker join token")
        worker_id = str(payload.get("worker_id") or uuid.uuid4())
        node_token = str(uuid.uuid4())
        with store.connect() as con:
            con.execute("INSERT INTO workers (id, name, url, token, node_token, created_at, capabilities_json, status, priority) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(id) DO UPDATE SET name=excluded.name, url=excluded.url, node_token=excluded.node_token, capabilities_json=excluded.capabilities_json, status='online'",
                        (worker_id, str(payload.get("name") or worker_id), str(payload.get("url") or "").rstrip("/"), "", node_token, now(), json.dumps(payload.get("capabilities", {})), "online", int(payload.get("priority", 0))))
        store.event("worker.registered", None, None, {"worker_id": worker_id, "name": payload.get("name", worker_id)})
        return {"worker_id": worker_id, "node_token": node_token, "heartbeat_seconds": 10}

    @app.post("/api/v1/workers/{worker_id}/heartbeat")
    async def worker_heartbeat(worker_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, Any]:
        worker = require_node(worker_id, authorization)
        with store.connect() as con:
            con.execute("UPDATE workers SET online=1, status='online', last_heartbeat=?, last_check=?, queue_count=?, capabilities_json=? WHERE id=?",
                        (now(), now(), int(payload.get("free_slots", 0)), json.dumps(payload.get("capabilities", {})), worker_id))
        for item in payload.get("running", []):
            if isinstance(item, dict) and item.get("task_id") and item.get("attempt_id") and item.get("progress"):
                attempt = store.attempt(item["task_id"], item["attempt_id"], item.get("lease_token", ""))
                if attempt:
                    store.apply_progress(item["task_id"], item["attempt_id"], item["progress"])
        with store.connect() as con:
            cancelled = con.execute(
                "SELECT id FROM tasks WHERE worker_id=? AND state='cancel_requested'", (worker_id,)
            ).fetchall()
        return {"status": "ok", "server_time": now(), "cancel_task_ids": [row["id"] for row in cancelled]}

    @app.post("/api/v1/workers/{worker_id}/lease")
    async def worker_lease(worker_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, Any]:
        require_node(worker_id, authorization)
        with store.connect() as con:
            row = con.execute("SELECT * FROM tasks WHERE worker_id=? AND state='queued' ORDER BY created_at LIMIT 1", (worker_id,)).fetchone()
            if row is None:
                return {"task": None}
            attempt = con.execute("SELECT * FROM attempts WHERE task_id=? ORDER BY attempt_no DESC LIMIT 1", (row["id"],)).fetchone()
            if attempt is None:
                raise HTTPException(409, "Task has no attempt")
            con.execute("UPDATE tasks SET state='leased', updated_at=? WHERE id=?", (now(), row["id"]))
            con.execute("UPDATE attempts SET state='leased' WHERE attempt_id=?", (attempt["attempt_id"],))
        store.event("task.leased", row["id"], attempt["attempt_id"], {"worker_id": worker_id})
        return {"task": json.loads(row["payload"]), "attempt_id": attempt["attempt_id"], "lease_token": attempt["lease_token"], "attempt_no": attempt["attempt_no"]}

    @app.post("/api/v1/tasks/{task_id}/attempts/{attempt_id}/progress")
    async def task_progress(task_id: str, attempt_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, str]:
        with store.connect() as con:
            task_row = con.execute("SELECT worker_id FROM tasks WHERE id=?", (task_id,)).fetchone()
        if task_row is None:
            raise HTTPException(404, "Task not found")
        require_node(task_row["worker_id"], authorization)
        attempt = store.attempt(task_id, attempt_id, str(payload.get("lease_token", "")))
        if attempt is None:
            raise HTTPException(410, "Lease expired or invalid")
        store.apply_progress(task_id, attempt_id, payload)
        return {"status": "accepted"}

    @app.post("/api/v1/tasks/{task_id}/attempts/{attempt_id}/complete")
    async def task_complete(task_id: str, attempt_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, str]:
        with store.connect() as con:
            task_row = con.execute("SELECT worker_id FROM tasks WHERE id=?", (task_id,)).fetchone()
        if task_row is None:
            raise HTTPException(404, "Task not found")
        require_node(task_row["worker_id"], authorization)
        attempt = store.attempt(task_id, attempt_id, str(payload.get("lease_token", "")))
        if attempt is None:
            raise HTTPException(410, "Lease expired or invalid")
        if payload.get("runs"):
            store.apply_progress(task_id, attempt_id, payload)
        store.finish_attempt(task_id, attempt_id, str(payload.get("state", "failed")), payload.get("exit_code"), str(payload.get("error", "")))
        return {"status": "accepted"}

    @app.put("/api/v1/artifacts/{artifact_id}")
    async def upload_artifact(artifact_id: str, task_id: str = Form(...), attempt_id: str = Form(...), lease_token: str = Form(...), kind: str = Form("result"), artifact: UploadFile = File(...), authorization: str | None = Header(None)) -> dict[str, Any]:
        with store.connect() as con:
            attempt = con.execute("SELECT task_id FROM attempts WHERE task_id=? AND attempt_id=?", (task_id, attempt_id)).fetchone()
            task_row = con.execute("SELECT worker_id FROM tasks WHERE id=?", (task_id,)).fetchone()
        if attempt is None or task_row is None:
            raise HTTPException(404, "Task attempt not found")
        require_node(task_row["worker_id"], authorization)
        if store.attempt(task_id, attempt_id, lease_token) is None:
            raise HTTPException(410, "Lease expired or invalid")
        content = await artifact.read()
        destination = store.results_dir / task_id / attempt_id
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / Path(artifact.filename or f"{artifact_id}.bin").name
        path.write_bytes(content)
        store.record_artifact(task_id, attempt_id, kind, path, hashlib.sha256(content).hexdigest())
        return {"status": "stored", "artifact_id": artifact_id, "sha256": hashlib.sha256(content).hexdigest(), "size": len(content)}

    @app.post("/api/v1/experiments")
    async def create_experiment(
        algorithms_json: str = Form(...),
        problems_json: str = Form(...),
        runs: int = Form(30),
        max_workers: int | None = Form(None),
        retain_points: int = Form(100),
        settings_file: str = Form(""),
        settings_upload: UploadFile | None = File(None),
        worker_ids: list[str] = Form(...),
    ) -> RedirectResponse:
        """Create batch tasks. Workers lease and execute them asynchronously."""
        try:
            algorithms = json.loads(algorithms_json)
            problems = json.loads(problems_json)
        except json.JSONDecodeError as exc:
            raise HTTPException(400, "Invalid experiment list") from exc
        if not algorithms or not problems:
            raise HTTPException(400, "Select at least one algorithm and one problem")
        if not 1 <= runs <= 1000 or retain_points < 1 or (max_workers is not None and max_workers < 1):
            raise HTTPException(400, "Runs must be between 1 and 1000")
        selected = [worker for worker in store.workers() if worker["id"] in set(worker_ids)]
        online = sorted((worker for worker in selected if worker["online"]),
                        key=lambda worker: (-int(worker.get("priority", 0)), worker["created_at"]))
        if max_workers is not None:
            online = online[:max_workers]
        if not online:
            raise HTTPException(400, "Select at least one online Worker")
        run_id = str(uuid.uuid4())
        uploaded_settings = ""
        if settings_upload and settings_upload.filename:
            settings_dir = store.uploads_dir / run_id
            settings_dir.mkdir(parents=True, exist_ok=True)
            uploaded_path = settings_dir / Path(settings_upload.filename).name
            uploaded_path.write_bytes(await settings_upload.read())
            uploaded_settings = str(uploaded_path)
        planned = [(algorithm, problem) for algorithm in algorithms for problem in problems]
        for index, (algorithm, problem) in enumerate(planned):
            worker = online[index % len(online)]
            task_id = str(uuid.uuid4())
            algorithm_payload = algorithm if isinstance(algorithm, dict) else {"name": str(algorithm), "parameters": {}}
            problem_payload = problem if isinstance(problem, dict) else {"name": str(problem), "parameters": {}}
            problem_parameters = dict(problem_payload.get("parameters", {}))
            payload = {
                    "id": task_id, "run_id": run_id, "algorithm": algorithm_payload,
                    "problem": problem_payload, "seeds": list(range(1, runs + 1)),
                    "N": problem_parameters.get("N"), "M": problem_parameters.get("M"), "D": problem_parameters.get("D"),
                    "max_fe": int(problem_parameters.get("maxFE", problem_parameters.get("max_fe", 50000)) or 50000),
                    "max_workers": max_workers, "retain_points": retain_points,
                    "cluster_profile": "local", "settings_file": uploaded_settings or settings_file, "created_at": now(),
            }
            with store.connect() as con:
                con.execute("INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, '')",
                            (task_id, worker["id"], "queued", json.dumps(payload), now(), now()))
            attempt_id, _ = store.create_attempt(task_id, payload["seeds"])
            store.event("task.created", task_id, attempt_id, {"worker_id": worker["id"], "run_id": run_id})
        return {"run_id": run_id, "batch_count": len(planned), "worker_count": len(online)}

    @app.post("/api/v1/tasks/{task_id}/cancel")
    async def cancel_task(task_id: str) -> dict[str, str]:
        with store.connect() as con:
            row = con.execute("SELECT state FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                raise HTTPException(404, "Task not found")
            if row["state"] == "queued":
                con.execute("UPDATE tasks SET state='cancelled', updated_at=? WHERE id=?", (now(), task_id))
                con.execute("UPDATE attempts SET state='cancelled', finished_at=?, error='cancelled before lease' WHERE task_id=? AND state IN ('dispatching','queued')", (now(), task_id))
            elif row["state"] in {"leased", "running"}:
                con.execute("UPDATE tasks SET state='cancel_requested', updated_at=? WHERE id=?", (now(), task_id))
            else:
                return {"status": row["state"]}
        store.event("task.cancel_requested", task_id, None, {})
        return {"status": "accepted"}

    # Vite emits immutable assets under /assets.  API routes are registered
    # first so a production UI shares its origin with the Master API.
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "assets"), check_dir=False), name="frontend-assets")
    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path.cwd() / "Data")
    parser.add_argument("--platemo-path", type=Path, default=None)
    parser.add_argument("--worker-join-token", default="")
    parser.add_argument("--host", default="0.0.0.0")
    # Chromium blocks port 6000 as unsafe, so use a nearby browser-safe default.
    parser.add_argument("--port", type=int, default=6080)
    args = parser.parse_args()
    application = create_app(args.data_dir.resolve(), args.platemo_path, args.worker_join_token)
    print(f"Worker Join Token: {application.state.worker_join_token}", flush=True)
    uvicorn.run(application, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
