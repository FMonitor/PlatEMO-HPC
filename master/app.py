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
                CREATE TABLE IF NOT EXISTS experiments (
                    id TEXT PRIMARY KEY, state TEXT NOT NULL, config_json TEXT NOT NULL,
                    max_workers INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS experiment_points (
                    id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                    algorithm_json TEXT NOT NULL, problem_json TEXT NOT NULL,
                    allowed_workers_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS seed_runs_v2 (
                    experiment_point_id TEXT NOT NULL, seed INTEGER NOT NULL,
                    state TEXT NOT NULL, batch_attempt_id TEXT, fe INTEGER NOT NULL DEFAULT 0,
                    total_fe INTEGER NOT NULL DEFAULT 0, elapsed_seconds REAL NOT NULL DEFAULT 0,
                    result_path TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
                    attempts INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL,
                    PRIMARY KEY (experiment_point_id, seed)
                );
                CREATE TABLE IF NOT EXISTS batch_attempts (
                    id TEXT PRIMARY KEY, experiment_point_id TEXT NOT NULL, worker_id TEXT NOT NULL,
                    lease_token TEXT NOT NULL, state TEXT NOT NULL, seed_json TEXT NOT NULL,
                    assigned_at TEXT NOT NULL, accepted_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT '', lease_deadline TEXT NOT NULL,
                    pid INTEGER, pool_json TEXT NOT NULL DEFAULT '{}', error TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS artifacts_v2 (
                    id TEXT PRIMARY KEY, experiment_point_id TEXT NOT NULL, batch_attempt_id TEXT NOT NULL,
                    seed INTEGER, kind TEXT NOT NULL, path TEXT NOT NULL, sha256 TEXT NOT NULL,
                    size INTEGER NOT NULL, created_at TEXT NOT NULL
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

    def record_v2_event(self, event_type: str, point_id: str | None, batch_id: str | None, payload: dict[str, Any]) -> None:
        self.event(event_type, point_id, batch_id, payload)

    def recover_batch(self, batch_id: str, reason: str, cancelled: bool = False) -> None:
        """Invalidate a batch and only release SeedRuns that are not terminal."""
        terminal = ("completed", "failed", "cancelled")
        with self.connect() as con:
            batch = con.execute("SELECT * FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()
            if batch is None or batch["state"] in {"completed", "failed", "cancelled", "reclaimed", "rejected"}:
                return
            state = "cancelled" if cancelled else "reclaimed"
            con.execute("UPDATE batch_attempts SET state=?, finished_at=?, error=? WHERE id=?", (state, now(), reason, batch_id))
            rows = con.execute("SELECT seed, state FROM seed_runs_v2 WHERE batch_attempt_id=?", (batch_id,)).fetchall()
            for row in rows:
                if row["state"] not in terminal:
                    con.execute("UPDATE seed_runs_v2 SET state=?, batch_attempt_id=NULL, error=?, updated_at=? WHERE batch_attempt_id=? AND seed=?",
                                ("cancelled" if cancelled else "pending", reason, now(), batch_id, row["seed"]))
            con.execute("UPDATE experiment_points SET updated_at=? WHERE id=?", (now(), batch["experiment_point_id"]))


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
            with store.connect() as con:
                rows = con.execute("SELECT id, last_heartbeat FROM workers WHERE status = 'online'").fetchall()
                for worker in rows:
                    try:
                        heartbeat = datetime.fromisoformat(worker["last_heartbeat"])
                    except (TypeError, ValueError):
                        heartbeat = datetime.min.replace(tzinfo=timezone.utc)
                    if heartbeat < cutoff:
                        con.execute("UPDATE workers SET online=0, status='suspect' WHERE id=?", (worker["id"],))
            with store.connect() as con:
                expired = con.execute(
                    "SELECT id, worker_id FROM batch_attempts WHERE state IN ('assigned','accepted','running') "
                    "AND (lease_deadline<? OR worker_id IN (SELECT id FROM workers WHERE status='suspect'))",
                    (now(),),
                ).fetchall()
            for batch in expired:
                store.recover_batch(batch["id"], "heartbeat or lease timeout")
                store.record_v2_event("batch.reclaimed", None, batch["id"], {"worker_id": batch["worker_id"], "reason": "three_missed_heartbeats"})
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
        """Return the v2 global SeedRun view used by the Vue task cards."""
        worker_names = {worker["id"]: worker["name"] for worker in store.workers()}
        result: list[dict[str, Any]] = []
        with store.connect() as con:
            rows = con.execute(
                "SELECT s.*, p.algorithm_json, p.problem_json, p.created_at, b.worker_id, b.pid, b.pool_json "
                "FROM seed_runs_v2 s JOIN experiment_points p ON p.id=s.experiment_point_id "
                "LEFT JOIN batch_attempts b ON b.id=s.batch_attempt_id "
                "ORDER BY p.created_at DESC, s.seed LIMIT 500"
            ).fetchall()
        for seed in rows:
            algorithm = json.loads(seed["algorithm_json"])
            problem = json.loads(seed["problem_json"])
            parameters = dict(problem.get("parameters", {}))
            result.append({
                "id": f"{seed['experiment_point_id']}:{seed['seed']}", "task_id": seed["experiment_point_id"],
                "attempt_id": seed["batch_attempt_id"] or "", "batch_attempt_id": seed["batch_attempt_id"] or "",
                "state": seed["state"], "worker_id": seed["worker_id"], "worker_name": worker_names.get(seed["worker_id"], "未分配"),
                "algorithm": algorithm.get("name", ""), "problem": problem.get("name", ""), "seed": seed["seed"],
                "parameters": parameters, "fe": seed["fe"], "total_fe": seed["total_fe"],
                "elapsed_seconds": seed["elapsed_seconds"], "created_at": seed["created_at"], "updated_at": seed["updated_at"], "error": seed["error"],
                "matlab_pid": seed["pid"], "pool": json.loads(seed["pool_json"] or "{}") if seed["pool_json"] else {},
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

    def assign_batch(worker_id: str, capacity: dict[str, Any]) -> dict[str, Any] | None:
        """Atomically choose pending seeds and issue one BatchAttempt for a heartbeat."""
        available_slots = int(capacity.get("available_batch_slots", 0) or 0)
        configured_pool_workers = int(capacity.get("configured_pool_workers", 0) or 0)
        max_seeds_per_batch = int(capacity.get("max_seeds_per_batch", 0) or 0)
        if available_slots < 1 or configured_pool_workers < 1 or max_seeds_per_batch < 1:
            return None
        batch_size = min(max_seeds_per_batch, configured_pool_workers, 10_000)
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            worker = con.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
            active_points = con.execute(
                "SELECT DISTINCT p.id FROM experiment_points p JOIN batch_attempts b ON b.experiment_point_id=p.id "
                "WHERE b.worker_id=? AND b.state IN ('assigned','accepted','running')", (worker_id,)
            ).fetchall()
            if worker is None:
                con.execute("ROLLBACK")
                return None
            points = con.execute("SELECT * FROM experiment_points WHERE state='pending' ORDER BY created_at, ordinal").fetchall()
            for point in points:
                allowed = json.loads(point["allowed_workers_json"])
                if worker_id not in allowed:
                    continue
                experiment = con.execute("SELECT max_workers FROM experiments WHERE id=? AND state='running'", (point["experiment_id"],)).fetchone()
                if experiment is None:
                    continue
                experiment_config = json.loads(con.execute("SELECT config_json FROM experiments WHERE id=?", (point["experiment_id"],)).fetchone()[0])
                worker_capabilities = json.loads(worker["capabilities_json"] or "{}")
                requested_profile = str(experiment_config.get("cluster_profile", "") or "")
                worker_profile = str(worker_capabilities.get("cluster_profile", "") or "")
                assignment_profile = requested_profile or worker_profile or "local"
                if requested_profile and worker_profile and requested_profile != worker_profile:
                    continue
                used = con.execute(
                    "SELECT COUNT(DISTINCT b.worker_id) FROM batch_attempts b "
                    "JOIN experiment_points p2 ON p2.id=b.experiment_point_id "
                    "WHERE p2.experiment_id=? AND b.state IN ('assigned','accepted','running')",
                    (point["experiment_id"],),
                ).fetchone()[0]
                worker_already_used = con.execute(
                    "SELECT 1 FROM batch_attempts b JOIN experiment_points p2 ON p2.id=b.experiment_point_id "
                    "WHERE p2.experiment_id=? AND b.worker_id=? AND b.state IN ('assigned','accepted','running') LIMIT 1",
                    (point["experiment_id"], worker_id),
                ).fetchone()
                if experiment["max_workers"] is not None and used >= experiment["max_workers"] and worker_already_used is None:
                    continue
                seeds = con.execute("SELECT seed FROM seed_runs_v2 WHERE experiment_point_id=? AND state='pending' ORDER BY seed LIMIT ?", (point["id"], batch_size)).fetchall()
                if not seeds:
                    continue
                batch_id, token = str(uuid.uuid4()), str(uuid.uuid4())
                seed_values = [row["seed"] for row in seeds]
                deadline = (datetime.now(timezone.utc) + timedelta(seconds=10)).isoformat()
                con.execute("INSERT INTO batch_attempts (id, experiment_point_id, worker_id, lease_token, state, seed_json, assigned_at, lease_deadline) VALUES (?, ?, ?, ?, 'assigned', ?, ?, ?)",
                            (batch_id, point["id"], worker_id, token, json.dumps(seed_values), now(), deadline))
                con.executemany("UPDATE seed_runs_v2 SET state='leased', batch_attempt_id=?, attempts=attempts+1, updated_at=? WHERE experiment_point_id=? AND seed=? AND state='pending'",
                                [(batch_id, now(), point["id"], seed) for seed in seed_values])
                con.execute("COMMIT")
                algorithm, problem = json.loads(point["algorithm_json"]), json.loads(point["problem_json"])
                params = problem.get("parameters", {})
                store.record_v2_event("seed.assigned", point["id"], batch_id, {"worker_id": worker_id, "seeds": seed_values})
                return {"batch_attempt_id": batch_id, "lease_token": token, "experiment_point_id": point["id"],
                        "algorithm": algorithm, "problem": problem, "seeds": seed_values,
                        "max_fe": int(params.get("maxFE", params.get("max_fe", 50000)) or 50000),
                        "retain_points": int(experiment_config.get("retain_points", 100)),
                        "cluster_profile": assignment_profile}
            con.execute("COMMIT")
        return None

    @app.post("/api/v1/workers/{worker_id}/heartbeat")
    async def worker_heartbeat(worker_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, Any]:
        require_node(worker_id, authorization)
        capacity = {key: payload.get(key, 0) for key in ("available_batch_slots", "max_concurrent_batches", "configured_pool_workers", "actual_pool_workers", "max_seeds_per_batch")}
        running_batches = payload.get("running_batches", [])
        if not isinstance(running_batches, list):
            raise HTTPException(422, "running_batches must be an array")
        with store.connect() as con:
            con.execute("UPDATE workers SET online=1, status='online', last_heartbeat=?, last_check=?, queue_count=?, capabilities_json=? WHERE id=?",
                        (now(), now(), int(capacity["available_batch_slots"]), json.dumps(payload.get("capabilities", {})), worker_id))
            for running in running_batches:
                if not isinstance(running, dict):
                    continue
                batch_id = str(running.get("batch_attempt_id", ""))
                lease_token = str(running.get("lease_token", ""))
                if not batch_id or not lease_token:
                    continue
                con.execute(
                    "UPDATE batch_attempts SET pid=?, pool_json=?, lease_deadline=? WHERE id=? AND worker_id=? AND lease_token=? AND state IN ('assigned','accepted','running')",
                    (running.get("matlab_pid", running.get("pid")), json.dumps({
                        "configured_pool_workers": running.get("configured_pool_workers", capacity["configured_pool_workers"]),
                        "actual_pool_workers": running.get("actual_pool_workers", capacity["actual_pool_workers"]),
                        "pool": running.get("pool", {}),
                    }), (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(), batch_id, worker_id, lease_token),
                )
            cancelled = con.execute("SELECT id FROM batch_attempts WHERE worker_id=? AND state='cancel_requested'", (worker_id,)).fetchall()
        assignment = assign_batch(worker_id, capacity)
        return {"status": "ok", "server_time": now(), "cancel_batch_attempt_ids": [row["id"] for row in cancelled], "assignment": assignment}

    def valid_batch(batch_id: str, token: str, authorization: str | None) -> sqlite3.Row:
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            batch = con.execute("SELECT * FROM batch_attempts WHERE id=? AND lease_token=?", (batch_id, token)).fetchone()
            if batch is not None:
                worker = con.execute("SELECT node_token FROM workers WHERE id=?", (batch["worker_id"],)).fetchone()
                supplied = authorization[7:] if authorization and authorization.lower().startswith("bearer ") else ""
                if worker is None or supplied != worker["node_token"]:
                    con.execute("ROLLBACK")
                    raise HTTPException(401, "Invalid node token")
            if batch is not None and batch["state"] in {"assigned", "accepted", "running", "cancel_requested"} and batch["lease_deadline"] <= now():
                con.execute("ROLLBACK")
                store.recover_batch(batch_id, "lease deadline expired")
                store.record_v2_event("batch.reclaimed", batch["experiment_point_id"], batch_id, {"reason": "lease_deadline_expired"})
                raise HTTPException(410, "Lease expired or invalid")
            con.execute("COMMIT")
        if batch is None or batch["state"] not in {"assigned", "accepted", "running", "cancel_requested"}:
            raise HTTPException(410, "Lease expired or invalid")
        return batch

    @app.post("/api/v1/batch-attempts/{batch_id}/progress")
    async def batch_progress(batch_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, str]:
        batch = valid_batch(batch_id, str(payload.get("lease_token", "")), authorization)
        phase = str(payload.get("phase", "running"))
        if phase == "rejected":
            store.recover_batch(batch_id, str(payload.get("error_code", "worker_rejected")))
            return {"status": "reclaimed"}
        with store.connect() as con:
            if batch["state"] == "assigned":
                con.execute("UPDATE batch_attempts SET state='accepted', accepted_at=?, lease_deadline=? WHERE id=?", (now(), (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(), batch_id))
            con.execute("UPDATE batch_attempts SET state=CASE WHEN state='accepted' OR state='assigned' THEN 'running' ELSE state END, pool_json=?, pid=?, lease_deadline=? WHERE id=?",
                        (json.dumps({"pool": payload.get("pool", {}), "actual_pool_workers": payload.get("actual_pool_workers", payload.get("pool", {}).get("workers", 0) if isinstance(payload.get("pool", {}), dict) else 0)}), payload.get("pid", payload.get("matlab_pid")), (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(), batch_id))
            assigned = set(json.loads(batch["seed_json"]))
            for run in payload.get("runs", []):
                if not isinstance(run, dict) or run.get("seed") not in assigned:
                    continue
                state = str(run.get("state", "running"))
                con.execute("UPDATE seed_runs_v2 SET state=?, fe=?, total_fe=?, elapsed_seconds=?, error=?, updated_at=? WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                            (state, int(run.get("fe", 0)), int(run.get("total_fe", 0)), float(run.get("elapsed_seconds", 0)), str(run.get("error", "")), now(), batch["experiment_point_id"], run["seed"], batch_id))
        store.record_v2_event("batch.progress", batch["experiment_point_id"], batch_id, {"phase": phase})
        return {"status": "accepted"}

    @app.post("/api/v1/batch-attempts/{batch_id}/complete")
    async def batch_complete(batch_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, str]:
        batch = valid_batch(batch_id, str(payload.get("lease_token", "")), authorization)
        await batch_progress(batch_id, payload, authorization)
        final_state = str(payload.get("state", "failed"))
        with store.connect() as con:
            con.execute("UPDATE batch_attempts SET state=?, finished_at=?, error=? WHERE id=?", (final_state, now(), str(payload.get("error", "")), batch_id))
            rows = con.execute("SELECT seed, state FROM seed_runs_v2 WHERE batch_attempt_id=?", (batch_id,)).fetchall()
            cancellation_requested = batch["state"] == "cancel_requested" or final_state == "cancelled"
            for row in rows:
                if row["state"] not in {"completed", "failed", "cancelled"}:
                    recovery_state = "cancelled" if cancellation_requested else ("pending" if final_state != "completed" else "pending")
                    con.execute("UPDATE seed_runs_v2 SET state=?, batch_attempt_id=NULL, updated_at=? WHERE experiment_point_id=? AND seed=?",
                                (recovery_state, now(), batch["experiment_point_id"], row["seed"]))
            remaining = con.execute(
                "SELECT COUNT(*) FROM seed_runs_v2 WHERE experiment_point_id=? AND state NOT IN ('completed','failed','cancelled')",
                (batch["experiment_point_id"],),
            ).fetchone()[0]
            if remaining == 0:
                con.execute("UPDATE experiment_points SET state='completed', updated_at=? WHERE id=? AND state!='cancelled'",
                            (now(), batch["experiment_point_id"]))
        store.record_v2_event("batch.completed", batch["experiment_point_id"], batch_id, {"state": final_state})
        return {"status": "accepted"}

    @app.put("/api/v1/artifacts/{artifact_id}")
    async def upload_artifact(artifact_id: str, experiment_point_id: str = Form(...), batch_attempt_id: str = Form(...), lease_token: str = Form(...), seed: int | None = Form(None), kind: str = Form("result"), artifact: UploadFile = File(...), authorization: str | None = Header(None)) -> dict[str, Any]:
        batch = valid_batch(batch_attempt_id, lease_token, authorization)
        if batch["experiment_point_id"] != experiment_point_id or (seed is not None and seed not in json.loads(batch["seed_json"])):
            raise HTTPException(422, "Artifact does not belong to this batch")
        content = await artifact.read()
        destination = store.results_dir / experiment_point_id / batch_attempt_id
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / Path(artifact.filename or f"{artifact_id}.bin").name
        path.write_bytes(content)
        digest = hashlib.sha256(content).hexdigest()
        with store.connect() as con:
            con.execute("INSERT INTO artifacts_v2 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (artifact_id, experiment_point_id, batch_attempt_id, seed, kind, str(path), digest, len(content), now()))
            if seed is not None:
                con.execute("UPDATE seed_runs_v2 SET result_path=? WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?", (str(path), experiment_point_id, seed, batch_attempt_id))
        return {"status": "stored", "artifact_id": artifact_id, "sha256": digest, "size": len(content)}

    @app.post("/api/v1/experiments")
    async def create_experiment(
        algorithms_json: str = Form(...),
        problems_json: str = Form(...),
        runs: int = Form(30),
        max_workers: int | None = Form(None),
        retain_points: int = Form(100),
        cluster_profile: str = Form(""),
        settings_file: str = Form(""),
        settings_upload: UploadFile | None = File(None),
        worker_ids: list[str] = Form(...),
    ) -> dict[str, Any]:
        """Create global SeedRuns. A later heartbeat assigns batch slices."""
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
        if not selected:
            raise HTTPException(400, "Select at least one Worker")
        experiment_id = str(uuid.uuid4())
        uploaded_settings = ""
        if settings_upload and settings_upload.filename:
            settings_dir = store.uploads_dir / experiment_id
            settings_dir.mkdir(parents=True, exist_ok=True)
            uploaded_path = settings_dir / Path(settings_upload.filename).name
            uploaded_path.write_bytes(await settings_upload.read())
            uploaded_settings = str(uploaded_path)
        allowed_workers = [worker["id"] for worker in selected]
        planned = [(algorithm, problem) for algorithm in algorithms for problem in problems]
        config = {"runs": runs, "retain_points": retain_points, "cluster_profile": cluster_profile.strip(), "settings_file": uploaded_settings or settings_file,
                  "allowed_workers": allowed_workers}
        with store.connect() as con:
            con.execute("INSERT INTO experiments VALUES (?, 'running', ?, ?, ?, ?)",
                        (experiment_id, json.dumps(config), max_workers, now(), now()))
            for index, (algorithm, problem) in enumerate(planned):
                point_id = str(uuid.uuid4())
                algorithm_payload = algorithm if isinstance(algorithm, dict) else {"name": str(algorithm), "parameters": {}}
                problem_payload = problem if isinstance(problem, dict) else {"name": str(problem), "parameters": {}}
                problem_parameters = dict(problem_payload.get("parameters", {}))
                con.execute("INSERT INTO experiment_points VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
                            (point_id, experiment_id, index, json.dumps(algorithm_payload), json.dumps(problem_payload), json.dumps(allowed_workers), now(), now()))
                max_fe = int(problem_parameters.get("maxFE", problem_parameters.get("max_fe", 50000)) or 50000)
                con.executemany("INSERT INTO seed_runs_v2 (experiment_point_id, seed, state, total_fe, updated_at) VALUES (?, ?, 'pending', ?, ?)",
                                [(point_id, seed, max_fe, now()) for seed in range(1, runs + 1)])
        store.record_v2_event("experiment.created", experiment_id, None, {"points": len(planned), "runs": runs})
        return {"experiment_id": experiment_id, "point_count": len(planned), "seed_count": len(planned) * runs, "worker_count": len(selected)}

    @app.post("/api/v1/experiment-points/{experiment_point_id}/cancel")
    async def cancel_point(experiment_point_id: str) -> dict[str, str]:
        with store.connect() as con:
            row = con.execute("SELECT id FROM experiment_points WHERE id=?", (experiment_point_id,)).fetchone()
            if row is None:
                raise HTTPException(404, "Experiment point not found")
            con.execute("UPDATE experiment_points SET state='cancelled', updated_at=? WHERE id=?", (now(), experiment_point_id))
            con.execute("UPDATE seed_runs_v2 SET state='cancelled', updated_at=? WHERE experiment_point_id=? AND state='pending'", (now(), experiment_point_id))
            con.execute("UPDATE batch_attempts SET state='cancel_requested' WHERE experiment_point_id=? AND state IN ('assigned','accepted','running')", (experiment_point_id,))
        store.record_v2_event("point.cancel_requested", experiment_point_id, None, {})
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
