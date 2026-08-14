"""Master web service for dispatching PlatEMO experiment tasks."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
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
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
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
from protocol_policy import completed_artifacts_are_covered, completed_seeds_with_artifacts, worker_can_run
from settings_files import is_distributable_upload, sha256_file, uploaded_settings_path

APP_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))
STATIC_DIR = APP_DIR / "static"


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, data_dir: Path, platemo_path: Path | None = None) -> None:
        self.data_dir = data_dir
        self.results_dir = data_dir / "results"
        self.experiments_dir = data_dir / "experiments"
        self.uploads_dir = data_dir / "uploads"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.experiments_dir.mkdir(parents=True, exist_ok=True)
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = data_dir / "master.sqlite3"
        with self.connect() as con:
            con.executescript("""
                CREATE TABLE IF NOT EXISTS workers (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL,
                    token TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                    online INTEGER NOT NULL DEFAULT 0, queue_count INTEGER,
                    last_check TEXT NOT NULL DEFAULT '', health_error TEXT NOT NULL DEFAULT '',
                    dispatch_paused INTEGER NOT NULL DEFAULT 0
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
                CREATE TABLE IF NOT EXISTS worker_point_blocks (
                    worker_id TEXT NOT NULL, experiment_point_id TEXT NOT NULL,
                    reason TEXT NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY (worker_id, experiment_point_id)
                );
                CREATE TABLE IF NOT EXISTS worker_sessions_v2 (
                    worker_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                    state TEXT NOT NULL, configured_workers INTEGER NOT NULL DEFAULT 0,
                    actual_workers INTEGER NOT NULL DEFAULT 0, free_seed_slots INTEGER NOT NULL DEFAULT 0,
                    last_heartbeat TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS seed_attempts_v3 (
                    id TEXT PRIMARY KEY, experiment_point_id TEXT NOT NULL, seed INTEGER NOT NULL,
                    worker_id TEXT NOT NULL, session_id TEXT NOT NULL, lease_token TEXT NOT NULL,
                    state TEXT NOT NULL, assigned_at TEXT NOT NULL, accepted_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT '', lease_deadline TEXT NOT NULL,
                    fe INTEGER NOT NULL DEFAULT 0, total_fe INTEGER NOT NULL DEFAULT 0,
                    elapsed_seconds REAL NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT ''
                );
                CREATE UNIQUE INDEX IF NOT EXISTS seed_attempts_v3_active_seed
                    ON seed_attempts_v3(experiment_point_id, seed)
                    WHERE state IN ('assigned','running','cancel_requested');
                CREATE TABLE IF NOT EXISTS artifacts_v3 (
                    id TEXT PRIMARY KEY, seed_attempt_id TEXT NOT NULL, experiment_point_id TEXT NOT NULL,
                    seed INTEGER NOT NULL, kind TEXT NOT NULL, path TEXT NOT NULL, sha256 TEXT NOT NULL,
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
                ("dispatch_paused", "INTEGER NOT NULL DEFAULT 0"),
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
            # A reachability probe is diagnostic only. Scheduling liveness and
            # capacity are authoritative only when supplied by a signed heartbeat.
            con.execute("UPDATE workers SET last_check = ?, health_error = ? WHERE id = ?",
                        (now(), error, worker_id))

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
        """Invalidate a batch and retain only completed Seeds with recoverable results."""
        terminal = ("completed", "failed", "cancelled")
        with self.connect() as con:
            batch = con.execute("SELECT * FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()
            if batch is None or batch["state"] in {"completed", "failed", "cancelled", "reclaimed", "rejected"}:
                return
            state = "cancelled" if cancelled else "reclaimed"
            con.execute("UPDATE batch_attempts SET state=?, finished_at=?, error=? WHERE id=?", (state, now(), reason, batch_id))
            rows = con.execute("SELECT seed, state FROM seed_runs_v2 WHERE batch_attempt_id=?", (batch_id,)).fetchall()
            reported_completed = {row["seed"] for row in rows if row["state"] == "completed"}
            artifacts = con.execute("SELECT seed, kind FROM artifacts_v2 WHERE batch_attempt_id=?", (batch_id,)).fetchall()
            covered_completed = completed_seeds_with_artifacts(
                reported_completed, ((row["seed"], row["kind"]) for row in artifacts)
            )
            for row in rows:
                needs_recovery = row["state"] not in terminal or (
                    row["state"] == "completed" and row["seed"] not in covered_completed
                )
                if needs_recovery:
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

    @app.middleware("http")
    async def reject_legacy_worker_protocol(request: Request, call_next):
        path = request.url.path
        legacy_worker_paths = (
            "/api/v1/workers/register",
            "/api/v1/artifacts/",
        )
        legacy_heartbeat = path.startswith("/api/v1/workers/") and path.endswith("/heartbeat")
        legacy_batch_delivery = path.startswith("/api/v1/batch-attempts/") and path.endswith(("/progress", "/complete"))
        if legacy_heartbeat or legacy_batch_delivery or any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in legacy_worker_paths):
            return JSONResponse(status_code=410, content={"detail": "protocol_v1_retired", "upgrade": "/api/v2"})
        return await call_next(request)
    app.state.store = store
    configured_join_token = join_token or store.setting("worker_join_token")
    if not configured_join_token:
        configured_join_token = str(uuid.uuid4())
        store.set_setting("worker_join_token", configured_join_token)
    app.state.worker_join_token = configured_join_token

    def seed_result_destination(con: sqlite3.Connection, experiment_point_id: str, seed: int) -> Path:
        point = con.execute(
            "SELECT experiment_id, algorithm_json, problem_json FROM experiment_points WHERE id=?",
            (experiment_point_id,),
        ).fetchone()
        if point is None:
            raise HTTPException(422, "Unknown experiment point")
        algorithm = json.loads(point["algorithm_json"])
        problem = json.loads(point["problem_json"])
        parameters = problem.get("parameters", {}) if isinstance(problem, dict) else {}
        algorithm_name = str(algorithm.get("name", "algorithm")) if isinstance(algorithm, dict) else "algorithm"
        problem_name = str(problem.get("name", "problem")) if isinstance(problem, dict) else "problem"
        safe_algorithm = "".join(char if char.isalnum() or char in "-_" else "_" for char in algorithm_name)
        safe_problem = "".join(char if char.isalnum() or char in "-_" else "_" for char in problem_name)
        try:
            objectives = int(parameters.get("M", 0))
            dimensions = int(parameters.get("D", 0))
        except (TypeError, ValueError):
            raise HTTPException(422, "Experiment point has invalid M/D") from None
        filename = f"{safe_algorithm}_{safe_problem}_M{objectives}_D{dimensions}_{seed}.mat"
        return store.experiments_dir / str(point["experiment_id"]) / safe_algorithm / filename

    def settings_catalog() -> list[dict[str, str]]:
        return list_setting_files(Path(store.setting("platemo_path")))

    catalog_cache: dict[str, Any] = {"root": "", "loaded_at": 0.0, "value": None}

    def current_catalogs() -> dict[str, list[dict[str, Any]]]:
        root = Path(store.setting("platemo_path"))
        root_key = str(root.resolve()) if root.exists() else str(root)
        # Heartbeats arrive every two seconds. Cache the expensive recursive
        # MATLAB source scan so scheduling does not block the HTTP event loop.
        if catalog_cache["value"] is None or catalog_cache["root"] != root_key or time.monotonic() - catalog_cache["loaded_at"] >= 60:
            catalog_cache["value"] = discover_catalogs(root) if root.is_dir() else {"algorithms": [], "problems": []}
            catalog_cache["root"] = root_key
            catalog_cache["loaded_at"] = time.monotonic()
        return catalog_cache["value"]

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

    def reclaim_expired_batches() -> None:
        """Recover lost leases while preserving the user's cancellation intent."""
        with store.connect() as con:
            expired = con.execute(
                "SELECT id, worker_id, experiment_point_id, state FROM batch_attempts "
                "WHERE state IN ('assigned','accepted','running','cancel_requested') "
                "AND (lease_deadline<? OR worker_id IN (SELECT id FROM workers WHERE status='suspect'))",
                (now(),),
            ).fetchall()
        for batch in expired:
            cancelled = batch["state"] == "cancel_requested"
            reason = "cancellation acknowledgement timeout" if cancelled else "heartbeat or lease timeout"
            store.recover_batch(batch["id"], reason, cancelled=cancelled)
            store.record_v2_event(
                "batch.cancelled" if cancelled else "batch.reclaimed",
                batch["experiment_point_id"], batch["id"],
                {"worker_id": batch["worker_id"], "reason": reason},
            )

    def reclaim_expired_dynamic_seeds() -> None:
        """Return lost dynamic Seed leases independently of the MATLAB session."""
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            rows = con.execute(
                "SELECT * FROM seed_attempts_v3 WHERE state IN ('assigned','running','cancel_requested') "
                "AND (lease_deadline<? OR worker_id IN (SELECT id FROM workers WHERE status='suspect'))",
                (now(),),
            ).fetchall()
            for attempt in rows:
                cancelled = attempt["state"] == "cancel_requested"
                state = "cancelled" if cancelled else "pending"
                con.execute("UPDATE seed_attempts_v3 SET state=?,finished_at=?,error=? WHERE id=?",
                            ("cancelled" if cancelled else "reclaimed", now(),
                             "cancellation acknowledgement timeout" if cancelled else "heartbeat or lease timeout", attempt["id"]))
                con.execute("UPDATE seed_runs_v2 SET state=?,batch_attempt_id=NULL,updated_at=? "
                            "WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                            (state, now(), attempt["experiment_point_id"], attempt["seed"], attempt["id"]))
            con.execute("COMMIT")

    app.state.reclaim_expired_dynamic_seeds = reclaim_expired_dynamic_seeds

    app.state.reclaim_expired_batches = reclaim_expired_batches

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
            reclaim_expired_batches()
            reclaim_expired_dynamic_seeds()
            await asyncio.sleep(10)

    @app.on_event("startup")
    async def start_health_probes() -> None:
        app.state.watchdog_task = asyncio.create_task(watchdog_forever())

    @app.on_event("shutdown")
    async def stop_health_probes() -> None:
        app.state.watchdog_task.cancel()
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
        catalog_cache["value"] = None
        return RedirectResponse(url="/?message=PlatEMO+path+updated", status_code=303)

    @app.put("/api/platemo-path")
    async def api_set_platemo_path(payload: dict[str, str]) -> dict[str, str]:
        """Set the configured PlatEMO root for the Vue client."""
        path = Path(payload.get("platemo_path", "")).expanduser()
        if not (path / "Algorithms").is_dir() or not (path / "Problems").is_dir() or not (path / "Data").is_dir():
            raise HTTPException(422, "PlatEMO path must contain Algorithms, Problems, and Data")
        resolved = str(path.resolve())
        store.set_setting("platemo_path", resolved)
        catalog_cache["value"] = None
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
        result: list[dict[str, Any]] = []
        with store.connect() as con:
            active_batches = {
                row["worker_id"]: row
                for row in con.execute(
                    "SELECT * FROM batch_attempts "
                    "WHERE state IN ('assigned','accepted','running','cancel_requested') "
                    "ORDER BY assigned_at DESC"
                ).fetchall()
            }
            dynamic_sessions = {
                row["worker_id"]: row for row in con.execute("SELECT * FROM worker_sessions_v2").fetchall()
            }
        for worker in store.workers():
            visible = {key: value for key, value in worker.items() if key not in {"token", "node_token"}}
            try:
                capabilities = json.loads(str(worker.get("capabilities_json", "") or "{}"))
            except json.JSONDecodeError:
                capabilities = {}
            if not isinstance(capabilities, dict):
                capabilities = {}
            visible["configured_pool_workers"] = capabilities.get("configured_pool_workers", 0)
            visible["max_seeds_per_batch"] = capabilities.get("max_seeds_per_batch", 0)
            visible["profile_ready"] = capabilities.get("profile_ready", int(visible["configured_pool_workers"] or 0) > 0)
            batch = active_batches.get(worker["id"])
            session = dynamic_sessions.get(worker["id"])
            visible["dynamic_session"] = ({
                "id": session["session_id"], "state": session["state"],
                "configured_workers": session["configured_workers"], "actual_workers": session["actual_workers"],
                "free_seed_slots": session["free_seed_slots"],
                "running_seeds": max(0, session["actual_workers"] - session["free_seed_slots"]),
                "last_heartbeat": session["last_heartbeat"],
            } if session is not None else None)
            if batch is not None:
                try:
                    pool = json.loads(batch["pool_json"] or "{}")
                except json.JSONDecodeError:
                    pool = {}
                visible["active_batch"] = {
                    "id": batch["id"], "state": batch["state"], "assigned_at": batch["assigned_at"],
                    "accepted_at": batch["accepted_at"], "lease_deadline": batch["lease_deadline"],
                    "matlab_pid": batch["pid"] or 0, "pool": pool,
                }
            else:
                visible["active_batch"] = None
            result.append(visible)
        return result

    @app.get("/api/v1/ui/tasks")
    async def list_tasks() -> list[dict[str, Any]]:
        """Return the v2 global SeedRun view used by the Vue task cards."""
        worker_names = {worker["id"]: worker["name"] for worker in store.workers()}
        result: list[dict[str, Any]] = []
        with store.connect() as con:
            rows = con.execute(
                "SELECT s.*, p.algorithm_json, p.problem_json, p.created_at, "
                "COALESCE(b.worker_id,d.worker_id) AS active_worker_id, b.pid, b.pool_json, "
                "d.id AS dynamic_attempt_id, d.session_id AS dynamic_session_id, ws.actual_workers AS dynamic_pool_workers "
                "FROM seed_runs_v2 s JOIN experiment_points p ON p.id=s.experiment_point_id "
                "LEFT JOIN batch_attempts b ON b.id=s.batch_attempt_id "
                "LEFT JOIN seed_attempts_v3 d ON d.id=s.batch_attempt_id "
                "LEFT JOIN worker_sessions_v2 ws ON ws.worker_id=d.worker_id "
                # The task panel derives both its active and pending counts
                # from this response. Do not truncate it before the client can
                # see an active Seed belonging to an older point.
                "ORDER BY p.created_at DESC, s.seed"
            ).fetchall()
        for seed in rows:
            algorithm = json.loads(seed["algorithm_json"])
            problem = json.loads(seed["problem_json"])
            parameters = dict(problem.get("parameters", {}))
            algorithm_parameters = algorithm.get("parameters", {})
            if not isinstance(algorithm_parameters, dict):
                algorithm_parameters = {}
            result.append({
                "id": f"{seed['experiment_point_id']}:{seed['seed']}", "task_id": seed["experiment_point_id"],
                "attempt_id": seed["batch_attempt_id"] or "", "batch_attempt_id": seed["batch_attempt_id"] or "",
                "state": seed["state"], "worker_id": seed["active_worker_id"], "worker_name": worker_names.get(seed["active_worker_id"], "未分配"),
                "algorithm": algorithm.get("name", ""), "problem": problem.get("name", ""), "seed": seed["seed"],
                "parameters": parameters, "algorithm_parameters": algorithm_parameters, "fe": seed["fe"], "total_fe": seed["total_fe"],
                "elapsed_seconds": seed["elapsed_seconds"], "created_at": seed["created_at"], "updated_at": seed["updated_at"], "error": seed["error"],
                "matlab_pid": seed["pid"] or 0,
                "attempt_kind": "dynamic_seed" if seed["dynamic_attempt_id"] else "batch",
                "pool": (json.loads(seed["pool_json"] or "{}") if seed["pool_json"] else
                         ({"actual_workers": seed["dynamic_pool_workers"] or 0, "session_id": seed["dynamic_session_id"]}
                          if seed["dynamic_attempt_id"] else {})),
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

    @app.post("/api/v2/workers/register")
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
        return {"worker_id": worker_id, "node_token": node_token, "heartbeat_seconds": 2, "protocol": "v2"}

    def assign_batch(worker_id: str, capacity: dict[str, Any]) -> dict[str, Any] | None:
        """Atomically choose pending seeds and issue one BatchAttempt for a heartbeat."""
        if store.setting("dispatch_paused", "0") == "1":
            return None
        available_slots = int(capacity.get("available_batch_slots", 0) or 0)
        configured_pool_workers = int(capacity.get("configured_pool_workers", 0) or 0)
        max_seeds_per_batch = int(capacity.get("max_seeds_per_batch", 0) or 0)
        if available_slots < 1 or configured_pool_workers < 1 or max_seeds_per_batch < 1:
            return None
        batch_size = min(max_seeds_per_batch, configured_pool_workers, 10_000)
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            worker = con.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
            if worker is None:
                con.execute("ROLLBACK")
                return None
            if int(worker["dispatch_paused"] or 0):
                con.execute("ROLLBACK")
                return None
            active_batches = con.execute(
                "SELECT COUNT(*) FROM batch_attempts "
                "WHERE worker_id=? AND state IN ('assigned','accepted','running','cancel_requested')",
                (worker_id,),
            ).fetchone()[0]
            if active_batches >= 1:
                con.execute("ROLLBACK")
                return None
            points = con.execute("SELECT * FROM experiment_points WHERE state='pending' ORDER BY created_at, ordinal").fetchall()
            for point in points:
                experiment = con.execute("SELECT id FROM experiments WHERE id=? AND state='running'", (point["experiment_id"],)).fetchone()
                if experiment is None:
                    continue
                experiment_config = json.loads(con.execute("SELECT config_json FROM experiments WHERE id=?", (point["experiment_id"],)).fetchone()[0])
                worker_capabilities = json.loads(worker["capabilities_json"] or "{}")
                if not worker_can_run(worker_capabilities, experiment_config):
                    continue
                blocked = con.execute(
                    "SELECT 1 FROM worker_point_blocks WHERE worker_id=? AND experiment_point_id=?",
                    (worker_id, point["id"]),
                ).fetchone()
                if blocked is not None:
                    continue
                requested_profile = str(experiment_config.get("cluster_profile", "") or "")
                worker_profile = str(worker_capabilities.get("cluster_profile", "") or "")
                assignment_profile = requested_profile or worker_profile or "local"
                if requested_profile and worker_profile and requested_profile != worker_profile:
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
                if not isinstance(params, dict):
                    params = {}
                problem_schema = next(
                    (item.get("parameters", []) for item in current_catalogs()["problems"]
                     if item.get("name") == problem.get("name")),
                    [],
                )
                custom_values = [
                    {"value": params.get(str(item.get("name")), item.get("default", ""))}
                    for item in problem_schema
                    if str(item.get("name")) not in {"N", "M", "D", "maxFE"}
                ]
                algorithm_params = algorithm.get("parameters", {})
                if not isinstance(algorithm_params, dict):
                    algorithm_params = {}
                algorithm_schema = next(
                    (item.get("parameters", []) for item in current_catalogs()["algorithms"]
                     if item.get("name") == algorithm.get("name")),
                    [],
                )
                algorithm_values = [
                    {"value": algorithm_params.get(str(item.get("name")), item.get("default", ""))}
                    for item in algorithm_schema
                ]
                store.record_v2_event("seed.assigned", point["id"], batch_id, {"worker_id": worker_id, "seeds": seed_values})
                settings_value = str(experiment_config.get("settings_file", "") or "")
                settings_name = Path(settings_value).name if settings_value else ""
                assignment = {"batch_attempt_id": batch_id, "lease_token": token, "experiment_point_id": point["id"],
                              "algorithm": algorithm, "problem": problem, "seeds": seed_values,
                              "max_fe": int(params.get("maxFE", params.get("max_fe", 50000)) or 50000),
                              "progress_interval_fe": max(1, int(params.get("maxFE", params.get("max_fe", 50000)) or 50000) // 100),
                              "retain_points": int(experiment_config.get("retain_points", 20)),
                              "algorithm_parameter_values": algorithm_values,
                              "problem_parameter_values": custom_values,
                               "settings_file": settings_name,
                               "settings_sha256": str(experiment_config.get("settings_sha256", "") or ""),
                               "settings_download_url": f"/api/v1/experiments/{point['experiment_id']}/settings" if experiment_config.get("settings_sha256") else "",
                              "required_platemo_commit": str(experiment_config.get("required_platemo_commit", "") or ""),
                              "minimum_disk_free_bytes": int(experiment_config.get("minimum_disk_free_bytes", 0) or 0),
                              "cluster_profile": assignment_profile}
                for key in ("N", "M", "D"):
                    if params.get(key) not in (None, ""):
                        assignment[key] = params[key]
                return assignment
            con.execute("COMMIT")
        return None

    def dynamic_seed_assignment(con: sqlite3.Connection, worker_id: str, session_id: str,
                                capabilities: dict[str, Any]) -> dict[str, Any] | None:
        """Lease one compatible pending Seed for a persistent Worker session."""
        if store.setting("dispatch_paused", "0") == "1":
            return None
        paused = con.execute("SELECT dispatch_paused FROM workers WHERE id=?", (worker_id,)).fetchone()
        if paused is None or paused["dispatch_paused"]:
            return None
        worker_profile = str(capabilities.get("cluster_profile", "") or "")
        for point in con.execute("SELECT * FROM experiment_points WHERE state='pending' ORDER BY created_at, ordinal"):
            experiment = con.execute("SELECT * FROM experiments WHERE id=? AND state='running'", (point["experiment_id"],)).fetchone()
            if experiment is None:
                continue
            config = json.loads(experiment["config_json"])
            if not worker_can_run(capabilities, config):
                continue
            if con.execute("SELECT 1 FROM worker_point_blocks WHERE worker_id=? AND experiment_point_id=?", (worker_id, point["id"])).fetchone():
                continue
            requested_profile = str(config.get("cluster_profile", "") or "")
            if requested_profile and worker_profile and requested_profile != worker_profile:
                continue
            seed_row = con.execute(
                "SELECT seed FROM seed_runs_v2 WHERE experiment_point_id=? AND state='pending' ORDER BY seed LIMIT 1", (point["id"],)
            ).fetchone()
            if seed_row is None:
                continue
            algorithm, problem = json.loads(point["algorithm_json"]), json.loads(point["problem_json"])
            params = problem.get("parameters", {}) if isinstance(problem, dict) else {}
            if not isinstance(params, dict):
                params = {}
            problem_schema = next((item.get("parameters", []) for item in current_catalogs()["problems"]
                                   if item.get("name") == problem.get("name")), [])
            problem_values = [{"value": params.get(str(item.get("name")), item.get("default", ""))}
                              for item in problem_schema if str(item.get("name")) not in {"N", "M", "D", "maxFE"}]
            algorithm_params = algorithm.get("parameters", {}) if isinstance(algorithm, dict) else {}
            if not isinstance(algorithm_params, dict):
                algorithm_params = {}
            algorithm_schema = next((item.get("parameters", []) for item in current_catalogs()["algorithms"]
                                     if item.get("name") == algorithm.get("name")), [])
            algorithm_values = [{"value": algorithm_params.get(str(item.get("name")), item.get("default", ""))}
                                for item in algorithm_schema]
            attempt_id, token = str(uuid.uuid4()), str(uuid.uuid4())
            assigned_at = now()
            deadline = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()
            con.execute(
                "INSERT INTO seed_attempts_v3 (id,experiment_point_id,seed,worker_id,session_id,lease_token,state,assigned_at,lease_deadline,total_fe) VALUES (?,?,?,?,?,?, 'assigned',?,?,?)",
                (attempt_id, point["id"], seed_row["seed"], worker_id, session_id, token, assigned_at, deadline,
                 int(params.get("maxFE", params.get("max_fe", 50000)) or 50000)),
            )
            con.execute(
                "UPDATE seed_runs_v2 SET state='leased', batch_attempt_id=?, attempts=attempts+1, updated_at=? WHERE experiment_point_id=? AND seed=? AND state='pending'",
                (attempt_id, assigned_at, point["id"], seed_row["seed"]),
            )
            return {"attempt_id": attempt_id, "lease_token": token, "experiment_point_id": point["id"],
                    "seed": seed_row["seed"], "algorithm": algorithm, "problem": problem,
                    "max_fe": int(params.get("maxFE", params.get("max_fe", 50000)) or 50000),
                    "retain_points": int(config.get("retain_points", 20)),
                    "progress_interval_fe": max(1, int(params.get("maxFE", params.get("max_fe", 50000)) or 50000) // 100),
                    "algorithm_parameter_values": algorithm_values,
                    "problem_parameter_values": problem_values,
                    "cluster_profile": requested_profile or worker_profile or "local",
                    "settings_file": Path(str(config.get("settings_file", "") or "")).name,
                    "settings_sha256": str(config.get("settings_sha256", "") or ""),
                    "settings_download_url": f"/api/v1/experiments/{point['experiment_id']}/settings" if config.get("settings_sha256") else "",
                    "required_platemo_commit": str(config.get("required_platemo_commit", "") or ""),
                    "minimum_disk_free_bytes": int(config.get("minimum_disk_free_bytes", 0) or 0),
                    **{key: params[key] for key in ("N", "M", "D") if params.get(key) not in (None, "")}}
        return None

    @app.post("/api/v2/workers/{worker_id}/heartbeat")
    async def dynamic_session_heartbeat(worker_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, Any]:
        worker = require_node(worker_id, authorization)
        session_id = str(payload.get("session_id", ""))
        pool = payload.get("pool", {})
        if not session_id or not isinstance(pool, dict):
            raise HTTPException(422, "session_id and pool are required")
        try:
            free_slots = max(0, int(payload.get("free_seed_slots", 0)))
            configured = max(0, int(pool.get("configured_workers", 0)))
            actual = max(0, int(pool.get("actual_workers", 0)))
        except (TypeError, ValueError) as exc:
            raise HTTPException(422, "invalid pool capacity") from exc
        capabilities = json.loads(worker.get("capabilities_json", "{}") or "{}")
        assignments: list[dict[str, Any]] = []
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            con.execute("UPDATE workers SET online=1,status='online',last_heartbeat=?,last_check=?,queue_count=? WHERE id=?",
                        (now(), now(), free_slots, worker_id))
            con.execute("INSERT INTO worker_sessions_v2 VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(worker_id) DO UPDATE SET session_id=excluded.session_id,state=excluded.state,configured_workers=excluded.configured_workers,actual_workers=excluded.actual_workers,free_seed_slots=excluded.free_seed_slots,last_heartbeat=excluded.last_heartbeat,updated_at=excluded.updated_at",
                        (worker_id, session_id, str(pool.get("state", "ready")), configured, actual, free_slots, now(), now()))
            for running in payload.get("running_seeds", []):
                if not isinstance(running, dict):
                    continue
                # A restarted Worker receives a new session_id. The node token
                # plus attempt lease token still prove ownership, so permit the
                # new session to resume a durable delivering Seed.
                attempt = con.execute("SELECT * FROM seed_attempts_v3 WHERE id=? AND worker_id=? AND lease_token=?", (str(running.get("attempt_id", "")), worker_id, str(running.get("lease_token", "")))).fetchone()
                if attempt is None or attempt["state"] not in {"assigned", "running"}:
                    continue
                con.execute("UPDATE seed_attempts_v3 SET session_id=?,state='running',accepted_at=CASE WHEN accepted_at='' THEN ? ELSE accepted_at END,fe=?,total_fe=?,elapsed_seconds=?,lease_deadline=? WHERE id=?",
                            (session_id, now(), int(running.get("fe", 0)), int(running.get("total_fe", attempt["total_fe"])), float(running.get("elapsed_seconds", 0)), (datetime.now(timezone.utc)+timedelta(seconds=30)).isoformat(), attempt["id"]))
            cancellations = [row["id"] for row in con.execute(
                "SELECT id FROM seed_attempts_v3 WHERE worker_id=? AND state='cancel_requested'",
                (worker_id,),
            )]
            # The Worker-reported free slots are advisory. Bound dispatch by
            # the Master lease table as well, so stale attempts from a
            # delivery/restart race can never overbook the MATLAB pool.
            active_count = con.execute(
                "SELECT COUNT(*) FROM seed_attempts_v3 "
                "WHERE worker_id=? AND state IN ('assigned','running','cancel_requested')",
                (worker_id,),
            ).fetchone()[0]
            effective_free_slots = min(free_slots, max(0, actual - int(active_count)))
            while effective_free_slots > len(assignments) and actual > 0:
                assignment = dynamic_seed_assignment(con, worker_id, session_id, capabilities)
                if assignment is None:
                    break
                assignments.append(assignment)
            con.execute("COMMIT")
        return {"status": "ok", "server_time": now(), "assignments": assignments,
                "cancel_attempt_ids": cancellations}

    def active_seed_attempt(con: sqlite3.Connection, attempt_id: str, token: str,
                            authorization: str | None) -> sqlite3.Row:
        attempt = con.execute("SELECT * FROM seed_attempts_v3 WHERE id=? AND lease_token=?", (attempt_id, token)).fetchone()
        supplied = authorization[7:] if authorization and authorization.lower().startswith("bearer ") else ""
        if attempt is None or attempt["state"] not in {"assigned", "running", "cancel_requested"}:
            raise HTTPException(410, "Seed lease expired or invalid")
        worker = con.execute("SELECT node_token FROM workers WHERE id=?", (attempt["worker_id"],)).fetchone()
        if worker is None or supplied != worker["node_token"] or attempt["lease_deadline"] <= now():
            raise HTTPException(410, "Seed lease expired or invalid")
        return attempt

    @app.post("/api/v2/seed-attempts/{attempt_id}/progress")
    async def dynamic_seed_progress(attempt_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, str]:
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            attempt = active_seed_attempt(con, attempt_id, str(payload.get("lease_token", "")), authorization)
            state = str(payload.get("state", "running"))
            if state != "running":
                raise HTTPException(422, "Seed terminal state requires complete")
            con.execute("UPDATE seed_attempts_v3 SET state=?,accepted_at=CASE WHEN accepted_at='' THEN ? ELSE accepted_at END,fe=?,total_fe=?,elapsed_seconds=?,error=?,lease_deadline=? WHERE id=?",
                        (state, now(), int(payload.get("fe", 0)), int(payload.get("total_fe", attempt["total_fe"])), float(payload.get("elapsed_seconds", 0)), str(payload.get("error", "")), (datetime.now(timezone.utc)+timedelta(seconds=30)).isoformat(), attempt_id))
            con.execute("UPDATE seed_runs_v2 SET state=?,fe=?,total_fe=?,elapsed_seconds=?,error=?,updated_at=? WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                        (state, int(payload.get("fe", 0)), int(payload.get("total_fe", attempt["total_fe"])), float(payload.get("elapsed_seconds", 0)), str(payload.get("error", "")), now(), attempt["experiment_point_id"], attempt["seed"], attempt_id))
            con.execute("COMMIT")
        return {"status": "accepted"}

    @app.post("/api/v2/seed-attempts/{attempt_id}/reject")
    async def dynamic_seed_reject(attempt_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, str]:
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            attempt = active_seed_attempt(con, attempt_id, str(payload.get("lease_token", "")), authorization)
            reason = str(payload.get("reason", "input_incompatible"))[:1000]
            con.execute("UPDATE seed_attempts_v3 SET state='rejected',finished_at=?,error=? WHERE id=?", (now(), reason, attempt_id))
            con.execute("UPDATE seed_runs_v2 SET state='pending',batch_attempt_id=NULL,error=?,updated_at=? "
                        "WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                        (reason, now(), attempt["experiment_point_id"], attempt["seed"], attempt_id))
            con.execute("COMMIT")
        return {"status": "accepted"}

    @app.post("/api/v2/seed-attempts/{attempt_id}/cancel")
    async def cancel_dynamic_seed(attempt_id: str) -> dict[str, str]:
        """Request cancellation of one dynamically scheduled Seed."""
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            attempt = con.execute("SELECT * FROM seed_attempts_v3 WHERE id=?", (attempt_id,)).fetchone()
            if attempt is None:
                raise HTTPException(404, "Seed attempt not found")
            if attempt["state"] in {"completed", "failed", "cancelled", "reclaimed", "rejected"}:
                raise HTTPException(409, "Seed attempt is already terminal")
            con.execute("UPDATE seed_attempts_v3 SET state='cancel_requested',error=? WHERE id=?",
                        ("cancel requested by user", attempt_id))
            con.execute("UPDATE seed_runs_v2 SET error=?,updated_at=? WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                        ("cancel requested by user", now(), attempt["experiment_point_id"], attempt["seed"], attempt_id))
            con.execute("COMMIT")
        store.record_v2_event("seed.cancel_requested", attempt["experiment_point_id"], attempt_id, {})
        return {"status": "accepted"}

    @app.put("/api/v2/artifacts/{artifact_id}")
    async def dynamic_seed_artifact(artifact_id: str, attempt_id: str = Form(...), lease_token: str = Form(...), artifact: UploadFile = File(...), authorization: str | None = Header(None)) -> dict[str, Any]:
        content = await artifact.read()
        digest = hashlib.sha256(content).hexdigest()
        # Resolve the final path before writing; no write transaction is held while bytes hit disk.
        with store.connect() as con:
            attempt = active_seed_attempt(con, attempt_id, lease_token, authorization)
            path = seed_result_destination(con, attempt["experiment_point_id"], attempt["seed"])
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + f".{artifact_id}.part")
        temporary.write_bytes(content)
        try:
            with store.connect() as con:
                con.execute("BEGIN IMMEDIATE")
                attempt = active_seed_attempt(con, attempt_id, lease_token, authorization)
                existing = con.execute("SELECT sha256,size FROM artifacts_v3 WHERE id=?", (artifact_id,)).fetchone()
                if existing is not None and (existing["sha256"] != digest or existing["size"] != len(content)):
                    raise HTTPException(409, "artifact_id content differs from the registered artifact")
                # The expensive write has completed; atomic replacement is the
                # only filesystem operation inside the lease transaction.
                temporary.replace(path)
                con.execute("INSERT INTO artifacts_v3 VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING",
                            (artifact_id, attempt_id, attempt["experiment_point_id"], attempt["seed"], "seed_result", str(path), digest, len(content), now()))
                con.execute("UPDATE seed_runs_v2 SET result_path=?,updated_at=? WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                            (str(path), now(), attempt["experiment_point_id"], attempt["seed"], attempt_id))
                con.execute("COMMIT")
        finally:
            temporary.unlink(missing_ok=True)
        return {"status": "stored", "artifact_id": artifact_id, "sha256": digest, "size": len(content)}

    @app.post("/api/v2/seed-attempts/{attempt_id}/complete")
    async def dynamic_seed_complete(attempt_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, str]:
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            attempt = active_seed_attempt(con, attempt_id, str(payload.get("lease_token", "")), authorization)
            state = str(payload.get("state", "failed"))
            if state not in {"completed", "failed", "cancelled"}:
                raise HTTPException(422, "Invalid Seed completion state")
            if state == "completed" and con.execute("SELECT 1 FROM artifacts_v3 WHERE seed_attempt_id=? AND kind='seed_result'", (attempt_id,)).fetchone() is None:
                raise HTTPException(409, "Completed Seed requires seed_result artifact")
            con.execute("UPDATE seed_attempts_v3 SET state=?,finished_at=?,error=? WHERE id=?", (state, now(), str(payload.get("error", "")), attempt_id))
            con.execute("UPDATE seed_runs_v2 SET state=?,fe=?,total_fe=?,elapsed_seconds=?,error=?,updated_at=? WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                        (state, int(payload.get("fe", attempt["total_fe"])), int(payload.get("total_fe", attempt["total_fe"])), float(payload.get("elapsed_seconds", 0)), str(payload.get("error", "")), now(), attempt["experiment_point_id"], attempt["seed"], attempt_id))
            remaining = con.execute(
                "SELECT COUNT(*) FROM seed_runs_v2 WHERE experiment_point_id=? "
                "AND state NOT IN ('completed','failed','cancelled')",
                (attempt["experiment_point_id"],),
            ).fetchone()[0]
            if remaining == 0:
                con.execute("UPDATE experiment_points SET state='completed',updated_at=? "
                            "WHERE id=? AND state!='cancelled'", (now(), attempt["experiment_point_id"]))
            con.execute("COMMIT")
        return {"status": "accepted"}

    @app.post("/api/v1/workers/{worker_id}/heartbeat")
    async def worker_heartbeat(worker_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, Any]:
        require_node(worker_id, authorization)
        capacity = {key: payload.get(key, 0) for key in ("available_batch_slots", "max_concurrent_batches", "configured_pool_workers", "actual_pool_workers", "max_seeds_per_batch")}
        running_batches = payload.get("running_batches", [])
        if not isinstance(running_batches, list):
            raise HTTPException(422, "running_batches must be an array")
        reclaimed: list[sqlite3.Row] = []
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            con.execute("UPDATE workers SET online=1, status='online', last_heartbeat=?, last_check=?, queue_count=?, capabilities_json=? WHERE id=?",
                        (now(), now(), int(capacity["available_batch_slots"]), json.dumps(payload.get("capabilities", {})), worker_id))
            for running in running_batches:
                if not isinstance(running, dict):
                    continue
                batch_id = str(running.get("batch_attempt_id", ""))
                lease_token = str(running.get("lease_token", ""))
                if not batch_id or not lease_token:
                    continue
                batch = con.execute(
                    "SELECT * FROM batch_attempts WHERE id=? AND worker_id=? AND lease_token=?",
                    (batch_id, worker_id, lease_token),
                ).fetchone()
                if batch is None or batch["state"] not in {"accepted", "running"}:
                    continue
                if batch["lease_deadline"] <= now():
                    recover_batch_in_transaction(con, batch, "lease deadline expired", False)
                    reclaimed.append(batch)
                    continue
                apply_heartbeat_summary_in_transaction(con, batch, running, capacity)
            cancelled = con.execute("SELECT id FROM batch_attempts WHERE worker_id=? AND state='cancel_requested'", (worker_id,)).fetchall()
            con.execute("COMMIT")
        for batch in reclaimed:
            store.record_v2_event("batch.reclaimed", batch["experiment_point_id"], batch["id"],
                                  {"reason": "lease_deadline_expired"})
        assignment = assign_batch(worker_id, capacity)
        cancel_ids = [row["id"] for row in cancelled] + [batch["id"] for batch in reclaimed]
        return {"status": "ok", "server_time": now(), "cancel_batch_attempt_ids": cancel_ids, "assignment": assignment}

    def recover_batch_in_transaction(con: sqlite3.Connection, batch: sqlite3.Row, reason: str, cancelled: bool) -> None:
        """Apply the v2 recovery transition without opening a second transaction."""
        terminal = ("completed", "failed", "cancelled")
        con.execute(
            "UPDATE batch_attempts SET state=?, finished_at=?, error=? WHERE id=?",
            ("cancelled" if cancelled else "reclaimed", now(), reason, batch["id"]),
        )
        rows = con.execute("SELECT seed, state FROM seed_runs_v2 WHERE batch_attempt_id=?", (batch["id"],)).fetchall()
        reported_completed = {row["seed"] for row in rows if row["state"] == "completed"}
        artifacts = con.execute("SELECT seed, kind FROM artifacts_v2 WHERE batch_attempt_id=?", (batch["id"],)).fetchall()
        covered_completed = completed_seeds_with_artifacts(
            reported_completed, ((row["seed"], row["kind"]) for row in artifacts)
        )
        for row in rows:
            # A progress/heartbeat "completed" marker only means MATLAB has
            # computed it.  It becomes durable only after a registered
            # aggregate result or its own Seed artifact covers it.
            needs_recovery = row["state"] not in terminal or (
                row["state"] == "completed" and row["seed"] not in covered_completed
            )
            if needs_recovery:
                con.execute(
                    "UPDATE seed_runs_v2 SET state=?, batch_attempt_id=NULL, error=?, updated_at=? "
                    "WHERE batch_attempt_id=? AND seed=?",
                    ("cancelled" if cancelled else "pending", reason, now(), batch["id"], row["seed"]),
                )
        con.execute("UPDATE experiment_points SET updated_at=? WHERE id=?", (now(), batch["experiment_point_id"]))

    def active_batch_in_transaction(con: sqlite3.Connection, batch_id: str, token: str,
                                    authorization: str | None) -> sqlite3.Row:
        """Authenticate and validate a lease while the caller holds BEGIN IMMEDIATE."""
        batch = con.execute("SELECT * FROM batch_attempts WHERE id=? AND lease_token=?", (batch_id, token)).fetchone()
        if batch is None or batch["state"] not in {"assigned", "accepted", "running", "cancel_requested"}:
            raise HTTPException(410, "Lease expired or invalid")
        worker = con.execute("SELECT node_token FROM workers WHERE id=?", (batch["worker_id"],)).fetchone()
        supplied = authorization[7:] if authorization and authorization.lower().startswith("bearer ") else ""
        if worker is None or supplied != worker["node_token"]:
            raise HTTPException(401, "Invalid node token")
        if batch["lease_deadline"] <= now():
            cancelled = batch["state"] == "cancel_requested"
            recover_batch_in_transaction(con, batch, "lease deadline expired", cancelled)
            con.execute("COMMIT")
            store.record_v2_event(
                "batch.cancelled" if cancelled else "batch.reclaimed",
                batch["experiment_point_id"], batch_id, {"reason": "lease_deadline_expired"},
            )
            raise HTTPException(410, "Lease expired or invalid")
        return batch

    def apply_progress_in_transaction(con: sqlite3.Connection, batch: sqlite3.Row,
                                      batch_id: str, payload: dict[str, Any]) -> None:
        if batch["state"] == "assigned":
            con.execute("UPDATE batch_attempts SET state='accepted', accepted_at=? WHERE id=?", (now(), batch_id))
        con.execute(
            "UPDATE batch_attempts SET state=CASE WHEN state IN ('accepted','assigned') THEN 'running' ELSE state END, "
            "pool_json=?, pid=?, lease_deadline=? WHERE id=?",
            (json.dumps({"pool": payload.get("pool", {}), "actual_pool_workers": payload.get("actual_pool_workers", payload.get("pool", {}).get("workers", 0) if isinstance(payload.get("pool", {}), dict) else 0)}),
             payload.get("pid", payload.get("matlab_pid")),
             (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(), batch_id),
        )
        assigned = set(json.loads(batch["seed_json"]))
        for run in payload.get("runs", []):
            if not isinstance(run, dict) or run.get("seed") not in assigned:
                continue
            existing = con.execute(
                "SELECT state FROM seed_runs_v2 WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                (batch["experiment_point_id"], run["seed"], batch_id),
            ).fetchone()
            if existing is not None and existing["state"] in {"completed", "failed", "cancelled"}:
                continue
            con.execute(
                "UPDATE seed_runs_v2 SET state=?, fe=?, total_fe=?, elapsed_seconds=?, error=?, updated_at=? "
                "WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                (str(run.get("state", "running")), int(run.get("fe", 0)), int(run.get("total_fe", 0)),
                 float(run.get("elapsed_seconds", 0)), str(run.get("error", "")), now(),
                 batch["experiment_point_id"], run["seed"], batch_id),
            )

    def apply_heartbeat_summary_in_transaction(con: sqlite3.Connection, batch: sqlite3.Row,
                                                payload: dict[str, Any], capacity: dict[str, Any]) -> None:
        """Persist an authenticated heartbeat without allowing a state transition."""
        pool = payload.get("pool", {})
        con.execute(
            "UPDATE batch_attempts SET pid=?, pool_json=?, lease_deadline=? WHERE id=?",
            (payload.get("matlab_pid", payload.get("pid")), json.dumps({
                "configured_pool_workers": payload.get("configured_pool_workers", capacity["configured_pool_workers"]),
                "actual_pool_workers": payload.get("actual_pool_workers", capacity["actual_pool_workers"]),
                "pool": pool if isinstance(pool, dict) else {},
            }), (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(), batch["id"]),
        )
        assigned = set(json.loads(batch["seed_json"]))
        runs = payload.get("runs", [])
        if not isinstance(runs, list):
            return
        for run in runs:
            if not isinstance(run, dict) or run.get("seed") not in assigned:
                continue
            try:
                fe, total_fe = int(run.get("fe", 0)), int(run.get("total_fe", 0))
                elapsed = float(run.get("elapsed_seconds", 0))
            except (TypeError, ValueError):
                continue
            existing = con.execute(
                "SELECT state FROM seed_runs_v2 WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                (batch["experiment_point_id"], run["seed"], batch["id"]),
            ).fetchone()
            if existing is not None and existing["state"] in {"completed", "failed", "cancelled"}:
                continue
            con.execute(
                "UPDATE seed_runs_v2 SET state=?, fe=?, total_fe=?, elapsed_seconds=?, error=?, updated_at=? "
                "WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                (str(run.get("state", "running")), fe, total_fe, elapsed, str(run.get("error", "")), now(),
                 batch["experiment_point_id"], run["seed"], batch["id"]),
            )

    @app.post("/api/v1/batch-attempts/{batch_id}/progress")
    async def batch_progress(batch_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, str]:
        phase = str(payload.get("phase", "running"))
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            batch = active_batch_in_transaction(con, batch_id, str(payload.get("lease_token", "")), authorization)
            if phase == "rejected":
                if batch["state"] != "assigned":
                    raise HTTPException(409, "Only an assigned batch can be rejected")
                error_code = str(payload.get("error_code", "worker_rejected"))
                if error_code in {"input_incompatible", "profile_unavailable", "version_mismatch", "insufficient_disk"}:
                    con.execute(
                        "INSERT OR REPLACE INTO worker_point_blocks VALUES (?, ?, ?, ?)",
                        (batch["worker_id"], batch["experiment_point_id"], error_code, now()),
                    )
                recover_batch_in_transaction(con, batch, error_code, False)
                con.execute("COMMIT")
                store.record_v2_event("batch.reclaimed", batch["experiment_point_id"], batch_id, {"reason": "worker_rejected"})
                return {"status": "reclaimed"}
            apply_progress_in_transaction(con, batch, batch_id, payload)
            con.execute("COMMIT")
        store.record_v2_event("batch.progress", batch["experiment_point_id"], batch_id, {"phase": phase})
        return {"status": "accepted"}

    @app.post("/api/v1/batch-attempts/{batch_id}/complete")
    async def batch_complete(batch_id: str, payload: dict[str, Any], authorization: str | None = Header(None)) -> dict[str, str]:
        final_state = str(payload.get("state", "failed"))
        if final_state not in {"completed", "failed", "cancelled"}:
            raise HTTPException(422, "Invalid completion state")
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            batch = active_batch_in_transaction(con, batch_id, str(payload.get("lease_token", "")), authorization)
            apply_progress_in_transaction(con, batch, batch_id, payload)
            cancellation_requested = batch["state"] == "cancel_requested" or final_state == "cancelled"
            completed_runs = con.execute(
                "SELECT seed FROM seed_runs_v2 WHERE batch_attempt_id=? AND state='completed'",
                (batch_id,),
            ).fetchall()
            if completed_runs:
                artifacts = con.execute(
                    "SELECT seed, kind FROM artifacts_v2 WHERE batch_attempt_id=?", (batch_id,)
                ).fetchall()
                completed_seeds = {row["seed"] for row in completed_runs}
                covered_completed = completed_seeds_with_artifacts(
                    completed_seeds, ((row["seed"], row["kind"]) for row in artifacts)
                )
                if final_state == "completed" and not cancellation_requested and not completed_artifacts_are_covered(
                    completed_seeds, ((row["seed"], row["kind"]) for row in artifacts)
                ):
                    raise HTTPException(409, "Completed seeds require covering artifacts")
                if final_state == "failed" or cancellation_requested:
                    for seed in completed_seeds - covered_completed:
                        recovery_state = "cancelled" if cancellation_requested else "pending"
                        recovery_error = ("completed seed result was unavailable after cancellation"
                                          if cancellation_requested else "completed seed result was unavailable after batch failure")
                        con.execute(
                            "UPDATE seed_runs_v2 SET state=?, batch_attempt_id=NULL, "
                            "error=?, updated_at=? WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?",
                            (recovery_state, recovery_error, now(),
                             batch["experiment_point_id"], seed, batch_id),
                        )
            con.execute("UPDATE batch_attempts SET state=?, finished_at=?, error=? WHERE id=?", (final_state, now(), str(payload.get("error", "")), batch_id))
            rows = con.execute("SELECT seed, state FROM seed_runs_v2 WHERE batch_attempt_id=?", (batch_id,)).fetchall()
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
            con.execute("COMMIT")
        store.record_v2_event("batch.completed", batch["experiment_point_id"], batch_id, {"state": final_state})
        return {"status": "accepted"}

    @app.put("/api/v1/artifacts/{artifact_id}")
    async def upload_artifact(artifact_id: str, experiment_point_id: str = Form(...), batch_attempt_id: str = Form(...), lease_token: str = Form(...), seed: int | None = Form(None), kind: str = Form("result"), artifact: UploadFile = File(...), authorization: str | None = Header(None)) -> dict[str, Any]:
        content = await artifact.read()
        digest = hashlib.sha256(content).hexdigest()
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            batch = active_batch_in_transaction(con, batch_attempt_id, lease_token, authorization)
            if batch["experiment_point_id"] != experiment_point_id or (seed is not None and seed not in json.loads(batch["seed_json"])):
                raise HTTPException(422, "Artifact does not belong to this batch")
            existing = con.execute("SELECT experiment_point_id, batch_attempt_id, seed, kind FROM artifacts_v2 WHERE id=?", (artifact_id,)).fetchone()
            if existing is not None and (existing["experiment_point_id"] != experiment_point_id or existing["batch_attempt_id"] != batch_attempt_id or existing["seed"] != seed or existing["kind"] != kind):
                raise HTTPException(409, "artifact_id belongs to another logical artifact")
            if existing is not None:
                recorded = con.execute("SELECT sha256, size FROM artifacts_v2 WHERE id=?", (artifact_id,)).fetchone()
                if recorded["sha256"] != digest or recorded["size"] != len(content):
                    raise HTTPException(409, "artifact_id content differs from the registered artifact")
            if kind == "seed_result":
                if seed is None:
                    raise HTTPException(422, "seed_result requires seed")
                path = seed_result_destination(con, experiment_point_id, seed)
                destination = path.parent
            else:
                destination = store.results_dir / experiment_point_id / batch_attempt_id
                path = destination / Path(artifact.filename or f"{artifact_id}.bin").name
            destination.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            con.execute("INSERT INTO artifacts_v2 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(id) DO UPDATE SET path=excluded.path, sha256=excluded.sha256, size=excluded.size, created_at=excluded.created_at",
                        (artifact_id, experiment_point_id, batch_attempt_id, seed, kind, str(path), digest, len(content), now()))
            if seed is not None:
                con.execute("UPDATE seed_runs_v2 SET result_path=? WHERE experiment_point_id=? AND seed=? AND batch_attempt_id=?", (str(path), experiment_point_id, seed, batch_attempt_id))
            con.execute("COMMIT")
        return {"status": "stored", "artifact_id": artifact_id, "sha256": digest, "size": len(content)}

    @app.get("/api/v1/experiments/{experiment_id}/settings")
    async def download_experiment_settings(experiment_id: str, authorization: str | None = Header(None)) -> FileResponse:
        """Serve an uploaded Settings MAT only to a Worker holding this experiment's batch."""
        supplied = authorization[7:] if authorization and authorization.lower().startswith("bearer ") else ""
        with store.connect() as con:
            worker = con.execute("SELECT id FROM workers WHERE node_token=?", (supplied,)).fetchone()
            experiment = con.execute("SELECT config_json FROM experiments WHERE id=?", (experiment_id,)).fetchone()
            authorized_batch = con.execute(
                "SELECT 1 FROM batch_attempts b JOIN experiment_points p ON p.id=b.experiment_point_id "
                "WHERE b.worker_id=? AND p.experiment_id=? "
                "AND b.state IN ('assigned','accepted','running','cancel_requested') LIMIT 1",
                (worker["id"], experiment_id),
            ).fetchone() if worker is not None else None
            authorized_dynamic = con.execute(
                "SELECT 1 FROM seed_attempts_v3 s JOIN experiment_points p ON p.id=s.experiment_point_id "
                "WHERE s.worker_id=? AND p.experiment_id=? AND s.state IN ('assigned','running','cancel_requested') LIMIT 1",
                (worker["id"], experiment_id),
            ).fetchone() if worker is not None else None
        if worker is None:
            raise HTTPException(401, "Invalid node token")
        if experiment is None:
            raise HTTPException(404, "Experiment not found")
        if authorized_batch is None and authorized_dynamic is None:
            raise HTTPException(403, "Worker has no active batch for this experiment")
        config = json.loads(experiment["config_json"])
        settings_path = Path(str(config.get("settings_file", "")))
        if not is_distributable_upload(settings_path, store.uploads_dir):
            raise HTTPException(404, "Settings file is unavailable")
        return FileResponse(settings_path, filename=settings_path.name, media_type="application/octet-stream")

    @app.post("/api/v1/experiments")
    async def create_experiment(
        algorithms_json: str = Form(...),
        problems_json: str = Form(...),
        runs: int = Form(30),
        retain_points: int = Form(20),
        cluster_profile: str = Form(""),
        settings_file: str = Form(""),
        required_platemo_commit: str = Form(""),
        minimum_disk_free_bytes: int = Form(0),
        settings_upload: UploadFile | None = File(None),
    ) -> dict[str, Any]:
        """Create global SeedRuns. A later heartbeat assigns batch slices."""
        try:
            algorithms = json.loads(algorithms_json)
            problems = json.loads(problems_json)
        except json.JSONDecodeError as exc:
            raise HTTPException(400, "Invalid experiment list") from exc
        if not algorithms or not problems:
            raise HTTPException(400, "Select at least one algorithm and one problem")
        if not 1 <= runs <= 1000 or retain_points < 1:
            raise HTTPException(400, "Runs must be between 1 and 1000")
        experiment_id = str(uuid.uuid4())
        uploaded_settings = ""
        uploaded_settings_sha256 = ""
        if settings_upload and settings_upload.filename:
            if not settings_upload.filename.lower().endswith(".mat"):
                raise HTTPException(400, "Settings upload must be a MAT file")
            content = await settings_upload.read()
            try:
                parsed_settings = parse_setting_data(
                    loadmat(BytesIO(content), simplify_cells=True), current_catalogs(), settings_upload.filename,
                )
            except (OSError, ValueError, TypeError) as exc:
                raise HTTPException(422, "Invalid Settings MAT") from exc
            if parsed_settings.get("format") == "platemo-setting":
                selected_algorithms = {str(item.get("name", "")) for item in algorithms if isinstance(item, dict)}
                selected_problems = {str(item.get("name", "")) for item in problems if isinstance(item, dict)}
                baseline_algorithms = {str(item.get("name", "")) for item in parsed_settings.get("algorithms", []) if isinstance(item, dict)}
                baseline_problems = {str(item.get("name", "")) for item in parsed_settings.get("problems", []) if isinstance(item, dict)}
                if not selected_algorithms.issubset(baseline_algorithms) or not selected_problems.issubset(baseline_problems):
                    raise HTTPException(422, "Settings MAT does not cover all selected algorithms and problems")
                settings_dir = store.uploads_dir / experiment_id
                settings_dir.mkdir(parents=True, exist_ok=True)
                try:
                    uploaded_path = uploaded_settings_path(store.uploads_dir, experiment_id, settings_upload.filename)
                except ValueError as exc:
                    raise HTTPException(400, "Invalid settings filename") from exc
                uploaded_path.write_bytes(content)
                uploaded_settings = str(uploaded_path)
                uploaded_settings_sha256 = sha256_file(uploaded_path)
            elif parsed_settings.get("format") != "platemo-hpc-settings":
                raise HTTPException(422, "Unsupported Settings MAT format")
        planned = [(algorithm, problem) for algorithm in algorithms for problem in problems]
        config = {"runs": runs, "retain_points": retain_points, "cluster_profile": cluster_profile.strip(), "settings_file": uploaded_settings or settings_file,
                  "settings_sha256": uploaded_settings_sha256,
                  "required_platemo_commit": required_platemo_commit.strip(), "minimum_disk_free_bytes": max(0, minimum_disk_free_bytes)}
        with store.connect() as con:
            # max_workers and allowed_workers_json are retained only for existing databases.
            con.execute("INSERT INTO experiments (id, state, config_json, max_workers, created_at, updated_at) VALUES (?, 'running', ?, NULL, ?, ?)",
                        (experiment_id, json.dumps(config), now(), now()))
            for index, (algorithm, problem) in enumerate(planned):
                point_id = str(uuid.uuid4())
                algorithm_payload = algorithm if isinstance(algorithm, dict) else {"name": str(algorithm), "parameters": {}}
                problem_payload = problem if isinstance(problem, dict) else {"name": str(problem), "parameters": {}}
                problem_parameters = dict(problem_payload.get("parameters", {}))
                con.execute("INSERT INTO experiment_points (id, experiment_id, ordinal, algorithm_json, problem_json, allowed_workers_json, state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, '[]', 'pending', ?, ?)",
                            (point_id, experiment_id, index, json.dumps(algorithm_payload), json.dumps(problem_payload), now(), now()))
                max_fe = int(problem_parameters.get("maxFE", problem_parameters.get("max_fe", 50000)) or 50000)
                con.executemany("INSERT INTO seed_runs_v2 (experiment_point_id, seed, state, total_fe, updated_at) VALUES (?, ?, 'pending', ?, ?)",
                                [(point_id, seed, max_fe, now()) for seed in range(1, runs + 1)])
        store.record_v2_event("experiment.created", experiment_id, None, {"points": len(planned), "runs": runs})
        return {"experiment_id": experiment_id, "point_count": len(planned), "seed_count": len(planned) * runs,
                "worker_count": len(store.workers())}

    @app.post("/api/v1/experiment-points/{experiment_point_id}/cancel")
    async def cancel_point(experiment_point_id: str) -> dict[str, str]:
        with store.connect() as con:
            row = con.execute("SELECT id FROM experiment_points WHERE id=?", (experiment_point_id,)).fetchone()
            if row is None:
                raise HTTPException(404, "Experiment point not found")
            con.execute("UPDATE experiment_points SET state='cancelled', updated_at=? WHERE id=?", (now(), experiment_point_id))
            con.execute("UPDATE seed_runs_v2 SET state='cancelled', updated_at=? WHERE experiment_point_id=? AND state='pending'", (now(), experiment_point_id))
            con.execute("UPDATE batch_attempts SET state='cancel_requested' WHERE experiment_point_id=? AND state IN ('assigned','accepted','running')", (experiment_point_id,))
            con.execute("UPDATE seed_attempts_v3 SET state='cancel_requested' WHERE experiment_point_id=? AND state IN ('assigned','running')", (experiment_point_id,))
        store.record_v2_event("point.cancel_requested", experiment_point_id, None, {})
        return {"status": "accepted"}

    @app.post("/api/v1/batch-attempts/{batch_id}/cancel")
    async def cancel_batch(batch_id: str) -> dict[str, str]:
        """Request cancellation for one Worker-owned Seed batch."""
        with store.connect() as con:
            batch = con.execute("SELECT * FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()
            if batch is None:
                raise HTTPException(404, "Batch attempt not found")
            if batch["state"] in {"completed", "failed", "cancelled", "reclaimed", "rejected"}:
                raise HTTPException(409, "Batch attempt is already terminal")
            con.execute("UPDATE batch_attempts SET state='cancel_requested' WHERE id=?", (batch_id,))
        store.record_v2_event("batch.cancel_requested", batch["experiment_point_id"], batch_id, {})
        return {"status": "accepted"}

    @app.get("/api/v1/scheduler/status")
    async def scheduler_status() -> dict[str, bool]:
        return {"paused": store.setting("dispatch_paused", "0") == "1"}

    @app.post("/api/v1/scheduler/pause")
    async def set_scheduler_pause(payload: dict[str, Any]) -> dict[str, bool]:
        paused = bool(payload.get("paused", False))
        store.set_setting("dispatch_paused", "1" if paused else "0")
        store.record_v2_event("scheduler.paused" if paused else "scheduler.resumed", None, None, {})
        return {"paused": paused}

    @app.post("/api/v1/workers/{worker_id}/dispatch-pause")
    async def set_worker_dispatch_pause(worker_id: str, payload: dict[str, Any]) -> dict[str, bool]:
        """Pause or resume assignment issuance for one Worker, without cancelling its batch."""
        paused = payload.get("paused")
        if not isinstance(paused, bool):
            raise HTTPException(422, "paused must be a boolean")
        with store.connect() as con:
            worker = con.execute("SELECT id FROM workers WHERE id=?", (worker_id,)).fetchone()
            if worker is None:
                raise HTTPException(404, "Worker not found")
            con.execute("UPDATE workers SET dispatch_paused=? WHERE id=?", (int(paused), worker_id))
        store.record_v2_event("worker.dispatch_paused" if paused else "worker.dispatch_resumed", None, None,
                              {"worker_id": worker_id})
        return {"paused": paused}

    @app.post("/api/v1/seed-runs/cancel-all")
    async def cancel_all_tasks() -> dict[str, str]:
        timestamp = now()
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            con.execute("UPDATE experiment_points SET state='cancelled', updated_at=? WHERE state NOT IN ('completed','cancelled')", (timestamp,))
            con.execute("UPDATE seed_runs_v2 SET state='cancelled', batch_attempt_id=NULL, updated_at=? WHERE state='pending'", (timestamp,))
            con.execute("UPDATE batch_attempts SET state='cancel_requested' WHERE state IN ('assigned','accepted','running')")
            con.execute("UPDATE seed_attempts_v3 SET state='cancel_requested' WHERE state IN ('assigned','running')")
            con.execute("COMMIT")
        store.record_v2_event("scheduler.cancel_all", None, None, {})
        return {"status": "accepted"}

    @app.delete("/api/v1/seed-runs/{experiment_point_id}/{seed}/history")
    async def delete_seed_history(experiment_point_id: str, seed: int) -> dict[str, str]:
        with store.connect() as con:
            row = con.execute(
                "SELECT state FROM seed_runs_v2 WHERE experiment_point_id=? AND seed=?",
                (experiment_point_id, seed),
            ).fetchone()
            if row is None:
                raise HTTPException(404, "Seed history not found")
            if row["state"] not in {"completed", "failed"}:
                raise HTTPException(409, "Only completed or failed history can be deleted")
            con.execute(
                "DELETE FROM seed_runs_v2 WHERE experiment_point_id=? AND seed=?",
                (experiment_point_id, seed),
            )
        store.record_v2_event("seed.history_deleted", experiment_point_id, None, {"seed": seed})
        return {"status": "deleted"}

    @app.post("/api/v1/seed-runs/history/delete")
    async def delete_seed_histories(payload: dict[str, Any]) -> dict[str, int | str]:
        """Delete selected completed/failed SeedRun history records atomically."""
        raw_runs = payload.get("runs")
        if not isinstance(raw_runs, list) or not raw_runs:
            raise HTTPException(422, "runs must be a non-empty list")
        if len(raw_runs) > 1000:
            raise HTTPException(422, "at most 1000 history records can be deleted at once")

        selected: list[tuple[str, int]] = []
        seen: set[tuple[str, int]] = set()
        for raw_run in raw_runs:
            if not isinstance(raw_run, dict):
                raise HTTPException(422, "each run must be an object")
            point_id = raw_run.get("experiment_point_id")
            seed = raw_run.get("seed")
            if not isinstance(point_id, str) or not point_id.strip():
                raise HTTPException(422, "experiment_point_id must be a non-empty string")
            if isinstance(seed, bool) or not isinstance(seed, int):
                raise HTTPException(422, "seed must be an integer")
            key = (point_id, seed)
            if key not in seen:
                seen.add(key)
                selected.append(key)

        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            for point_id, seed in selected:
                row = con.execute(
                    "SELECT state FROM seed_runs_v2 WHERE experiment_point_id=? AND seed=?",
                    (point_id, seed),
                ).fetchone()
                if row is None:
                    con.execute("ROLLBACK")
                    raise HTTPException(404, "Seed history not found")
                if row["state"] not in {"completed", "failed"}:
                    con.execute("ROLLBACK")
                    raise HTTPException(409, "Only completed or failed history can be deleted")
            con.executemany(
                "DELETE FROM seed_runs_v2 WHERE experiment_point_id=? AND seed=?",
                selected,
            )
            con.execute("COMMIT")
        store.record_v2_event("seed.history_batch_deleted", None, None, {"count": len(selected)})
        return {"status": "deleted", "count": len(selected)}

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
    if sys.platform == "win32":
        # Python 3.14's Proactor loop can race with Uvicorn while closing
        # sockets, leaving Ctrl+C stuck in _start_serving. Master has no
        # subprocess requirement, so the selector loop is the safer server
        # backend on Windows.
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    application = create_app(args.data_dir.resolve(), args.platemo_path, args.worker_join_token)
    print(f"Worker Join Token: {application.state.worker_join_token}", flush=True)
    uvicorn.run(
        application,
        host=args.host,
        port=args.port,
        loop="asyncio",
        http="h11",
        workers=1,
        timeout_graceful_shutdown=5,
    )


if __name__ == "__main__":
    main()
