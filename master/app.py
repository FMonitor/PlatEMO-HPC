"""Master web service for dispatching PlatEMO experiment tasks."""
from __future__ import annotations

import argparse
import asyncio
import json
from io import BytesIO
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
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
            con.execute("UPDATE workers SET online = ?, queue_count = ?, last_check = ?, health_error = ? WHERE id = ?",
                        (int(online), queue_count, now(), error, worker_id))

    def setting(self, key: str, default: str = "") -> str:
        with self.connect() as con:
            row = con.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self.connect() as con:
            con.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))


def create_app(data_dir: Path, platemo_path: Path | None = None) -> FastAPI:
    store = Store(data_dir, platemo_path)
    app = FastAPI(title="PlatEMO HPC Master")
    app.state.store = store

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
        headers = {"X-Worker-Token": worker["token"]} if worker["token"] else {}
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                response = await client.get(f"{worker['url']}/api/health", headers=headers)
                response.raise_for_status()
                health = response.json()
            store.update_health(worker["id"], True, int(health.get("queued", 0)), "")
        except httpx.HTTPError as exc:
            store.update_health(worker["id"], False, None, str(exc))
        return next(item for item in store.workers() if item["id"] == worker["id"])

    async def probe_all_forever() -> None:
        while True:
            workers = store.workers()
            if workers:
                await asyncio.gather(*(probe_worker(worker) for worker in workers))
            await asyncio.sleep(30)

    @app.on_event("startup")
    async def start_health_probes() -> None:
        app.state.probe_task = asyncio.create_task(probe_all_forever())

    @app.on_event("shutdown")
    async def stop_health_probes() -> None:
        app.state.probe_task.cancel()
        try:
            await app.state.probe_task
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
        return [{key: value for key, value in worker.items() if key != "token"} for worker in store.workers()]

    @app.get("/api/tasks")
    async def list_tasks() -> list[dict[str, Any]]:
        """Return recent task attempts for the run-status panel without secrets."""
        worker_names = {worker["id"]: worker["name"] for worker in store.workers()}
        result: list[dict[str, Any]] = []
        for task in store.tasks():
            try:
                payload = json.loads(task["payload"])
            except json.JSONDecodeError:
                payload = {}
            result.append({
                "id": task["id"],
                "state": task["state"],
                "worker_id": task["worker_id"],
                "worker_name": worker_names.get(task["worker_id"], "未分配"),
                "algorithm": payload.get("algorithm", {}).get("name", "") if isinstance(payload.get("algorithm"), dict) else payload.get("algorithm", ""),
                "problem": payload.get("problem", {}).get("name", "") if isinstance(payload.get("problem"), dict) else payload.get("problem", ""),
                "seed": payload.get("seed", "-"),
                "parameters": payload.get("problem", {}).get("parameters", {}) if isinstance(payload.get("problem"), dict) else {},
                "created_at": task["created_at"],
                "updated_at": task["updated_at"],
                "error": task["error"],
            })
        return result

    @app.post("/api/workers")
    async def add_worker(request: Request, name: str = Form(...), url: str = Form(...), token: str = Form("")) -> RedirectResponse:
        worker_id = str(uuid.uuid4())
        with store.connect() as con:
            con.execute("INSERT INTO workers (id, name, url, token, created_at) VALUES (?, ?, ?, ?, ?)",
                        (worker_id, name, url.rstrip("/"), token, now()))
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
    async def edit_worker(worker_id: str, name: str = Form(...), url: str = Form(...), token: str = Form("")) -> RedirectResponse:
        with store.connect() as con:
            existing = con.execute("SELECT id FROM workers WHERE id = ?", (worker_id,)).fetchone()
            if existing is None:
                raise HTTPException(404, "Worker not found")
            if token:
                con.execute("UPDATE workers SET name = ?, url = ?, token = ? WHERE id = ?",
                            (name, url.rstrip("/"), token, worker_id))
            else:
                con.execute("UPDATE workers SET name = ?, url = ? WHERE id = ?",
                            (name, url.rstrip("/"), worker_id))
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

    @app.post("/api/mock-runs")
    async def create_mock_run(
        algorithms_json: str = Form(...),
        problems_json: str = Form(...),
        runs: int = Form(30),
        max_workers: int | None = Form(None),
        retain_points: int = Form(100),
        settings_file: str = Form(""),
        settings_upload: UploadFile | None = File(None),
        worker_ids: list[str] = Form(...),
    ) -> RedirectResponse:
        """Create local placeholder tasks to validate the assignment policy without MATLAB."""
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
        online = [worker for worker in selected if worker["online"]]
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
        planned = [(algorithm, problem, seed) for algorithm in algorithms for problem in problems for seed in range(1, runs + 1)]
        with store.connect() as con:
            for index, (algorithm, problem, seed) in enumerate(planned):
                worker = online[index % len(online)]
                task_id = str(uuid.uuid4())
                payload = {
                    "id": task_id, "run_id": run_id, "mock": True, "algorithm": algorithm,
                    "problem": problem, "seed": seed, "max_workers": max_workers,
                    "retain_points": retain_points, "settings_file": uploaded_settings or settings_file, "created_at": now(),
                }
                con.execute("INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, '')",
                            (task_id, worker["id"], "mock_queued", json.dumps(payload), now(), now()))
        return RedirectResponse(url=f"/?message=Mock+run+created%3A+{len(planned)}+tasks+across+{len(online)}+workers", status_code=303)

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

    # Vite emits immutable assets under /assets.  API routes are registered
    # first so a production UI shares its origin with the Master API.
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "assets"), check_dir=False), name="frontend-assets")
    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path.cwd() / "Data")
    parser.add_argument("--platemo-path", type=Path, default=None)
    parser.add_argument("--host", default="0.0.0.0")
    # Chromium blocks port 6000 as unsafe, so use a nearby browser-safe default.
    parser.add_argument("--port", type=int, default=6080)
    args = parser.parse_args()
    uvicorn.run(create_app(args.data_dir.resolve(), args.platemo_path), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
